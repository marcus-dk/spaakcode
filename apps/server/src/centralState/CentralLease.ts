// @effect-diagnostics nodeBuiltinImport:off - lease write nonces are UUIDs from node:crypto.
import * as NodeCrypto from "node:crypto";

import { SERVER_EXIT_CODE_STATE_DIR_OWNED } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";

import { isProcessAlive } from "../serverRuntimeState.ts";
import * as CentralObjectStore from "./CentralObjectStore.ts";
import * as Manifest from "./Manifest.ts";

export class CentralStateLeaseHeldError extends Schema.TaggedError<CentralStateLeaseHeldError>()(
  "CentralStateLeaseHeldError",
  {
    homeKey: Schema.String,
    holderHost: Schema.String,
    holderPid: Schema.Number,
    holderStateDir: Schema.String,
  },
) {
  override readonly [Runtime.errorExitCode] = SERVER_EXIT_CODE_STATE_DIR_OWNED;

  override get message(): string {
    return `T3 home ${this.homeKey} is served by ${this.holderHost} (pid ${this.holderPid}). Stop that server, set T3CODE_CENTRAL_STATE_TAKEOVER=1 to move it here, or use another T3CODE_CENTRAL_STATE_HOME_KEY.`;
  }
}

export type LeaseLoss = "superseded" | "expired";

export class CentralStateFencedError extends Schema.TaggedError<CentralStateFencedError>()(
  "CentralStateFencedError",
  { homeKey: Schema.String, reason: Schema.Literals(["superseded", "expired"]) },
) {
  // Only a superseded server must stay down; an expired lease is a store outage a restart outlives.
  override readonly [Runtime.errorExitCode] =
    this.reason === "superseded" ? SERVER_EXIT_CODE_STATE_DIR_OWNED : 1;

  override get message(): string {
    return this.reason === "superseded"
      ? `Another server took over T3 home ${this.homeKey}, so this server stopped without saving again.`
      : `T3 home ${this.homeKey} could not renew its lease in time, so this server stopped. Changes since the last save may be lost.`;
  }
}

export interface HeldLease {
  readonly holderId: string;
  /** The record observed before acquiring. */
  readonly previous: Option.Option<typeof Manifest.LeaseRecord.Type>;
  readonly lost: Deferred.Deferred<LeaseLoss>;
  /** Fails with CentralStateFencedError once the lease is lost. */
  readonly ensureHeld: Effect.Effect<void, CentralStateFencedError>;
  /** Time left before this holder must stop itself; 0 once lost. No taker steals before it ends. */
  readonly remainingMs: Effect.Effect<number>;
  /**
   * Completes with a takeover request that is still in the lease record and younger than the
   * requester's timeout, so a holder never hands off to a requester that gave up.
   */
  readonly awaitHandoff: Effect.Effect<{ readonly host: string; readonly pid: number }>;
  /** Interrupts renewal, then CAS-writes holder: null. No-op once lost. Errors are logged, never raised. */
  readonly release: Effect.Effect<void>;
}

type LeaseRecord = typeof Manifest.LeaseRecord.Type;
type Holder = typeof Manifest.Holder.Type;

const TAKEOVER_TIMEOUT_MS = 120_000;
const BOOT_TIMEOUT_MS = 120_000;
const BOOT_RETRY_MIN_MS = 1_000;
const BOOT_RETRY_MAX_MS = 15_000;
const RELEASE_RETRY = Duration.seconds(5);
// One CAS attempt can outlast RELEASE_RETRY (a write and a read-back of up to 30 s each).
const RELEASE_TIMEOUT = Duration.seconds(15);
const RELEASE_ATTEMPTS = 3;

// Every elapsed time is monotonic and every wait is Effect.sleep, so TestClock drives both.
const monotonicMillis = Effect.map(Clock.monotonicTimeNanos, (nanos) => Number(nanos) / 1e6);
const isoNow = Effect.map(DateTime.now, DateTime.formatIso);

/** A crashed server of this same pod and T3 home: safe to replace without waiting. */
const isDeadSibling = (holder: Holder, me: Holder) =>
  holder.host === me.host &&
  holder.bootId !== null &&
  holder.bootId === me.bootId &&
  holder.stateDir === me.stateDir &&
  !isProcessAlive(holder.pid);

/**
 * The session service hands each new pod of a home a higher generation and its broker refuses the
 * older ones, so an older holder is already fenced and a newer one makes this server the stale pod.
 */
const generationOrder = (holder: Holder, me: Holder) =>
  holder.generation === undefined ||
  me.generation === undefined ||
  holder.generation === me.generation
    ? "unordered"
    : holder.generation < me.generation
      ? "older"
      : "newer";

export const acquire = Effect.fn("CentralLease.acquire")(function* (input: {
  readonly homeKey: string;
  readonly holder: Holder;
  readonly ttl: Duration.Duration;
  readonly takeover: boolean;
  readonly heartbeat: Effect.Effect<number | null>;
}) {
  const key = Manifest.keys.lease(input.homeKey);
  const me = input.holder;
  const ttlMs = Duration.toMillis(input.ttl);
  const renewEvery = Duration.millis(ttlMs / 4);
  const pollMs = ttlMs / 20;
  const stealAfterMs = ttlMs * 1.5;

  let requestedAt: number | undefined;
  let unreachableSince: number | undefined;
  let unreachableRetries = 0;

  const readLease = Manifest.readJson(key, Manifest.LeaseRecord).pipe(
    Effect.tap(() =>
      Effect.sync(() => {
        unreachableSince = undefined;
        unreachableRetries = 0;
      }),
    ),
  );

  const supersededError = () =>
    new CentralStateFencedError({ homeKey: input.homeKey, reason: "superseded" });

  const heldError = (holder: Holder) =>
    new CentralStateLeaseHeldError({
      homeKey: input.homeKey,
      holderHost: holder.host,
      holderPid: holder.pid,
      holderStateDir: holder.stateDir,
    });

  /** A requester that gives up takes its request back, so the holder never hands off to nobody. */
  const withdrawRequest = Effect.gen(function* () {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const current = yield* readLease;
      if (Option.isNone(current) || current.value.value.takeoverRequest?.holderId !== me.holderId) {
        return;
      }
      const withdrawn = yield* Effect.result(
        Manifest.casWrite({
          key,
          schema: Manifest.LeaseRecord,
          value: {
            ...current.value.value,
            takeoverRequest: null,
            writeNonce: NodeCrypto.randomUUID(),
          },
          precondition: { _tag: "IfVersion", version: current.value.version },
          retryFor: renewEvery,
        }),
      );
      if (Result.isSuccess(withdrawn) || withdrawn.failure._tag !== "CentralCasLostError") return;
    }
  }).pipe(Effect.ignore);

  const claim = Effect.fnUntraced(function* (
    previous: Option.Option<LeaseRecord>,
    precondition:
      | { readonly _tag: "IfAbsent" }
      | { readonly _tag: "IfVersion"; readonly version: string },
  ) {
    const startedAt = yield* monotonicMillis;
    const now = yield* isoNow;
    const record: LeaseRecord = {
      formatVersion: 1,
      homeKey: input.homeKey,
      holder: { ...me, acquiredAt: now },
      renewedAt: now,
      headSequence: null,
      takeoverRequest: null,
      writeNonce: NodeCrypto.randomUUID(),
    };
    const { version } = yield* Manifest.casWrite({
      key,
      schema: Manifest.LeaseRecord,
      value: record,
      precondition,
      retryFor: renewEvery,
    });
    return { previous, record, version, startedAt };
  });

  /** One pass of the acquire loop; none means look again (the lease was released or re-held). */
  const attempt = Effect.fnUntraced(function* () {
    const current = yield* readLease;
    if (Option.isNone(current))
      return Option.some(yield* claim(Option.none(), { _tag: "IfAbsent" }));
    let record = current.value.value;
    let version = current.value.version;
    let holder = record.holder;
    if (
      holder === null ||
      holder.holderId === me.holderId ||
      isDeadSibling(holder, me) ||
      generationOrder(holder, me) === "older"
    ) {
      return Option.some(yield* claim(Option.some(record), { _tag: "IfVersion", version }));
    }
    if (generationOrder(holder, me) === "newer") return yield* supersededError();

    if (input.takeover) {
      requestedAt ??= yield* monotonicMillis;
      if (record.takeoverRequest?.holderId !== me.holderId) {
        const request: LeaseRecord = {
          ...record,
          takeoverRequest: {
            holderId: me.holderId,
            host: me.host,
            pid: me.pid,
            requestedAt: yield* isoNow,
          },
          writeNonce: NodeCrypto.randomUUID(),
        };
        const written = yield* Manifest.casWrite({
          key,
          schema: Manifest.LeaseRecord,
          value: request,
          precondition: { _tag: "IfVersion", version },
          retryFor: renewEvery,
        });
        record = request;
        version = written.version;
      }
    }

    let observedAt = yield* monotonicMillis;
    while (true) {
      const now = yield* monotonicMillis;
      const stealAt = observedAt + stealAfterMs;
      if (now >= stealAt) {
        return Option.some(yield* claim(Option.some(record), { _tag: "IfVersion", version }));
      }
      const giveUpAt = requestedAt === undefined ? Infinity : requestedAt + TAKEOVER_TIMEOUT_MS;
      if (now >= giveUpAt) {
        yield* withdrawRequest;
        return yield* heldError(holder);
      }
      yield* Effect.sleep(Duration.millis(Math.min(pollMs, stealAt - now, giveUpAt - now)));

      const polled = yield* readLease;
      const polledHolder = Option.isSome(polled) ? polled.value.value.holder : null;
      if (Option.isNone(polled) || polledHolder === null) return Option.none();
      if (polled.value.version !== version) {
        if (generationOrder(polledHolder, me) !== "unordered") return Option.none();
        if (!input.takeover) return yield* heldError(polledHolder);
        record = polled.value.value;
        holder = polledHolder;
        version = polled.value.version;
        observedAt = yield* monotonicMillis;
      }
    }
  });

  const claimed = yield* Effect.gen(function* () {
    while (true) {
      const outcome = yield* Effect.result(attempt());
      if (Result.isSuccess(outcome)) {
        if (Option.isSome(outcome.success)) return outcome.success.value;
        continue;
      }
      const error = outcome.failure;
      if (error._tag === "CentralCasLostError") continue;
      if (error._tag === "CentralObjectFencedError") return yield* supersededError();
      if (error._tag !== "CentralObjectStoreError") return yield* error;
      const now = yield* monotonicMillis;
      unreachableSince ??= now;
      if (now - unreachableSince >= BOOT_TIMEOUT_MS) return yield* error;
      const delayMs = Math.min(BOOT_RETRY_MAX_MS, BOOT_RETRY_MIN_MS * 2 ** unreachableRetries);
      unreachableRetries += 1;
      yield* Effect.logWarning("Central state store unreachable; retrying the lease.").pipe(
        Effect.annotateLogs({ homeKey: input.homeKey, status: error.status, cause: error }),
      );
      yield* Effect.sleep(Duration.millis(delayMs));
    }
  });

  const lost = yield* Deferred.make<LeaseLoss>();
  // Completed by renewals while the record carries a live request; replaced once one is withdrawn.
  let handoffSignal = yield* Deferred.make<void>();
  let record = claimed.record;
  let version = claimed.version;
  let lastOk = claimed.startedAt;

  // `release` runs wherever the caller runs it, so it keeps the store it acquired with.
  const store = yield* CentralObjectStore.CentralObjectStore;
  const withStore = <A, E>(effect: Effect.Effect<A, E, CentralObjectStore.CentralObjectStore>) =>
    effect.pipe(Effect.provideService(CentralObjectStore.CentralObjectStore, store));
  const casLease = (value: LeaseRecord, retryFor: Duration.Duration, atVersion = version) =>
    withStore(
      Manifest.casWrite({
        key,
        schema: Manifest.LeaseRecord,
        value,
        precondition: { _tag: "IfVersion", version: atVersion },
        retryFor,
      }),
    );
  const readRemote = withStore(Manifest.readJson(key, Manifest.LeaseRecord));

  /** A request the requester still waits on: it withdraws its request when it gives up. */
  const isLive = (request: LeaseRecord["takeoverRequest"], nowMs: number) =>
    request !== null &&
    request.holderId !== me.holderId &&
    nowMs - Date.parse(request.requestedAt) < TAKEOVER_TIMEOUT_MS;

  // The self-fence deadline runs from the start of the last successful renew request, so a taker
  // that saw that write and waits 1.5·T leaves this holder 0.5·T to stop.
  const renew = Effect.gen(function* () {
    let roundAt = lastOk;
    while (true) {
      yield* Effect.sleep(
        Duration.millis(Math.max(0, roundAt + ttlMs / 4 - (yield* monotonicMillis))),
      );
      roundAt = yield* monotonicMillis;
      if (roundAt - lastOk >= ttlMs) return yield* Deferred.succeed(lost, "expired");
      // A request left by a requester that crashed must not stop a later, healthy holder.
      const takeoverRequest = isLive(record.takeoverRequest, yield* Clock.currentTimeMillis)
        ? record.takeoverRequest
        : null;
      const value: LeaseRecord = {
        ...record,
        takeoverRequest,
        renewedAt: yield* isoNow,
        headSequence: yield* input.heartbeat,
        writeNonce: NodeCrypto.randomUUID(),
      };
      const written = yield* casLease(value, Duration.zero).pipe(
        Effect.timeout(renewEvery),
        Effect.result,
      );
      if (Result.isSuccess(written)) {
        record = value;
        version = written.success.version;
        lastOk = roundAt;
      } else if (written.failure._tag === "CentralObjectFencedError") {
        return yield* Deferred.succeed(lost, "superseded");
      } else if (written.failure._tag === "CentralCasLostError") {
        const remote = yield* Effect.result(Manifest.readJson(key, Manifest.LeaseRecord));
        // A failed re-read is like a failed renewal: try again next round.
        if (Result.isSuccess(remote)) {
          const current = Option.getOrUndefined(remote.success);
          if (current === undefined || current.value.holder?.holderId !== me.holderId) {
            return yield* Deferred.succeed(lost, "superseded");
          }
          const request = current.value.takeoverRequest;
          const live = isLive(request, yield* Clock.currentTimeMillis);
          record = { ...record, takeoverRequest: live ? request : null };
          version = current.version;
        }
      }
      if (record.takeoverRequest !== null) yield* Deferred.succeed(handoffSignal, undefined);
      if ((yield* monotonicMillis) - lastOk >= ttlMs) {
        return yield* Deferred.succeed(lost, "expired");
      }
    }
  });
  const renewal = yield* Effect.forkScoped(renew);

  const awaitHandoff = Effect.gen(function* () {
    while (true) {
      yield* Deferred.await(handoffSignal);
      // The requester may have given up since a renewal saw its request.
      const remote = yield* Effect.result(readRemote);
      const current = Result.isSuccess(remote) ? Option.getOrUndefined(remote.success) : undefined;
      const request = current?.value.takeoverRequest ?? null;
      if (
        request !== null &&
        current?.value.holder?.holderId === me.holderId &&
        isLive(request, yield* Clock.currentTimeMillis)
      ) {
        return { host: request.host, pid: request.pid };
      }
      handoffSignal = yield* Deferred.make<void>();
    }
  });

  const warnRelease = (cause: unknown) =>
    Effect.logWarning("Could not release the central state lease.").pipe(
      Effect.annotateLogs({ homeKey: input.homeKey, cause }),
    );

  let released = false;
  const release: Effect.Effect<void> = Effect.gen(function* () {
    if (released) return;
    released = true;
    yield* Fiber.interrupt(renewal);
    if (yield* Deferred.isDone(lost)) return;
    let base = record;
    let baseVersion = version;
    for (let attempt = 1; ; attempt += 1) {
      const value: LeaseRecord = {
        ...base,
        holder: null,
        takeoverRequest: null,
        renewedAt: yield* isoNow,
        headSequence: (yield* input.heartbeat) ?? base.headSequence,
        writeNonce: NodeCrypto.randomUUID(),
      };
      const written = yield* Effect.result(casLease(value, RELEASE_RETRY, baseVersion));
      if (Result.isSuccess(written)) return;
      if (written.failure._tag !== "CentralCasLostError" || attempt >= RELEASE_ATTEMPTS) {
        return yield* warnRelease(written.failure);
      }
      // A renewal cut short by the interrupt can still land: follow the lease while it is ours.
      const remote = yield* Effect.result(readRemote);
      if (Result.isFailure(remote)) return yield* warnRelease(remote.failure);
      const current = Option.getOrUndefined(remote.success);
      if (current === undefined || current.value.holder?.holderId !== me.holderId) return;
      base = current.value;
      baseVersion = current.version;
    }
  }).pipe(
    // Finalizers are uninterruptible; the timeout must still be able to cut a hung store short.
    Effect.interruptible,
    Effect.timeout(RELEASE_TIMEOUT),
    Effect.catch(warnRelease),
    Effect.withSpan("CentralLease.release"),
  );

  const lease: HeldLease = {
    holderId: me.holderId,
    previous: claimed.previous,
    lost,
    ensureHeld: Effect.gen(function* () {
      if (!(yield* Deferred.isDone(lost))) return;
      return yield* new CentralStateFencedError({
        homeKey: input.homeKey,
        reason: yield* Deferred.await(lost),
      });
    }),
    remainingMs: Effect.gen(function* () {
      if (yield* Deferred.isDone(lost)) return 0;
      return Math.max(0, lastOk + ttlMs - (yield* monotonicMillis));
    }),
    awaitHandoff,
    release,
  };
  return lease;
});
