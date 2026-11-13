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

export interface HeldLease {
  readonly holderId: string;
  /** The record observed before acquiring. */
  readonly previous: Option.Option<typeof Manifest.LeaseRecord.Type>;
  readonly lost: Deferred.Deferred<LeaseLoss>;
  readonly handoffRequested: Deferred.Deferred<{ readonly host: string; readonly pid: number }>;
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

  const heldError = (holder: Holder) =>
    new CentralStateLeaseHeldError({
      homeKey: input.homeKey,
      holderHost: holder.host,
      holderPid: holder.pid,
      holderStateDir: holder.stateDir,
    });

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

  /** One pass of the acquire loop; none means the lease was released while we watched. */
  const attempt = Effect.fnUntraced(function* () {
    const current = yield* readLease;
    if (Option.isNone(current))
      return Option.some(yield* claim(Option.none(), { _tag: "IfAbsent" }));
    let record = current.value.value;
    let version = current.value.version;
    let holder = record.holder;
    if (holder === null || holder.holderId === me.holderId || isDeadSibling(holder, me)) {
      return Option.some(yield* claim(Option.some(record), { _tag: "IfVersion", version }));
    }

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
      if (now >= giveUpAt) return yield* heldError(holder);
      yield* Effect.sleep(Duration.millis(Math.min(pollMs, stealAt - now, giveUpAt - now)));

      const polled = yield* readLease;
      const polledHolder = Option.isSome(polled) ? polled.value.value.holder : null;
      if (Option.isNone(polled) || polledHolder === null) return Option.none();
      if (polled.value.version !== version) {
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
  const handoffRequested = yield* Deferred.make<{ readonly host: string; readonly pid: number }>();
  let record = claimed.record;
  let version = claimed.version;
  let lastOk = claimed.startedAt;

  // `release` runs wherever the caller runs it, so it keeps the store it acquired with.
  const store = yield* CentralObjectStore.CentralObjectStore;
  const casLease = (value: LeaseRecord, retryFor: Duration.Duration) =>
    Manifest.casWrite({
      key,
      schema: Manifest.LeaseRecord,
      value,
      precondition: { _tag: "IfVersion", version },
      retryFor,
    }).pipe(Effect.provideService(CentralObjectStore.CentralObjectStore, store));

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
      const value: LeaseRecord = {
        ...record,
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
      } else if (written.failure._tag === "CentralCasLostError") {
        const remote = yield* Effect.result(Manifest.readJson(key, Manifest.LeaseRecord));
        // A failed re-read is like a failed renewal: try again next round.
        if (Result.isSuccess(remote)) {
          const current = Option.getOrUndefined(remote.success);
          if (current === undefined || current.value.holder?.holderId !== me.holderId) {
            return yield* Deferred.succeed(lost, "superseded");
          }
          record = { ...record, takeoverRequest: current.value.takeoverRequest };
          version = current.version;
          const request = current.value.takeoverRequest;
          if (request !== null) {
            yield* Deferred.succeed(handoffRequested, { host: request.host, pid: request.pid });
          }
        }
      }
      if ((yield* monotonicMillis) - lastOk >= ttlMs) {
        return yield* Deferred.succeed(lost, "expired");
      }
    }
  });
  const renewal = yield* Effect.forkScoped(renew);

  let released = false;
  const release: Effect.Effect<void> = Effect.gen(function* () {
    if (released) return;
    released = true;
    yield* Fiber.interrupt(renewal);
    if (yield* Deferred.isDone(lost)) return;
    const value: LeaseRecord = {
      ...record,
      holder: null,
      takeoverRequest: null,
      renewedAt: yield* isoNow,
      headSequence: (yield* input.heartbeat) ?? record.headSequence,
      writeNonce: NodeCrypto.randomUUID(),
    };
    yield* casLease(value, RELEASE_RETRY).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Could not release the central state lease.").pipe(
          Effect.annotateLogs({ homeKey: input.homeKey, cause: error }),
        ),
      ),
    );
  }).pipe(Effect.withSpan("CentralLease.release"));

  const lease: HeldLease = {
    holderId: me.holderId,
    previous: claimed.previous,
    lost,
    handoffRequested,
    release,
  };
  return lease;
});
