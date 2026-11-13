import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

import * as CentralObjectStore from "./CentralObjectStore.ts";
import * as DatabaseSnapshot from "./DatabaseSnapshot.ts";
import * as SnapshotPolicy from "./SnapshotPolicy.ts";
import * as StateBundle from "./StateBundle.ts";

export interface SnapshotLoop {
  /** Completed when a push loses the manifest CAS: another server owns the home now. */
  readonly fenced: Deferred.Deferred<void>;
  /** Waits for an in-flight push, then stops the loop and closes its database reader. */
  readonly stop: Effect.Effect<void>;
  /** Pushes from the closed database, bounded by finalPushTimeout; failures are logged. */
  readonly finalPush: Effect.Effect<void>;
}

const TICK = Duration.seconds(2);

const outcomeKind = (outcome: StateBundle.PushOutcome) =>
  outcome._tag === "Committed"
    ? "committed"
    : outcome._tag === "Unchanged"
      ? "unchanged"
      : "failed";

export const start = Effect.fn("SnapshotLoop.start")(function* (input: {
  readonly ctx: StateBundle.BundleContext;
  readonly committed: Ref.Ref<StateBundle.Committed>;
  readonly cache: StateBundle.PushCache;
  readonly claude: StateBundle.ClaudeHooks;
  readonly policy: SnapshotPolicy.PolicyConfig;
  readonly finalPushTimeout: Duration.Duration;
  readonly head: Ref.Ref<number | null>;
  /** True once the lease is lost; the loop then never pushes again. */
  readonly isFenced: Effect.Effect<boolean>;
}) {
  const services = yield* Effect.context<
    CentralObjectStore.CentralObjectStore | FileSystem.FileSystem | Path.Path
  >();
  const fenced = yield* Deferred.make<void>();
  // A push and the final push never overlap, and stop never cuts a push short.
  const pushing = yield* Semaphore.make(1);
  // Policy times count from the start, so the idle sweep first fires one idle interval in.
  const origin = yield* Clock.monotonicTimeNanos;
  const monotonicMillis = Effect.map(
    Clock.monotonicTimeNanos,
    (nanos) => Number(nanos - origin) / 1e6,
  );
  const initialManifest = (yield* Ref.get(input.committed)).manifest;
  let policy = SnapshotPolicy.initial(initialManifest.database === null);
  let lastHead = initialManifest.database?.maxSequence ?? 0;
  let lastDataVersion: number | null = null;

  const push = Effect.fn("SnapshotLoop.push")(
    function* (source: StateBundle.DbSource) {
      const outcome = yield* StateBundle.push({
        ctx: input.ctx,
        committed: yield* Ref.get(input.committed),
        cache: input.cache,
        source,
        claude: input.claude,
      });
      if (outcome._tag === "Committed") yield* Ref.set(input.committed, outcome.committed);
      return outcome;
    },
    Effect.tapError((error) =>
      error._tag === "CentralCasLostError" ? Deferred.succeed(fenced, undefined) : Effect.void,
    ),
  );

  const warn = (message: string, error: { readonly _tag: string }) =>
    Effect.logWarning(message).pipe(
      Effect.annotateLogs({ homeKey: input.ctx.homeKey, error: error._tag }),
    );

  const loop = Effect.gen(function* () {
    const scope = yield* Scope.Scope;
    let reader = Option.none<DatabaseSnapshot.LiveReader>();

    /** One poll; false ends the loop. */
    const tick = Effect.gen(function* () {
      if (Option.isNone(reader)) {
        const opened = yield* DatabaseSnapshot.openLiveReader(input.ctx.paths.dbPath).pipe(
          Scope.provide(scope),
          Effect.catch((error) =>
            warn("Could not open the database for central state snapshots.", error).pipe(
              Effect.as(Option.none<DatabaseSnapshot.LiveReader>()),
            ),
          ),
        );
        if (Option.isNone(opened)) return true;
        reader = opened;
      }
      const live = reader.value;
      const observed = yield* Effect.all({
        dataVersion: live.dataVersion,
        headSequence: live.headSequence,
      }).pipe(Effect.result);
      if (Result.isFailure(observed)) {
        yield* warn("Could not read the database for central state snapshots.", observed.failure);
        return true;
      }
      const { dataVersion, headSequence } = observed.success;
      yield* Ref.set(input.head, headSequence);
      const now = yield* monotonicMillis;
      // data_version is per connection, so the first reading is only a baseline.
      policy = SnapshotPolicy.observe(policy, now, {
        headAdvanced: headSequence > lastHead,
        dataVersionChanged: lastDataVersion !== null && dataVersion !== lastDataVersion,
      });
      lastHead = Math.max(lastHead, headSequence);
      lastDataVersion = dataVersion;
      if (SnapshotPolicy.decide(policy, now, input.policy) === "wait") return true;
      if (yield* input.isFenced) return false;

      return yield* pushing.withPermits(1)(
        Effect.gen(function* () {
          const startedAt = yield* monotonicMillis;
          const outcome = yield* push({ _tag: "Live", reader: live }).pipe(Effect.result);
          const finishedAt = yield* monotonicMillis;
          if (Result.isSuccess(outcome)) {
            policy = SnapshotPolicy.pushed(
              policy,
              startedAt,
              finishedAt,
              outcomeKind(outcome.success),
            );
            return true;
          }
          if (outcome.failure._tag === "CentralCasLostError") return false;
          yield* warn("Central state push failed; retrying later.", outcome.failure);
          policy = SnapshotPolicy.pushed(policy, startedAt, finishedAt, "failed");
          return true;
        }),
      );
    });

    yield* Effect.repeat(tick, {
      schedule: Schedule.spaced(TICK),
      while: (keepGoing) => keepGoing,
    });
  }).pipe(Effect.scoped, Effect.provideContext(services));

  const fiber = yield* Effect.forkScoped(loop);

  const stop = pushing.withPermits(1)(Fiber.interrupt(fiber));

  const finalPush = pushing.withPermits(1)(
    push({ _tag: "Closed" }).pipe(
      // Finalizers are uninterruptible; the timeout must still be able to cut the push short.
      Effect.interruptible,
      Effect.timeout(input.finalPushTimeout),
      Effect.catch((error) => warn("Final central state push failed.", error)),
      Effect.asVoid,
      Effect.provideContext(services),
    ),
  );

  const loopHandle: SnapshotLoop = { fenced, stop, finalPush };
  return loopHandle;
});
