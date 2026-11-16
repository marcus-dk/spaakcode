import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as TestClock from "effect/testing/TestClock";

import * as CentralLease from "./CentralLease.ts";
import * as CentralObjectStore from "./CentralObjectStore.ts";
import * as Manifest from "./Manifest.ts";

const homeKey = "dev";
const key = Manifest.keys.lease(homeKey);
const ttl = Duration.seconds(60);
const ttlMs = Duration.toMillis(ttl);

// Lease timing is asserted to the millisecond under TestClock, which only waits for fibers it
// schedules. The filesystem backend's async file I/O would let the clock run ahead of a pending
// read, so these tests use an in-memory store with the same CAS semantics.
const makeStore = () => {
  const objects = new Map<string, { readonly bytes: Uint8Array; readonly version: string }>();
  let counter = 0;
  return CentralObjectStore.CentralObjectStore.of({
    read: (key) => Effect.sync(() => Option.fromNullishOr(objects.get(key))),
    download: () => Effect.die("unused"),
    write: (key, body, precondition) =>
      Effect.suspend(() => {
        if (body._tag !== "Bytes") return Effect.die("bytes only");
        const current = objects.get(key);
        const matches =
          precondition._tag === "None" ||
          (precondition._tag === "IfAbsent"
            ? current === undefined
            : current?.version === precondition.version);
        if (!matches) {
          return Effect.fail(new CentralObjectStore.CentralObjectPreconditionError({ key }));
        }
        counter += 1;
        const version = String(counter);
        objects.set(key, { bytes: body.bytes, version });
        return Effect.succeed({ version });
      }),
    remove: (key) => Effect.sync(() => void objects.delete(key)),
  });
};

/** The same store, unreachable while `down()` holds. */
const flaky = (store: CentralObjectStore.CentralObjectStore["Service"], down: () => boolean) => {
  const unreachable = (operation: "read" | "write", key: string) =>
    new CentralObjectStore.CentralObjectStoreError({
      operation,
      key,
      status: 503,
      cause: new Error("unavailable"),
    });
  return CentralObjectStore.CentralObjectStore.of({
    ...store,
    read: (key) => (down() ? Effect.fail(unreachable("read", key)) : store.read(key)),
    write: (key, body, precondition) =>
      down() ? Effect.fail(unreachable("write", key)) : store.write(key, body, precondition),
  });
};

/** The same store, refusing every request as the broker does once a newer pod exists. */
const fencedWhile = (
  store: CentralObjectStore.CentralObjectStore["Service"],
  fenced: () => boolean,
) => {
  const refused = (key: string) =>
    new CentralObjectStore.CentralObjectFencedError({ key, status: 409 });
  return CentralObjectStore.CentralObjectStore.of({
    ...store,
    read: (key) => (fenced() ? Effect.fail(refused(key)) : store.read(key)),
    write: (key, body, precondition) =>
      fenced() ? Effect.fail(refused(key)) : store.write(key, body, precondition),
  });
};

const holder = (id: string, overrides: Partial<typeof Manifest.Holder.Type> = {}) => ({
  holderId: id,
  host: `host-${id}`,
  pid: process.pid,
  bootId: "boot-1",
  stateDir: `/homes/${id}/userdata`,
  ...overrides,
});

const acquire = (
  store: CentralObjectStore.CentralObjectStore["Service"],
  me: typeof Manifest.Holder.Type,
  takeover = false,
) =>
  CentralLease.acquire({ homeKey, holder: me, ttl, takeover, heartbeat: Effect.succeed(7) }).pipe(
    Effect.provideService(CentralObjectStore.CentralObjectStore, store),
  );

const readLease = (store: CentralObjectStore.CentralObjectStore["Service"]) =>
  Manifest.readJson(key, Manifest.LeaseRecord).pipe(
    Effect.map(Option.getOrThrow),
    Effect.provideService(CentralObjectStore.CentralObjectStore, store),
  );

const holderIdOf = (store: CentralObjectStore.CentralObjectStore["Service"]) =>
  Effect.map(readLease(store), (lease) => lease.value.holder?.holderId ?? null);

/** A record left by a holder that never renews again. */
const seed = (
  store: CentralObjectStore.CentralObjectStore["Service"],
  previous: typeof Manifest.Holder.Type,
) =>
  Manifest.casWrite({
    key,
    schema: Manifest.LeaseRecord,
    value: {
      formatVersion: 1,
      homeKey,
      holder: { ...previous, acquiredAt: "2026-01-01T00:00:00.000Z" },
      renewedAt: "2026-01-01T00:00:00.000Z",
      headSequence: 41,
      takeoverRequest: null,
      writeNonce: "seed",
    },
    precondition: { _tag: "IfAbsent" },
    retryFor: Duration.zero,
  }).pipe(Effect.provideService(CentralObjectStore.CentralObjectStore, store));

const failureOf = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  Effect.map(Effect.result(Fiber.join(fiber)), (result) => {
    assert.isTrue(Result.isFailure(result), "expected the fiber to fail");
    return (result as Result.Failure<A, E>).failure;
  });

describe("CentralLease", () => {
  it.effect("acquires an absent lease, releases it, and reacquires a released one at once", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = makeStore();
        const a = yield* acquire(store, holder("a"));
        assert.isTrue(Option.isNone(a.previous));
        assert.strictEqual(yield* holderIdOf(store), "a");

        yield* a.release;
        const released = yield* readLease(store);
        assert.strictEqual(released.value.holder, null);
        assert.strictEqual(released.value.headSequence, 7);
        yield* TestClock.adjust(ttl);
        assert.strictEqual((yield* readLease(store)).version, released.version);

        const b = yield* acquire(store, holder("b"));
        assert.strictEqual(Option.getOrThrow(b.previous).holder, null);
        assert.strictEqual(yield* holderIdOf(store), "b");
      }),
    ),
  );

  it.effect("waits 1.5·T before stealing a silent holder unless it crashed in this pod", () =>
    Effect.forEach(
      [
        { previous: holder("a"), waitMs: ttlMs * 1.5 },
        {
          // Same host and boot, but another T3 home on it: not ours to judge.
          previous: holder("a", { host: "host-b", pid: 2147483646 }),
          waitMs: ttlMs * 1.5,
        },
        {
          previous: holder("a", { host: "host-b", pid: 2147483646, stateDir: "/homes/b/userdata" }),
          waitMs: 0,
        },
        // An older generation of the session is already fenced by the broker.
        { previous: holder("a", { generation: 4 }), generation: 5, waitMs: 0 },
        { previous: holder("a", { generation: 5 }), generation: 5, waitMs: ttlMs * 1.5 },
        { previous: holder("a"), generation: 5, waitMs: ttlMs * 1.5 },
        { previous: holder("a", { generation: 4 }), waitMs: ttlMs * 1.5 },
      ],
      ({ previous, generation, waitMs }) =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = makeStore();
            yield* seed(store, previous);
            const me = holder("b", generation === undefined ? {} : { generation });
            const taker = yield* Effect.forkChild(acquire(store, me));
            if (waitMs > 0) {
              yield* TestClock.adjust(Duration.millis(waitMs - 1));
              assert.strictEqual(yield* holderIdOf(store), "a");
              yield* TestClock.adjust(Duration.millis(1));
            }
            const lease = yield* Fiber.join(taker);
            assert.strictEqual(Option.getOrThrow(lease.previous).headSequence, 41);
            assert.strictEqual(yield* holderIdOf(store), "b");
            assert.strictEqual((yield* readLease(store)).value.holder?.generation, generation);
          }),
        ),
      { discard: true },
    ),
  );

  it.effect("stops at once as superseded when a newer generation holds the home", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = makeStore();
        yield* seed(store, holder("a", { generation: 6 }));
        const leaseBefore = yield* readLease(store);
        const stale = yield* Effect.flip(acquire(store, holder("b", { generation: 5 })));
        assert.deepInclude(stale, { _tag: "CentralStateFencedError", reason: "superseded" });
        assert.strictEqual((yield* readLease(store)).version, leaseBefore.version);

        // The broker refusing this generation says the same.
        const refused = yield* Effect.flip(
          acquire(
            fencedWhile(store, () => true),
            holder("c", { generation: 5 }),
          ),
        );
        assert.deepInclude(refused, { _tag: "CentralStateFencedError", reason: "superseded" });
      }),
    ),
  );

  it.effect("takes over from an older generation that stole the lease while it waited", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = makeStore();
        yield* seed(store, holder("a"));
        const older = yield* Effect.forkChild(acquire(store, holder("b", { generation: 5 })));
        yield* TestClock.adjust(Duration.millis(ttlMs / 20));
        const newer = yield* Effect.forkChild(acquire(store, holder("c", { generation: 6 })));
        yield* TestClock.adjust(Duration.millis(ttlMs * 1.5 - ttlMs / 20));
        const b = yield* Fiber.join(older);
        yield* TestClock.adjust(Duration.millis(ttlMs / 20));
        yield* Fiber.join(newer);
        assert.strictEqual(yield* holderIdOf(store), "c");
        yield* TestClock.adjust(Duration.millis(ttlMs / 4));
        assert.strictEqual(yield* Deferred.await(b.lost), "superseded");
      }),
    ),
  );

  it.effect("fails fast against a renewing holder and never steals from it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = makeStore();
        const a = yield* acquire(store, holder("a"));

        const plain = yield* Effect.forkChild(acquire(store, holder("b")));
        yield* TestClock.adjust(Duration.millis(ttlMs / 4 + ttlMs / 20));
        const held = yield* failureOf(plain);
        assert.deepInclude(held, {
          _tag: "CentralStateLeaseHeldError",
          holderHost: "host-a",
          holderStateDir: "/homes/a/userdata",
        });

        // A takeover requester waits for a release, and gives up if the holder keeps renewing.
        const requester = yield* Effect.forkChild(acquire(store, holder("c"), true));
        yield* TestClock.adjust(Duration.minutes(2));
        assert.strictEqual((yield* failureOf(requester))._tag, "CentralStateLeaseHeldError");
        assert.strictEqual(yield* holderIdOf(store), "a");
        assert.isFalse(yield* Deferred.isDone(a.lost));

        // It takes its request back, so a holder that only now looks never hands off to nobody.
        assert.isNull((yield* readLease(store)).value.takeoverRequest);
        const handoff = yield* Effect.forkChild(a.awaitHandoff);
        yield* TestClock.adjust(Duration.millis(ttlMs / 2));
        assert.isUndefined(handoff.pollUnsafe());
        assert.isNull((yield* readLease(store)).value.takeoverRequest);
      }),
    ),
  );

  it.effect("hands off to a takeover requester once the holder releases", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = makeStore();
        const a = yield* acquire(store, holder("a"));
        const requester = yield* Effect.forkChild(acquire(store, holder("b"), true));
        yield* TestClock.adjust(Duration.millis(ttlMs / 4));
        assert.deepStrictEqual(yield* a.awaitHandoff, {
          host: "host-b",
          pid: process.pid,
        });
        assert.strictEqual(yield* holderIdOf(store), "a");

        yield* a.release;
        yield* TestClock.adjust(Duration.millis(ttlMs / 20));
        const b = yield* Fiber.join(requester);
        assert.strictEqual(Option.getOrThrow(b.previous).holder, null);
        assert.strictEqual(yield* holderIdOf(store), "b");
        assert.isFalse(yield* Deferred.isDone(a.lost));
      }),
    ),
  );

  it.effect("releases a lease whose interrupted renewal still landed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = makeStore();
        const a = yield* acquire(store, holder("a"));
        // The renewal's PUT reaches the store after release interrupted it.
        const current = yield* readLease(store);
        yield* Manifest.casWrite({
          key,
          schema: Manifest.LeaseRecord,
          value: { ...current.value, writeNonce: "late-renewal" },
          precondition: { _tag: "IfVersion", version: current.version },
          retryFor: Duration.zero,
        }).pipe(Effect.provideService(CentralObjectStore.CentralObjectStore, store));

        yield* a.release;
        assert.isNull(yield* holderIdOf(store));
      }),
    ),
  );

  it.effect("marks the holder superseded when another process overwrites its lease", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = makeStore();
        const a = yield* acquire(store, holder("a"));
        const current = yield* readLease(store);
        yield* Manifest.casWrite({
          key,
          schema: Manifest.LeaseRecord,
          value: {
            ...current.value,
            holder: { ...holder("x"), acquiredAt: current.value.renewedAt },
            writeNonce: "overwrite",
          },
          precondition: { _tag: "IfVersion", version: current.version },
          retryFor: Duration.zero,
        }).pipe(Effect.provideService(CentralObjectStore.CentralObjectStore, store));

        yield* TestClock.adjust(Duration.millis(ttlMs / 4));
        assert.strictEqual(yield* Deferred.await(a.lost), "superseded");
        yield* a.release;
        assert.strictEqual(yield* holderIdOf(store), "x");
      }),
    ),
  );

  it.effect("marks the holder superseded at once when the broker refuses its generation", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let fenced = false;
        const a = yield* acquire(
          fencedWhile(makeStore(), () => fenced),
          holder("a", { generation: 5 }),
        );
        fenced = true;
        yield* TestClock.adjust(Duration.millis(ttlMs / 4));
        assert.strictEqual(yield* Deferred.await(a.lost), "superseded");
      }),
    ),
  );

  it.effect("expires T after the last good renewal, before a waiting taker can steal", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const store = makeStore();
        let down = false;
        const a = yield* acquire(
          flaky(store, () => down),
          holder("a"),
        );
        // The renewal at T/4 succeeds; every later one fails.
        yield* TestClock.adjust(Duration.millis(ttlMs / 4));
        down = true;
        const lastOk = ttlMs / 4;
        const taker = yield* Effect.forkChild(acquire(store, holder("b")));

        yield* TestClock.adjust(Duration.millis(lastOk + ttlMs - 1 - ttlMs / 4));
        assert.isFalse(yield* Deferred.isDone(a.lost));
        yield* TestClock.adjust(Duration.millis(1));
        assert.strictEqual(yield* Deferred.await(a.lost), "expired");

        yield* TestClock.adjust(Duration.millis(ttlMs / 2 - 1));
        assert.strictEqual(yield* holderIdOf(store), "a");
        yield* TestClock.adjust(Duration.millis(1));
        yield* Fiber.join(taker);
        assert.strictEqual(yield* holderIdOf(store), "b");
      }),
    ),
  );
});
