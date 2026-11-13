import { assert, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import * as CentralObjectStore from "./CentralObjectStore.ts";
import * as Manifest from "./Manifest.ts";

type Fault = "precondition-after-apply" | "transport-after-apply" | "transport-before-apply";

/** An in-memory store whose next writes fail as scripted, with or without the write landing. */
const makeMemoryStore = (faults: Array<Fault>) => {
  const objects = new Map<string, { readonly bytes: Uint8Array; readonly version: string }>();
  let counter = 0;
  let writes = 0;
  const apply = (
    key: string,
    bytes: Uint8Array,
    precondition: CentralObjectStore.WritePrecondition,
  ) => {
    const current = objects.get(key);
    const matches =
      precondition._tag === "None" ||
      (precondition._tag === "IfAbsent"
        ? current === undefined
        : current?.version === precondition.version);
    if (!matches)
      return Effect.fail(new CentralObjectStore.CentralObjectPreconditionError({ key }));
    counter += 1;
    const version = String(counter);
    objects.set(key, { bytes, version });
    return Effect.succeed({ version });
  };
  const transport = (key: string) =>
    new CentralObjectStore.CentralObjectStoreError({
      operation: "write",
      key,
      status: null,
      cause: new Error("connection reset"),
    });
  const service = CentralObjectStore.CentralObjectStore.of({
    read: (key) => Effect.sync(() => Option.fromNullishOr(objects.get(key))),
    download: () => Effect.die("unused"),
    write: (key, body, precondition) =>
      Effect.suspend(() => {
        writes += 1;
        if (body._tag !== "Bytes") return Effect.die("bytes only");
        switch (faults.shift()) {
          case "precondition-after-apply":
            return apply(key, body.bytes, precondition).pipe(
              Effect.andThen(
                Effect.fail(new CentralObjectStore.CentralObjectPreconditionError({ key })),
              ),
            );
          case "transport-after-apply":
            return apply(key, body.bytes, precondition).pipe(
              Effect.andThen(Effect.fail(transport(key))),
            );
          case "transport-before-apply":
            return Effect.fail(transport(key));
          case undefined:
            return apply(key, body.bytes, precondition);
        }
      }),
    remove: (key) => Effect.sync(() => void objects.delete(key)),
  });
  const seed = (key: string, text: string) => {
    counter += 1;
    objects.set(key, { bytes: new TextEncoder().encode(text), version: String(counter) });
    return String(counter);
  };
  return { service, seed, writes: () => writes };
};

const Record = Schema.Struct({ value: Schema.String, writeNonce: Schema.String });
const key = "homes/a/lease.json";
const ours = { value: "ours", writeNonce: "nonce-ours" };

it.effect.each([
  { name: "plain success", faults: [], seed: null, outcome: "written", writes: 1 },
  {
    name: "precondition failure whose remote nonce is ours",
    faults: ["precondition-after-apply"],
    seed: null,
    outcome: "written",
    writes: 1,
  },
  {
    name: "precondition failure with a foreign nonce",
    faults: [],
    seed: JSON.stringify({ value: "theirs", writeNonce: "nonce-theirs" }),
    outcome: "CentralCasLostError",
    writes: 1,
  },
  {
    name: "transport failure where the read-back shows our nonce",
    faults: ["transport-after-apply"],
    seed: null,
    outcome: "written",
    writes: 1,
  },
  {
    name: "transport failure with the key unchanged retries the PUT",
    faults: ["transport-before-apply"],
    seed: null,
    outcome: "written",
    writes: 2,
  },
  {
    name: "remote body that does not decode",
    faults: [],
    seed: JSON.stringify({ unrelated: true }),
    outcome: "CentralStateCorruptObjectError",
    writes: 1,
  },
] satisfies ReadonlyArray<{
  readonly name: string;
  readonly faults: ReadonlyArray<Fault>;
  readonly seed: string | null;
  readonly outcome: "written" | "CentralCasLostError" | "CentralStateCorruptObjectError";
  readonly writes: number;
}>)("casWrite: $name", (row) =>
  Effect.gen(function* () {
    const memory = makeMemoryStore([...row.faults]);
    const precondition =
      row.seed === null
        ? ({ _tag: "IfAbsent" } as const)
        : ({ _tag: "IfVersion", version: `stale-${memory.seed(key, row.seed)}` } as const);
    const fiber = yield* Manifest.casWrite({
      key,
      schema: Record,
      value: ours,
      precondition,
      retryFor: Duration.seconds(30),
    }).pipe(
      Effect.provideService(CentralObjectStore.CentralObjectStore, memory.service),
      Effect.result,
      Effect.forkChild,
    );
    yield* TestClock.adjust(Duration.seconds(5));
    const result = yield* Fiber.join(fiber);
    assert.strictEqual(memory.writes(), row.writes);
    if (row.outcome !== "written") {
      assert.isTrue(Result.isFailure(result));
      assert.strictEqual(Result.isFailure(result) ? result.failure._tag : null, row.outcome);
      return;
    }
    const stored = yield* memory.service.read(key);
    assert.isTrue(Option.isSome(stored));
    if (Result.isFailure(result) || Option.isNone(stored)) return;
    assert.strictEqual(result.success.version, stored.value.version);
    assert.deepStrictEqual(JSON.parse(new TextDecoder().decode(stored.value.bytes)), ours);
  }),
);

it.effect("readHead refuses a manifest that needs a newer reader", () =>
  Effect.gen(function* () {
    const memory = makeMemoryStore([]);
    memory.seed(Manifest.keys.head("a"), JSON.stringify({ minReaderVersion: 2, formatVersion: 2 }));
    const result = yield* Manifest.readHead("a").pipe(
      Effect.provideService(CentralObjectStore.CentralObjectStore, memory.service),
      Effect.result,
    );
    assert.isTrue(Result.isFailure(result));
    if (Result.isSuccess(result)) return;
    assert.instanceOf(result.failure, Manifest.CentralStateUnsupportedManifestError);
    assert.strictEqual(result.failure.minReaderVersion, 2);
  }),
);
