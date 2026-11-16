// @effect-diagnostics nodeBuiltinImport:off - the marker's temp file name needs a UUID.
import * as NodeCrypto from "node:crypto";

import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Random from "effect/Random";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import * as CentralObjectStore from "./CentralObjectStore.ts";

export const MANIFEST_READER_VERSION = 1;
export const RETAIN_GENERATIONS = 10;
export const RETAIN_PINNED = 2;
export const TRANSCRIPT_MAX_CHUNKS = 64;

export const keys = {
  lease: (homeKey: string) => `homes/${homeKey}/lease.json`,
  head: (homeKey: string) => `homes/${homeKey}/manifest.json`,
  history: (homeKey: string, epoch: string, generation: number) =>
    `homes/${homeKey}/manifests/${epoch}/${String(generation).padStart(12, "0")}.json`,
  blob: (homeKey: string, sha256: string) => `homes/${homeKey}/blobs/${sha256}`,
} as const;

export const BlobRef = Schema.Struct({
  /** Hex sha256 of the logical (uncompressed) bytes; also the object name. */
  sha256: Schema.String,
  size: Schema.Number,
  encoding: Schema.Literal("zstd"),
});
export const StateFileEntry = Schema.Struct({ blob: BlobRef, mtimeMs: Schema.Number });
export const TranscriptEntry = Schema.Struct({
  /** Bytes represented by chunks; for .jsonl, through the last "\n". */
  size: Schema.Number,
  mtimeMs: Schema.Number,
  /** sha256 of bytes [max(0, size - 4096), size); null when size is 0. */
  tailSha256: Schema.NullOr(Schema.String),
  chunks: Schema.Array(BlobRef),
});
export const ClaudeSection = Schema.Struct({
  /** Keyed by the "/"-separated path under <claudeHome>/projects. */
  files: Schema.Record(Schema.String, TranscriptEntry),
  /** Claude's per-session task lists, keyed by the "/"-separated path under <claudeHome>/tasks. */
  tasks: Schema.optionalKey(Schema.Record(Schema.String, TranscriptEntry)),
});
export const MigrationRow = Schema.Struct({ id: Schema.Number, name: Schema.String });
export const DatabaseRef = Schema.Struct({
  blob: BlobRef,
  migrations: Schema.Array(MigrationRow),
  maxSequence: Schema.Number,
});
export const Holder = Schema.Struct({
  holderId: Schema.String,
  host: Schema.String,
  pid: Schema.Number,
  bootId: Schema.NullOr(Schema.String),
  stateDir: Schema.String,
  /** The session service's pod generation, when it assigns one. */
  generation: Schema.optionalKey(Schema.Number),
});
export const HistoryRef = Schema.Struct({ epoch: Schema.String, generation: Schema.Number });
export const PinnedRef = Schema.Struct({
  epoch: Schema.String,
  generation: Schema.Number,
  migrationHead: Schema.Number,
});

export const Manifest = Schema.Struct({
  formatVersion: Schema.Literal(1),
  minReaderVersion: Schema.Number,
  homeKey: Schema.String,
  /** Lineage id; new on every restore. */
  epoch: Schema.String,
  /** +1 per committed push; fence writes keep it. */
  generation: Schema.Number,
  holder: Holder,
  /** holderId whose database produced `database`; changed only by commits. */
  committedBy: Schema.NullOr(Schema.String),
  committedAt: Schema.NullOr(Schema.String),
  buildVersion: Schema.String,
  paths: Schema.Struct({
    baseDir: Schema.String,
    stateDir: Schema.String,
    claudeHome: Schema.String,
  }),
  /** Highest sequence floor ever reserved for this home. */
  sequenceFloor: Schema.Number,
  database: Schema.NullOr(DatabaseRef),
  /** Keyed by the "/"-separated path under stateDir. */
  files: Schema.Record(Schema.String, StateFileEntry),
  claude: ClaudeSection,
  /** Newest first; includes this generation once committed. */
  history: Schema.Array(HistoryRef),
  pinned: Schema.Array(PinnedRef),
  writeNonce: Schema.String,
});
export type Manifest = typeof Manifest.Type;

export const LeaseRecord = Schema.Struct({
  formatVersion: Schema.Literal(1),
  homeKey: Schema.String,
  holder: Schema.NullOr(Schema.Struct({ ...Holder.fields, acquiredAt: Schema.String })),
  /** Informational only; never compared across hosts. */
  renewedAt: Schema.String,
  headSequence: Schema.NullOr(Schema.Number),
  takeoverRequest: Schema.NullOr(
    Schema.Struct({
      holderId: Schema.String,
      host: Schema.String,
      pid: Schema.Number,
      requestedAt: Schema.String,
    }),
  ),
  writeNonce: Schema.String,
});

/** Stored at <stateDir>/central-state/marker.json. */
export const LocalMarker = Schema.Struct({
  formatVersion: Schema.Literal(1),
  homeKey: Schema.String,
  epoch: Schema.String,
  generation: Schema.Number,
  holderId: Schema.String,
});

export class CentralCasLostError extends Schema.TaggedError<CentralCasLostError>()(
  "CentralCasLostError",
  { key: Schema.String },
) {
  override get message(): string {
    return `Another writer changed ${this.key} first.`;
  }
}

export class CentralStateCorruptObjectError extends Schema.TaggedError<CentralStateCorruptObjectError>()(
  "CentralStateCorruptObjectError",
  { key: Schema.String, cause: Schema.Defect() },
) {
  override get message(): string {
    return `Central state object ${this.key} could not be decoded.`;
  }
}

export class CentralStateUnsupportedManifestError extends Schema.TaggedError<CentralStateUnsupportedManifestError>()(
  "CentralStateUnsupportedManifestError",
  { key: Schema.String, minReaderVersion: Schema.Number },
) {
  override get message(): string {
    return `Central state ${this.key} needs reader version ${this.minReaderVersion}; this build reads version ${MANIFEST_READER_VERSION}.`;
  }
}

const textDecoder = new TextDecoder();
const textEncoder = new TextEncoder();
const decodeJsonText = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const ReaderVersion = Schema.Struct({ minReaderVersion: Schema.Number });

const decodeObject = <A, I>(key: string, schema: Schema.Codec<A, I>, json: unknown) =>
  Schema.decodeUnknownEffect(schema)(json).pipe(
    Effect.mapError((cause) => new CentralStateCorruptObjectError({ key, cause })),
  );

const parseObject = (key: string, text: string) =>
  decodeJsonText(text).pipe(
    Effect.mapError((cause) => new CentralStateCorruptObjectError({ key, cause })),
  );

const encodeJson = <A, I>(schema: Schema.Codec<A, I>, value: A) =>
  // A value of the schema's own type always encodes.
  Schema.encodeEffect(schema)(value).pipe(Effect.map(JSON.stringify), Effect.orDie);

/** A present object that does not decode is corrupt, never absent. */
export const readJson = Effect.fn("Manifest.readJson")(function* <A, I>(
  key: string,
  schema: Schema.Codec<A, I>,
) {
  const store = yield* CentralObjectStore.CentralObjectStore;
  const object = yield* store.read(key);
  if (Option.isNone(object)) return Option.none<{ readonly value: A; readonly version: string }>();
  const json = yield* parseObject(key, textDecoder.decode(object.value.bytes));
  const value = yield* decodeObject(key, schema, json);
  return Option.some({ value, version: object.value.version });
});

export const readHead = Effect.fn("Manifest.readHead")(function* (homeKey: string) {
  const store = yield* CentralObjectStore.CentralObjectStore;
  const key = keys.head(homeKey);
  const object = yield* store.read(key);
  if (Option.isNone(object)) {
    return Option.none<{ readonly manifest: Manifest; readonly version: string }>();
  }
  const json = yield* parseObject(key, textDecoder.decode(object.value.bytes));
  const { minReaderVersion } = yield* decodeObject(key, ReaderVersion, json);
  if (minReaderVersion > MANIFEST_READER_VERSION) {
    return yield* new CentralStateUnsupportedManifestError({ key, minReaderVersion });
  }
  const manifest = yield* decodeObject(key, Manifest, json);
  return Option.some({ manifest, version: object.value.version });
});

const RETRY_BASE_MS = 200;
const RETRY_MAX_MS = 5_000;

// A wall-clock step must not stretch or cut the retry budget.
const monotonicMillis = Effect.map(Clock.monotonicTimeNanos, (nanos) => Number(nanos) / 1e6);

/**
 * The only way CAS keys are written. An ambiguous PUT is resolved by reading the key back: our
 * `writeNonce` there means the write landed. Transport failures retry while the key is unchanged.
 */
export const casWrite = Effect.fn("Manifest.casWrite")(function* <
  A extends { readonly writeNonce: string },
  I,
>(input: {
  readonly key: string;
  readonly schema: Schema.Codec<A, I>;
  readonly value: A;
  readonly precondition:
    | { readonly _tag: "IfAbsent" }
    | { readonly _tag: "IfVersion"; readonly version: string };
  readonly retryFor: Duration.Duration;
}) {
  const store = yield* CentralObjectStore.CentralObjectStore;
  const bytes = textEncoder.encode(yield* encodeJson(input.schema, input.value));
  const budgetMs = Duration.toMillis(input.retryFor);
  const startedAt = yield* monotonicMillis;
  let attempt = 0;
  while (true) {
    const written = yield* Effect.result(
      store.write(input.key, { _tag: "Bytes", bytes }, input.precondition),
    );
    if (Result.isSuccess(written)) return written.success;
    const writeError = written.failure;
    if (writeError._tag === "CentralObjectFencedError") return yield* writeError;

    let lastError: CentralObjectStore.CentralObjectStoreError;
    const remote = yield* readJson(input.key, input.schema).pipe(
      Effect.map(Result.succeed),
      Effect.catchTags({
        CentralObjectStoreError: (error) => Effect.succeed(Result.fail(error)),
      }),
    );
    if (Result.isSuccess(remote)) {
      const current = remote.success;
      if (Option.isSome(current) && current.value.value.writeNonce === input.value.writeNonce) {
        return { version: current.value.version };
      }
      if (writeError._tag === "CentralObjectPreconditionError") {
        return yield* new CentralCasLostError({ key: input.key });
      }
      const unchanged =
        input.precondition._tag === "IfAbsent"
          ? Option.isNone(current)
          : Option.isSome(current) && current.value.version === input.precondition.version;
      if (!unchanged) return yield* new CentralCasLostError({ key: input.key });
      lastError = writeError;
    } else {
      lastError = remote.failure;
    }

    const jitter = 0.8 + 0.4 * (yield* Random.next);
    const delayMs = Math.min(RETRY_MAX_MS, RETRY_BASE_MS * 2 ** attempt) * jitter;
    attempt += 1;
    const elapsedMs = (yield* monotonicMillis) - startedAt;
    if (elapsedMs + delayMs > budgetMs) return yield* lastError;
    yield* Effect.sleep(Duration.millis(delayMs));
  }
});

const markerPath = (path: Path.Path, stateDir: string) =>
  path.join(stateDir, "central-state", "marker.json");

export const readMarker = Effect.fn("Manifest.readMarker")(function* (stateDir: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const file = markerPath(path, stateDir);
  const contents = yield* fileSystem.readFileString(file).pipe(
    Effect.asSome,
    Effect.catchIf(
      (error: PlatformError.PlatformError) => error.reason._tag === "NotFound",
      () => Effect.succeedNone,
    ),
  );
  if (Option.isNone(contents)) return Option.none<typeof LocalMarker.Type>();
  const json = yield* parseObject(file, contents.value);
  return Option.some(yield* decodeObject(file, LocalMarker, json));
});

export const writeMarker = Effect.fn("Manifest.writeMarker")(function* (
  stateDir: string,
  marker: typeof LocalMarker.Type,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const file = markerPath(path, stateDir);
  const temporary = `${file}.${NodeCrypto.randomUUID()}.tmp`;
  yield* fileSystem.makeDirectory(path.dirname(file), { recursive: true });
  yield* fileSystem.writeFileString(temporary, yield* encodeJson(LocalMarker, marker));
  yield* fileSystem
    .rename(temporary, file)
    .pipe(Effect.tapError(() => fileSystem.remove(temporary, { force: true }).pipe(Effect.ignore)));
});
