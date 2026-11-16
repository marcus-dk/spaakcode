// @effect-diagnostics nodeBuiltinImport:off - blobs are zstd streams named by the sha256 of their logical bytes.
import * as NodeCrypto from "node:crypto";
import * as NodeZlib from "node:zlib";

import * as NodeStream from "@effect/platform-node-shared/NodeStream";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as CentralObjectStore from "./CentralObjectStore.ts";
import { keys, type BlobRef } from "./Manifest.ts";

export class CentralBlobMissingError extends Schema.TaggedError<CentralBlobMissingError>()(
  "CentralBlobMissingError",
  { key: Schema.String },
) {
  override get message(): string {
    return `Central state blob ${this.key} is missing.`;
  }
}

export class CentralBlobIntegrityError extends Schema.TaggedError<CentralBlobIntegrityError>()(
  "CentralBlobIntegrityError",
  {
    key: Schema.String,
    expectedSha256: Schema.String,
    actualSha256: Schema.String,
    expectedSize: Schema.Number,
    actualSize: Schema.Number,
  },
) {
  override get message(): string {
    return `Central state blob ${this.key} does not match its reference (expected ${this.expectedSize} bytes, got ${this.actualSize}).`;
  }
}

class ZstdStreamError extends Schema.TaggedError<ZstdStreamError>()("ZstdStreamError", {
  cause: Schema.Defect(),
}) {}

const zstdFailure = (method: "compress" | "decompress", cause: unknown) =>
  PlatformError.systemError({ _tag: "InvalidData", module: "Blobs", method, cause });

/** Counts and hashes the logical bytes a stream carries, as they pass. */
const measure = () => {
  const hash = NodeCrypto.createHash("sha256");
  let size = 0;
  let digest: { readonly sha256: string; readonly size: number } | undefined;
  return {
    // Per-chunk bookkeeping inside the caller's span.
    tap: Stream.map((chunk: Uint8Array) => {
      hash.update(chunk);
      size += chunk.length;
      return chunk;
    }),
    result: () => (digest ??= { sha256: hash.digest("hex"), size }),
  };
};

// The staging dir and the destination can be on different filesystems.
const isCrossDevice = (error: PlatformError.PlatformError) =>
  error.reason._tag === "Unknown" &&
  (error.reason.cause as { readonly code?: unknown } | undefined)?.code === "EXDEV";

const temporaryPath = (path: Path.Path, workDir: string, suffix: string) =>
  path.join(workDir, "tmp", `blob-${NodeCrypto.randomUUID()}${suffix}`);

/** Where a blob is stored: with the manifests' blobs, or with the transcript tail's chunks. */
export type Kind = "blob" | "tailChunk";

/**
 * Hashes and zstd-compresses [offset, offset+length) of path in one pass into a temp file under
 * workDir/tmp, then PUTs IfAbsent unless isKnown(sha256). A precondition failure means present.
 */
export const upload = Effect.fn("Blobs.upload")(function* (input: {
  readonly homeKey: string;
  readonly workDir: string;
  readonly path: string;
  readonly offset?: number;
  readonly length?: number;
  readonly isKnown: (sha256: string) => boolean;
  readonly kind?: Kind;
}) {
  const store = yield* CentralObjectStore.CentralObjectStore;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fileSystem.makeDirectory(path.join(input.workDir, "tmp"), { recursive: true });
  const compressed = temporaryPath(path, input.workDir, ".zst");
  return yield* Effect.gen(function* () {
    const measured = measure();
    yield* fileSystem
      .stream(input.path, {
        ...(input.offset === undefined ? {} : { offset: input.offset }),
        ...(input.length === undefined ? {} : { bytesToRead: input.length }),
      })
      .pipe(
        measured.tap,
        NodeStream.pipeThroughDuplex({
          evaluate: () => NodeZlib.createZstdCompress(),
          onError: (cause) => zstdFailure("compress", cause),
        }),
        Stream.run(fileSystem.sink(compressed)),
      );
    const { sha256, size } = measured.result();
    const ref: typeof BlobRef.Type = { sha256, size, encoding: "zstd" };
    if (input.isKnown(sha256)) return ref;
    yield* store
      .write(
        keys[input.kind ?? "blob"](input.homeKey, sha256),
        { _tag: "File", path: compressed },
        { _tag: "IfAbsent" },
      )
      .pipe(Effect.catchTags({ CentralObjectPreconditionError: () => Effect.void }));
    return ref;
  }).pipe(Effect.ensuring(fileSystem.remove(compressed, { force: true }).pipe(Effect.ignore)));
});

/**
 * Downloads, zstd-decompresses while hashing, verifies sha256 and size, then writes (or appends to)
 * destinationPath. Nothing is written to destinationPath unless verification passes.
 */
export const download = Effect.fn("Blobs.download")(function* (input: {
  readonly homeKey: string;
  readonly workDir: string;
  readonly ref: typeof BlobRef.Type;
  readonly destinationPath: string;
  readonly append: boolean;
  readonly kind?: Kind;
}) {
  const store = yield* CentralObjectStore.CentralObjectStore;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const key = keys[input.kind ?? "blob"](input.homeKey, input.ref.sha256);
  yield* fileSystem.makeDirectory(path.join(input.workDir, "tmp"), { recursive: true });
  const compressed = temporaryPath(path, input.workDir, ".zst");
  const plain = temporaryPath(path, input.workDir, "");
  const cleanup = Effect.forEach(
    [compressed, plain],
    (file) => fileSystem.remove(file, { force: true }).pipe(Effect.ignore),
    { discard: true },
  );
  yield* Effect.gen(function* () {
    if (!(yield* store.download(key, compressed))) {
      return yield* new CentralBlobMissingError({ key });
    }
    const measured = measure();
    const integrityError = () => {
      const actual = measured.result();
      return new CentralBlobIntegrityError({
        key,
        expectedSha256: input.ref.sha256,
        actualSha256: actual.sha256,
        expectedSize: input.ref.size,
        actualSize: actual.size,
      });
    };
    yield* fileSystem.stream(compressed).pipe(
      NodeStream.pipeThroughDuplex({
        evaluate: () => NodeZlib.createZstdDecompress(),
        onError: (cause) => new ZstdStreamError({ cause }),
      }),
      measured.tap,
      Stream.run(fileSystem.sink(plain)),
      // A stream that does not decompress is a blob that does not match its reference.
      Effect.catchTags({ ZstdStreamError: () => Effect.fail(integrityError()) }),
    );
    const actual = measured.result();
    if (actual.sha256 !== input.ref.sha256 || actual.size !== input.ref.size) {
      return yield* integrityError();
    }
    yield* fileSystem.makeDirectory(path.dirname(input.destinationPath), { recursive: true });
    if (input.append) {
      yield* fileSystem
        .stream(plain)
        .pipe(Stream.run(fileSystem.sink(input.destinationPath, { flag: "a" })));
    } else {
      yield* fileSystem
        .rename(plain, input.destinationPath)
        .pipe(
          Effect.catchIf(isCrossDevice, () => fileSystem.copyFile(plain, input.destinationPath)),
        );
    }
  }).pipe(Effect.ensuring(cleanup));
});
