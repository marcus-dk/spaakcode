import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";

import {
  PENDING_ATTACHMENT_THREAD_SEGMENT,
  parseThreadSegmentFromAttachmentId,
} from "../attachmentStore.ts";
import { DPOP_REPLAY_MARKER_PREFIX } from "../auth/dpop.ts";
import { CLOUD_REPLAY_MARKER_PREFIXES } from "../cloud/CloudLink.ts";
import type { CentralObjectStoreError } from "./CentralObjectStore.ts";
import type { BlobRef, StateFileEntry } from "./Manifest.ts";

const SYNCED_FILES = new Set([
  "environment-id",
  "anonymous-id",
  "settings.json",
  "keybindings.json",
]);
const SYNCED_DIRECTORIES = ["themes", "secrets", "attachments"];
const REPLAY_MARKER_PREFIXES = [DPOP_REPLAY_MARKER_PREFIX, ...CLOUD_REPLAY_MARKER_PREFIXES];
const ASIDE_ITEMS = [
  "statev2.sqlite",
  "statev2.sqlite-wal",
  "statev2.sqlite-shm",
  "environment-id",
  "environment-id.recovery",
  "anonymous-id",
  "settings.json",
  "keybindings.json",
  "themes",
  "secrets",
  "attachments",
];

type CacheEntry = {
  readonly size: number;
  readonly mtimeMs: number;
  readonly entry: typeof StateFileEntry.Type;
};

/**
 * Pure. True when a "/"-separated key from a manifest stays inside the directory it is joined to:
 * no empty, "." or ".." segment, and no "\\", ":" or NUL, which Windows paths read as separators or
 * drives.
 */
export const isSafeRelativePath = (relativePath: string): boolean =>
  relativePath
    .split("/")
    .every(
      (segment) =>
        segment !== "" && segment !== "." && segment !== ".." && !/[\\:\0]/.test(segment),
    );

/** Pure. relativePath is under stateDir with "/" separators. */
export const isSynced = (relativePath: string): boolean => {
  if (!isSafeRelativePath(relativePath)) return false;
  const segments = relativePath.split("/");
  const [root = "", ...rest] = segments;
  const name = rest.at(-1);
  if (name === undefined) return SYNCED_FILES.has(root);
  switch (root) {
    case "themes":
      return true;
    case "secrets":
      return (
        !name.endsWith(".tmp") && !REPLAY_MARKER_PREFIXES.some((prefix) => name.startsWith(prefix))
      );
    case "attachments":
      // Ids hold no dots, so a pending upload's extracted-text sidecar (`<id>.pdf.txt`) is caught too.
      return (
        !name.endsWith(".part") &&
        parseThreadSegmentFromAttachmentId(name.split(".")[0] ?? "") !==
          PENDING_ATTACHMENT_THREAD_SEGMENT
      );
    default:
      return false;
  }
};

export const scan = Effect.fn("StateFiles.scan")(function* (input: {
  readonly stateDir: string;
  readonly cache: Map<string, CacheEntry>;
  readonly upload: (
    path: string,
  ) => Effect.Effect<typeof BlobRef.Type, CentralObjectStoreError | PlatformError.PlatformError>;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const absolute = (relativePath: string) => path.join(input.stateDir, ...relativePath.split("/"));
  // lstat: a symlink is never synced, so nothing outside the home is uploaded through one.
  const stat = (relativePath: string) =>
    fs.lstat(absolute(relativePath)).pipe(
      Effect.map((info) =>
        info.type === "File"
          ? Option.some({
              size: Number(info.size),
              mtimeMs: Option.getOrUndefined(info.mtime)?.getTime() ?? 0,
            })
          : Option.none(),
      ),
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeedNone),
    );

  // Not readDirectory's recursive mode: it descends into symlinked directories.
  const walk = (
    relativeDir: string,
  ): Effect.Effect<ReadonlyArray<string>, PlatformError.PlatformError> =>
    fs.readDirectoryEntries(absolute(relativeDir)).pipe(
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed([])),
      Effect.flatMap((entries) =>
        Effect.forEach(entries, (entry) => {
          const relativePath = `${relativeDir}/${entry.name}`;
          if (entry.type === "Directory") return walk(relativePath);
          return Effect.succeed(entry.type === "File" ? [relativePath] : []);
        }),
      ),
      Effect.map((nested) => nested.flat()),
    );
  const nested = yield* Effect.forEach(SYNCED_DIRECTORIES, walk);
  const candidates = [...SYNCED_FILES, ...nested.flat()].filter(isSynced).sort();

  const scanned = yield* Effect.forEach(
    candidates,
    (relativePath) =>
      Effect.gen(function* () {
        const before = yield* stat(relativePath);
        if (Option.isNone(before)) return Option.none();
        const cached = input.cache.get(relativePath);
        if (
          cached !== undefined &&
          cached.size === before.value.size &&
          cached.mtimeMs === before.value.mtimeMs
        ) {
          return Option.some([relativePath, cached.entry] as const);
        }
        const blob = yield* input.upload(absolute(relativePath));
        const entry = { blob, mtimeMs: before.value.mtimeMs };
        const after = yield* stat(relativePath);
        // A file that changed while it was read is uploaded again on the next scan.
        if (
          Option.isSome(after) &&
          after.value.size === before.value.size &&
          after.value.mtimeMs === before.value.mtimeMs
        ) {
          input.cache.set(relativePath, { ...before.value, entry });
        } else {
          input.cache.delete(relativePath);
        }
        return Option.some([relativePath, entry] as const);
      }).pipe(
        // Attachments and secrets can be deleted while a scan runs.
        Effect.catchReason("PlatformError", "NotFound", () => Effect.succeedNone),
      ),
    { concurrency: 4 },
  );

  const files: Record<string, typeof StateFileEntry.Type> = {};
  for (const item of scanned) {
    if (Option.isSome(item)) files[item.value[0]] = item.value[1];
  }
  for (const relativePath of input.cache.keys()) {
    if (!(relativePath in files)) input.cache.delete(relativePath);
  }
  return files;
});

export const moveAside = Effect.fn("StateFiles.moveAside")(function* (input: {
  readonly stateDir: string;
  readonly asideDir: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fs.makeDirectory(input.asideDir, { recursive: true });
  for (const name of ASIDE_ITEMS) {
    yield* fs
      .rename(path.join(input.stateDir, name), path.join(input.asideDir, name))
      .pipe(Effect.catchReason("PlatformError", "NotFound", () => Effect.void));
  }
});

export const place = Effect.fn("StateFiles.place")(function* (input: {
  readonly stagingDir: string;
  readonly stateDir: string;
  readonly files: Record<string, typeof StateFileEntry.Type>;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  let placedSecrets = false;
  for (const [relativePath, entry] of Object.entries(input.files)) {
    const segments = relativePath.split("/");
    const from = path.join(input.stagingDir, ...segments);
    const to = path.join(input.stateDir, ...segments);
    const secret = segments[0] === "secrets";
    placedSecrets ||= secret;
    yield* fs.makeDirectory(
      path.dirname(to),
      secret ? { recursive: true, mode: 0o700 } : { recursive: true },
    );
    if (secret) yield* fs.chmod(from, 0o600);
    yield* fs.rename(from, to);
    // A Date, not a number: Node reads numbers as seconds, and float seconds lose the exact milliseconds.
    const mtime = DateTime.toDate(DateTime.makeUnsafe(entry.mtimeMs));
    yield* fs.utimes(to, mtime, mtime);
  }
  if (placedSecrets) yield* fs.chmod(path.join(input.stateDir, "secrets"), 0o700);
  yield* fs.makeDirectory(path.join(input.stateDir, "attachments"), { recursive: true });
});
