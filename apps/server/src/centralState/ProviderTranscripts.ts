// @effect-diagnostics nodeBuiltinImport:off - transcript tails are compared by sha256.
import * as NodeCrypto from "node:crypto";

import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";

import type { CentralObjectFencedError, CentralObjectStoreError } from "./CentralObjectStore.ts";
import {
  type ClaudeSection,
  type CodexSection,
  TRANSCRIPT_MAX_CHUNKS,
  type TranscriptEntry,
} from "./Manifest.ts";
import type { TranscriptHooks, TranscriptFiles } from "./StateBundle.ts";
import { isSafeRelativePath } from "./StateFiles.ts";

const NEWLINE_SEARCH_BLOCK = 64 * 1024;
const TAIL_BYTES = 4096;

type Entry = typeof TranscriptEntry.Type;

// Untraced: called per 64 KiB block inside the collect span.
const readRange = Effect.fnUntraced(function* (file: FileSystem.File, start: number, end: number) {
  const buffer = new Uint8Array(end - start);
  let filled = 0;
  while (filled < buffer.length) {
    const read = yield* file.read(buffer.subarray(filled), { position: BigInt(start + filled) });
    if (read === 0) break;
    filled += read;
  }
  return buffer.subarray(0, filled);
});

const tailSha256 = (file: FileSystem.File, end: number) =>
  end === 0
    ? Effect.succeed(null)
    : readRange(file, Math.max(0, end - TAIL_BYTES), end).pipe(
        Effect.map((bytes) => NodeCrypto.createHash("sha256").update(bytes).digest("hex")),
      );

/** One past the last "\n" in [0, size), or 0 when there is none. */
const completeLinesEnd = Effect.fnUntraced(function* (file: FileSystem.File, size: number) {
  let position = size;
  while (position > 0) {
    const start = Math.max(0, position - NEWLINE_SEARCH_BLOCK);
    const block = yield* readRange(file, start, position);
    const index = block.lastIndexOf(0x0a);
    if (index >= 0) return start + index + 1;
    position = start;
  }
  return 0;
});

/**
 * The directories of a provider home that let its threads resume natively, and how they map onto
 * the provider's manifest section. Nothing else in the home is read, so logins (Codex's auth.json
 * and auth.lock), caches, logs and the provider's own databases stay on the machine.
 */
export interface Layout<Section> {
  readonly roots: ReadonlyArray<string>;
  readonly toFiles: (section: Section) => TranscriptFiles;
  readonly fromFiles: (files: TranscriptFiles) => Section;
}

const hasFiles = (files: Readonly<Record<string, Entry>>) => Object.keys(files).length > 0;

export const claude: Layout<typeof ClaudeSection.Type> = {
  // Claude's TaskGet and TaskUpdate read <claudeHome>/tasks/<session id> back on resume.
  roots: ["projects", "tasks"],
  toFiles: (section) => ({ projects: section.files, tasks: section.tasks ?? {} }),
  fromFiles: ({ projects = {}, tasks = {} }) =>
    hasFiles(tasks) ? { files: projects, tasks } : { files: projects },
};

export const codex: Layout<typeof CodexSection.Type | undefined> = {
  // thread/resume looks a rollout up by thread id under sessions/, or archived_sessions/ once archived.
  roots: ["sessions", "archived_sessions"],
  toFiles: (section) => ({
    sessions: section?.sessions ?? {},
    archived_sessions: section?.archivedSessions ?? {},
  }),
  fromFiles: ({ sessions = {}, archived_sessions: archivedSessions = {} }) => {
    if (!hasFiles(sessions) && !hasFiles(archivedSessions)) return undefined;
    return hasFiles(archivedSessions) ? { sessions, archivedSessions } : { sessions };
  },
};

type CollectInput = Parameters<TranscriptHooks["collect"]>[0];

const collectRoot = Effect.fn("ProviderTranscripts.collectRoot")(function* (
  input: Omit<CollectInput, "roots" | "previous"> & {
    readonly root: string;
    readonly previous: Readonly<Record<string, Entry>>;
  },
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const rootDir = path.join(input.home, input.root);
  // Not readDirectory's recursive mode: it descends into symlinked directories.
  const walk = (
    relativeDir: string,
  ): Effect.Effect<ReadonlyArray<string>, PlatformError.PlatformError> =>
    fileSystem.readDirectoryEntries(path.join(rootDir, ...relativeDir.split("/"))).pipe(
      Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed([])),
      Effect.flatMap((entries) =>
        Effect.forEach(entries, (entry) => {
          const relativePath = relativeDir === "" ? entry.name : `${relativeDir}/${entry.name}`;
          if (entry.type === "Directory") return walk(relativePath);
          return Effect.succeed(entry.type === "SymbolicLink" ? [] : [relativePath]);
        }),
      ),
      Effect.map((nested) => nested.flat()),
    );
  const relativePaths = [...(yield* walk(""))].sort();

  // Untraced: runs once per file inside this span.
  const collectFile = Effect.fnUntraced(
    function* (
      relativePath: string,
    ): Effect.fn.Return<
      Option.Option<Entry>,
      CentralObjectStoreError | CentralObjectFencedError | PlatformError.PlatformError
    > {
      const absolutePath = path.join(rootDir, ...relativePath.split("/"));
      const info = yield* fileSystem.lstat(absolutePath);
      if (info.type !== "File") return Option.none();
      const size = Number(info.size);
      const mtimeMs = Option.getOrUndefined(info.mtime)?.getTime() ?? 0;
      const previous = input.previous[relativePath];
      if (previous !== undefined && previous.size === size && previous.mtimeMs === mtimeMs) {
        return Option.some(previous);
      }

      const isJsonl = relativePath.endsWith(".jsonl");
      return yield* Effect.scoped(
        Effect.gen(function* () {
          const file = yield* fileSystem.open(absolutePath, { flag: "r" });
          const end = isJsonl ? yield* completeLinesEnd(file, size) : size;
          if (isJsonl && end === 0) return Option.fromUndefinedOr(previous);

          const appendable =
            previous !== undefined &&
            isJsonl &&
            end >= previous.size &&
            previous.chunks.length < TRANSCRIPT_MAX_CHUNKS &&
            (yield* tailSha256(file, previous.size)) === previous.tailSha256;
          if (appendable && end === previous.size) {
            return Option.some({ ...previous, mtimeMs });
          }
          const offset = appendable ? previous.size : 0;
          const chunk =
            end > offset
              ? [yield* input.upload({ path: absolutePath, offset, length: end - offset })]
              : [];
          return Option.some({
            size: end,
            mtimeMs,
            tailSha256: yield* tailSha256(file, end),
            chunks: appendable ? [...previous.chunks, ...chunk] : chunk,
          });
        }),
      );
    },
    // Providers delete, rotate and archive files while a push runs.
    Effect.catchReason("PlatformError", "NotFound", () => Effect.succeedNone),
  );

  const collected = yield* Effect.forEach(relativePaths, (file) => collectFile(file), {
    concurrency: 4,
  });
  const files: Record<string, Entry> = {};
  for (const [index, entry] of collected.entries()) {
    if (Option.isSome(entry)) files[relativePaths[index]!] = entry.value;
  }
  return files;
});

export const collect = Effect.fn("ProviderTranscripts.collect")(function* (input: CollectInput) {
  const collected: Record<string, Readonly<Record<string, Entry>>> = {};
  for (const root of input.roots) {
    collected[root] = yield* collectRoot({ ...input, root, previous: input.previous[root] ?? {} });
  }
  return collected;
});

export const stage = Effect.fn("ProviderTranscripts.stage")(function* (
  input: Parameters<TranscriptHooks["stage"]>[0],
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const entries = Object.entries(input.files).flatMap(([root, files]) =>
    Object.entries(files)
      .filter(([relativePath]) => isSafeRelativePath(relativePath))
      .map(([relativePath, entry]) => ({ root, relativePath, entry })),
  );
  const staged = (root: string, relativePath: string) =>
    path.join(input.stagingDir, root, ...relativePath.split("/"));

  yield* Effect.forEach(
    entries,
    ({ root, relativePath, entry }) =>
      Effect.gen(function* () {
        const destinationPath = staged(root, relativePath);
        if (entry.chunks.length === 0) {
          yield* fileSystem.makeDirectory(path.dirname(destinationPath), { recursive: true });
          yield* fileSystem.writeFile(destinationPath, new Uint8Array());
          return;
        }
        for (const [index, ref] of entry.chunks.entries()) {
          yield* input.download({ ref, destinationPath, append: index > 0 });
        }
      }),
    { concurrency: 8, discard: true },
  );

  const place = Effect.gen(function* () {
    for (const { root, relativePath, entry } of entries) {
      const target = path.join(input.home, root, ...relativePath.split("/"));
      yield* fileSystem.makeDirectory(path.dirname(target), { recursive: true });
      yield* fileSystem.rename(staged(root, relativePath), target);
      // A Date, not a number: Node reads numbers as seconds, and float seconds lose the exact milliseconds.
      const mtime = DateTime.toDate(DateTime.makeUnsafe(entry.mtimeMs));
      yield* fileSystem.utimes(target, mtime, mtime);
    }
    yield* fileSystem.remove(input.stagingDir, { recursive: true, force: true });
  }).pipe(Effect.withSpan("ProviderTranscripts.place"));
  return { place };
});

/** TranscriptHooks run inside StateBundle with no requirements, so they close over these services. */
export const hooks = Effect.gen(function* () {
  const services = yield* Effect.context<FileSystem.FileSystem | Path.Path>();
  const transcriptHooks: TranscriptHooks = {
    collect: (input) => collect(input).pipe(Effect.provideContext(services)),
    stage: (input) => stage(input).pipe(Effect.provideContext(services)),
  };
  return transcriptHooks;
});
