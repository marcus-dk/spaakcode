// @effect-diagnostics nodeBuiltinImport:off - write nonces are UUIDs, like the manifest's.
import * as NodeCrypto from "node:crypto";

import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import * as CentralObjectStore from "./CentralObjectStore.ts";
import {
  CentralCasLostError,
  type Manifest,
  type TailEntry,
  TRANSCRIPT_MAX_CHUNKS,
  TranscriptTail,
  casWrite,
  keys,
  readHead,
} from "./Manifest.ts";
import type { TranscriptFiles } from "./StateBundle.ts";

export type Tail = typeof TranscriptTail.Type;
export type Provider = "claude" | "codex";
type TailFiles = Tail[Provider];
type Entry = typeof TailEntry.Type;

/** The index as this process last wrote or read it; value is null when it did not decode. */
export interface Stored {
  readonly value: Tail | null;
  readonly version: string;
}

// The next tick writes again, so a short budget only bounds how long one tick can take.
const WRITE_RETRY = Duration.seconds(10);
const decodeTail = Schema.decodeUnknownEffect(Schema.fromJsonString(TranscriptTail));
const textDecoder = new TextDecoder();

export const read = Effect.fn("TranscriptTail.read")(function* (homeKey: string) {
  const store = yield* CentralObjectStore.CentralObjectStore;
  const key = keys.tail(homeKey);
  const object = yield* store.read(key);
  if (Option.isNone(object)) return Option.none<Stored>();
  // Only the last few seconds of transcript depend on it, so a corrupt index is replaced, not fatal.
  const value = yield* decodeTail(textDecoder.decode(object.value.bytes)).pipe(
    Effect.catch(() =>
      Effect.logWarning("Replacing a central state transcript tail that does not decode.").pipe(
        Effect.annotateLogs({ key }),
        Effect.as(null),
      ),
    ),
  );
  const stored: Stored = { value, version: object.value.version };
  return Option.some(stored);
});

export const extendsHead = (tail: Tail, head: Manifest): boolean =>
  tail.generation === head.generation &&
  (tail.epoch === head.epoch || (head.fencedFrom ?? []).includes(tail.epoch));

export const make = (input: {
  readonly homeKey: string;
  readonly holderId: string;
  readonly head: Manifest;
  readonly files?: Readonly<Record<Provider, TailFiles>>;
}): Tail => ({
  formatVersion: 1,
  homeKey: input.homeKey,
  epoch: input.head.epoch,
  generation: input.head.generation,
  holderId: input.holderId,
  claude: input.files?.claude ?? {},
  codex: input.files?.codex ?? {},
  writeNonce: NodeCrypto.randomUUID(),
});

/**
 * CAS over `previous`. Lost to another write, it writes over that one only while the head names this
 * holder: the other writer was then a superseded server, or this one's write that landed after it
 * looked failed. `replaced` is the content the write overwrote.
 */
export const write = Effect.fn("TranscriptTail.write")(function* (
  homeKey: string,
  previous: Option.Option<Stored>,
  value: Tail,
) {
  const attempt = (base: Option.Option<Stored>) =>
    casWrite({
      key: keys.tail(homeKey),
      schema: TranscriptTail,
      value,
      precondition: Option.match(base, {
        onNone: () => ({ _tag: "IfAbsent" }) as const,
        onSome: ({ version }) => ({ _tag: "IfVersion", version }) as const,
      }),
      retryFor: WRITE_RETRY,
    }).pipe(
      Effect.map(({ version }) => ({
        stored: { value, version } satisfies Stored,
        replaced: Option.getOrUndefined(base)?.value ?? null,
      })),
    );
  const overwriteAsHolder = Effect.fn("TranscriptTail.overwriteAsHolder")(function* (
    lost: CentralCasLostError,
  ) {
    const head = Option.getOrUndefined(yield* readHead(homeKey))?.manifest;
    if (head?.holder.holderId !== value.holderId) return yield* lost;
    return yield* attempt(yield* read(homeKey));
  });
  return yield* attempt(previous).pipe(
    Effect.catchTags({
      CentralCasLostError: overwriteAsHolder,
      // A failed write reads the key back, and an index that does not decode fails that read.
      CentralStateCorruptObjectError: () =>
        overwriteAsHolder(new CentralCasLostError({ key: keys.tail(homeKey) })),
    }),
  );
});

export const chunksOf = (tail: Tail | null | undefined): ReadonlySet<string> =>
  new Set(
    tail == null
      ? []
      : [tail.claude, tail.codex].flatMap((files) =>
          Object.values(files).flatMap((byPath) =>
            Object.values(byPath).flatMap((entry) => entry.chunks.map((chunk) => chunk.sha256)),
          ),
        ),
  );

export const isEmpty = (tail: Tail | null): boolean => tail !== null && chunksOf(tail).size === 0;

/**
 * `files` with each tail entry's lines appended; an entry that follows other content is dropped.
 * Only roots `files` names (toFiles names every layout root) are read: an index root is a path.
 */
export const appendTo = (files: TranscriptFiles, tail: TailFiles): TranscriptFiles => {
  const merged = { ...files };
  for (const [root, committed] of Object.entries(files)) {
    const entries = tail[root];
    if (entries === undefined) continue;
    const byPath = { ...committed };
    for (const [relativePath, { baseSize, ...entry }] of Object.entries(entries)) {
      const base = committed[relativePath];
      if ((base?.size ?? 0) !== baseSize) continue;
      byPath[relativePath] = { ...entry, chunks: [...(base?.chunks ?? []), ...entry.chunks] };
    }
    merged[root] = byPath;
  }
  return merged;
};

/**
 * What the store holds of each file: the tail's entry, else the committed one. A tail entry at the
 * chunk cap is left out, so its lines ship again as one chunk from the committed size.
 */
export const shippedFiles = (files: TranscriptFiles, tail: TailFiles): TranscriptFiles => {
  const shipped = { ...files };
  for (const [root, entries] of Object.entries(tail)) {
    const byPath = { ...files[root] };
    for (const [relativePath, entry] of Object.entries(entries)) {
      if (entry.chunks.length < TRANSCRIPT_MAX_CHUNKS) byPath[relativePath] = entry;
    }
    shipped[root] = byPath;
  }
  return shipped;
};

/** The tail with `appended` (from collectAppends over shippedFiles) added. */
export const extend = (
  files: TranscriptFiles,
  tail: TailFiles,
  appended: TranscriptFiles,
): TailFiles => {
  const next: Record<string, Readonly<Record<string, Entry>>> = { ...tail };
  for (const [root, entries] of Object.entries(appended)) {
    const byPath: Record<string, Entry> = { ...tail[root] };
    for (const [relativePath, entry] of Object.entries(entries)) {
      const previous = byPath[relativePath];
      const continues = previous !== undefined && previous.chunks.length < TRANSCRIPT_MAX_CHUNKS;
      byPath[relativePath] = {
        ...entry,
        baseSize: previous?.baseSize ?? files[root]?.[relativePath]?.size ?? 0,
        chunks: continues ? [...previous.chunks, ...entry.chunks] : entry.chunks,
      };
    }
    next[root] = byPath;
  }
  return next;
};
