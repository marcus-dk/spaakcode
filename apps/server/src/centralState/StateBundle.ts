// @effect-diagnostics nodeBuiltinImport:off - epochs, nonces and staging names need UUIDs.
import * as NodeCrypto from "node:crypto";

import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import { recordedMigrations } from "../persistence/databaseMigrationHead.ts";
import * as Blobs from "./Blobs.ts";
import type { CentralStateFencedError, HeldLease } from "./CentralLease.ts";
import * as CentralObjectStore from "./CentralObjectStore.ts";
import * as DatabaseSnapshot from "./DatabaseSnapshot.ts";
import {
  type BlobRef,
  type CentralCasLostError,
  type CentralStateCorruptObjectError,
  type CentralStateUnsupportedManifestError,
  type DatabaseRef,
  type Holder,
  type LocalMarker,
  type StateFileEntry,
  type TranscriptEntry,
  Manifest,
  RETAIN_GENERATIONS,
  RETAIN_PINNED,
  casWrite,
  keys,
  minReaderVersionFor,
  readHead,
  readJson,
  readMarker,
  writeMarker,
} from "./Manifest.ts";
import * as ProviderTranscripts from "./ProviderTranscripts.ts";
import * as StateFiles from "./StateFiles.ts";

export interface BundleContext {
  readonly homeKey: string;
  readonly holder: typeof Holder.Type;
  readonly buildVersion: string;
  readonly paths: {
    readonly baseDir: string;
    readonly stateDir: string;
    readonly dbPath: string;
    readonly attachmentsDir: string;
    readonly claudeHome: string;
    readonly codexHome: string;
    /** <stateDir>/central-state */
    readonly workDir: string;
  };
}
export interface Committed {
  readonly manifest: Manifest;
  readonly version: string;
}

/** Files keyed by root directory, then by "/"-separated path under that root. */
export type TranscriptFiles = Readonly<
  Record<string, Readonly<Record<string, typeof TranscriptEntry.Type>>>
>;

/** Implemented by ProviderTranscripts.ts and passed in by CentralState.ts. */
export interface TranscriptHooks {
  readonly collect: (input: {
    readonly home: string;
    readonly roots: ReadonlyArray<string>;
    readonly previous: TranscriptFiles;
    readonly upload: (input: {
      readonly path: string;
      readonly offset: number;
      readonly length: number;
    }) => Effect.Effect<
      typeof BlobRef.Type,
      | CentralObjectStore.CentralObjectStoreError
      | CentralObjectStore.CentralObjectFencedError
      | PlatformError.PlatformError
    >;
  }) => Effect.Effect<
    TranscriptFiles,
    | CentralObjectStore.CentralObjectStoreError
    | CentralObjectStore.CentralObjectFencedError
    | PlatformError.PlatformError
  >;
  readonly stage: (input: {
    readonly home: string;
    readonly files: TranscriptFiles;
    readonly stagingDir: string;
    readonly download: (input: {
      readonly ref: typeof BlobRef.Type;
      readonly destinationPath: string;
      readonly append: boolean;
    }) => Effect.Effect<
      void,
      | Blobs.CentralBlobMissingError
      | Blobs.CentralBlobIntegrityError
      | CentralObjectStore.CentralObjectStoreError
      | CentralObjectStore.CentralObjectFencedError
      | PlatformError.PlatformError
    >;
  }) => Effect.Effect<
    { readonly place: Effect.Effect<void, PlatformError.PlatformError> },
    | Blobs.CentralBlobMissingError
    | Blobs.CentralBlobIntegrityError
    | CentralObjectStore.CentralObjectStoreError
    | CentralObjectStore.CentralObjectFencedError
    | PlatformError.PlatformError
  >;
}

export type Mode = "fresh" | "adopt" | "keep" | "restore";

export class CentralStateHomeKeyMismatchError extends Schema.TaggedError<CentralStateHomeKeyMismatchError>()(
  "CentralStateHomeKeyMismatchError",
  { stateDir: Schema.String, enrolledKey: Schema.String, configuredKey: Schema.String },
) {
  override get message(): string {
    return `${this.stateDir} is enrolled in T3 home ${this.enrolledKey}, not ${this.configuredKey}. Use that key or another T3 home directory.`;
  }
}

export class CentralStatePathMismatchError extends Schema.TaggedError<CentralStatePathMismatchError>()(
  "CentralStatePathMismatchError",
  {
    homeKey: Schema.String,
    field: Schema.Literals(["baseDir", "stateDir", "claudeHome", "codexHome"]),
    recorded: Schema.String,
    current: Schema.String,
  },
) {
  override get message(): string {
    return `T3 home ${this.homeKey} was saved with ${this.field} ${this.recorded}, but this server uses ${this.current}. Run it with the same paths.`;
  }
}

/** Pure; the first matching rule wins. */
export const decideMode = (input: {
  readonly homeKey: string;
  readonly head: Option.Option<Manifest>;
  readonly marker: Option.Option<typeof LocalMarker.Type>;
  readonly localDbExists: boolean;
}):
  | { readonly _tag: "Mode"; readonly mode: Mode }
  | { readonly _tag: "KeyMismatch"; readonly enrolledKey: string } => {
  const marker = Option.getOrUndefined(input.marker);
  if (marker !== undefined && marker.homeKey !== input.homeKey) {
    return { _tag: "KeyMismatch", enrolledKey: marker.homeKey };
  }
  const head = Option.getOrUndefined(input.head);
  if (head === undefined || head.database === null) {
    return { _tag: "Mode", mode: input.localDbExists ? "adopt" : "fresh" };
  }
  // A local database that produced the head (a crash between commit and marker) is a superset of it.
  const keep =
    input.localDbExists &&
    marker !== undefined &&
    marker.epoch === head.epoch &&
    (marker.generation === head.generation || marker.holderId === head.committedBy);
  return { _tag: "Mode", mode: keep ? "keep" : "restore" };
};

type FileCacheEntry = {
  readonly size: number;
  readonly mtimeMs: number;
  readonly entry: typeof StateFileEntry.Type;
};

/** Mutable push state owned by one server process. */
export interface PushCache {
  readonly files: Map<string, FileCacheEntry>;
  /** Blobs referenced by retained manifests: present in the store, so never uploaded again. */
  readonly knownBlobs: Set<string>;
  /** Committed history copies by key; immutable once committed. */
  readonly historyManifests: Map<string, Manifest>;
  lastPushedDataVersion: number | null;
}
export type DbSource =
  | { readonly _tag: "Live"; readonly reader: DatabaseSnapshot.LiveReader }
  | {
      readonly _tag: "Closed";
      /** The live reader's last data_version, read after the server closed its connection. */
      readonly dataVersion?: number | null;
    };
export type PushOutcome =
  | { readonly _tag: "Committed"; readonly committed: Committed }
  /**
   * The head is a commit of this process that it never saw confirmed (a PUT that landed after it
   * looked failed, or a push cut short). It is the base to push from; this push's state is not in it.
   */
  | { readonly _tag: "Adopted"; readonly committed: Committed }
  | { readonly _tag: "Unchanged" }
  | { readonly _tag: "NoDatabaseYet" };

export type PrepareError =
  | CentralStateHomeKeyMismatchError
  | CentralStatePathMismatchError
  | DatabaseSnapshot.CentralStateMigrationRefusedError
  | CentralStateUnsupportedManifestError
  | CentralStateCorruptObjectError
  | Blobs.CentralBlobMissingError
  | Blobs.CentralBlobIntegrityError
  | DatabaseSnapshot.CentralStateSnapshotError
  | CentralObjectStore.CentralObjectStoreError
  | CentralObjectStore.CentralObjectFencedError
  | CentralCasLostError
  | CentralStateFencedError
  | PlatformError.PlatformError;

const MAX_FENCE_ATTEMPTS = 5;
// Outlasts one 30 s request timeout plus its read-back, so a slow PUT still gets a second attempt.
const CAS_RETRY = Duration.seconds(75);
// The store outage boot tolerates, like the lease acquire before it.
const PREPARE_RETRY_FOR_MS = 120_000;
const PREPARE_RETRY_MIN_MS = 1_000;
const PREPARE_RETRY_MAX_MS = 15_000;
const RESTORE_DOWNLOAD_CONCURRENCY = 8;
const ASIDE_KEPT = 2;

const encodeManifestJson = Schema.encodeSync(Schema.fromJsonString(Manifest));
const textEncoder = new TextEncoder();

const refsOf = (manifest: Manifest): ReadonlyArray<string> => [
  ...(manifest.database === null ? [] : [manifest.database.blob.sha256]),
  ...Object.values(manifest.files).map((entry) => entry.blob.sha256),
  ...[
    ...Object.values(ProviderTranscripts.claude.toFiles(manifest.claude)),
    ...Object.values(ProviderTranscripts.codex.toFiles(manifest.codex)),
  ]
    .flatMap((files) => Object.values(files))
    .flatMap((entry) => entry.chunks.map((chunk) => chunk.sha256)),
];

const monotonicMillis = Effect.map(Clock.monotonicTimeNanos, (nanos) => Number(nanos) / 1e6);

const migrationHead = (migrations: ReadonlyArray<{ readonly id: number }>) =>
  Math.max(0, ...migrations.map(({ id }) => id));

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

const writeHead = (
  ctx: BundleContext,
  manifest: Manifest,
  previousVersion: Option.Option<string>,
) =>
  casWrite({
    key: keys.head(ctx.homeKey),
    schema: Manifest,
    value: manifest,
    precondition: Option.match(previousVersion, {
      onNone: () => ({ _tag: "IfAbsent" }) as const,
      onSome: (version) => ({ _tag: "IfVersion", version }) as const,
    }),
    retryFor: CAS_RETRY,
  }).pipe(Effect.map(({ version }): Committed => ({ manifest, version })));

const markerFor = (ctx: BundleContext, manifest: Manifest): typeof LocalMarker.Type => ({
  formatVersion: 1,
  homeKey: ctx.homeKey,
  epoch: manifest.epoch,
  generation: manifest.generation,
  holderId: ctx.holder.holderId,
});

const currentPaths = (ctx: BundleContext) => ({
  baseDir: ctx.paths.baseDir,
  stateDir: ctx.paths.stateDir,
  claudeHome: ctx.paths.claudeHome,
  codexHome: ctx.paths.codexHome,
});

const checkPaths = (ctx: BundleContext, manifest: Manifest) => {
  const current = currentPaths(ctx);
  const fields = ["baseDir", "stateDir", "claudeHome", "codexHome"] as const;
  // Codex continuation keys hold its home path, so it matters once the home has Codex sessions.
  for (const field of manifest.codex === undefined ? fields.slice(0, 3) : fields) {
    const recorded = manifest.paths[field] ?? "";
    if (recorded !== current[field]) {
      return Effect.fail(
        new CentralStatePathMismatchError({
          homeKey: ctx.homeKey,
          field,
          recorded,
          current: current[field],
        }),
      );
    }
  }
  return Effect.void;
};

const localMigrations = (dbPath: string) =>
  Effect.suspend(() => {
    const recorded = recordedMigrations(dbPath);
    return recorded === null
      ? Effect.fail(
          new DatabaseSnapshot.CentralStateSnapshotError({
            stage: "inspect",
            path: dbPath,
            cause: new Error("The database's migration history could not be read."),
          }),
        )
      : Effect.succeed(recorded);
  });

/** File stat cache from the manifest's entries whose size and mtime still match on disk. */
const seedCache = Effect.fn("StateBundle.seedCache")(function* (
  ctx: BundleContext,
  manifest: Manifest,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const files = new Map<string, FileCacheEntry>();
  for (const [relativePath, entry] of Object.entries(manifest.files)) {
    const info = yield* fileSystem
      .stat(path.join(ctx.paths.stateDir, ...relativePath.split("/")))
      .pipe(
        Effect.asSome,
        Effect.catchReason("PlatformError", "NotFound", () => Effect.succeedNone),
      );
    if (Option.isNone(info) || info.value.type !== "File") continue;
    const size = Number(info.value.size);
    const mtimeMs = Option.getOrUndefined(info.value.mtime)?.getTime() ?? 0;
    if (size === entry.blob.size && mtimeMs === entry.mtimeMs) {
      files.set(relativePath, { size, mtimeMs, entry });
    }
  }
  const cache: PushCache = {
    files,
    knownBlobs: new Set(refsOf(manifest)),
    historyManifests: new Map(),
    lastPushedDataVersion: null,
  };
  return cache;
});

const freshManifest = (
  ctx: BundleContext,
  head: Option.Option<{ readonly manifest: Manifest }>,
): Manifest =>
  Option.match(head, {
    onNone: () => ({
      formatVersion: 1,
      minReaderVersion: 1,
      homeKey: ctx.homeKey,
      epoch: NodeCrypto.randomUUID(),
      generation: 0,
      holder: ctx.holder,
      committedBy: null,
      committedAt: null,
      buildVersion: ctx.buildVersion,
      paths: currentPaths(ctx),
      sequenceFloor: 0,
      database: null,
      files: {},
      claude: { files: {} },
      history: [],
      pinned: [],
      writeNonce: NodeCrypto.randomUUID(),
    }),
    onSome: ({ manifest }) => ({
      ...manifest,
      holder: ctx.holder,
      paths: currentPaths(ctx),
      writeNonce: NodeCrypto.randomUUID(),
    }),
  });

const asideName = (now: string, oldEpoch: string) =>
  `${now.replaceAll(":", "-")}-${oldEpoch.slice(0, 8)}`;

const pruneAside = Effect.fn("StateBundle.pruneAside")(function* (directory: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const entries = (yield* fileSystem.readDirectory(directory)).sort();
  yield* Effect.forEach(
    entries.slice(0, Math.max(0, entries.length - ASIDE_KEPT)),
    (entry) => fileSystem.remove(path.join(directory, entry), { recursive: true, force: true }),
    { discard: true },
  );
});

/** Each provider home with the part of `manifest` that belongs in it. */
const providerHomes = (ctx: BundleContext, manifest: Manifest) => [
  {
    home: ctx.paths.claudeHome,
    roots: ProviderTranscripts.claude.roots,
    files: ProviderTranscripts.claude.toFiles(manifest.claude),
  },
  {
    home: ctx.paths.codexHome,
    roots: ProviderTranscripts.codex.roots,
    files: ProviderTranscripts.codex.toFiles(manifest.codex),
  },
];

const restore = Effect.fn("StateBundle.restore")(function* (input: {
  readonly ctx: BundleContext;
  readonly lease: HeldLease;
  readonly transcripts: TranscriptHooks;
  readonly head: Committed;
  readonly database: typeof DatabaseRef.Type;
}) {
  const { ctx, head, database } = input;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const services = yield* Effect.context<
    CentralObjectStore.CentralObjectStore | FileSystem.FileSystem | Path.Path
  >();
  const previous = head.manifest;

  yield* DatabaseSnapshot.checkMigrations(ctx.homeKey, database.migrations);
  const previousHead = Option.getOrUndefined(input.lease.previous)?.headSequence ?? null;
  const floor =
    Math.max(previous.sequenceFloor, database.maxSequence, previousHead ?? 0) +
    DatabaseSnapshot.RESTORE_SEQUENCE_JUMP;
  // The fence reserves the floor before any download, so a superseded holder can no longer commit.
  yield* input.lease.ensureHeld;
  const fenced = yield* writeHead(
    ctx,
    {
      ...previous,
      epoch: NodeCrypto.randomUUID(),
      holder: ctx.holder,
      sequenceFloor: floor,
      writeNonce: NodeCrypto.randomUUID(),
    },
    Option.some(head.version),
  );

  const staging = path.join(ctx.paths.workDir, "tmp", `restore-${NodeCrypto.randomUUID()}`);
  const homes = providerHomes(ctx, previous).map((home) => ({
    ...home,
    stagingDir: path.join(home.home, `.central-restore-${NodeCrypto.randomUUID()}`),
  }));
  const download = (ref: typeof BlobRef.Type, destinationPath: string, append: boolean) =>
    Blobs.download({
      homeKey: ctx.homeKey,
      workDir: ctx.paths.workDir,
      ref,
      destinationPath,
      append,
    });
  const files = Object.fromEntries(
    Object.entries(previous.files).filter(([relativePath]) => StateFiles.isSynced(relativePath)),
  );

  yield* Effect.gen(function* () {
    const stagedState = path.join(staging, "state");
    const stagedDb = path.join(staging, "db.sqlite");
    yield* fileSystem.makeDirectory(stagedState, { recursive: true });
    yield* Effect.forEach(
      Object.entries(files),
      ([relativePath, entry]) =>
        download(entry.blob, path.join(stagedState, ...relativePath.split("/")), false),
      { concurrency: RESTORE_DOWNLOAD_CONCURRENCY, discard: true },
    );
    yield* download(database.blob, stagedDb, false);
    const inspected = yield* DatabaseSnapshot.inspect(stagedDb);
    if (inspected.quickCheck !== "ok") {
      return yield* new DatabaseSnapshot.CentralStateSnapshotError({
        stage: "inspect",
        path: stagedDb,
        cause: new Error(`quick_check reported: ${inspected.quickCheck.slice(0, 200)}`),
      });
    }
    yield* DatabaseSnapshot.checkMigrations(ctx.homeKey, inspected.migrations);
    if (inspected.maxSequence !== database.maxSequence) {
      return yield* new DatabaseSnapshot.CentralStateSnapshotError({
        stage: "inspect",
        path: stagedDb,
        cause: new Error(
          `The copy's sequence head ${inspected.maxSequence} differs from the manifest's ${database.maxSequence}.`,
        ),
      });
    }
    const now = yield* nowIso;
    yield* DatabaseSnapshot.applySequenceFloor({ path: stagedDb, floor, now });
    const places = yield* Effect.forEach(homes, (home) =>
      input.transcripts.stage({
        home: home.home,
        files: home.files,
        stagingDir: home.stagingDir,
        download: ({ ref, destinationPath, append }) =>
          download(ref, destinationPath, append).pipe(Effect.provideContext(services)),
      }),
    );

    // Nothing local moves until everything is staged and verified, and only while the lease holds.
    yield* input.lease.ensureHeld;
    const name = asideName(now, previous.epoch);
    const stateAside = path.join(ctx.paths.workDir, "aside");
    yield* StateFiles.moveAside({
      stateDir: ctx.paths.stateDir,
      asideDir: path.join(stateAside, name),
    });
    yield* pruneAside(stateAside);
    // Transcripts and task lists of a lost lineage would carry turns the restored database lacks.
    for (const home of homes) {
      const providerAside = path.join(home.home, ".central-aside");
      for (const directory of home.roots) {
        const current = path.join(home.home, directory);
        if (!(yield* fileSystem.exists(current))) continue;
        yield* fileSystem.makeDirectory(path.join(providerAside, name), { recursive: true });
        yield* fileSystem.rename(current, path.join(providerAside, name, directory));
      }
      if (yield* fileSystem.exists(providerAside)) yield* pruneAside(providerAside);
    }

    yield* StateFiles.place({ stagingDir: stagedState, stateDir: ctx.paths.stateDir, files });
    for (const { place } of places) yield* place;
    yield* DatabaseSnapshot.publish(stagedDb, ctx.paths.dbPath);
  }).pipe(
    Effect.ensuring(
      Effect.forEach(
        [staging, ...homes.map(({ stagingDir }) => stagingDir)],
        (directory) => fileSystem.remove(directory, { recursive: true, force: true }),
        { discard: true },
      ).pipe(Effect.ignore),
    ),
  );

  yield* writeMarker(ctx.paths.stateDir, markerFor(ctx, fenced.manifest));

  const writtenAt = previous.committedAt === null ? Number.NaN : Date.parse(previous.committedAt);
  const age = Number.isNaN(writtenAt)
    ? "an unknown time"
    : Duration.format(
        Duration.millis(Math.max(0, DateTime.toEpochMillis(yield* DateTime.now) - writtenAt)),
      );
  const lost = previousHead === null ? 0 : previousHead - database.maxSequence;
  yield* Effect.logInfo(
    `Restored T3 home ${ctx.homeKey} generation ${previous.generation} (written ${age} ago by ${previous.holder.host})` +
      (lost > 0 ? `; at least ${lost} committed events after it were lost.` : "."),
  );
  return fenced;
});

const prepareOnce = Effect.fn("StateBundle.prepareOnce")(function* (input: {
  readonly ctx: BundleContext;
  readonly lease: HeldLease;
  readonly transcripts: TranscriptHooks;
}) {
  const { ctx } = input;
  const fileSystem = yield* FileSystem.FileSystem;
  // A holder whose lease lapsed during boot must not fence over the server that took it.
  yield* input.lease.ensureHeld;
  const head = yield* readHead(ctx.homeKey);
  const marker = yield* readMarker(ctx.paths.stateDir);
  const localDbExists = yield* fileSystem.exists(ctx.paths.dbPath);
  const decision = decideMode({
    homeKey: ctx.homeKey,
    head: Option.map(head, ({ manifest }) => manifest),
    marker,
    localDbExists,
  });
  if (decision._tag === "KeyMismatch") {
    return yield* new CentralStateHomeKeyMismatchError({
      stateDir: ctx.paths.stateDir,
      enrolledKey: decision.enrolledKey,
      configuredKey: ctx.homeKey,
    });
  }
  const mode = decision.mode;
  const headVersion = Option.map(head, ({ version }) => version);

  switch (mode) {
    case "fresh": {
      yield* input.lease.ensureHeld;
      const committed = yield* writeHead(ctx, freshManifest(ctx, head), headVersion);
      yield* writeMarker(ctx.paths.stateDir, markerFor(ctx, committed.manifest));
      return { committed, mode, cache: yield* seedCache(ctx, committed.manifest) };
    }
    case "adopt": {
      yield* DatabaseSnapshot.checkMigrations(
        ctx.homeKey,
        yield* localMigrations(ctx.paths.dbPath),
      );
      yield* input.lease.ensureHeld;
      const fenced = yield* writeHead(ctx, freshManifest(ctx, head), headVersion);
      yield* writeMarker(ctx.paths.stateDir, markerFor(ctx, fenced.manifest));
      const cache = yield* seedCache(ctx, fenced.manifest);
      // Serve nothing a later restore could not rebuild under the same environment id.
      const outcome = yield* push({
        ctx,
        committed: fenced,
        cache,
        source: { _tag: "Closed" },
        transcripts: input.transcripts,
        lease: input.lease,
      });
      const committed =
        outcome._tag === "Committed" || outcome._tag === "Adopted" ? outcome.committed : fenced;
      return { committed, mode, cache };
    }
    case "keep":
    case "restore": {
      // decideMode picks these only for a head with a database.
      const current = Option.getOrThrow(head);
      const database = current.manifest.database;
      if (database === null) return yield* Effect.die("keep and restore need a database");
      yield* checkPaths(ctx, current.manifest);
      if (mode === "keep") {
        yield* DatabaseSnapshot.checkMigrations(
          ctx.homeKey,
          yield* localMigrations(ctx.paths.dbPath),
        );
        yield* input.lease.ensureHeld;
        const committed = yield* writeHead(
          ctx,
          { ...current.manifest, holder: ctx.holder, writeNonce: NodeCrypto.randomUUID() },
          headVersion,
        );
        yield* writeMarker(ctx.paths.stateDir, markerFor(ctx, committed.manifest));
        return { committed, mode, cache: yield* seedCache(ctx, committed.manifest) };
      }
      const committed = yield* restore({
        ctx,
        lease: input.lease,
        transcripts: input.transcripts,
        head: current,
        database,
      });
      return { committed, mode, cache: yield* seedCache(ctx, committed.manifest) };
    }
  }
});

export const prepareHome = Effect.fn("StateBundle.prepareHome")(function* (input: {
  readonly ctx: BundleContext;
  readonly lease: HeldLease;
  readonly transcripts: TranscriptHooks;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const tmp = path.join(input.ctx.paths.workDir, "tmp");
  const startedAt = yield* monotonicMillis;
  let fenceAttempts = 0;
  let storeRetries = 0;
  while (true) {
    yield* fileSystem.remove(tmp, { recursive: true, force: true });
    yield* fileSystem.makeDirectory(tmp, { recursive: true });
    // prepareOnce re-reads the head and checks the lease each time, so it can always run again.
    const outcome = yield* Effect.result(prepareOnce(input));
    if (Result.isSuccess(outcome)) return outcome.success;
    const error = outcome.failure;
    // A lost fence write means the head moved since it was read, e.g. the old holder's last commit.
    if (error._tag === "CentralCasLostError") {
      fenceAttempts += 1;
      if (fenceAttempts >= MAX_FENCE_ATTEMPTS) return yield* error;
      continue;
    }
    // Boot outlasts the same store outage the lease acquire does.
    if (error._tag !== "CentralObjectStoreError") return yield* error;
    const delayMs = Math.min(PREPARE_RETRY_MAX_MS, PREPARE_RETRY_MIN_MS * 2 ** storeRetries);
    storeRetries += 1;
    if ((yield* monotonicMillis) - startedAt + delayMs > PREPARE_RETRY_FOR_MS) {
      return yield* error;
    }
    yield* Effect.logWarning("Central state store failed during boot; retrying.").pipe(
      Effect.annotateLogs({ homeKey: input.ctx.homeKey, status: error.status, cause: error }),
    );
    yield* Effect.sleep(Duration.millis(delayMs));
  }
});

const collectGarbage = Effect.fn("StateBundle.collectGarbage")(function* (input: {
  readonly homeKey: string;
  readonly previous: Manifest;
  readonly next: Manifest;
  readonly cache: PushCache;
  readonly lease: Pick<HeldLease, "remainingMs">;
}) {
  const store = yield* CentralObjectStore.CentralObjectStore;
  const { cache } = input;
  // A taker steals no sooner than this holder must stop, so a delete that ends inside the lease
  // can never remove a blob the next holder has just uploaded.
  const remove = (key: string) =>
    Effect.flatMap(input.lease.remainingMs, (remainingMs) =>
      remainingMs <= 0
        ? Effect.fail(new Cause.TimeoutError("The lease may have passed to another server."))
        : store.remove(key).pipe(Effect.timeout(Duration.millis(remainingMs))),
    );
  const historyKey = (ref: { readonly epoch: string; readonly generation: number }) =>
    keys.history(input.homeKey, ref.epoch, ref.generation);
  const retained = new Set([...input.next.history, ...input.next.pinned].map(historyKey));
  const dropped = new Set(
    [...input.previous.history, ...input.previous.pinned]
      .map(historyKey)
      .filter((key) => !retained.has(key)),
  );
  if (dropped.size === 0) return;

  // Untraced: one read per retained generation, inside the GC span.
  const load = Effect.fnUntraced(function* (key: string) {
    const cached = cache.historyManifests.get(key);
    if (cached !== undefined) return Option.some(cached);
    const read = yield* readJson(key, Manifest);
    if (Option.isNone(read)) return Option.none<Manifest>();
    cache.historyManifests.set(key, read.value.value);
    return Option.some(read.value.value);
  });

  const keep = new Set<string>();
  for (const key of retained) {
    const manifest = yield* load(key);
    if (Option.isNone(manifest)) {
      return yield* Effect.logWarning(
        "Skipped central state GC: a retained history copy is missing.",
      ).pipe(Effect.annotateLogs({ key }));
    }
    for (const sha256 of refsOf(manifest.value)) keep.add(sha256);
  }
  // The dedupe set never names a blob GC may delete, so content that returns is uploaded again.
  cache.knownBlobs.clear();
  for (const sha256 of keep) cache.knownBlobs.add(sha256);

  for (const key of dropped) {
    const manifest = yield* load(key);
    if (Option.isSome(manifest)) {
      const orphaned = new Set(refsOf(manifest.value).filter((sha256) => !keep.has(sha256)));
      yield* Effect.forEach(orphaned, (sha256) => remove(keys.blob(input.homeKey, sha256)), {
        concurrency: 4,
        discard: true,
      });
    }
    yield* remove(key);
    cache.historyManifests.delete(key);
  }
});

/** The head is the commit point; a marker it cannot write only costs a restore on a crash. */
const writeMarkerQuietly = (ctx: BundleContext, manifest: Manifest) =>
  writeMarker(ctx.paths.stateDir, markerFor(ctx, manifest)).pipe(
    Effect.catch((error) =>
      Effect.logWarning("Could not write the central state marker.").pipe(
        Effect.annotateLogs({ homeKey: ctx.homeKey, error: error._tag }),
      ),
    ),
  );

/**
 * A head that carries this process's holderId was written by this process, since no other process
 * writes it. CAS lost to such a head means an earlier write of ours landed unconfirmed: adopt it.
 */
const adoptOwnHead = Effect.fn("StateBundle.adoptOwnHead")(function* (
  ctx: BundleContext,
  cache: PushCache,
  lost: CentralCasLostError,
) {
  const head = yield* readHead(ctx.homeKey);
  if (Option.isNone(head) || head.value.manifest.holder.holderId !== ctx.holder.holderId) {
    return yield* lost;
  }
  const { manifest } = head.value;
  const historyKey = keys.history(ctx.homeKey, manifest.epoch, manifest.generation);
  if (manifest.committedBy === ctx.holder.holderId) {
    // This push may have overwritten the head's history copy with its own, uncommitted manifest.
    const store = yield* CentralObjectStore.CentralObjectStore;
    yield* store
      .write(
        historyKey,
        { _tag: "Bytes", bytes: textEncoder.encode(encodeManifestJson(manifest)) },
        { _tag: "None" },
      )
      .pipe(Effect.catchTags({ CentralObjectPreconditionError: (error) => Effect.die(error) }));
    cache.historyManifests.set(historyKey, manifest);
  }
  for (const sha256 of refsOf(manifest)) cache.knownBlobs.add(sha256);
  yield* writeMarkerQuietly(ctx, manifest);
  yield* Effect.logWarning("Adopted an unconfirmed central state commit of this server.").pipe(
    Effect.annotateLogs({ homeKey: ctx.homeKey, generation: manifest.generation }),
  );
  const outcome: PushOutcome = { _tag: "Adopted", committed: head.value };
  return outcome;
});

export const push = Effect.fn("StateBundle.push")(function* (input: {
  readonly ctx: BundleContext;
  readonly committed: Committed;
  readonly cache: PushCache;
  readonly source: DbSource;
  readonly transcripts: TranscriptHooks;
  readonly lease: Pick<HeldLease, "remainingMs">;
}) {
  const { ctx, cache } = input;
  const previous = input.committed.manifest;
  const store = yield* CentralObjectStore.CentralObjectStore;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const services = yield* Effect.context<
    CentralObjectStore.CentralObjectStore | FileSystem.FileSystem | Path.Path
  >();
  const upload = (file: string, range?: { readonly offset: number; readonly length: number }) =>
    Blobs.upload({
      homeKey: ctx.homeKey,
      workDir: ctx.paths.workDir,
      path: file,
      ...range,
      isKnown: (sha256) => cache.knownBlobs.has(sha256),
    }).pipe(Effect.provideContext(services));

  const pushDir = path.join(ctx.paths.workDir, "tmp", `push-${NodeCrypto.randomUUID()}`);
  return yield* Effect.gen(function* () {
    // Read before the backup, so a write that lands during it is pushed next time.
    const dataVersion =
      input.source._tag === "Live"
        ? yield* input.source.reader.dataVersion
        : (input.source.dataVersion ?? null);
    let database: typeof DatabaseRef.Type;
    if (
      dataVersion !== null &&
      dataVersion === cache.lastPushedDataVersion &&
      previous.database !== null
    ) {
      database = previous.database;
    } else {
      if (!(yield* fileSystem.exists(ctx.paths.dbPath))) {
        const outcome: PushOutcome = { _tag: "NoDatabaseYet" };
        return outcome;
      }
      const copy = path.join(pushDir, "db.sqlite");
      yield* fileSystem.makeDirectory(pushDir, { recursive: true });
      if (input.source._tag === "Live") {
        yield* input.source.reader.backupTo(copy);
      } else {
        yield* DatabaseSnapshot.backupClosed(ctx.paths.dbPath, copy);
      }
      const inspected = yield* DatabaseSnapshot.inspect(copy, { quickCheck: false });
      database = {
        blob: yield* upload(copy),
        migrations: inspected.migrations,
        maxSequence: inspected.maxSequence,
      };
    }

    // Captured after the database, so they are a superset of what it references.
    const files = yield* StateFiles.scan({
      stateDir: ctx.paths.stateDir,
      cache: cache.files,
      upload: (file) => upload(file),
    });
    const collect = <Section>(
      home: string,
      layout: ProviderTranscripts.Layout<Section>,
      section: Section,
    ) =>
      input.transcripts
        .collect({
          home,
          roots: layout.roots,
          previous: layout.toFiles(section),
          upload: ({ path: file, offset, length }) => upload(file, { offset, length }),
        })
        .pipe(Effect.map(layout.fromFiles));
    const claude = yield* collect(
      ctx.paths.claudeHome,
      ProviderTranscripts.claude,
      previous.claude,
    );
    const codex = yield* collect(ctx.paths.codexHome, ProviderTranscripts.codex, previous.codex);

    if (
      Equal.equals(database, previous.database) &&
      Equal.equals(files, previous.files) &&
      Equal.equals(claude, previous.claude) &&
      Equal.equals(codex, previous.codex)
    ) {
      if (dataVersion !== null) cache.lastPushedDataVersion = dataVersion;
      const outcome: PushOutcome = { _tag: "Unchanged" };
      return outcome;
    }

    const generation = previous.generation + 1;
    const lastCommit = previous.history[0];
    const pinned =
      previous.database !== null &&
      lastCommit !== undefined &&
      migrationHead(database.migrations) > migrationHead(previous.database.migrations)
        ? [
            {
              epoch: lastCommit.epoch,
              generation: lastCommit.generation,
              migrationHead: migrationHead(previous.database.migrations),
            },
            ...previous.pinned,
          ].slice(0, RETAIN_PINNED)
        : previous.pinned;
    const { codex: _previousCodex, ...base } = previous;
    const draft: Manifest = {
      ...base,
      generation,
      holder: ctx.holder,
      committedBy: ctx.holder.holderId,
      committedAt: yield* nowIso,
      buildVersion: ctx.buildVersion,
      // Restore checks the recorded codexHome, which manifests from before Codex sync lack.
      paths: currentPaths(ctx),
      database,
      files,
      claude,
      ...(codex === undefined ? {} : { codex }),
      history: [{ epoch: previous.epoch, generation }, ...previous.history].slice(
        0,
        RETAIN_GENERATIONS,
      ),
      pinned,
      writeNonce: NodeCrypto.randomUUID(),
    };
    const next: Manifest = { ...draft, minReaderVersion: minReaderVersionFor(draft) };

    const historyKey = keys.history(ctx.homeKey, next.epoch, generation);
    // Only a crashed, uncommitted push of this same epoch can have written this key before.
    yield* store
      .write(
        historyKey,
        { _tag: "Bytes", bytes: textEncoder.encode(encodeManifestJson(next)) },
        {
          _tag: "None",
        },
      )
      .pipe(Effect.catchTags({ CentralObjectPreconditionError: (error) => Effect.die(error) }));
    const written = yield* writeHead(ctx, next, Option.some(input.committed.version)).pipe(
      Effect.map((committed): PushOutcome => ({ _tag: "Committed", committed })),
      Effect.catchTags({ CentralCasLostError: (lost) => adoptOwnHead(ctx, cache, lost) }),
    );
    if (written._tag !== "Committed") return written;
    const committed = written.committed;

    yield* writeMarkerQuietly(ctx, next);
    if (dataVersion !== null) cache.lastPushedDataVersion = dataVersion;
    for (const sha256 of refsOf(next)) cache.knownBlobs.add(sha256);
    cache.historyManifests.set(historyKey, next);

    yield* collectGarbage({ homeKey: ctx.homeKey, previous, next, cache, lease: input.lease }).pipe(
      Effect.catch((error) =>
        Effect.logWarning("Central state GC failed; unreferenced blobs may remain.").pipe(
          Effect.annotateLogs({ error: error._tag }),
        ),
      ),
    );
    const outcome: PushOutcome = { _tag: "Committed", committed };
    return outcome;
  }).pipe(
    Effect.ensuring(
      fileSystem.remove(pushDir, { recursive: true, force: true }).pipe(Effect.ignore),
    ),
  );
});
