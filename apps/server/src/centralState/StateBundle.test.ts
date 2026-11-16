// @effect-diagnostics nodeBuiltinImport:off - seeds T3 homes and inspects restored databases directly.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { BUILD_MIGRATION_HEAD } from "../persistence/databaseMigrationHead.ts";
import { runMigrations } from "../persistence/Migrations.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { CentralStateFencedError, type HeldLease, type LeaseLoss } from "./CentralLease.ts";
import * as CentralObjectStore from "./CentralObjectStore.ts";
import { RESTORE_SEQUENCE_JUMP } from "./DatabaseSnapshot.ts";
import * as Manifest from "./Manifest.ts";
import * as ProviderTranscripts from "./ProviderTranscripts.ts";
import * as SnapshotLoop from "./SnapshotLoop.ts";
import * as StateBundle from "./StateBundle.ts";
import * as TranscriptTail from "./TranscriptTail.ts";

const homeKey = "dev";
const occurredAt = "2026-09-03T00:00:00.000Z";
const attachmentId = "thread-0a0a0a0a-0000-4000-8000-000000000001";
const pendingId = "pending-0a0a0a0a-0000-4000-8000-000000000002";
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const noTranscripts: StateBundle.TranscriptHooks = {
  collect: () => Effect.succeed({}),
  collectAppends: () => Effect.succeed({}),
  stage: () => Effect.succeed({ place: Effect.void }),
};

const SYNCED = [
  "environment-id",
  "settings.json",
  "secrets/server-signing-key.bin",
  "secrets/files/x",
  `attachments/${attachmentId}.png`,
];
const EXCLUDED = [
  "secrets/dpop-proof-y.bin",
  "secrets/z.bin.0a0a0a0a-0000-4000-8000-000000000003.tmp",
  `attachments/${pendingId}.png`,
  "attachments/upload.part",
  // A key Windows would read as a path out of the home.
  "themes/..\\escape.json",
];

const makeLease = (headSequence: number | null = null, remainingMs = 60_000) =>
  Effect.gen(function* () {
    const lost = yield* Deferred.make<LeaseLoss>();
    const lease: HeldLease = {
      holderId: "lease",
      previous:
        headSequence === null
          ? Option.none()
          : Option.some({
              formatVersion: 1,
              homeKey,
              holder: null,
              renewedAt: occurredAt,
              headSequence,
              takeoverRequest: null,
              writeNonce: "previous",
            }),
      lost,
      ensureHeld: Effect.gen(function* () {
        if (!(yield* Deferred.isDone(lost))) return;
        return yield* new CentralStateFencedError({ homeKey, reason: yield* Deferred.await(lost) });
      }),
      remainingMs: Effect.succeed(remainingMs),
      awaitHandoff: Effect.never,
      release: Effect.void,
    };
    return lease;
  });

/** A filesystem store whose downloads, writes and removes are counted. */
const setup = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-state-bundle-" });
  const inner = Context.get(
    yield* Layer.build(CentralObjectStore.layerFilesystem(path.join(directory, "store"))),
    CentralObjectStore.CentralObjectStore,
  );
  const counts = { downloads: 0, removes: [] as Array<string>, writes: [] as Array<string> };
  const store = CentralObjectStore.CentralObjectStore.of({
    read: inner.read,
    download: (key, destination) =>
      Effect.suspend(() => {
        counts.downloads += 1;
        return inner.download(key, destination);
      }),
    write: (key, body, precondition) =>
      Effect.suspend(() => {
        counts.writes.push(key);
        return inner.write(key, body, precondition);
      }),
    remove: (key) =>
      Effect.suspend(() => {
        counts.removes.push(key);
        return inner.remove(key);
      }),
  });
  const root = path.join(directory, "home");
  const outside = path.join(directory, "outside");
  const stateDir = path.join(root, "userdata");
  const context = (holderId: string): StateBundle.BundleContext => ({
    homeKey,
    holder: { holderId, host: "test-host", pid: 1, bootId: null, stateDir },
    buildVersion: "0.0.0-test",
    paths: {
      baseDir: root,
      stateDir,
      dbPath: path.join(stateDir, "statev2.sqlite"),
      attachmentsDir: path.join(stateDir, "attachments"),
      claudeHome: path.join(root, "claude"),
      codexHome: path.join(root, "codex"),
      workDir: path.join(stateDir, "central-state"),
    },
  });
  const file = (relativePath: string) => path.join(stateDir, ...relativePath.split("/"));
  let mtime = 1_800_000_000;
  /** Whole seconds, so every write gets a distinct mtime the scan cache notices. */
  const write = (relativePath: string, contents: string) => {
    NodeFS.mkdirSync(path.dirname(file(relativePath)), { recursive: true });
    NodeFS.writeFileSync(file(relativePath), contents);
    mtime += 1;
    NodeFS.utimesSync(file(relativePath), mtime, mtime);
  };
  const read = (relativePath: string) =>
    NodeFS.existsSync(file(relativePath)) ? NodeFS.readFileSync(file(relativePath), "utf8") : null;
  const run = <A, E>(
    effect: Effect.Effect<
      A,
      E,
      CentralObjectStore.CentralObjectStore | FileSystem.FileSystem | Path.Path
    >,
  ) => effect.pipe(Effect.provideService(CentralObjectStore.CentralObjectStore, store));
  const wipe = () => NodeFS.rmSync(root, { recursive: true, force: true });
  const blobExists = (sha256: string) =>
    run(store.read(Manifest.keys.blob(homeKey, sha256))).pipe(Effect.map(Option.isSome));
  const objectExists = (key: string) => run(store.read(key)).pipe(Effect.map(Option.isSome));
  const head = () => run(Manifest.readHead(homeKey)).pipe(Effect.map(Option.getOrThrow));
  return {
    store,
    counts,
    context,
    file,
    outside,
    write,
    read,
    run,
    wipe,
    blobExists,
    objectExists,
    head,
  };
});
type Fixture = Effect.Success<typeof setup>;

const withDatabase = <A, E>(fixture: Fixture, effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
  Effect.gen(function* () {
    const { dbPath, stateDir } = fixture.context("seed").paths;
    NodeFS.mkdirSync(stateDir, { recursive: true });
    return yield* effect.pipe(Effect.provide(Layer.fresh(SqlitePersistence.layerFromPath(dbPath))));
  });

const insertEvent = (ordinal: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_events ${sql.insert({
      event_id: `event:${ordinal}`,
      aggregate_kind: "thread",
      stream_id: "thread-a",
      stream_version: ordinal,
      event_type: "provider-session.detached",
      occurred_at: occurredAt,
      command_id: null,
      causation_event_id: null,
      correlation_id: null,
      actor_kind: "server",
      payload_json: encodeJson({ providerSessionId: `session:${ordinal}`, detachedAt: occurredAt }),
      metadata_json: "{}",
      application_event_version: 2,
    })}`;
  });

const seedDatabase = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    INSERT INTO orchestration_v2_projection_threads
      (thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
       created_at, updated_at, payload_json)
    VALUES ('thread-a', 'project-a', 'Thread A', 'codex', 'full-access', 'default',
      ${occurredAt}, ${occurredAt}, '{}')
  `;
  yield* sql`
    INSERT INTO orchestration_v2_projection_messages
      (message_id, thread_id, role, streaming, created_at, updated_at, payload_json)
    VALUES ('message-a', 'thread-a', 'user', 0, ${occurredAt}, ${occurredAt},
      ${JSON.stringify({ text: "the needle message", createdBy: "user" })})
  `;
  yield* Effect.forEach([1, 2, 3], insertEvent, { discard: true });
});

const seedHome = (fixture: Fixture) =>
  Effect.gen(function* () {
    yield* withDatabase(fixture, seedDatabase);
    fixture.write("environment-id", "environment-a");
    fixture.write("settings.json", '{"theme":"dark"}');
    fixture.write("secrets/server-signing-key.bin", "signing-key");
    fixture.write("secrets/files/x", "nested secret");
    fixture.write(`attachments/${attachmentId}.png`, "png bytes");
    for (const relativePath of EXCLUDED) fixture.write(relativePath, "excluded");
    // Links out of the home are never followed.
    NodeFS.mkdirSync(fixture.outside, { recursive: true });
    NodeFS.writeFileSync(`${fixture.outside}/key`, "outside the home");
    NodeFS.symlinkSync(fixture.outside, fixture.file("secrets/linked"));
    NodeFS.symlinkSync(`${fixture.outside}/key`, fixture.file("themes/linked.json"));
  });

const prepare = (
  fixture: Fixture,
  holderId: string,
  transcripts = noTranscripts,
  headSequence?: number,
  lease?: HeldLease,
) =>
  Effect.gen(function* () {
    return yield* fixture.run(
      StateBundle.prepareHome({
        ctx: fixture.context(holderId),
        lease: lease ?? (yield* makeLease(headSequence ?? null)),
        transcripts,
      }),
    );
  });

const pushClosed = (
  fixture: Fixture,
  holderId: string,
  committed: StateBundle.Committed,
  cache: StateBundle.PushCache,
  options: {
    readonly dataVersion?: number;
    readonly remainingMs?: number;
    readonly transcripts?: StateBundle.TranscriptHooks;
  } = {},
) =>
  fixture.run(
    StateBundle.push({
      ctx: fixture.context(holderId),
      committed,
      cache,
      source: { _tag: "Closed", dataVersion: options.dataVersion ?? null },
      transcripts: options.transcripts ?? noTranscripts,
      lease: { remainingMs: Effect.succeed(options.remainingMs ?? 60_000) },
    }),
  );

const shipTail = (
  fixture: Fixture,
  holderId: string,
  prepared: { readonly committed: StateBundle.Committed; readonly cache: StateBundle.PushCache },
  transcripts: StateBundle.TranscriptHooks,
) =>
  fixture.run(
    StateBundle.shipTail({
      ctx: fixture.context(holderId),
      committed: prepared.committed,
      cache: prepared.cache,
      transcripts,
      lease: { remainingMs: Effect.succeed(60_000) },
    }),
  );

const storedTail = (cache: StateBundle.PushCache) => {
  const tail = Option.getOrThrow(cache.tail).value;
  assert.isNotNull(tail);
  return tail!;
};

const appendAt = (file: string, contents: string) => {
  NodeFS.mkdirSync(file.slice(0, file.lastIndexOf("/")), { recursive: true });
  NodeFS.appendFileSync(file, contents);
};

const committedOf = (outcome: StateBundle.PushOutcome) => {
  assert.strictEqual(outcome._tag, "Committed");
  if (outcome._tag !== "Committed") throw new Error("expected a commit");
  return outcome.committed;
};

const refsOf = (manifest: Manifest.Manifest) => [
  ...(manifest.database === null ? [] : [manifest.database.blob.sha256]),
  ...Object.values(manifest.files).map((entry) => entry.blob.sha256),
];

const assertRefsPresent = (fixture: Fixture, manifest: Manifest.Manifest) =>
  Effect.forEach(refsOf(manifest), (sha256) =>
    fixture.blobExists(sha256).pipe(Effect.map((exists) => assert.isTrue(exists, sha256))),
  );

const openDatabase = (fixture: Fixture) =>
  new NodeSqlite.DatabaseSync(fixture.context("inspect").paths.dbPath, { readOnly: true });

const manifestWith = (fields: {
  readonly epoch: string;
  readonly generation: number;
  readonly committedBy: string | null;
  readonly database: boolean;
}): Manifest.Manifest => ({
  formatVersion: 1,
  minReaderVersion: 1,
  homeKey,
  epoch: fields.epoch,
  generation: fields.generation,
  holder: { holderId: "holder", host: "host", pid: 1, bootId: null, stateDir: "/state" },
  committedBy: fields.committedBy,
  committedAt: null,
  buildVersion: "0.0.0-test",
  paths: { baseDir: "/base", stateDir: "/state", claudeHome: "/claude" },
  sequenceFloor: 0,
  database: fields.database
    ? { blob: { sha256: "db", size: 1, encoding: "zstd" }, migrations: [], maxSequence: 0 }
    : null,
  files: {},
  claude: { files: {} },
  history: [],
  pinned: [],
  writeNonce: "nonce",
});

it("decideMode", () => {
  const committed = manifestWith({
    epoch: "e1",
    generation: 4,
    committedBy: "a",
    database: true,
  });
  const empty = manifestWith({ epoch: "e1", generation: 0, committedBy: null, database: false });
  const marker = (fields: { epoch: string; generation: number; holderId: string; key?: string }) =>
    Option.some({
      formatVersion: 1 as const,
      homeKey: fields.key ?? homeKey,
      epoch: fields.epoch,
      generation: fields.generation,
      holderId: fields.holderId,
    });
  const rows = [
    {
      name: "a different home key",
      head: Option.some(committed),
      marker: marker({ epoch: "e1", generation: 4, holderId: "a", key: "other" }),
      localDbExists: true,
      expected: { _tag: "KeyMismatch", enrolledKey: "other" },
    },
    {
      name: "no head, no local db",
      head: Option.none(),
      marker: Option.none(),
      localDbExists: false,
      expected: "fresh",
    },
    {
      name: "empty head, no local db",
      head: Option.some(empty),
      marker: Option.none(),
      localDbExists: false,
      expected: "fresh",
    },
    {
      name: "no head, local db",
      head: Option.none(),
      marker: Option.none(),
      localDbExists: true,
      expected: "adopt",
    },
    {
      name: "empty head, local db",
      head: Option.some(empty),
      marker: marker({ epoch: "e1", generation: 0, holderId: "a" }),
      localDbExists: true,
      expected: "adopt",
    },
    {
      name: "same epoch and generation",
      head: Option.some(committed),
      marker: marker({ epoch: "e1", generation: 4, holderId: "b" }),
      localDbExists: true,
      expected: "keep",
    },
    {
      name: "stale generation, but this home committed the head",
      head: Option.some(committed),
      marker: marker({ epoch: "e1", generation: 3, holderId: "a" }),
      localDbExists: true,
      expected: "keep",
    },
    {
      name: "stale generation from another holder",
      head: Option.some(committed),
      marker: marker({ epoch: "e1", generation: 3, holderId: "b" }),
      localDbExists: true,
      expected: "restore",
    },
    {
      name: "epoch mismatch",
      head: Option.some(committed),
      marker: marker({ epoch: "e0", generation: 4, holderId: "a" }),
      localDbExists: true,
      expected: "restore",
    },
    {
      name: "local db without a marker",
      head: Option.some(committed),
      marker: Option.none(),
      localDbExists: true,
      expected: "restore",
    },
    {
      name: "matching marker without a local db",
      head: Option.some(committed),
      marker: marker({ epoch: "e1", generation: 4, holderId: "a" }),
      localDbExists: false,
      expected: "restore",
    },
  ] as const;
  for (const row of rows) {
    const decided = StateBundle.decideMode({
      homeKey,
      head: row.head,
      marker: row.marker,
      localDbExists: row.localDbExists,
    });
    assert.deepStrictEqual(
      decided,
      typeof row.expected === "string" ? { _tag: "Mode", mode: row.expected } : row.expected,
      row.name,
    );
  }
});

it.layer(NodeServices.layer)("StateBundle", (it) => {
  it.effect("restores an adopted home on an empty pod", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      yield* seedHome(fixture);
      const adopted = yield* prepare(fixture, "holder-a");
      assert.strictEqual(adopted.mode, "adopt");
      assert.strictEqual(adopted.committed.manifest.generation, 1);
      assert.isNotNull(adopted.committed.manifest.database);

      fixture.write("settings.json", '{"theme":"light"}');
      const second = committedOf(
        yield* pushClosed(fixture, "holder-a", adopted.committed, adopted.cache),
      );
      assert.strictEqual(second.manifest.generation, 2);
      assert.strictEqual(second.manifest.minReaderVersion, 1);
      assert.deepStrictEqual(Object.keys(second.manifest.files).sort(), [...SYNCED].sort());
      const expected = Object.fromEntries(SYNCED.map((file) => [file, fixture.read(file)]));
      const database = second.manifest.database!;
      // A head from before Codex sync records no codexHome, and has no Codex sessions to check it for.
      const { codexHome: _codexHome, ...legacyPaths } = second.manifest.paths;
      yield* fixture.run(
        fixture.store.write(
          Manifest.keys.head(homeKey),
          {
            _tag: "Bytes",
            bytes: new TextEncoder().encode(
              JSON.stringify({ ...second.manifest, paths: legacyPaths }),
            ),
          },
          { _tag: "None" },
        ),
      );

      fixture.wipe();
      const restored = yield* prepare(fixture, "holder-b");
      assert.strictEqual(restored.mode, "restore");
      const manifest = restored.committed.manifest;

      for (const file of SYNCED) assert.strictEqual(fixture.read(file), expected[file], file);
      for (const file of EXCLUDED) assert.isNull(fixture.read(file), file);
      assert.strictEqual(NodeFS.statSync(fixture.file("secrets")).mode & 0o777, 0o700);
      for (const file of SYNCED.filter((name) => name.startsWith("secrets/"))) {
        assert.strictEqual(NodeFS.statSync(fixture.file(file)).mode & 0o777, 0o600, file);
      }
      for (const file of SYNCED) {
        assert.strictEqual(
          NodeFS.statSync(fixture.file(file)).mtimeMs,
          manifest.files[file]?.mtimeMs,
          file,
        );
      }

      const floor = database.maxSequence + RESTORE_SEQUENCE_JUMP;
      assert.strictEqual(manifest.sequenceFloor, floor);
      assert.notStrictEqual(manifest.epoch, second.manifest.epoch);
      const db = openDatabase(fixture);
      try {
        assert.strictEqual(db.prepare("PRAGMA quick_check").get()?.quick_check, "ok");
        assert.strictEqual(
          db.prepare("SELECT title FROM orchestration_v2_projection_threads").get()?.title,
          "Thread A",
        );
        const found = db
          .prepare(
            `SELECT messages.message_id FROM orchestration_v2_message_search AS search
             INNER JOIN orchestration_v2_projection_messages AS messages ON messages.rowid = search.rowid
             WHERE orchestration_v2_message_search MATCH '"needle"'`,
          )
          .all();
        assert.deepStrictEqual(
          found.map((row) => row.message_id),
          ["message-a"],
        );
        assert.strictEqual(
          db.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'orchestration_events'").get()
            ?.seq,
          floor - 1,
        );
      } finally {
        db.close();
      }
      const marker = yield* Manifest.readMarker(fixture.context("b").paths.stateDir);
      assert.strictEqual(Option.getOrThrow(marker).epoch, manifest.epoch);

      // The floored database is new content; the restored files, with their mtimes, are not.
      const floored = committedOf(
        yield* pushClosed(fixture, "holder-b", restored.committed, restored.cache),
      );
      assert.deepStrictEqual(floored.manifest.files, manifest.files);
      assert.strictEqual(floored.manifest.paths.codexHome, fixture.context("b").paths.codexHome);
      assert.notStrictEqual(floored.manifest.database?.blob.sha256, database.blob.sha256);
      const again = yield* pushClosed(fixture, "holder-b", floored, restored.cache);
      assert.strictEqual(again._tag, "Unchanged");
    }),
  );

  it.effect("restores Claude and Codex sessions, and sets aside a lost lineage's", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const transcripts = yield* ProviderTranscripts.hooks;
      const { claudeHome, codexHome } = fixture.context("a").paths;
      const claudeFile = (relativePath: string) => `${claudeHome}/${relativePath}`;
      const codexFile = (relativePath: string) => `${codexHome}/${relativePath}`;
      const writeAt = (file: string, contents: string) => {
        NodeFS.mkdirSync(file.slice(0, file.lastIndexOf("/")), { recursive: true });
        NodeFS.writeFileSync(file, contents);
      };
      const writeClaude = (relativePath: string, contents: string) =>
        writeAt(claudeFile(relativePath), contents);
      const rollout = "2026/11/16/rollout-2026-11-16T10-00-00-thread-a.jsonl";
      const archived = "archived_sessions/rollout-2026-11-15T10-00-00-thread-b.jsonl";
      yield* seedHome(fixture);
      writeClaude("projects/-work/session-a.jsonl", '{"type":"user"}\n');
      writeClaude("tasks/session-a/1.json", '{"id":"1","subject":"Ship it"}');
      writeAt(codexFile(`sessions/${rollout}`), '{"type":"session_meta"}\n');
      writeAt(codexFile(archived), '{"type":"session_meta"}\n');
      writeAt(codexFile("auth.json"), '{"tokens":"secret"}');
      yield* prepare(fixture, "holder-a", transcripts);
      const saved = (yield* fixture.head()).manifest;
      assert.deepStrictEqual(Object.keys(saved.codex?.sessions ?? {}), [rollout]);
      assert.strictEqual(saved.minReaderVersion, 2);

      fixture.wipe();
      writeClaude("projects/-work/stale.jsonl", '{"type":"lost"}\n');
      writeClaude("tasks/stale/1.json", "{}");
      writeAt(codexFile("sessions/2026/11/16/rollout-stale.jsonl"), '{"type":"lost"}\n');
      const restored = yield* prepare(fixture, "holder-b", transcripts);
      assert.strictEqual(restored.mode, "restore");
      for (const file of [`sessions/${rollout}`, archived]) {
        assert.strictEqual(
          NodeFS.readFileSync(codexFile(file), "utf8"),
          '{"type":"session_meta"}\n',
        );
      }
      assert.isFalse(NodeFS.existsSync(codexFile("auth.json")));
      assert.isFalse(NodeFS.existsSync(codexFile("sessions/2026/11/16/rollout-stale.jsonl")));
      const [codexAside] = NodeFS.readdirSync(codexFile(".central-aside"));
      assert.isTrue(
        NodeFS.existsSync(
          codexFile(`.central-aside/${codexAside}/sessions/2026/11/16/rollout-stale.jsonl`),
        ),
      );
      assert.strictEqual(
        NodeFS.readFileSync(claudeFile("projects/-work/session-a.jsonl"), "utf8"),
        '{"type":"user"}\n',
      );
      assert.strictEqual(
        NodeFS.readFileSync(claudeFile("tasks/session-a/1.json"), "utf8"),
        '{"id":"1","subject":"Ship it"}',
      );
      assert.isFalse(NodeFS.existsSync(claudeFile("projects/-work/stale.jsonl")));
      assert.isFalse(NodeFS.existsSync(claudeFile("tasks/stale")));
      const [aside] = NodeFS.readdirSync(claudeFile(".central-aside"));
      assert.isTrue(
        NodeFS.existsSync(claudeFile(`.central-aside/${aside}/projects/-work/stale.jsonl`)),
      );
      assert.isTrue(NodeFS.existsSync(claudeFile(`.central-aside/${aside}/tasks/stale/1.json`)));
    }),
  );

  it.effect(
    "restores transcript lines shipped after the last push, then folds them into the next",
    () =>
      Effect.gen(function* () {
        const fixture = yield* setup;
        const transcripts = yield* ProviderTranscripts.hooks;
        const { claudeHome, codexHome } = fixture.context("a").paths;
        const session = `${claudeHome}/projects/-work/session-a.jsonl`;
        const rollout = `${codexHome}/sessions/2026/11/16/rollout-a.jsonl`;
        yield* seedHome(fixture);
        appendAt(session, '{"n":1}\n');
        const adopted = yield* prepare(fixture, "holder-a", transcripts);

        appendAt(session, '{"n":2}\n{"n":3');
        appendAt(rollout, '{"type":"session_meta"}\n');
        assert.isTrue(yield* shipTail(fixture, "holder-a", adopted, transcripts));
        const shipped = storedTail(adopted.cache);
        assert.strictEqual(shipped.generation, adopted.committed.manifest.generation);
        assert.deepStrictEqual(
          shipped.claude["projects"]?.["-work/session-a.jsonl"]?.chunks.map((chunk) => chunk.size),
          [8],
        );
        // Nothing complete was appended since, so nothing is uploaded or written.
        fixture.counts.writes.length = 0;
        assert.isFalse(yield* shipTail(fixture, "holder-a", adopted, transcripts));
        assert.deepStrictEqual(fixture.counts.writes, []);

        // A hard kill: no final push.
        fixture.wipe();
        const restored = yield* prepare(fixture, "holder-b", transcripts);
        assert.strictEqual(restored.mode, "restore");
        assert.strictEqual(NodeFS.readFileSync(session, "utf8"), '{"n":1}\n{"n":2}\n');
        assert.strictEqual(NodeFS.readFileSync(rollout, "utf8"), '{"type":"session_meta"}\n');
        // The restorer carries the tail on its own head until its first push.
        const adoptedTail = storedTail(restored.cache);
        assert.strictEqual(adoptedTail.epoch, restored.committed.manifest.epoch);
        assert.strictEqual(adoptedTail.holderId, "holder-b");
        assert.deepStrictEqual(
          [adoptedTail.claude, adoptedTail.codex],
          [shipped.claude, shipped.codex],
        );

        const folded = committedOf(
          yield* pushClosed(fixture, "holder-b", restored.committed, restored.cache, {
            transcripts,
          }),
        );
        assert.strictEqual(folded.manifest.claude.files["-work/session-a.jsonl"]?.size, 16);
        const reset = Option.getOrThrow(yield* fixture.run(TranscriptTail.read(homeKey))).value;
        assert.deepStrictEqual(
          [reset?.generation, reset?.claude, reset?.codex],
          [folded.manifest.generation, {}, {}],
        );
        for (const sha256 of TranscriptTail.chunksOf(shipped)) {
          assert.isFalse(yield* fixture.objectExists(Manifest.keys.tailChunk(homeKey, sha256)));
        }
      }),
  );

  it.effect("ignores and deletes a transcript tail that extends another save", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const transcripts = yield* ProviderTranscripts.hooks;
      const session = `${fixture.context("a").paths.claudeHome}/projects/-work/session-a.jsonl`;
      yield* seedHome(fixture);
      appendAt(session, '{"n":1}\n');
      const adopted = yield* prepare(fixture, "holder-a", transcripts);
      appendAt(session, '{"n":2}\n');
      yield* shipTail(fixture, "holder-a", adopted, transcripts);
      const shipped = storedTail(adopted.cache);
      yield* fixture.run(
        fixture.store.write(
          Manifest.keys.tail(homeKey),
          {
            _tag: "Bytes",
            bytes: new TextEncoder().encode(JSON.stringify({ ...shipped, epoch: "another" })),
          },
          { _tag: "None" },
        ),
      );

      fixture.wipe();
      const restored = yield* prepare(fixture, "holder-b", transcripts);
      assert.strictEqual(NodeFS.readFileSync(session, "utf8"), '{"n":1}\n');
      const emptied = storedTail(restored.cache);
      assert.strictEqual(emptied.epoch, restored.committed.manifest.epoch);
      assert.deepStrictEqual(emptied.claude, {});
      for (const sha256 of TranscriptTail.chunksOf(shipped)) {
        assert.isFalse(yield* fixture.objectExists(Manifest.keys.tailChunk(homeKey, sha256)));
      }
    }),
  );

  it.effect("lets only the head's holder write the transcript tail", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const transcripts = yield* ProviderTranscripts.hooks;
      const session = `${fixture.context("a").paths.claudeHome}/projects/-work/session-a.jsonl`;
      yield* seedHome(fixture);
      appendAt(session, '{"n":1}\n');
      const holderA = yield* prepare(fixture, "holder-a", transcripts);
      appendAt(session, '{"n":2}\n');
      yield* shipTail(fixture, "holder-a", holderA, transcripts);
      const staleTail = storedTail(holderA.cache);

      fixture.wipe();
      const holderB = yield* prepare(fixture, "holder-b", transcripts);
      const adopted = Option.getOrThrow(holderB.cache.tail);

      // The superseded server cannot overwrite the tail of the server that took its home.
      appendAt(session, '{"n":3}\n');
      const lost = yield* Effect.flip(shipTail(fixture, "holder-a", holderA, transcripts));
      assert.strictEqual(lost._tag, "CentralCasLostError");
      const kept = Option.getOrThrow(yield* fixture.run(TranscriptTail.read(homeKey)));
      assert.strictEqual(kept.version, adopted.version);

      // A late write of the superseded server does not stop the holder, which writes over it.
      yield* fixture.run(
        fixture.store.write(
          Manifest.keys.tail(homeKey),
          { _tag: "Bytes", bytes: new TextEncoder().encode(JSON.stringify(staleTail)) },
          { _tag: "None" },
        ),
      );
      assert.isTrue(yield* shipTail(fixture, "holder-b", holderB, transcripts));
      const current = Option.getOrThrow(yield* fixture.run(TranscriptTail.read(homeKey))).value;
      assert.strictEqual(current?.holderId, "holder-b");
    }),
  );

  it.effect("never fences the head once its lease lapsed during boot", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      yield* seedHome(fixture);
      yield* prepare(fixture, "holder-a");
      const before = yield* fixture.head();

      fixture.wipe();
      const lease = yield* makeLease();
      yield* Deferred.succeed(lease.lost, "expired");
      const error = yield* Effect.flip(
        prepare(fixture, "holder-b", noTranscripts, undefined, lease),
      );
      assert.strictEqual(error._tag, "CentralStateFencedError");
      assert.strictEqual((yield* fixture.head()).version, before.version);
    }),
  );

  it.effect("retries a store that fails during boot", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      yield* seedHome(fixture);
      yield* prepare(fixture, "holder-a");
      fixture.wipe();

      const failed = yield* Deferred.make<void>();
      let failures = 1;
      const flaky = CentralObjectStore.CentralObjectStore.of({
        ...fixture.store,
        read: (key) =>
          failures > 0
            ? Effect.suspend(() => {
                failures -= 1;
                return Effect.andThen(
                  Deferred.succeed(failed, undefined),
                  Effect.fail(
                    new CentralObjectStore.CentralObjectStoreError({
                      operation: "read",
                      key,
                      status: 503,
                      cause: new Error("unavailable"),
                    }),
                  ),
                );
              })
            : fixture.store.read(key),
      });
      const lease = yield* makeLease();
      const restoring = yield* Effect.forkChild(
        StateBundle.prepareHome({
          ctx: fixture.context("holder-b"),
          lease,
          transcripts: noTranscripts,
        }).pipe(Effect.provideService(CentralObjectStore.CentralObjectStore, flaky)),
      );
      yield* Deferred.await(failed);
      yield* TestClock.adjust(Duration.seconds(1));
      assert.strictEqual((yield* Fiber.join(restoring)).mode, "restore");
    }),
  );

  it.effect("refuses a head it cannot restore before moving anything aside", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      yield* seedHome(fixture);
      const adopted = yield* prepare(fixture, "holder-a");
      const good = adopted.committed.manifest;
      const database = good.database!;

      fixture.wipe();
      fixture.write("settings.json", "local");
      const markerBefore = {
        formatVersion: 1 as const,
        homeKey,
        epoch: "stale",
        generation: 0,
        holderId: "old",
      };
      const { stateDir, workDir } = fixture.context("b").paths;
      yield* Manifest.writeMarker(stateDir, markerBefore);
      const headKey = Manifest.keys.head(homeKey);
      const dbBlobKey = Manifest.keys.blob(homeKey, database.blob.sha256);
      const setHead = (manifest: object) =>
        fixture.run(
          fixture.store.write(
            headKey,
            { _tag: "Bytes", bytes: new TextEncoder().encode(JSON.stringify(manifest)) },
            { _tag: "None" },
          ),
        );

      const rows = [
        {
          error: "CentralStateMigrationRefusedError",
          prepare: setHead({
            ...good,
            database: {
              ...database,
              migrations: [
                ...database.migrations,
                { id: BUILD_MIGRATION_HEAD + 1, name: "FromTheFuture" },
              ],
            },
          }),
          downloads: 0,
        },
        ...(["baseDir", "stateDir", "claudeHome"] as const).map((field) => ({
          error: "CentralStatePathMismatchError",
          prepare: setHead({ ...good, paths: { ...good.paths, [field]: "/elsewhere" } }),
          downloads: 0,
        })),
        {
          error: "CentralStatePathMismatchError",
          prepare: setHead({
            ...good,
            paths: { ...good.paths, codexHome: "/elsewhere" },
            codex: { sessions: {} },
          }),
          downloads: 0,
        },
        {
          error: "CentralStateUnsupportedManifestError",
          prepare: setHead({ ...good, minReaderVersion: Manifest.MANIFEST_READER_VERSION + 1 }),
          downloads: 0,
        },
        {
          error: "CentralBlobMissingError",
          prepare: Effect.andThen(setHead(good), fixture.run(fixture.store.remove(dbBlobKey))),
          downloads: null,
        },
        {
          error: "CentralBlobIntegrityError",
          prepare: fixture.run(
            fixture.store.write(
              dbBlobKey,
              { _tag: "Bytes", bytes: new TextEncoder().encode("not zstd") },
              { _tag: "None" },
            ),
          ),
          downloads: null,
        },
      ];
      for (const row of rows) {
        yield* row.prepare;
        fixture.counts.downloads = 0;
        const error = yield* Effect.flip(prepare(fixture, "holder-b"));
        assert.strictEqual(error._tag, row.error);
        if (row.downloads !== null) assert.strictEqual(fixture.counts.downloads, row.downloads);
        assert.strictEqual(fixture.read("settings.json"), "local");
        assert.isFalse(NodeFS.existsSync(`${workDir}/aside`));
        assert.deepStrictEqual(
          Option.getOrThrow(yield* Manifest.readMarker(stateDir)),
          markerBefore,
        );
        assert.deepStrictEqual(
          NodeFS.readdirSync(`${workDir}/tmp`).filter((name) => name.startsWith("restore-")),
          [],
        );
      }
    }),
  );

  it.effect("restores again after a crash between placing files and publishing the database", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      yield* seedHome(fixture);
      yield* prepare(fixture, "holder-a");
      const expected = Object.fromEntries(SYNCED.map((file) => [file, fixture.read(file)]));
      const { dbPath, stateDir } = fixture.context("b").paths;

      fixture.wipe();
      const crashing: StateBundle.TranscriptHooks = {
        ...noTranscripts,
        stage: () =>
          Effect.succeed({
            place: Effect.fail(
              PlatformError.systemError({ _tag: "Unknown", module: "test", method: "place" }),
            ),
          }),
      };
      const crashed = yield* Effect.flip(prepare(fixture, "holder-b", crashing));
      assert.strictEqual(crashed._tag, "PlatformError");
      assert.strictEqual(fixture.read("settings.json"), expected["settings.json"]);
      assert.isFalse(NodeFS.existsSync(dbPath));
      assert.isTrue(Option.isNone(yield* Manifest.readMarker(stateDir)));
      const fencedEpoch = (yield* fixture.head()).manifest.epoch;

      const rerun = yield* prepare(fixture, "holder-c");
      assert.strictEqual(rerun.mode, "restore");
      assert.notStrictEqual(rerun.committed.manifest.epoch, fencedEpoch);
      for (const file of SYNCED) assert.strictEqual(fixture.read(file), expected[file], file);
      const db = openDatabase(fixture);
      try {
        assert.strictEqual(db.prepare("PRAGMA quick_check").get()?.quick_check, "ok");
      } finally {
        db.close();
      }
      assert.strictEqual(
        Option.getOrThrow(yield* Manifest.readMarker(stateDir)).epoch,
        rerun.committed.manifest.epoch,
      );
    }),
  );

  it.effect("fences a superseded holder before it can commit or collect garbage", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      yield* seedHome(fixture);
      const adopted = yield* prepare(fixture, "holder-a");
      let committed = adopted.committed;
      for (let generation = 2; generation <= Manifest.RETAIN_GENERATIONS; generation += 1) {
        fixture.write("settings.json", `{"generation":${generation}}`);
        committed = committedOf(yield* pushClosed(fixture, "holder-a", committed, adopted.cache));
      }
      const firstCopy = Manifest.keys.history(homeKey, committed.manifest.epoch, 1);

      fixture.wipe();
      const restored = yield* prepare(fixture, "holder-b");
      assert.strictEqual(restored.mode, "restore");

      fixture.write("settings.json", '{"from":"a"}');
      fixture.counts.removes.length = 0;
      const lost = yield* Effect.flip(pushClosed(fixture, "holder-a", committed, adopted.cache));
      assert.strictEqual(lost._tag, "CentralCasLostError");
      assert.deepStrictEqual(fixture.counts.removes, []);
      const head = yield* fixture.head();
      assert.strictEqual(head.version, restored.committed.version);
      yield* assertRefsPresent(fixture, head.manifest);
      assert.isTrue(yield* fixture.objectExists(firstCopy));
    }),
  );

  it.effect("stops the snapshot loop, with no retry, once the broker fences this generation", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* setup;
        yield* seedHome(fixture);
        const adopted = yield* prepare(fixture, "holder-a");
        fixture.write("settings.json", '{"theme":"light"}');
        let writes = 0;
        const fenced = CentralObjectStore.CentralObjectStore.of({
          ...fixture.store,
          write: (key) =>
            Effect.suspend(() => {
              writes += 1;
              return Effect.fail(
                new CentralObjectStore.CentralObjectFencedError({ key, status: 409 }),
              );
            }),
        });

        // The idle sweep is due at once, so the loop's first poll pushes.
        const loop = yield* SnapshotLoop.start({
          ctx: fixture.context("holder-a"),
          committed: yield* Ref.make(adopted.committed),
          cache: adopted.cache,
          transcripts: noTranscripts,
          policy: { debounceMs: 0, minSpacingMs: 2_000, maxIntervalMs: 0, idleIntervalMs: 0 },
          transcriptInterval: Duration.seconds(2),
          finalPushTimeout: Duration.seconds(20),
          head: yield* Ref.make<number | null>(null),
          isFenced: Effect.succeed(false),
          lease: { remainingMs: Effect.succeed(60_000) },
        }).pipe(Effect.provideService(CentralObjectStore.CentralObjectStore, fenced));
        yield* Deferred.await(loop.fenced);
        yield* loop.stop;
        assert.isFalse(yield* loop.finalPush);
        assert.strictEqual(writes, 2);
      }),
    ),
  );

  it.effect("ships transcript lines between pushes, and writes nothing once fenced", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fixture = yield* setup;
        const transcripts = yield* ProviderTranscripts.hooks;
        const session = `${fixture.context("a").paths.claudeHome}/projects/-work/session-a.jsonl`;
        yield* seedHome(fixture);
        appendAt(session, '{"n":1}\n');
        const adopted = yield* prepare(fixture, "holder-a", transcripts);
        const shipped = yield* Deferred.make<void>();
        const store = CentralObjectStore.CentralObjectStore.of({
          ...fixture.store,
          write: (key, body, precondition) =>
            fixture.store
              .write(key, body, precondition)
              .pipe(
                Effect.tap(() =>
                  key === Manifest.keys.tail(homeKey)
                    ? Deferred.succeed(shipped, undefined)
                    : Effect.void,
                ),
              ),
        });
        const fenced = yield* Ref.make(false);

        appendAt(session, '{"n":2}\n');
        const never = 3_600_000;
        const loop = yield* SnapshotLoop.start({
          ctx: fixture.context("holder-a"),
          committed: yield* Ref.make(adopted.committed),
          cache: adopted.cache,
          transcripts,
          policy: {
            debounceMs: never,
            minSpacingMs: never,
            maxIntervalMs: never,
            idleIntervalMs: never,
          },
          transcriptInterval: Duration.seconds(2),
          finalPushTimeout: Duration.seconds(20),
          head: yield* Ref.make<number | null>(null),
          isFenced: Ref.get(fenced),
          lease: { remainingMs: Effect.succeed(60_000) },
        }).pipe(Effect.provideService(CentralObjectStore.CentralObjectStore, store));
        yield* Deferred.await(shipped);
        const tail = Option.getOrThrow(yield* fixture.run(TranscriptTail.read(homeKey))).value;
        assert.deepStrictEqual(Object.keys(tail?.claude["projects"] ?? {}), [
          "-work/session-a.jsonl",
        ]);

        yield* Ref.set(fenced, true);
        appendAt(session, '{"n":3}\n');
        fixture.counts.writes.length = 0;
        yield* TestClock.adjust(Duration.seconds(10));
        // Stopping waits for a shipment that holds the loop's permit.
        yield* loop.stop;
        assert.deepStrictEqual(fixture.counts.writes, []);
      }),
    ),
  );

  it.effect("reserves a new floor above every earlier restore", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      yield* seedHome(fixture);
      yield* prepare(fixture, "holder-a");

      fixture.wipe();
      const restoredB = yield* prepare(fixture, "holder-b", noTranscripts, 7);
      const floorB = restoredB.committed.manifest.sequenceFloor;
      assert.strictEqual(floorB, 7 + RESTORE_SEQUENCE_JUMP);
      const sequence = yield* withDatabase(
        fixture,
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* insertEvent(100);
          const rows = yield* sql<{ readonly sequence: number }>`
            SELECT MAX(sequence) AS sequence FROM orchestration_events
          `;
          return rows[0]?.sequence;
        }),
      );
      assert.strictEqual(sequence, floorB);

      fixture.wipe();
      const restoredC = yield* prepare(fixture, "holder-c");
      assert.strictEqual(
        restoredC.committed.manifest.sequenceFloor,
        floorB + RESTORE_SEQUENCE_JUMP,
      );
    }),
  );

  it.effect("keeps a home that crashed between its head commit and its marker", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      yield* seedHome(fixture);
      const adopted = yield* prepare(fixture, "holder-a");
      const { stateDir } = fixture.context("a").paths;
      const staleMarker = Option.getOrThrow(yield* Manifest.readMarker(stateDir));
      fixture.write("settings.json", '{"theme":"light"}');
      const second = committedOf(
        yield* pushClosed(fixture, "holder-a", adopted.committed, adopted.cache),
      );
      yield* Manifest.writeMarker(stateDir, staleMarker);

      fixture.counts.downloads = 0;
      const kept = yield* prepare(fixture, "holder-a2");
      assert.strictEqual(kept.mode, "keep");
      assert.strictEqual(fixture.counts.downloads, 0);
      assert.strictEqual(kept.committed.manifest.epoch, second.manifest.epoch);
      assert.strictEqual(kept.committed.manifest.generation, second.manifest.generation);
      assert.deepStrictEqual(Option.getOrThrow(yield* Manifest.readMarker(stateDir)), {
        ...staleMarker,
        generation: second.manifest.generation,
        holderId: "holder-a2",
      });
    }),
  );

  it.effect("adopts its own commit that landed after the push looked failed", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      yield* seedHome(fixture);
      const adopted = yield* prepare(fixture, "holder-a");
      fixture.write("settings.json", '{"theme":"light"}');
      // The loop never saw this commit, so it still pushes from the generation before it.
      const landed = committedOf(
        yield* pushClosed(fixture, "holder-a", adopted.committed, adopted.cache),
      );

      fixture.write("settings.json", '{"theme":"blue"}');
      const outcome = yield* pushClosed(fixture, "holder-a", adopted.committed, adopted.cache);
      assert.strictEqual(outcome._tag, "Adopted");
      if (outcome._tag !== "Adopted") return;
      assert.strictEqual(outcome.committed.version, landed.version);
      // The retried push wrote its own manifest to the landed generation's history copy.
      const copy = yield* fixture.run(
        Manifest.readJson(
          Manifest.keys.history(homeKey, landed.manifest.epoch, landed.manifest.generation),
          Manifest.Manifest,
        ),
      );
      assert.strictEqual(Option.getOrThrow(copy).value.writeNonce, landed.manifest.writeNonce);

      const next = committedOf(
        yield* pushClosed(fixture, "holder-a", outcome.committed, adopted.cache),
      );
      assert.strictEqual(next.manifest.generation, landed.manifest.generation + 1);
    }),
  );

  it.effect("reuses the database in the final push when the reader saw no write since", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      yield* seedHome(fixture);
      const adopted = yield* prepare(fixture, "holder-a");
      const database = adopted.committed.manifest.database!;
      adopted.cache.lastPushedDataVersion = 5;
      yield* withDatabase(fixture, insertEvent(4));
      fixture.write("settings.json", '{"theme":"light"}');

      const reused = committedOf(
        yield* pushClosed(fixture, "holder-a", adopted.committed, adopted.cache, {
          dataVersion: 5,
        }),
      );
      assert.deepStrictEqual(reused.manifest.database, database);
      const copied = committedOf(
        yield* pushClosed(fixture, "holder-a", reused, adopted.cache, { dataVersion: 6 }),
      );
      assert.strictEqual(copied.manifest.database?.maxSequence, database.maxSequence + 1);
    }),
  );

  it.effect("collects only blobs no retained generation references", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      yield* seedHome(fixture);
      fixture.write("settings.json", "content A");
      const adopted = yield* prepare(fixture, "holder-a");
      const settingsBlob = (manifest: Manifest.Manifest) =>
        manifest.files["settings.json"]!.blob.sha256;
      const blobA = settingsBlob(adopted.committed.manifest);
      let committed = adopted.committed;
      const commit = Effect.gen(function* () {
        committed = committedOf(yield* pushClosed(fixture, "holder-a", committed, adopted.cache));
        yield* assertRefsPresent(fixture, committed.manifest);
        return committed;
      });
      const copy = (generation: number) =>
        fixture.objectExists(Manifest.keys.history(homeKey, committed.manifest.epoch, generation));

      fixture.write("settings.json", "content B");
      const blobB = settingsBlob((yield* commit).manifest);
      fixture.write("settings.json", "content A");
      assert.strictEqual(settingsBlob((yield* commit).manifest), blobA);
      for (let generation = 4; generation <= Manifest.RETAIN_GENERATIONS + 1; generation += 1) {
        fixture.write("keybindings.json", `[${generation}]`);
        yield* commit;
      }
      // Generation 1 left the window, but generation 3 still holds content A.
      assert.isFalse(yield* copy(1));
      assert.isTrue(yield* copy(2));
      assert.isTrue(yield* fixture.blobExists(blobA));
      assert.isTrue(yield* fixture.blobExists(blobB));

      fixture.write("keybindings.json", "[12]");
      yield* commit;
      assert.isFalse(yield* copy(2));
      assert.isFalse(yield* fixture.blobExists(blobB));

      // Content that returns after its blob was collected is uploaded again.
      fixture.write("settings.json", "content B");
      assert.strictEqual(settingsBlob((yield* commit).manifest), blobB);
      assert.isTrue(yield* fixture.blobExists(blobB));

      // A holder whose lease may have passed to a taker deletes nothing; what it skips stays.
      const oldest = committed.manifest.history.at(-1)!;
      fixture.write("keybindings.json", "[14]");
      fixture.counts.removes.length = 0;
      yield* pushClosed(fixture, "holder-a", committed, adopted.cache, { remainingMs: 0 });
      assert.deepStrictEqual(fixture.counts.removes, []);
      assert.isTrue(yield* copy(oldest.generation));
    }),
  );

  it.effect("pins the generation before a migration past the history window", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const { dbPath, stateDir } = fixture.context("a").paths;
      NodeFS.mkdirSync(stateDir, { recursive: true });
      yield* runMigrations({ toMigrationInclusive: BUILD_MIGRATION_HEAD - 1 }).pipe(
        Effect.provide(NodeSqliteClient.layer({ filename: dbPath })),
      );
      const adopted = yield* prepare(fixture, "holder-a");
      const oldDatabase = adopted.committed.manifest.database!;
      assert.strictEqual(oldDatabase.migrations.at(-1)?.id, BUILD_MIGRATION_HEAD - 1);

      yield* runMigrations().pipe(Effect.provide(NodeSqliteClient.layer({ filename: dbPath })));
      let committed = committedOf(
        yield* pushClosed(fixture, "holder-a", adopted.committed, adopted.cache),
      );
      const epoch = committed.manifest.epoch;
      assert.deepStrictEqual(committed.manifest.pinned, [
        { epoch, generation: 1, migrationHead: BUILD_MIGRATION_HEAD - 1 },
      ]);
      for (let generation = 3; generation <= Manifest.RETAIN_GENERATIONS + 2; generation += 1) {
        fixture.write("settings.json", `{"generation":${generation}}`);
        committed = committedOf(yield* pushClosed(fixture, "holder-a", committed, adopted.cache));
      }
      assert.isFalse(committed.manifest.history.some((ref) => ref.generation === 1));
      assert.strictEqual(committed.manifest.pinned[0]?.generation, 1);
      assert.isTrue(yield* fixture.objectExists(Manifest.keys.history(homeKey, epoch, 1)));
      assert.isTrue(yield* fixture.blobExists(oldDatabase.blob.sha256));
      assert.isFalse(yield* fixture.objectExists(Manifest.keys.history(homeKey, epoch, 2)));
    }),
  );
});
