// @effect-diagnostics nodeBuiltinImport:off - inspects snapshot copies with raw node:sqlite.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { EventId, ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { canResumeShellFrom } from "../orchestration-v2/ShellStream.ts";
import { BUILD_MIGRATION_HEAD } from "../persistence/databaseMigrationHead.ts";
import { migrationManifest, runMigrations } from "../persistence/Migrations.ts";
import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as DatabaseSnapshot from "./DatabaseSnapshot.ts";

const occurredAt = "2026-09-03T00:00:00.000Z";
const target = ThreadId.make("target");
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const eventRow = (ordinal: number) => ({
  event_id: `event:${ordinal}`,
  aggregate_kind: "thread",
  stream_id: target,
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
});

const insertEvent = (ordinal: number) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_events ${sql.insert(eventRow(ordinal))}`;
  });

const insertMessage = (id: string, text: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO orchestration_v2_projection_messages
        (message_id, thread_id, role, streaming, created_at, updated_at, payload_json)
      VALUES (${id}, ${target}, 'user', 0, ${occurredAt}, ${occurredAt},
        ${JSON.stringify({ text, createdBy: "user" })})
    `;
  });

const messageRowids = (db: NodeSqlite.DatabaseSync) =>
  db
    .prepare("SELECT message_id, rowid FROM orchestration_v2_projection_messages ORDER BY rowid")
    .all();

// The thread-resume predicate at ws.ts:1043-1046.
const threadResumeTakesSnapshot = (after: number, highWater: number, prunedThrough: number) =>
  after > highWater || (after < highWater && after < prunedThrough);

const withTempDir = <A, E, R>(
  use: (directory: string, path: Path.Path) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    return yield* use(yield* fs.makeTempDirectoryScoped({ prefix: "t3-central-snapshot-" }), path);
  });

it.layer(NodeServices.layer)("DatabaseSnapshot", (it) => {
  it.effect("backs up a live database while the server keeps writing", () =>
    withTempDir((directory, path) =>
      Effect.gen(function* () {
        const dbPath = path.join(directory, "statev2.sqlite");
        const copyPath = path.join(directory, "copy.sqlite");
        assert.isTrue(Option.isNone(yield* DatabaseSnapshot.openLiveReader(dbPath)));

        const sourceRowids = yield* Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* insertMessage("a", "first message");
          yield* insertMessage("b", "deleted message");
          yield* insertMessage("c", "the needle message");
          yield* sql`DELETE FROM orchestration_v2_projection_messages WHERE message_id = 'b'`;
          yield* insertMessage("d", "last message");

          const reader = Option.getOrThrow(yield* DatabaseSnapshot.openLiveReader(dbPath));
          const versionBefore = yield* reader.dataVersion;
          const writing = yield* Deferred.make<void>();
          let ordinal = 0;
          const writer = yield* Effect.forkChild(
            Effect.forever(
              insertEvent(++ordinal).pipe(Effect.andThen(Deferred.succeed(writing, undefined))),
            ),
          );
          yield* Deferred.await(writing);
          yield* reader.backupTo(copyPath);
          yield* Fiber.interrupt(writer);
          assert.notEqual(yield* reader.dataVersion, versionBefore);
          assert.isAtLeast(yield* reader.headSequence, 1);
          return yield* sql<{ readonly message_id: string; readonly rowid: number }>`
            SELECT message_id, rowid FROM orchestration_v2_projection_messages ORDER BY rowid
          `;
        }).pipe(Effect.provide(SqlitePersistence.layerFromPath(dbPath)));

        const copy = new NodeSqlite.DatabaseSync(copyPath, { readOnly: true });
        try {
          assert.equal(copy.prepare("PRAGMA journal_mode").get()?.journal_mode, "wal");
          assert.deepEqual(
            messageRowids(copy).map((row) => ({ ...row })),
            sourceRowids.map((row) => ({ ...row })),
          );
          const found = copy
            .prepare(
              `SELECT messages.message_id FROM orchestration_v2_message_search AS search
               INNER JOIN orchestration_v2_projection_messages AS messages ON messages.rowid = search.rowid
               WHERE orchestration_v2_message_search MATCH '"needle"'`,
            )
            .all();
          assert.deepEqual(
            found.map((row) => row.message_id),
            ["c"],
          );
          const copiedMax = Number(
            copy.prepare("SELECT MAX(sequence) AS sequence FROM orchestration_events").get()
              ?.sequence,
          );
          const inspected = yield* DatabaseSnapshot.inspect(copyPath);
          assert.equal(inspected.quickCheck, "ok");
          assert.isAtLeast(copiedMax, 1);
          assert.isAtLeast(inspected.maxSequence, copiedMax);
          assert.equal(inspected.migrations.at(-1)?.id, BUILD_MIGRATION_HEAD);
        } finally {
          copy.close();
        }
      }),
    ),
  );

  it.effect("has no live reader until the database reaches the build's migration head", () =>
    withTempDir((directory, path) =>
      Effect.gen(function* () {
        const dbPath = path.join(directory, "statev2.sqlite");
        yield* runMigrations({ toMigrationInclusive: BUILD_MIGRATION_HEAD - 1 }).pipe(
          Effect.provide(NodeSqliteClient.layer({ filename: dbPath })),
        );
        assert.isTrue(Option.isNone(yield* DatabaseSnapshot.openLiveReader(dbPath)));
      }),
    ),
  );

  it.effect("makes clients drop cursors from the range a restore lost", () =>
    withTempDir((directory, path) =>
      Effect.gen(function* () {
        const dbPath = path.join(directory, "statev2.sqlite");
        const layerDatabase = Layer.fresh(SqlitePersistence.layerFromPath(dbPath));
        yield* Effect.forEach([1, 2, 3, 4, 5], insertEvent, { discard: true }).pipe(
          Effect.provide(layerDatabase),
        );
        const head = 5;
        const floor = head + DatabaseSnapshot.RESTORE_SEQUENCE_JUMP;
        yield* DatabaseSnapshot.applySequenceFloor({ path: dbPath, floor, now: occurredAt });
        assert.isFalse(NodeFS.existsSync(`${dbPath}-wal`));

        yield* Effect.gen(function* () {
          const store = yield* OrchestrationEventStore.OrchestrationEventStore;
          const lost = floor - 10;
          const prunedThrough = yield* store.prunedThroughSequence;
          assert.equal(prunedThrough, floor - 1);
          assert.equal(yield* store.latestApplicationSequence, head);
          assert.isFalse(yield* canResumeShellFrom(store, lost, head));
          assert.isTrue(yield* canResumeShellFrom(store, head, head));
          const beforeHighWater = yield* store.latestAgentSequence(target);
          assert.equal(beforeHighWater, head);
          assert.isTrue(threadResumeTakesSnapshot(lost, beforeHighWater, prunedThrough));

          const [stored] = yield* store.appendAgentEvents({
            events: [
              {
                id: EventId.make("after-restore"),
                type: "provider-session.detached",
                threadId: target,
                occurredAt: DateTime.makeUnsafe(occurredAt),
                payload: {
                  providerSessionId: ProviderSessionId.make("after-restore"),
                  detachedAt: DateTime.makeUnsafe(occurredAt),
                },
              },
            ],
          });
          assert.equal(stored?.sequence, floor);
          const nextHead = yield* store.latestApplicationSequence;
          assert.equal(nextHead, floor);
          assert.isFalse(yield* canResumeShellFrom(store, lost, nextHead));
          const afterHighWater = yield* store.latestAgentSequence(target);
          assert.equal(afterHighWater, floor);
          assert.isTrue(threadResumeTakesSnapshot(lost, afterHighWater, prunedThrough));
        }).pipe(
          Effect.provide(OrchestrationEventStore.layer.pipe(Layer.provideMerge(layerDatabase))),
        );
      }),
    ),
  );

  it.effect("inserts a missing sequence row and skips a missing metadata table", () =>
    withTempDir((directory, path) =>
      Effect.gen(function* () {
        const dbPath = path.join(directory, "bare.sqlite");
        const seed = new NodeSqlite.DatabaseSync(dbPath);
        seed.exec(`
          PRAGMA journal_mode = WAL;
          CREATE TABLE orchestration_events (sequence INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT);
        `);
        seed.close();
        yield* DatabaseSnapshot.applySequenceFloor({ path: dbPath, floor: 42, now: occurredAt });
        assert.isFalse(NodeFS.existsSync(`${dbPath}-wal`));
        const db = new NodeSqlite.DatabaseSync(dbPath, { readOnly: true });
        try {
          assert.deepEqual(
            { ...db.prepare("SELECT name, seq, typeof(seq) AS type FROM sqlite_sequence").get() },
            { name: "orchestration_events", seq: 41, type: "integer" },
          );
          assert.isUndefined(
            db
              .prepare(
                "SELECT 1 FROM sqlite_master WHERE name = 'orchestration_v2_projection_metadata'",
              )
              .get(),
          );
        } finally {
          db.close();
        }
      }),
    ),
  );

  it.effect("publishes a staged database only where none exists", () =>
    withTempDir((directory, path) =>
      Effect.gen(function* () {
        const staged = path.join(directory, "staged.sqlite");
        const dbPath = path.join(directory, "statev2.sqlite");
        NodeFS.writeFileSync(staged, "staged");
        NodeFS.writeFileSync(dbPath, "existing");
        const refused = yield* Effect.flip(DatabaseSnapshot.publish(staged, dbPath));
        assert.equal(refused.stage, "publish");
        assert.equal(NodeFS.readFileSync(dbPath, "utf8"), "existing");
        NodeFS.rmSync(dbPath);
        yield* DatabaseSnapshot.publish(staged, dbPath);
        assert.equal(NodeFS.readFileSync(dbPath, "utf8"), "staged");
      }),
    ),
  );
});

it.effect("refuses migration histories this build cannot run", () =>
  Effect.gen(function* () {
    const build = migrationManifest.map(([id, name]) => ({ id, name }));
    const future = BUILD_MIGRATION_HEAD + 1;
    const rows = [
      { recorded: build, refused: undefined },
      {
        recorded: [...build, { id: future, name: "FromTheFuture" }],
        refused: { reason: "newer", migrations: [`${future}_FromTheFuture`] },
      },
      {
        recorded: build.map((row) => (row.id === 69 ? { ...row, name: "SomethingElse" } : row)),
        refused: {
          reason: "incompatible",
          migrations: ["69_ThreadMessageSearch (recorded: SomethingElse)"],
        },
      },
    ] as const;
    for (const row of rows) {
      const refused = yield* DatabaseSnapshot.checkMigrations("home", row.recorded).pipe(
        Effect.match({
          onFailure: (error) => ({ reason: error.reason, migrations: error.migrations }),
          onSuccess: () => undefined,
        }),
      );
      assert.deepEqual(refused, row.refused);
    }
  }),
);
