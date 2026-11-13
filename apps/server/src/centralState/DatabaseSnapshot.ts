// @effect-diagnostics nodeBuiltinImport:off - snapshots use node:sqlite connections apart from the server's SqlClient.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION } from "../orchestration-v2/ProjectionStore.ts";
import { BUILD_MIGRATION_HEAD } from "../persistence/databaseMigrationHead.ts";
import { skippedV2Migrations } from "../persistence/Migrations.ts";
import { EVENT_LOG_PRUNED_THROUGH } from "../persistence/OrchestrationEventStore.ts";
import { reconciledV2PreviewHistory } from "../persistence/reconcileV2PreviewMigration.ts";
import type { MigrationRow } from "./Manifest.ts";

export const RESTORE_SEQUENCE_JUMP = 1_000_000_000;

// One step copies the whole database; a smaller rate never finishes while the server keeps writing.
const SINGLE_STEP_BACKUP_RATE = 2_147_483_647;

export class CentralStateSnapshotError extends Schema.TaggedError<CentralStateSnapshotError>()(
  "CentralStateSnapshotError",
  {
    stage: Schema.Literals(["open", "backup", "inspect", "floor", "publish"]),
    path: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Central state could not ${this.stage} the database at ${this.path}.`;
  }
}

export class CentralStateMigrationRefusedError extends Schema.TaggedError<CentralStateMigrationRefusedError>()(
  "CentralStateMigrationRefusedError",
  {
    homeKey: Schema.String,
    reason: Schema.Literals(["newer", "incompatible"]),
    migrations: Schema.Array(Schema.String),
    buildHead: Schema.Number,
  },
) {
  override get message(): string {
    return this.reason === "newer"
      ? `T3 home ${this.homeKey} was written by a newer T3 Code (${this.migrations.join(", ")}; this build knows up to ${this.buildHead}). Run that build or newer.`
      : `T3 home ${this.homeKey} has incompatible migration history (${this.migrations.join(", ")}; this build knows up to ${this.buildHead}). Run the build that wrote it.`;
  }
}

export interface LiveReader {
  readonly dataVersion: Effect.Effect<number, CentralStateSnapshotError>;
  readonly headSequence: Effect.Effect<number, CentralStateSnapshotError>;
  readonly backupTo: (outPath: string) => Effect.Effect<void, CentralStateSnapshotError>;
}

const MAX_SEQUENCE_SQL = `
  SELECT MAX(
    COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'orchestration_events'), 0),
    COALESCE((SELECT MAX(sequence) FROM orchestration_events), 0)
  ) AS sequence
`;

const hasTable = (db: NodeSqlite.DatabaseSync, name: string) =>
  db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name) !==
  undefined;

const readMigrations = (db: NodeSqlite.DatabaseSync): Array<typeof MigrationRow.Type> =>
  hasTable(db, "effect_sql_migrations")
    ? db
        .prepare("SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id")
        .all()
        .map((row) => ({ id: Number(row.migration_id), name: String(row.name) }))
    : [];

const readMaxSequence = (db: NodeSqlite.DatabaseSync) =>
  hasTable(db, "orchestration_events")
    ? Number(db.prepare(MAX_SEQUENCE_SQL).get()?.sequence ?? 0)
    : 0;

const closeQuietly = (db: NodeSqlite.DatabaseSync) => {
  try {
    db.close();
  } catch {
    // Closing a handle whose open or query already failed has nothing left to report.
  }
};

const backup = (db: NodeSqlite.DatabaseSync, outPath: string) =>
  NodeSqlite.backup(db, outPath, { rate: SINGLE_STEP_BACKUP_RATE });

/** Opens a read-only DatabaseSync once dbPath exists AND its recorded migration head >= BUILD_MIGRATION_HEAD
 *  (the server finished migrating). None until then. Closed with the scope. */
export const openLiveReader = Effect.fn("DatabaseSnapshot.openLiveReader")(function* (
  dbPath: string,
) {
  // Stat by path only: closing any descriptor on a live SQLite file drops its POSIX locks.
  if (!NodeFS.existsSync(dbPath)) return Option.none<LiveReader>();
  const fail = (stage: "open" | "backup" | "inspect") => (cause: unknown) =>
    new CentralStateSnapshotError({ stage, path: dbPath, cause });
  const opened = yield* Effect.acquireRelease(
    Effect.try({
      try: () => {
        const db = new NodeSqlite.DatabaseSync(dbPath, { readOnly: true });
        try {
          const head = Math.max(0, ...readMigrations(db).map(({ id }) => id));
          if (head >= BUILD_MIGRATION_HEAD) return Option.some(db);
        } catch (cause) {
          db.close();
          throw cause;
        }
        db.close();
        return Option.none<NodeSqlite.DatabaseSync>();
      },
      catch: fail("open"),
    }),
    (db) => Effect.sync(() => Option.map(db, closeQuietly)),
  );
  return Option.map(opened, (db): LiveReader => {
    const dataVersion = db.prepare("PRAGMA data_version");
    const headSequence = db.prepare(MAX_SEQUENCE_SQL);
    return {
      dataVersion: Effect.try({
        try: () => Number(dataVersion.get()?.data_version),
        catch: fail("inspect"),
      }),
      headSequence: Effect.try({
        try: () => Number(headSequence.get()?.sequence ?? 0),
        catch: fail("inspect"),
      }),
      backupTo: (outPath) =>
        Effect.tryPromise({ try: () => backup(db, outPath), catch: fail("backup") }).pipe(
          Effect.asVoid,
          Effect.withSpan("DatabaseSnapshot.backupLive"),
        ),
    };
  });
});

/** Read-write open (recovers a leftover WAL), single-step backup, close. Only when no server holds dbPath. */
export const backupClosed = Effect.fn("DatabaseSnapshot.backupClosed")(function* (
  dbPath: string,
  outPath: string,
) {
  const db = yield* Effect.try({
    try: () => new NodeSqlite.DatabaseSync(dbPath),
    catch: (cause) => new CentralStateSnapshotError({ stage: "open", path: dbPath, cause }),
  });
  yield* Effect.tryPromise({
    try: () => backup(db, outPath),
    catch: (cause) => new CentralStateSnapshotError({ stage: "backup", path: dbPath, cause }),
  }).pipe(Effect.ensuring(Effect.sync(() => closeQuietly(db))));
});

export const inspect = Effect.fn("DatabaseSnapshot.inspect")(function* (path: string) {
  return yield* Effect.try({
    try: () => {
      const db = new NodeSqlite.DatabaseSync(path, { readOnly: true });
      try {
        const quickCheck = db
          .prepare("PRAGMA quick_check")
          .all()
          .map((row) => String(row.quick_check))
          .join("\n");
        return { migrations: readMigrations(db), maxSequence: readMaxSequence(db), quickCheck };
      } finally {
        closeQuietly(db);
      }
    },
    catch: (cause) => new CentralStateSnapshotError({ stage: "inspect", path, cause }),
  });
});

/** BUILD_MIGRATION_HEAD check, then reconciledV2PreviewHistory, then skippedV2Migrations, exactly as servicePreflight.ts:46-71. */
export const checkMigrations = Effect.fn("DatabaseSnapshot.checkMigrations")(function* (
  homeKey: string,
  recorded: ReadonlyArray<typeof MigrationRow.Type>,
) {
  const newer = recorded
    .filter(({ id }) => id > BUILD_MIGRATION_HEAD)
    .map(({ id, name }) => `${id}_${name}`);
  if (newer.length > 0) {
    return yield* new CentralStateMigrationRefusedError({
      homeKey,
      reason: "newer",
      migrations: newer,
      buildHead: BUILD_MIGRATION_HEAD,
    });
  }
  // Startup reconciles V2 preview ids before it checks the history, so check what it will see.
  const reconciled = reconciledV2PreviewHistory(recorded);
  const incompatible =
    reconciled === null ? ["an unexpected V2 preview history"] : skippedV2Migrations(reconciled);
  if (incompatible.length > 0) {
    return yield* new CentralStateMigrationRefusedError({
      homeKey,
      reason: "incompatible",
      migrations: incompatible,
      buildHead: BUILD_MIGRATION_HEAD,
    });
  }
});

export const applySequenceFloor = Effect.fn("DatabaseSnapshot.applySequenceFloor")(
  function* (input: { readonly path: string; readonly floor: number; readonly now: string }) {
    yield* Effect.try({
      try: () => {
        // A JS number binds as REAL; sqlite_sequence has no column affinity to convert it back.
        const f1 = BigInt(input.floor - 1);
        const db = new NodeSqlite.DatabaseSync(input.path);
        try {
          db.exec("BEGIN IMMEDIATE");
          try {
            db.prepare(
              `INSERT INTO sqlite_sequence (name, seq)
             SELECT 'orchestration_events', :f1
             WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'orchestration_events')`,
            ).run({ f1 });
            db.prepare(
              "UPDATE sqlite_sequence SET seq = MAX(seq, :f1) WHERE name = 'orchestration_events'",
            ).run({ f1 });
            if (hasTable(db, "orchestration_v2_projection_metadata")) {
              db.prepare(
                `INSERT INTO orchestration_v2_projection_metadata (projection_name, schema_version, last_sequence, updated_at)
               VALUES (:name, :schemaVersion, :f1, :now)
               ON CONFLICT (projection_name) DO UPDATE SET
                 last_sequence = MAX(last_sequence, excluded.last_sequence),
                 updated_at = excluded.updated_at`,
              ).run({
                name: EVENT_LOG_PRUNED_THROUGH,
                schemaVersion: ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
                f1,
                now: input.now,
              });
            }
            db.exec("COMMIT");
          } catch (cause) {
            if (db.isTransaction) db.exec("ROLLBACK");
            throw cause;
          }
          db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
        } finally {
          db.close();
        }
        // The staged file is published by hard link alone, so every committed page must be in it.
        if (NodeFS.existsSync(`${input.path}-wal`)) {
          throw new Error("The write-ahead log remained after the database closed.");
        }
      },
      catch: (cause) => new CentralStateSnapshotError({ stage: "floor", path: input.path, cause }),
    });
  },
);

/** fs.link(staged, dbPath); fails if dbPath exists (same as initializeV2Database.ts:74-80 but an error, not a no-op). */
export const publish = Effect.fn("DatabaseSnapshot.publish")(function* (
  stagedPath: string,
  dbPath: string,
) {
  const fs = yield* FileSystem.FileSystem;
  yield* fs
    .link(stagedPath, dbPath)
    .pipe(
      Effect.mapError(
        (cause) => new CentralStateSnapshotError({ stage: "publish", path: dbPath, cause }),
      ),
    );
});
