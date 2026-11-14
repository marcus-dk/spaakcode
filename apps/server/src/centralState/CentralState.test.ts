// @effect-diagnostics nodeBuiltinImport:off - seeds and inspects T3 homes directly, and runs lease I/O synchronously.
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as ConfigProvider from "effect/ConfigProvider";
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
import * as Result from "effect/Result";
import * as Scheduler from "effect/Scheduler";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as ServerConfig from "../config.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerOwnership from "../serverOwnership.ts";
import * as CentralObjectStore from "./CentralObjectStore.ts";
import * as CentralState from "./CentralState.ts";
import * as Manifest from "./Manifest.ts";

const homeKey = "dev";
const occurredAt = "2026-09-03T00:00:00.000Z";
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/**
 * TestClock only waits for work the scheduler sees, so a renewal or poll still waiting on file I/O
 * would let the clock run past it. The store's lease reads and writes run synchronously here.
 */
const synchronousFileSystem = (fs: FileSystem.FileSystem) => {
  const attempt = <A>(method: string, path: string, run: () => A) =>
    Effect.try({
      try: run,
      catch: (cause) =>
        PlatformError.systemError({
          _tag: (cause as NodeJS.ErrnoException).code === "ENOENT" ? "NotFound" : "Unknown",
          module: "FileSystem",
          method,
          pathOrDescriptor: path,
          cause,
        }),
    });
  return FileSystem.make({
    ...fs,
    readFile: (path) => attempt("readFile", path, () => new Uint8Array(NodeFS.readFileSync(path))),
    writeFile: (path, data, options) =>
      attempt("writeFile", path, () =>
        NodeFS.writeFileSync(path, data, { flag: options?.flag, mode: options?.mode }),
      ),
    remove: (path, options) =>
      attempt("remove", path, () =>
        NodeFS.rmSync(path, { recursive: options?.recursive, force: options?.force }),
      ),
    makeDirectory: (path, options) =>
      attempt("makeDirectory", path, () => {
        NodeFS.mkdirSync(path, { recursive: options?.recursive, mode: options?.mode });
      }),
  });
};

const insertEvent = (eventId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO orchestration_events ${sql.insert({
      event_id: eventId,
      aggregate_kind: "thread",
      stream_id: `thread:${eventId}`,
      stream_version: 1,
      event_type: "provider-session.detached",
      occurred_at: occurredAt,
      command_id: null,
      causation_event_id: null,
      correlation_id: null,
      actor_kind: "server",
      payload_json: encodeJson({ providerSessionId: eventId, detachedAt: occurredAt }),
      metadata_json: "{}",
      application_event_version: 2,
    })}`;
  });

interface ServerView {
  readonly events: ReadonlyArray<{ readonly sequence: number; readonly eventId: string }>;
  readonly prunedThrough: number | null;
}

interface Hold {
  readonly serving: Deferred.Deferred<void>;
  readonly release: Deferred.Deferred<void>;
  readonly interrupted: Deferred.Deferred<void>;
}

const makeHold = Effect.gen(function* () {
  const hold: Hold = {
    serving: yield* Deferred.make<void>(),
    release: yield* Deferred.make<void>(),
    interrupted: yield* Deferred.make<void>(),
  };
  return hold;
});

/**
 * Stands in for runServer: opens the database, writes the environment id and events, then serves
 * until released. `lock` holds the ownership lock as the real server does; leave it off to play a
 * server on another machine with the same paths.
 */
const fakeServer = (
  config: ServerConfig.ServerConfig["Service"],
  options: { readonly write: ReadonlyArray<string>; readonly hold?: Hold; readonly lock?: boolean },
) =>
  Effect.gen(function* () {
    if (options.lock === true) {
      yield* ServerOwnership.holdServerOwnershipLock(config.serverRuntimeStatePath);
    }
    if (!NodeFS.existsSync(config.environmentIdPath)) {
      NodeFS.writeFileSync(
        config.environmentIdPath,
        `environment:${NodeFS.realpathSync(config.baseDir)}`,
      );
    }
    yield* Effect.forEach(options.write, insertEvent, { discard: true });
    const hold = options.hold;
    if (hold !== undefined) {
      yield* Deferred.succeed(hold.serving, undefined);
      yield* Deferred.await(hold.release).pipe(
        Effect.onInterrupt(() => Deferred.succeed(hold.interrupted, undefined)),
      );
    }
    const sql = yield* SqlClient.SqlClient;
    const events = yield* sql<{ readonly sequence: number; readonly event_id: string }>`
      SELECT sequence, event_id FROM orchestration_events ORDER BY sequence
    `;
    const pruned = yield* sql<{ readonly last_sequence: number }>`
      SELECT last_sequence FROM orchestration_v2_projection_metadata
      WHERE projection_name = 'event-log-pruned-through'
    `;
    const view: ServerView = {
      events: events.map((row) => ({ sequence: Number(row.sequence), eventId: row.event_id })),
      prunedThrough: pruned[0] === undefined ? null : Number(pruned[0].last_sequence),
    };
    return view;
  }).pipe(
    Effect.scoped,
    Effect.provide(Layer.fresh(SqlitePersistence.layerFromPath(config.dbPath))),
  );

const setup = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.realPath(
    yield* fs.makeTempDirectoryScoped({ prefix: "t3-central-state-" }),
  );
  const storeRoot = path.join(directory, "store");
  const storeUrl = NodeURL.pathToFileURL(storeRoot).href;
  const store = Context.get(
    yield* Layer.build(CentralObjectStore.layerFilesystem(storeRoot)),
    CentralObjectStore.CentralObjectStore,
  );
  const fileSystem = synchronousFileSystem(fs);

  const home = (name: string) =>
    Effect.gen(function* () {
      const baseDir = path.join(directory, name);
      return Context.get(
        yield* Layer.build(ServerConfig.layerTest(baseDir, baseDir)),
        ServerConfig.ServerConfig,
      );
    });
  const wipe = (config: ServerConfig.ServerConfig["Service"]) =>
    NodeFS.rmSync(config.baseDir, { recursive: true, force: true });

  const supervise = <A, E>(
    config: ServerConfig.ServerConfig["Service"],
    server: Effect.Effect<A, E, FileSystem.FileSystem | Path.Path>,
    env: Record<string, string> = {},
    options: CentralState.SuperviseOptions<never> = {},
  ) =>
    CentralState.supervise(
      config,
      options,
    )(server).pipe(
      Effect.provide(
        ConfigProvider.layer(
          ConfigProvider.fromEnv({
            env: {
              T3CODE_CENTRAL_STATE_URL: storeUrl,
              T3CODE_CENTRAL_STATE_HOME_KEY: homeKey,
              ...env,
            },
          }),
        ),
      ),
      // Never the test process's own environment.
      Effect.provideService(HostProcess.Environment, {}),
      Effect.provideService(FileSystem.FileSystem, fileSystem),
    );

  const withStore = <A, E>(effect: Effect.Effect<A, E, CentralObjectStore.CentralObjectStore>) =>
    effect.pipe(Effect.provideService(CentralObjectStore.CentralObjectStore, store));
  const lease = () =>
    withStore(Manifest.readJson(Manifest.keys.lease(homeKey), Manifest.LeaseRecord)).pipe(
      Effect.map(Option.getOrThrow),
    );
  const head = () => withStore(Manifest.readHead(homeKey)).pipe(Effect.map(Option.getOrThrow));
  const marker = (config: ServerConfig.ServerConfig["Service"]) =>
    Manifest.readMarker(config.stateDir).pipe(Effect.map(Option.getOrThrow));
  /** Another process takes the lease without asking. */
  const supersede = Effect.gen(function* () {
    const current = yield* lease();
    const holder = current.value.holder;
    assert.isNotNull(holder);
    if (holder === null) return;
    yield* withStore(
      Manifest.casWrite({
        key: Manifest.keys.lease(homeKey),
        schema: Manifest.LeaseRecord,
        value: {
          ...current.value,
          holder: { ...holder, holderId: "intruder", host: "elsewhere" },
          writeNonce: "intruder",
        },
        precondition: { _tag: "IfVersion", version: current.version },
        retryFor: Duration.zero,
      }),
    );
  });

  /** Another process fences the manifest without touching the lease. */
  const fenceManifest = Effect.gen(function* () {
    const current = yield* head();
    return yield* withStore(
      Manifest.casWrite({
        key: Manifest.keys.head(homeKey),
        schema: Manifest.Manifest,
        value: {
          ...current.manifest,
          holder: { ...current.manifest.holder, holderId: "intruder", host: "elsewhere" },
          writeNonce: "intruder",
        },
        precondition: { _tag: "IfVersion", version: current.version },
        retryFor: Duration.zero,
      }),
    );
  });

  return { home, wipe, supervise, lease, head, marker, supersede, fenceManifest };
});
type Fixture = Effect.Success<typeof setup>;

const failureOf = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  Effect.map(Effect.result(Fiber.join(fiber)), (result) => {
    assert.isTrue(Result.isFailure(result), "expected the fiber to fail");
    return (result as Result.Failure<A, E>).failure;
  });

/** Runs a home to completion once, so the next start on it KEEPs a committed head. */
const committedHome = (fixture: Fixture, name: string) =>
  Effect.gen(function* () {
    const config = yield* fixture.home(name);
    yield* fixture.supervise(
      config,
      fakeServer(config, { write: ["event:1", "event:2", "event:3"] }),
    );
    return config;
  });

/**
 * Forks a supervise whose start runs without yielding, so it reaches its first lease poll before
 * the test moves the clock.
 */
const forkStarted = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.forkChild(effect).pipe(Effect.provideService(Scheduler.PreventSchedulerYield, true));

it.layer(NodeServices.layer)("CentralState.supervise", (it) => {
  it.effect("runs the server unchanged when no URL is set", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const config = yield* fixture.home("disabled");
      const view = yield* CentralState.supervise(config)(
        fakeServer(config, { write: ["event:1"] }),
      ).pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ env: {} }))));
      assert.deepStrictEqual(view.events, [{ sequence: 1, eventId: "event:1" }]);
      assert.isFalse(NodeFS.existsSync(`${config.stateDir}/central-state`));
      assert.isFalse(NodeFS.existsSync(`${config.baseDir}/claude`));
    }),
  );

  it.effect("saves on exit and restores the home elsewhere past a sequence floor", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const config = yield* fixture.home("home");
      yield* fixture.supervise(
        config,
        fakeServer(config, { write: ["event:1", "event:2", "event:3"] }),
      );
      const environmentId = NodeFS.readFileSync(config.environmentIdPath, "utf8");
      const saved = yield* fixture.head();
      assert.strictEqual(saved.manifest.database?.maxSequence, 3);
      assert.isNull((yield* fixture.lease()).value.holder);

      // Another pod: the same paths, nothing on disk.
      fixture.wipe(config);
      const restoredConfig = yield* fixture.home("home");
      const view = yield* fixture.supervise(
        restoredConfig,
        fakeServer(restoredConfig, { write: ["event:b"] }),
      );
      const restored = yield* fixture.head();
      const floor = restored.manifest.sequenceFloor;
      assert.strictEqual(floor, 3 + 1_000_000_000);
      assert.deepStrictEqual(view.events, [
        { sequence: 1, eventId: "event:1" },
        { sequence: 2, eventId: "event:2" },
        { sequence: 3, eventId: "event:3" },
        { sequence: floor, eventId: "event:b" },
      ]);
      assert.strictEqual(view.prunedThrough, floor - 1);
      assert.strictEqual(
        NodeFS.readFileSync(restoredConfig.environmentIdPath, "utf8"),
        environmentId,
      );
      assert.notStrictEqual(restored.manifest.epoch, saved.manifest.epoch);
      const marker = yield* fixture.marker(restoredConfig);
      assert.strictEqual(marker.epoch, restored.manifest.epoch);
      assert.strictEqual(marker.generation, restored.manifest.generation);
    }),
  );

  it.effect("hands the home over to a takeover request after a final save", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const env = { T3CODE_CENTRAL_STATE_LEASE_TTL: "4s" };
      const config = yield* fixture.home("home");
      const hold = yield* makeHold;
      const first = yield* fixture
        .supervise(
          config,
          fakeServer(config, { write: ["event:1", "event:2", "event:last"], hold }),
          env,
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(hold.serving);

      const second = yield* forkStarted(
        fixture.supervise(config, fakeServer(config, { write: [] }), {
          ...env,
          T3CODE_CENTRAL_STATE_TAKEOVER: "true",
        }),
      );
      // The holder's first renewal sees the request.
      yield* TestClock.adjust(Duration.seconds(1));
      const handedOff = yield* failureOf(first);
      assert.strictEqual(handedOff._tag, "CentralStateHandedOffError");
      assert.isTrue(yield* Deferred.isDone(hold.interrupted));
      assert.isNull((yield* fixture.lease()).value.holder);

      // The requester runs on another pod: the same paths, nothing on disk.
      fixture.wipe(config);
      yield* TestClock.adjust(Duration.millis(200));
      const view = yield* Fiber.join(second);
      assert.deepStrictEqual(
        view.events.slice(0, 3).map(({ eventId }) => eventId),
        ["event:1", "event:2", "event:last"],
      );
    }),
  );

  it.effect("keeps the lease when the handoff's final save fails", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const env = { T3CODE_CENTRAL_STATE_LEASE_TTL: "4s" };
      const config = yield* fixture.home("home");
      const hold = yield* makeHold;
      const first = yield* fixture
        .supervise(config, fakeServer(config, { write: ["event:1"], hold }), {
          ...env,
          // No time to save.
          T3CODE_CENTRAL_STATE_FINAL_PUSH_TIMEOUT: "0s",
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(hold.serving);

      yield* forkStarted(
        fixture.supervise(config, fakeServer(config, { write: [] }), {
          ...env,
          T3CODE_CENTRAL_STATE_TAKEOVER: "true",
        }),
      );
      yield* TestClock.adjust(Duration.seconds(1));
      const unsaved = yield* failureOf(first);
      assert.strictEqual(unsaved._tag, "CentralStateHandoffUnsavedError");
      assert.isTrue(yield* Deferred.isDone(hold.interrupted));
      // The requester must not restore an older save while it believes nothing was lost.
      assert.isNotNull((yield* fixture.lease()).value.holder);
    }),
  );

  it.effect("fails fast against a live holder without takeover and leaves it serving", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const env = { T3CODE_CENTRAL_STATE_LEASE_TTL: "4s" };
      const config = yield* fixture.home("home");
      const hold = yield* makeHold;
      const first = yield* fixture
        .supervise(config, fakeServer(config, { write: ["event:1"], hold }), env)
        .pipe(Effect.forkChild);
      yield* Deferred.await(hold.serving);

      const otherConfig = yield* fixture.home("other");
      const second = yield* forkStarted(
        fixture.supervise(otherConfig, fakeServer(otherConfig, { write: [] }), env),
      );
      yield* TestClock.adjust(Duration.seconds(1));
      const held = yield* failureOf(second);
      assert.strictEqual(held._tag, "CentralStateLeaseHeldError");
      assert.isFalse(yield* Deferred.isDone(hold.interrupted));
      assert.isFalse(NodeFS.existsSync(otherConfig.dbPath));

      const holder = (yield* fixture.lease()).value.holder;
      assert.strictEqual(holder?.stateDir, config.stateDir);
      yield* Deferred.succeed(hold.release, undefined);
      yield* Fiber.join(first);
    }),
  );

  it.effect("stops a superseded server without a final save or release", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const config = yield* committedHome(fixture, "home");
      const hold = yield* makeHold;
      const server = yield* fixture
        .supervise(config, fakeServer(config, { write: ["event:4"], hold }), {
          T3CODE_CENTRAL_STATE_LEASE_TTL: "4s",
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(hold.serving);
      const before = yield* fixture.head();

      yield* fixture.supersede;
      yield* TestClock.adjust(Duration.seconds(1));
      const fenced = yield* failureOf(server);
      assert.deepStrictEqual(
        { tag: fenced._tag, reason: "reason" in fenced ? fenced.reason : null },
        { tag: "CentralStateFencedError", reason: "superseded" },
      );
      assert.isTrue(yield* Deferred.isDone(hold.interrupted));
      assert.strictEqual((yield* fixture.head()).version, before.version);
      assert.strictEqual((yield* fixture.lease()).value.holder?.holderId, "intruder");
    }),
  );

  it.effect("stops a server whose manifest another server fenced, without a release", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const config = yield* committedHome(fixture, "home");
      const hold = yield* makeHold;
      const server = yield* fixture
        .supervise(config, fakeServer(config, { write: ["event:4"], hold }))
        .pipe(Effect.forkChild);
      yield* Deferred.await(hold.serving);
      const holderBefore = (yield* fixture.lease()).value.holder;
      const fenced = yield* fixture.fenceManifest;

      yield* TestClock.adjust(Duration.seconds(10 + 2));
      const error = yield* failureOf(server);
      assert.deepStrictEqual(
        { tag: error._tag, reason: "reason" in error ? error.reason : null },
        { tag: "CentralStateFencedError", reason: "superseded" },
      );
      assert.isTrue(yield* Deferred.isDone(hold.interrupted));
      assert.strictEqual((yield* fixture.head()).version, fenced.version);
      assert.strictEqual((yield* fixture.lease()).value.holder?.holderId, holderBefore?.holderId);
    }),
  );

  it.effect("runs afterServerStop once the snapshot reader has closed", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const config = yield* fixture.home("home");
      const hold = yield* makeHold;
      let exclusive: boolean | undefined;
      // What Settings' Reclaim needs: no other connection may hold the database open.
      const afterServerStop = Effect.sync(() => {
        const db = new NodeSqlite.DatabaseSync(config.dbPath);
        try {
          db.exec("PRAGMA busy_timeout = 0; PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE;");
          db.exec("ROLLBACK");
          exclusive = true;
        } catch {
          exclusive = false;
        } finally {
          db.close();
        }
      });
      const server = yield* fixture
        .supervise(
          config,
          fakeServer(config, { write: ["event:1"], hold }),
          {},
          { afterServerStop },
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(hold.serving);
      // The loop's next poll opens its reader on the migrated database.
      yield* TestClock.adjust(Duration.seconds(2));
      yield* Deferred.succeed(hold.release, undefined);
      yield* Fiber.join(server);
      assert.isTrue(exclusive);
      assert.isNull((yield* fixture.lease()).value.holder);
    }),
  );

  it.effect("pushes a history write once the debounce has passed", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const config = yield* committedHome(fixture, "home");
      const before = yield* fixture.head();
      const hold = yield* makeHold;
      const server = yield* fixture
        .supervise(config, fakeServer(config, { write: ["event:4"], hold }))
        .pipe(Effect.forkChild);
      yield* Deferred.await(hold.serving);

      yield* TestClock.adjust(Duration.seconds(10 + 2));
      // Superseding the server at its first renewal skips the final save, so only the loop pushed.
      yield* fixture.supersede;
      yield* TestClock.adjust(Duration.seconds(15 - 12));
      yield* failureOf(server);
      const after = yield* fixture.head();
      assert.strictEqual(after.manifest.generation, before.manifest.generation + 1);
      assert.strictEqual(after.manifest.database?.maxSequence, 4);
    }),
  );

  it.effect("refuses a second server on a home in use here before touching the lease", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const config = yield* fixture.home("home");
      const hold = yield* makeHold;
      const first = yield* fixture
        .supervise(config, fakeServer(config, { write: ["event:1"], hold, lock: true }))
        .pipe(Effect.forkChild);
      yield* Deferred.await(hold.serving);
      const leaseBefore = yield* fixture.lease();

      const refused = yield* fixture
        .supervise(config, fakeServer(config, { write: [] }), {
          T3CODE_CENTRAL_STATE_TAKEOVER: "true",
        })
        .pipe(Effect.flip);
      assert.strictEqual(refused._tag, "ServerAlreadyRunningError");
      assert.strictEqual((yield* fixture.lease()).version, leaseBefore.version);

      yield* Deferred.succeed(hold.release, undefined);
      yield* Fiber.join(first);
    }),
  );
});
