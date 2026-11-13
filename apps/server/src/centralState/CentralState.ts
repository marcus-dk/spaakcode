// @effect-diagnostics nodeBuiltinImport:off - holder ids are UUIDs from node:crypto, like the lease and manifest nonces.
import * as NodeCrypto from "node:crypto";

import { SERVER_EXIT_CODE_STATE_DIR_OWNED } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import { FetchHttpClient } from "effect/http";

import packageJson from "../../package.json" with { type: "json" };
import type * as ServerConfig from "../config.ts";
import * as ServerOwnership from "../serverOwnership.ts";
import * as CentralLease from "./CentralLease.ts";
import * as CentralObjectStore from "./CentralObjectStore.ts";
import {
  type CentralStateSettings,
  CentralStateConfigError,
  centralStateConfig,
} from "./CentralStateConfig.ts";
import * as ClaudeProcessEnvironment from "./ClaudeProcessEnvironment.ts";
import * as ClaudeTranscripts from "./ClaudeTranscripts.ts";
import * as SnapshotLoop from "./SnapshotLoop.ts";
import * as StateBundle from "./StateBundle.ts";

export class CentralStateFencedError extends Schema.TaggedError<CentralStateFencedError>()(
  "CentralStateFencedError",
  { homeKey: Schema.String, reason: Schema.Literals(["superseded", "expired"]) },
) {
  override readonly [Runtime.errorExitCode] = SERVER_EXIT_CODE_STATE_DIR_OWNED;

  override get message(): string {
    return this.reason === "superseded"
      ? `Another server took over T3 home ${this.homeKey}, so this server stopped without saving again.`
      : `T3 home ${this.homeKey} could not renew its lease in time, so this server stopped. Changes since the last save may be lost.`;
  }
}

export class CentralStateHandedOffError extends Schema.TaggedError<CentralStateHandedOffError>()(
  "CentralStateHandedOffError",
  { homeKey: Schema.String, toHost: Schema.String, toPid: Schema.Number },
) {
  override readonly [Runtime.errorExitCode] = SERVER_EXIT_CODE_STATE_DIR_OWNED;

  override get message(): string {
    return `T3 home ${this.homeKey} was saved and handed over to ${this.toHost} (pid ${this.toPid}).`;
  }
}

export type CentralStateError =
  | CentralStateConfigError
  | CentralStateFencedError
  | CentralStateHandedOffError
  | CentralLease.CentralStateLeaseHeldError
  | StateBundle.PrepareError
  | ServerOwnership.ServerAlreadyRunningError
  | ServerOwnership.ServerOwnershipError;

const storeLayer = (store: CentralStateSettings["store"]) =>
  store._tag === "Filesystem"
    ? CentralObjectStore.layerFilesystem(store.root)
    : CentralObjectStore.layerHttp(store).pipe(Layer.provide(FetchHttpClient.layer));

const readBootId = Effect.gen(function* () {
  if ((yield* HostProcess.Platform) !== "linux") return null;
  const fileSystem = yield* FileSystem.FileSystem;
  return yield* fileSystem.readFileString("/proc/sys/kernel/random/boot_id").pipe(
    Effect.map((bootId) => bootId.trim() || null),
    Effect.orElseSucceed(() => null),
  );
});

const run = Effect.fn("CentralState.supervise")(function* <A, E, R>(
  config: ServerConfig.ServerConfig["Service"],
  server: Effect.Effect<A, E, R>,
) {
  const loaded = yield* centralStateConfig.pipe(
    Effect.mapError((cause) => new CentralStateConfigError({ cause })),
  );
  if (Option.isNone(loaded)) return yield* server;
  const settings = loaded.value;
  const path = yield* Path.Path;

  const claudeHome = settings.claudeHome ?? path.join(config.baseDir, "claude");
  yield* ClaudeProcessEnvironment.apply(
    ClaudeProcessEnvironment.plan(yield* HostProcess.Environment, claudeHome),
  );

  const ctx: StateBundle.BundleContext = {
    homeKey: settings.homeKey,
    holder: {
      holderId: NodeCrypto.randomUUID(),
      host: yield* HostProcess.Hostname,
      pid: process.pid,
      bootId: yield* readBootId,
      stateDir: config.stateDir,
    },
    buildVersion: packageJson.version,
    paths: {
      baseDir: config.baseDir,
      stateDir: config.stateDir,
      dbPath: config.dbPath,
      attachmentsDir: config.attachmentsDir,
      claudeHome,
      workDir: path.join(config.stateDir, "central-state"),
    },
  };
  yield* Effect.logInfo("Central state is on.").pipe(
    Effect.annotateLogs({
      homeKey: settings.homeKey,
      store: settings.store._tag === "Filesystem" ? "filesystem" : settings.store.baseUrl.host,
    }),
  );

  return yield* Effect.gen(function* () {
    const outerScope = yield* Scope.Scope;
    const claude = yield* ClaudeTranscripts.hooks;
    const head = yield* Ref.make<number | null>(null);

    // The local lock comes first, so a home in use here fails fast and restore never moves files
    // out from under a live process. The server takes the lock again once this scope closes.
    const { lease, prepared } = yield* Effect.scoped(
      Effect.gen(function* () {
        yield* ServerOwnership.holdServerOwnershipLock(config.serverRuntimeStatePath);
        const lease = yield* CentralLease.acquire({
          homeKey: settings.homeKey,
          holder: ctx.holder,
          ttl: settings.leaseTtl,
          takeover: settings.takeover,
          heartbeat: Ref.get(head),
        }).pipe(Scope.provide(outerScope));
        const prepared = yield* StateBundle.prepareHome({ ctx, lease, claude }).pipe(
          Effect.onError(() => lease.release),
        );
        return { lease, prepared };
      }),
    );

    const loop = yield* SnapshotLoop.start({
      ctx,
      committed: yield* Ref.make(prepared.committed),
      cache: prepared.cache,
      claude,
      policy: {
        debounceMs: Duration.toMillis(settings.debounce),
        maxIntervalMs: Duration.toMillis(settings.maxInterval),
        idleIntervalMs: Duration.toMillis(settings.idleInterval),
      },
      finalPushTimeout: settings.finalPushTimeout,
      head,
      isFenced: Deferred.isDone(lease.lost),
    });

    const isFenced = Effect.map(
      Effect.all([Deferred.isDone(lease.lost), Deferred.isDone(loop.fenced)]),
      ([lost, fenced]) => lost || fenced,
    );
    const fencedError = (reason: CentralLease.LeaseLoss) =>
      new CentralStateFencedError({ homeKey: settings.homeKey, reason });
    const awaitStop = Effect.raceFirst(
      Effect.flatMap(Deferred.await(lease.lost), (reason) => Effect.fail(fencedError(reason))),
      Effect.raceFirst(
        Effect.andThen(Deferred.await(loop.fenced), Effect.fail(fencedError("superseded"))),
        Effect.flatMap(Deferred.await(lease.handoffRequested), (to) =>
          Effect.fail(
            new CentralStateHandedOffError({
              homeKey: settings.homeKey,
              toHost: to.host,
              toPid: to.pid,
            }),
          ),
        ),
      ),
    );

    // Runs on every exit, SIGTERM included. The renewal stays live until the lease is released.
    const finalize = Effect.gen(function* () {
      yield* loop.stop;
      if (yield* isFenced) return;
      yield* loop.finalPush;
      if (yield* isFenced) return;
      yield* lease.release;
    });

    return yield* Effect.raceFirst(server, awaitStop).pipe(Effect.onExit(() => finalize));
  }).pipe(Effect.scoped, Effect.provide(storeLayer(settings.store)));
});

/** Identity when T3CODE_CENTRAL_STATE_URL is unset. Data-last, so cli/server.ts adds one pipe line. */
export const supervise =
  (config: ServerConfig.ServerConfig["Service"]) =>
  <A, E, R>(
    server: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | CentralStateError, R | FileSystem.FileSystem | Path.Path> =>
    run(config, server);
