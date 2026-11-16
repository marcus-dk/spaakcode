// @effect-diagnostics nodeBuiltinImport:off - holder ids are UUIDs from node:crypto, like the lease and manifest nonces.
import * as NodeCrypto from "node:crypto";

import { SERVER_EXIT_CODE_STATE_DIR_OWNED } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

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
import * as ProviderProcessEnvironment from "./ProviderProcessEnvironment.ts";
import * as ProviderTranscripts from "./ProviderTranscripts.ts";
import * as SnapshotLoop from "./SnapshotLoop.ts";
import * as StateBundle from "./StateBundle.ts";

export class CentralStateHandedOffError extends Schema.TaggedError<CentralStateHandedOffError>()(
  "CentralStateHandedOffError",
  { homeKey: Schema.String, toHost: Schema.String, toPid: Schema.Number },
) {
  override readonly [Runtime.errorExitCode] = SERVER_EXIT_CODE_STATE_DIR_OWNED;

  override get message(): string {
    return `T3 home ${this.homeKey} was saved and handed over to ${this.toHost} (pid ${this.toPid}).`;
  }
}

/** Not the do-not-restart code: a restart in this pod reclaims the lease and keeps the local home. */
export class CentralStateHandoffUnsavedError extends Schema.TaggedError<CentralStateHandoffUnsavedError>()(
  "CentralStateHandoffUnsavedError",
  { homeKey: Schema.String, toHost: Schema.String, toPid: Schema.Number },
) {
  override get message(): string {
    return `T3 home ${this.homeKey} could not be saved for the handover to ${this.toHost} (pid ${this.toPid}), so this server stopped and kept the lease. Changes since the last save are only on this machine; restart T3 here to keep them.`;
  }
}

export type CentralStateError =
  | CentralStateConfigError
  | CentralLease.CentralStateFencedError
  | CentralStateHandedOffError
  | CentralStateHandoffUnsavedError
  | CentralLease.CentralStateLeaseHeldError
  | StateBundle.PrepareError
  | ServerOwnership.ServerAlreadyRunningError
  | ServerOwnership.ServerOwnershipError;

const storeLayer = ({ store, generation }: CentralStateSettings) =>
  store._tag === "Filesystem"
    ? CentralObjectStore.layerFilesystem(store.root)
    : CentralObjectStore.layerHttpUndici({ ...store, generation });

const readBootId = Effect.gen(function* () {
  if ((yield* HostProcess.Platform) !== "linux") return null;
  const fileSystem = yield* FileSystem.FileSystem;
  return yield* fileSystem.readFileString("/proc/sys/kernel/random/boot_id").pipe(
    Effect.map((bootId) => bootId.trim() || null),
    Effect.orElseSucceed(() => null),
  );
});

export interface SuperviseOptions<R> {
  /**
   * Runs once the server has stopped and the snapshot loop has closed its database reader, before
   * the final push, so it may take the database exclusively and its result is what gets saved.
   */
  readonly afterServerStop?: Effect.Effect<void, never, R>;
}

const run = Effect.fn("CentralState.supervise")(function* <A, E, R, R2>(
  config: ServerConfig.ServerConfig["Service"],
  server: Effect.Effect<A, E, R>,
  options: SuperviseOptions<R2>,
) {
  const afterServerStop = options.afterServerStop ?? Effect.void;
  const loaded = yield* centralStateConfig.pipe(
    Effect.mapError((cause) => new CentralStateConfigError({ cause })),
  );
  if (Option.isNone(loaded)) return yield* server.pipe(Effect.ensuring(afterServerStop));
  const settings = loaded.value;
  const path = yield* Path.Path;

  const homes = {
    claudeHome: settings.claudeHome ?? path.join(config.baseDir, "claude"),
    codexHome: settings.codexHome ?? path.join(config.baseDir, "codex"),
  };
  yield* ProviderProcessEnvironment.apply(
    ProviderProcessEnvironment.plan(yield* HostProcess.Environment, homes),
  );

  const ctx: StateBundle.BundleContext = {
    homeKey: settings.homeKey,
    holder: {
      holderId: NodeCrypto.randomUUID(),
      host: yield* HostProcess.Hostname,
      pid: process.pid,
      bootId: yield* readBootId,
      stateDir: config.stateDir,
      ...(settings.generation === undefined ? {} : { generation: settings.generation }),
    },
    buildVersion: packageJson.version,
    paths: {
      baseDir: config.baseDir,
      stateDir: config.stateDir,
      dbPath: config.dbPath,
      attachmentsDir: config.attachmentsDir,
      ...homes,
      workDir: path.join(config.stateDir, "central-state"),
    },
  };
  yield* Effect.logInfo("Central state is on.").pipe(
    Effect.annotateLogs({
      homeKey: settings.homeKey,
      store: settings.store._tag === "Filesystem" ? "filesystem" : settings.store.baseUrl.host,
      generation: settings.generation ?? null,
    }),
  );

  const fencedError = (reason: CentralLease.LeaseLoss) =>
    new CentralLease.CentralStateFencedError({ homeKey: settings.homeKey, reason });

  return yield* Effect.gen(function* () {
    const outerScope = yield* Scope.Scope;
    const transcripts = yield* ProviderTranscripts.hooks;
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
        const prepared = yield* StateBundle.prepareHome({ ctx, lease, transcripts }).pipe(
          Effect.onError(() => lease.release),
          Effect.catchTags({
            CentralObjectFencedError: () => Effect.fail(fencedError("superseded")),
          }),
        );
        return { lease, prepared };
      }),
    );
    if (settings.publicUrl !== undefined) {
      yield* Effect.logInfo(`Serving T3 home ${settings.homeKey} at ${settings.publicUrl}.`);
    }

    const loop = yield* SnapshotLoop.start({
      ctx,
      committed: yield* Ref.make(prepared.committed),
      cache: prepared.cache,
      transcripts,
      policy: {
        debounceMs: Duration.toMillis(settings.debounce),
        minSpacingMs: Duration.toMillis(settings.minSpacing),
        maxIntervalMs: Duration.toMillis(settings.maxInterval),
        idleIntervalMs: Duration.toMillis(settings.idleInterval),
      },
      transcriptInterval: settings.transcriptInterval,
      finalPushTimeout: settings.finalPushTimeout,
      head,
      isFenced: Deferred.isDone(lease.lost),
      lease,
    });

    const isFenced = Effect.map(
      Effect.all([Deferred.isDone(lease.lost), Deferred.isDone(loop.fenced)]),
      ([lost, fenced]) => lost || fenced,
    );
    const handoff = yield* Ref.make<{ readonly host: string; readonly pid: number } | null>(null);
    const awaitStop = Effect.raceFirst(
      Effect.flatMap(Deferred.await(lease.lost), (reason) => Effect.fail(fencedError(reason))),
      Effect.raceFirst(
        Effect.andThen(Deferred.await(loop.fenced), Effect.fail(fencedError("superseded"))),
        Effect.flatMap(lease.awaitHandoff, (to) =>
          Effect.andThen(
            Ref.set(handoff, to),
            Effect.fail(
              new CentralStateHandedOffError({
                homeKey: settings.homeKey,
                toHost: to.host,
                toPid: to.pid,
              }),
            ),
          ),
        ),
      ),
    );

    // Runs on every exit, SIGTERM included. The renewal stays live until the lease is released,
    // and the lease is released only once the head holds this home: otherwise the next server
    // would restore an older save, while a restart here keeps the local home.
    const finalize = Effect.gen(function* () {
      yield* loop.stop;
      yield* afterServerStop;
      if (yield* isFenced) return false;
      const saved = yield* loop.finalPush;
      if (!saved || (yield* isFenced)) return false;
      yield* lease.release;
      return true;
    });

    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(restore(Effect.raceFirst(server, awaitStop)));
        const saved = yield* finalize;
        const to = yield* Ref.get(handoff);
        if (to !== null && !saved && !(yield* isFenced)) {
          return yield* new CentralStateHandoffUnsavedError({
            homeKey: settings.homeKey,
            toHost: to.host,
            toPid: to.pid,
          });
        }
        return yield* exit;
      }),
    );
  }).pipe(Effect.scoped, Effect.provide(storeLayer(settings)));
});

/**
 * The server, then afterServerStop, when T3CODE_CENTRAL_STATE_URL is unset. Data-last, so
 * cli/server.ts adds one pipe line.
 */
export const supervise =
  <R2 = never>(config: ServerConfig.ServerConfig["Service"], options: SuperviseOptions<R2> = {}) =>
  <A, E, R>(
    server: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | CentralStateError, R | R2 | FileSystem.FileSystem | Path.Path> =>
    run(config, server, options);
