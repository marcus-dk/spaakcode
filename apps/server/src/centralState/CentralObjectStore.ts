// @effect-diagnostics nodeBuiltinImport:off - the filesystem backend keeps its CAS index in node:sqlite and must rename inside one transaction.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeSqlite from "node:sqlite";

import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import type * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import {
  HttpClient,
  type HttpClientError,
  HttpClientRequest,
  type HttpClientResponse,
} from "effect/http";

export class CentralObjectPreconditionError extends Schema.TaggedError<CentralObjectPreconditionError>()(
  "CentralObjectPreconditionError",
  { key: Schema.String },
) {
  override get message(): string {
    return `Object ${this.key} changed since it was read.`;
  }
}

export class CentralObjectStoreError extends Schema.TaggedError<CentralObjectStoreError>()(
  "CentralObjectStoreError",
  {
    operation: Schema.Literals(["read", "download", "write", "remove"]),
    key: Schema.String,
    status: Schema.NullOr(Schema.Number),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    const status = this.status === null ? "" : ` (HTTP ${this.status})`;
    return `Central object store could not ${this.operation} ${this.key}${status}.`;
  }
}

/** The broker refused this server's session generation: a newer pod of the session holds the home. */
export class CentralObjectFencedError extends Schema.TaggedError<CentralObjectFencedError>()(
  "CentralObjectFencedError",
  { key: Schema.String, status: Schema.Number },
) {
  override get message(): string {
    return `The central state broker refused ${this.key} (HTTP ${this.status}): a newer generation of this session holds the home.`;
  }
}

type Operation = CentralObjectStoreError["operation"];

export type WriteBody =
  | { readonly _tag: "Bytes"; readonly bytes: Uint8Array }
  | { readonly _tag: "File"; readonly path: string };
export type WritePrecondition =
  | { readonly _tag: "None" }
  | { readonly _tag: "IfAbsent" }
  | { readonly _tag: "IfVersion"; readonly version: string };

export class CentralObjectStore extends Context.Service<
  CentralObjectStore,
  {
    readonly read: (
      key: string,
    ) => Effect.Effect<
      Option.Option<{ readonly bytes: Uint8Array; readonly version: string }>,
      CentralObjectStoreError | CentralObjectFencedError
    >;
    /** Streams the stored bytes to destinationPath; false when absent. */
    readonly download: (
      key: string,
      destinationPath: string,
    ) => Effect.Effect<boolean, CentralObjectStoreError | CentralObjectFencedError>;
    readonly write: (
      key: string,
      body: WriteBody,
      precondition: WritePrecondition,
    ) => Effect.Effect<
      { readonly version: string },
      CentralObjectStoreError | CentralObjectPreconditionError | CentralObjectFencedError
    >;
    /** Absent is success. */
    readonly remove: (
      key: string,
    ) => Effect.Effect<void, CentralObjectStoreError | CentralObjectFencedError>;
  }
>()("t3/centralState/CentralObjectStore") {}

const storeError = (
  operation: Operation,
  key: string,
  cause: unknown,
  status: number | null = null,
) => new CentralObjectStoreError({ operation, key, status, cause });

const isNotFound = (error: CentralObjectStoreError | PlatformError.PlatformError) =>
  error._tag === "PlatformError" && error.reason._tag === "NotFound";

const makeFilesystem = Effect.fn("CentralObjectStore.makeFilesystem")(function* (root: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const dataDir = path.join(root, "data");
  const tmpDir = path.join(root, "tmp");
  yield* fileSystem.makeDirectory(dataDir, { recursive: true }).pipe(Effect.orDie);
  yield* fileSystem.makeDirectory(tmpDir, { recursive: true }).pipe(Effect.orDie);

  const db = yield* Effect.acquireRelease(
    Effect.sync(() => {
      const database = new NodeSqlite.DatabaseSync(path.join(root, "cas.sqlite"));
      database.exec(`
        PRAGMA journal_mode = WAL;
        PRAGMA busy_timeout = 5000;
        CREATE TABLE IF NOT EXISTS objects (key TEXT PRIMARY KEY, version INTEGER NOT NULL, file TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL);
        INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0);
      `);
      return database;
    }),
    (database) => Effect.sync(() => database.close()),
  );

  const selectObject = db.prepare("SELECT version, file FROM objects WHERE key = ?");
  const selectCounter = db.prepare("SELECT value FROM counter WHERE id = 1");
  const upsertObject = db.prepare(
    "INSERT INTO objects (key, version, file) VALUES (?, ?, ?) ON CONFLICT (key) DO UPDATE SET version = excluded.version, file = excluded.file",
  );
  const updateCounter = db.prepare("UPDATE counter SET value = ? WHERE id = 1");
  const deleteObject = db.prepare("DELETE FROM objects WHERE key = ? RETURNING file");

  const lookup = (operation: Operation, key: string) =>
    Effect.try({
      try: () => {
        const row = selectObject.get(key);
        return row === undefined
          ? Option.none<{ readonly version: string; readonly file: string }>()
          : Option.some({ version: String(row.version), file: String(row.file) });
      },
      catch: (cause) => storeError(operation, key, cause),
    });

  // A concurrent writer may unlink the file between the row lookup and the read; one retry sees
  // the replacement row.
  const withCurrentFile = <A>(
    operation: Operation,
    key: string,
    use: (object: {
      readonly version: string;
      readonly file: string;
    }) => Effect.Effect<A, PlatformError.PlatformError>,
  ): Effect.Effect<Option.Option<A>, CentralObjectStoreError> => {
    const attempt = Effect.flatMap(lookup(operation, key), (object) =>
      Option.isNone(object) ? Effect.succeedNone : Effect.asSome(use(object.value)),
    );
    return attempt.pipe(
      Effect.catchIf(isNotFound, () => attempt),
      Effect.mapError((cause) =>
        cause._tag === "CentralObjectStoreError" ? cause : storeError(operation, key, cause),
      ),
    );
  };

  const commit = (
    key: string,
    staged: string,
    precondition: WritePrecondition,
  ):
    | { readonly _tag: "Committed"; readonly version: string; readonly replaced: string | null }
    | {
        readonly _tag: "PreconditionFailed";
      } => {
    db.exec("BEGIN IMMEDIATE");
    try {
      const current = selectObject.get(key);
      const matches =
        precondition._tag === "None" ||
        (precondition._tag === "IfAbsent"
          ? current === undefined
          : current !== undefined && String(current.version) === precondition.version);
      if (!matches) {
        db.exec("ROLLBACK");
        return { _tag: "PreconditionFailed" };
      }
      const next = Number(selectCounter.get()?.value ?? 0) + 1;
      const file = String(next);
      NodeFS.renameSync(staged, path.join(dataDir, file));
      upsertObject.run(key, next, file);
      updateCounter.run(next);
      db.exec("COMMIT");
      return {
        _tag: "Committed",
        version: file,
        replaced: current === undefined ? null : String(current.file),
      };
    } catch (cause) {
      if (db.isTransaction) db.exec("ROLLBACK");
      throw cause;
    }
  };

  return CentralObjectStore.of({
    read: Effect.fn("CentralObjectStore.filesystem.read")(function* (key) {
      return yield* withCurrentFile("read", key, (object) =>
        Effect.map(fileSystem.readFile(path.join(dataDir, object.file)), (bytes) => ({
          bytes,
          version: object.version,
        })),
      );
    }),
    download: Effect.fn("CentralObjectStore.filesystem.download")(function* (key, destinationPath) {
      const partPath = `${destinationPath}.part`;
      const copied = yield* withCurrentFile("download", key, (object) =>
        fileSystem.copyFile(path.join(dataDir, object.file), partPath),
      );
      if (Option.isNone(copied)) return false;
      yield* fileSystem
        .rename(partPath, destinationPath)
        .pipe(Effect.mapError((cause) => storeError("download", key, cause)));
      return true;
    }),
    write: Effect.fn("CentralObjectStore.filesystem.write")(function* (key, body, precondition) {
      const staged = path.join(tmpDir, NodeCrypto.randomUUID());
      const discardStaged = fileSystem.remove(staged, { force: true }).pipe(Effect.ignore);
      yield* (
        body._tag === "Bytes"
          ? fileSystem.writeFile(staged, body.bytes)
          : fileSystem.copyFile(body.path, staged)
      ).pipe(
        Effect.mapError((cause) => storeError("write", key, cause)),
        Effect.tapError(() => discardStaged),
      );
      const outcome = yield* Effect.try({
        try: () => commit(key, staged, precondition),
        catch: (cause) => storeError("write", key, cause),
      }).pipe(Effect.tapError(() => discardStaged));
      if (outcome._tag === "PreconditionFailed") {
        yield* discardStaged;
        return yield* new CentralObjectPreconditionError({ key });
      }
      if (outcome.replaced !== null) {
        yield* fileSystem
          .remove(path.join(dataDir, outcome.replaced), { force: true })
          .pipe(Effect.ignore);
      }
      return { version: outcome.version };
    }),
    remove: Effect.fn("CentralObjectStore.filesystem.remove")(function* (key) {
      const removed = yield* Effect.try({
        try: () => deleteObject.get(key),
        catch: (cause) => storeError("remove", key, cause),
      });
      if (removed === undefined) return;
      yield* fileSystem
        .remove(path.join(dataDir, String(removed.file)), { force: true })
        .pipe(Effect.mapError((cause) => storeError("remove", key, cause)));
    }),
  });
});

/** Single-writer backend for tests and a local or RWO volume; SQLite locking must be reliable. */
export const layerFilesystem = (
  root: string,
): Layer.Layer<CentralObjectStore, never, FileSystem.FileSystem | Path.Path> =>
  Layer.effect(CentralObjectStore, makeFilesystem(root));

const BYTES_TIMEOUT = Duration.seconds(30);

type LowerLevelFailure =
  | HttpClientError.HttpClientError
  | Cause.TimeoutError
  | PlatformError.PlatformError;

const isLowerLevelFailure = (error: { readonly _tag: string }): error is LowerLevelFailure =>
  error._tag === "HttpClientError" ||
  error._tag === "TimeoutError" ||
  error._tag === "PlatformError";

const makeHttp = Effect.fn("CentralObjectStore.makeHttp")(function* (options: {
  readonly baseUrl: URL;
  readonly tokenFile: string | undefined;
  readonly preconditions: "etag" | "gcs";
  readonly generation: number | undefined;
}) {
  const client = yield* HttpClient.HttpClient;
  const fileSystem = yield* FileSystem.FileSystem;
  const versionHeader = options.preconditions === "gcs" ? "x-goog-generation" : "etag";

  // URL resolution would apply "." and ".." segments before the request is sent, so a key that
  // names one could reach outside the base path.
  const objectUrl = (operation: Operation, key: string) => {
    const segments = key.split("/");
    if (segments.some((segment) => segment === "" || segment === "." || segment === "..")) {
      return Effect.fail(
        storeError(operation, key, new Error("Object keys have no empty, . or .. segments.")),
      );
    }
    const url = new URL(options.baseUrl);
    url.pathname = `${url.pathname.replace(/\/+$/, "")}/${segments.map(encodeURIComponent).join("/")}`;
    return Effect.succeed(url);
  };

  const generationHeader: Record<string, string> =
    options.generation === undefined ? {} : { "x-t3-generation": String(options.generation) };

  const requestHeaders = (operation: Operation, key: string) => {
    const tokenFile = options.tokenFile;
    if (tokenFile === undefined) return Effect.succeed(generationHeader);
    return fileSystem.readFileString(tokenFile).pipe(
      Effect.mapError((cause) => storeError(operation, key, cause)),
      Effect.flatMap((contents) => {
        const token = contents.trim();
        return token === ""
          ? Effect.fail(storeError(operation, key, new Error("The token file is empty.")))
          : Effect.succeed({ ...generationHeader, authorization: `Bearer ${token}` });
      }),
    );
  };

  const preconditionHeaders = (precondition: WritePrecondition): Record<string, string> => {
    switch (precondition._tag) {
      case "None":
        return {};
      case "IfAbsent":
        return options.preconditions === "gcs"
          ? { "x-goog-if-generation-match": "0" }
          : { "if-none-match": "*" };
      case "IfVersion":
        return options.preconditions === "gcs"
          ? { "x-goog-if-generation-match": precondition.version }
          : { "if-match": precondition.version };
    }
  };

  // Unread bodies keep the connection busy until collected.
  const drain = (response: HttpClientResponse.HttpClientResponse) =>
    Effect.ignore(response.arrayBuffer);

  const isFenced = (response: HttpClientResponse.HttpClientResponse) =>
    (response.status === 403 || response.status === 409) && response.headers["x-t3-fenced"] === "1";

  const unexpectedStatus = (
    operation: Operation,
    key: string,
    response: HttpClientResponse.HttpClientResponse,
  ) =>
    drain(response).pipe(
      Effect.andThen(
        Effect.fail(
          isFenced(response)
            ? new CentralObjectFencedError({ key, status: response.status })
            : storeError(
                operation,
                key,
                `Unexpected HTTP status ${response.status}.`,
                response.status,
              ),
        ),
      ),
    );

  const versionOf = (
    operation: Operation,
    key: string,
    response: HttpClientResponse.HttpClientResponse,
  ) => {
    const version = response.headers[versionHeader];
    return version === undefined || version === ""
      ? Effect.fail(storeError(operation, key, `Missing ${versionHeader} header.`, response.status))
      : Effect.succeed(version);
  };

  // Lower-level failures become transport errors with no status; domain errors pass through.
  const transportFailure =
    (operation: Operation, key: string) =>
    <
      A,
      E extends CentralObjectStoreError | CentralObjectPreconditionError | CentralObjectFencedError,
      R,
    >(
      effect: Effect.Effect<A, E | LowerLevelFailure, R>,
    ) =>
      effect.pipe(
        Effect.mapError((error) =>
          isLowerLevelFailure(error) ? storeError(operation, key, error) : error,
        ),
      );

  return CentralObjectStore.of({
    read: Effect.fn("CentralObjectStore.http.read")(
      function* (key) {
        const url = yield* objectUrl("read", key);
        const headers = yield* requestHeaders("read", key);
        const response = yield* client.execute(HttpClientRequest.get(url, { headers }));
        if (response.status === 404) {
          yield* drain(response);
          return Option.none();
        }
        if (response.status !== 200) return yield* unexpectedStatus("read", key, response);
        const version = yield* versionOf("read", key, response);
        const bytes = new Uint8Array(yield* response.arrayBuffer);
        return Option.some({ bytes, version });
      },
      (effect, key) => effect.pipe(Effect.timeout(BYTES_TIMEOUT), transportFailure("read", key)),
    ),
    download: Effect.fn("CentralObjectStore.http.download")(
      function* (key, destinationPath) {
        const url = yield* objectUrl("download", key);
        const headers = yield* requestHeaders("download", key);
        const response = yield* client.execute(HttpClientRequest.get(url, { headers }));
        if (response.status === 404) {
          yield* drain(response);
          return false;
        }
        if (response.status !== 200) return yield* unexpectedStatus("download", key, response);
        const partPath = `${destinationPath}.part`;
        yield* response.stream.pipe(
          Stream.run(fileSystem.sink(partPath)),
          Effect.tapError(() => fileSystem.remove(partPath, { force: true }).pipe(Effect.ignore)),
        );
        yield* fileSystem.rename(partPath, destinationPath);
        return true;
      },
      (effect, key) => effect.pipe(transportFailure("download", key)),
    ),
    write: Effect.fn("CentralObjectStore.http.write")(
      function* (key, body, precondition) {
        const url = yield* objectUrl("write", key);
        const headers = {
          ...(yield* requestHeaders("write", key)),
          ...preconditionHeaders(precondition),
        };
        const base = HttpClientRequest.put(url, { headers });
        const request =
          body._tag === "Bytes"
            ? HttpClientRequest.bodyUint8Array(base, body.bytes, "application/octet-stream")
            : yield* HttpClientRequest.bodyFile(base, body.path, {
                contentType: "application/octet-stream",
              }).pipe(Effect.provideService(FileSystem.FileSystem, fileSystem));
        const send = client.execute(request);
        const response = yield* body._tag === "Bytes" ? Effect.timeout(send, BYTES_TIMEOUT) : send;
        if (!isFenced(response) && (response.status === 412 || response.status === 409)) {
          yield* drain(response);
          return yield* new CentralObjectPreconditionError({ key });
        }
        if (response.status !== 200 && response.status !== 201 && response.status !== 204) {
          return yield* unexpectedStatus("write", key, response);
        }
        const version = yield* versionOf("write", key, response);
        yield* drain(response);
        return { version };
      },
      (effect, key) => effect.pipe(transportFailure("write", key)),
    ),
    remove: Effect.fn("CentralObjectStore.http.remove")(
      function* (key) {
        const url = yield* objectUrl("remove", key);
        const headers = yield* requestHeaders("remove", key);
        const response = yield* client.execute(HttpClientRequest.delete(url, { headers }));
        if (
          response.status !== 200 &&
          response.status !== 202 &&
          response.status !== 204 &&
          response.status !== 404
        ) {
          return yield* unexpectedStatus("remove", key, response);
        }
        yield* drain(response);
      },
      (effect, key) => effect.pipe(Effect.timeout(BYTES_TIMEOUT), transportFailure("remove", key)),
    ),
  });
});

/**
 * Bearer-token auth only: the Spaak broker (or another Bearer-authenticated gateway with If-Match,
 * If-None-Match and ETag) speaks `etag`; the GCS XML API speaks `gcs`. Never retries. With a
 * generation, every request carries `x-t3-generation`, and a 403 or 409 carrying `x-t3-fenced: 1`
 * is CentralObjectFencedError.
 */
export const layerHttp = (options: {
  readonly baseUrl: URL;
  readonly tokenFile: string | undefined;
  readonly preconditions: "etag" | "gcs";
  readonly generation: number | undefined;
}): Layer.Layer<CentralObjectStore, never, HttpClient.HttpClient | FileSystem.FileSystem> =>
  Layer.effect(CentralObjectStore, makeHttp(options));

/** layerHttp over undici, which sends a file body with its Content-Length; fetch sends it chunked. */
export const layerHttpUndici = (
  options: Parameters<typeof layerHttp>[0],
): Layer.Layer<CentralObjectStore, never, FileSystem.FileSystem> =>
  layerHttp(options).pipe(Layer.provide(NodeHttpClient.layerUndici));
