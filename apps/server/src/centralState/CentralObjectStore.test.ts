// @effect-diagnostics nodeBuiltinImport:off - a real HTTP server shows the framing on the wire.
import * as NodeHttp from "node:http";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import { HttpClient, HttpClientError, HttpClientResponse } from "effect/http";

import * as CentralObjectStore from "./CentralObjectStore.ts";

const encoder = new TextEncoder();
const bytes = (text: string) => ({ _tag: "Bytes", bytes: encoder.encode(text) }) as const;
const IfAbsent = { _tag: "IfAbsent" } as const;
const IfVersion = (version: string) => ({ _tag: "IfVersion", version }) as const;

const buildStore = <E, R>(layer: Layer.Layer<CentralObjectStore.CentralObjectStore, E, R>) =>
  Effect.map(Layer.build(layer), (context) =>
    Context.get(context, CentralObjectStore.CentralObjectStore),
  );

const makeFilesystemStore = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const root = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-central-store-" });
  return { root, store: yield* buildStore(CentralObjectStore.layerFilesystem(root)) };
});

const failureOf = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.map(Effect.result(effect), (result) => {
    assert.isTrue(Result.isFailure(result), "expected the effect to fail");
    return (result as Result.Failure<A, E>).failure;
  });

interface RecordedRequest {
  readonly method: string;
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
}

const makeHttpStore = (input: {
  readonly respond: (request: RecordedRequest) => Response;
  readonly preconditions?: "etag" | "gcs";
  readonly tokenFile?: string;
  readonly baseUrl?: string;
  readonly generation?: number;
}) =>
  Effect.gen(function* () {
    const requests: Array<RecordedRequest> = [];
    const client = HttpClient.make((request) => {
      const recorded = { method: request.method, url: request.url, headers: request.headers };
      requests.push(recorded);
      return Effect.succeed(HttpClientResponse.fromWeb(request, input.respond(recorded)));
    });
    const store = yield* buildStore(
      CentralObjectStore.layerHttp({
        baseUrl: new URL(input.baseUrl ?? "https://store.test/v1/t3/objects/"),
        tokenFile: input.tokenFile,
        preconditions: input.preconditions ?? "etag",
        generation: input.generation,
      }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, client))),
    );
    return { store, requests };
  });

it.layer(NodeServices.layer)("CentralObjectStore filesystem backend", (it) => {
  it.effect("IfAbsent creates once, then reports a precondition failure", () =>
    Effect.gen(function* () {
      const { store } = yield* makeFilesystemStore;
      const created = yield* store.write("homes/a/lease.json", bytes("one"), IfAbsent);
      const error = yield* failureOf(store.write("homes/a/lease.json", bytes("two"), IfAbsent));
      assert.instanceOf(error, CentralObjectStore.CentralObjectPreconditionError);
      const current = yield* store.read("homes/a/lease.json");
      assert.deepStrictEqual(
        Option.map(current, ({ bytes, version }) => [new TextDecoder().decode(bytes), version]),
        Option.some(["one", created.version]),
      );
    }),
  );

  it.effect("IfVersion fails on a stale version and replaces on the current one", () =>
    Effect.gen(function* () {
      const { store } = yield* makeFilesystemStore;
      const first = yield* store.write("k", bytes("one"), IfAbsent);
      const second = yield* store.write("k", bytes("two"), IfVersion(first.version));
      assert.notStrictEqual(second.version, first.version);
      const stale = yield* failureOf(store.write("k", bytes("three"), IfVersion(first.version)));
      assert.instanceOf(stale, CentralObjectStore.CentralObjectPreconditionError);
      const third = yield* store.write("k", bytes("three"), IfVersion(second.version));
      assert.notStrictEqual(third.version, second.version);
    }),
  );

  it.effect("concurrent IfVersion writes from one base have exactly one winner", () =>
    Effect.gen(function* () {
      const { store } = yield* makeFilesystemStore;
      const base = yield* store.write("k", bytes("base"), IfAbsent);
      const results = yield* Effect.forEach(
        ["left", "right", "middle"],
        (text) => Effect.result(store.write("k", bytes(text), IfVersion(base.version))),
        { concurrency: "unbounded" },
      );
      assert.strictEqual(results.filter(Result.isSuccess).length, 1);
      for (const result of results.filter(Result.isFailure)) {
        assert.instanceOf(result.failure, CentralObjectStore.CentralObjectPreconditionError);
      }
    }),
  );

  it.effect("versions are never reused after remove and re-create", () =>
    Effect.gen(function* () {
      const { store } = yield* makeFilesystemStore;
      const original = yield* store.write("k", bytes("same"), IfAbsent);
      yield* store.remove("k");
      yield* store.remove("k");
      const recreated = yield* store.write("k", bytes("same"), IfAbsent);
      assert.notStrictEqual(recreated.version, original.version);
      const error = yield* failureOf(store.write("k", bytes("x"), IfVersion(original.version)));
      assert.instanceOf(error, CentralObjectStore.CentralObjectPreconditionError);
    }),
  );

  it.effect("absent keys read as none and download as false", () =>
    Effect.gen(function* () {
      const { root, store } = yield* makeFilesystemStore;
      const path = yield* Path.Path;
      const fileSystem = yield* FileSystem.FileSystem;
      const destination = path.join(root, "absent.out");
      assert.isTrue(Option.isNone(yield* store.read("missing")));
      assert.isFalse(yield* store.download("missing", destination));
      assert.isFalse(yield* fileSystem.exists(destination));
    }),
  );

  it.effect("download returns the stored bytes exactly", () =>
    Effect.gen(function* () {
      const { root, store } = yield* makeFilesystemStore;
      const path = yield* Path.Path;
      const fileSystem = yield* FileSystem.FileSystem;
      const content = Uint8Array.from({ length: 300_000 }, (_, index) => (index * 31 + 7) % 256);
      const source = path.join(root, "source.bin");
      yield* fileSystem.writeFile(source, content);
      yield* store.write("homes/a/blobs/abc", { _tag: "File", path: source }, IfAbsent);
      const destination = path.join(root, "copy.bin");
      assert.isTrue(yield* store.download("homes/a/blobs/abc", destination));
      assert.deepStrictEqual(yield* fileSystem.readFile(destination), content);
      assert.isFalse(yield* fileSystem.exists(`${destination}.part`));
    }),
  );
});

it.layer(NodeServices.layer)("CentralObjectStore HTTP backend", (it) => {
  it.effect("etag dialect sends If-None-Match and If-Match and reads ETag", () =>
    Effect.gen(function* () {
      const { store, requests } = yield* makeHttpStore({
        respond: (request) =>
          new Response(request.method === "GET" ? "body" : null, {
            status: 200,
            headers: { etag: '"7"' },
          }),
      });
      assert.deepStrictEqual(yield* store.write("homes/a b/lease.json", bytes("x"), IfAbsent), {
        version: '"7"',
      });
      yield* store.write("homes/a b/lease.json", bytes("y"), IfVersion('"7"'));
      const read = yield* store.read("homes/a b/lease.json");
      assert.deepStrictEqual(
        Option.map(read, ({ version }) => version),
        Option.some('"7"'),
      );
      assert.deepStrictEqual(
        requests.map(({ method, url, headers }) => [
          method,
          url,
          headers["if-none-match"],
          headers["if-match"],
          headers["x-goog-if-generation-match"],
        ]),
        [
          [
            "PUT",
            "https://store.test/v1/t3/objects/homes/a%20b/lease.json",
            "*",
            undefined,
            undefined,
          ],
          [
            "PUT",
            "https://store.test/v1/t3/objects/homes/a%20b/lease.json",
            undefined,
            '"7"',
            undefined,
          ],
          [
            "GET",
            "https://store.test/v1/t3/objects/homes/a%20b/lease.json",
            undefined,
            undefined,
            undefined,
          ],
        ],
      );
    }),
  );

  it.effect("gcs dialect sends generation preconditions and reads x-goog-generation", () =>
    Effect.gen(function* () {
      const { store, requests } = yield* makeHttpStore({
        preconditions: "gcs",
        baseUrl: "https://storage.test/bucket/prefix",
        respond: (request) =>
          new Response(request.method === "GET" ? "body" : null, {
            status: 200,
            headers: { etag: '"md5"', "x-goog-generation": "1712" },
          }),
      });
      assert.deepStrictEqual(yield* store.write("k", bytes("x"), IfAbsent), { version: "1712" });
      yield* store.write("k", bytes("y"), IfVersion("1712"));
      const read = yield* store.read("k");
      assert.deepStrictEqual(
        Option.map(read, ({ version }) => version),
        Option.some("1712"),
      );
      assert.deepStrictEqual(
        requests.map(({ url, headers }) => [
          url,
          headers["x-goog-if-generation-match"],
          headers["if-none-match"],
          headers["if-match"],
        ]),
        [
          ["https://storage.test/bucket/prefix/k", "0", undefined, undefined],
          ["https://storage.test/bucket/prefix/k", "1712", undefined, undefined],
          ["https://storage.test/bucket/prefix/k", undefined, undefined, undefined],
        ],
      );
    }),
  );

  it.effect.each([412, 409])("PUT status %d is a precondition failure", (status) =>
    Effect.gen(function* () {
      const { store } = yield* makeHttpStore({ respond: () => new Response("no", { status }) });
      const error = yield* failureOf(store.write("k", bytes("x"), IfVersion('"1"')));
      assert.instanceOf(error, CentralObjectStore.CentralObjectPreconditionError);
    }),
  );

  it.effect.each([409, 403])("status %d with x-t3-fenced is a fence on every operation", (status) =>
    Effect.gen(function* () {
      const { store, requests } = yield* makeHttpStore({
        generation: 7,
        respond: () => new Response("stale", { status, headers: { "x-t3-fenced": "1" } }),
      });
      const errors = [
        yield* failureOf(store.read("k")),
        yield* failureOf(store.download("k", "/nonexistent/never-written")),
        yield* failureOf(store.write("k", bytes("x"), IfVersion('"1"'))),
        yield* failureOf(store.remove("k")),
      ];
      for (const error of errors) {
        assert.deepInclude(error, { _tag: "CentralObjectFencedError", key: "k", status });
      }
      assert.deepStrictEqual(
        requests.map(({ headers }) => headers["x-t3-generation"]),
        ["7", "7", "7", "7"],
      );
    }),
  );

  it.effect("GET 404 is absent and DELETE 404 succeeds", () =>
    Effect.gen(function* () {
      const { store } = yield* makeHttpStore({
        respond: () => new Response("missing", { status: 404 }),
      });
      assert.isTrue(Option.isNone(yield* store.read("k")));
      assert.isFalse(yield* store.download("k", "/nonexistent/never-written"));
      yield* store.remove("k");
    }),
  );

  it.effect("re-reads the token file per request and never leaks it or the URL query", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const dir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-central-token-" });
      const tokenFile = path.join(dir, "token");
      const { store, requests } = yield* makeHttpStore({
        tokenFile,
        baseUrl: "https://store.test/objects?userProject=secret-query",
        respond: (request) =>
          request.method === "DELETE"
            ? new Response("unavailable", { status: 503 })
            : new Response("body", { status: 200, headers: { etag: '"1"' } }),
      });
      yield* fileSystem.writeFileString(tokenFile, "secret-token-one\n");
      yield* store.read("k");
      yield* fileSystem.writeFileString(tokenFile, "secret-token-two");
      yield* store.read("k");
      assert.deepStrictEqual(
        requests.map(({ headers }) => headers.authorization),
        ["Bearer secret-token-one", "Bearer secret-token-two"],
      );

      const unavailable = yield* failureOf(store.remove("homes/a/blobs/x"));
      assert.instanceOf(unavailable, CentralObjectStore.CentralObjectStoreError);
      assert.strictEqual(unavailable.status, 503);
      assert.strictEqual(unavailable.operation, "remove");
      const serialized = `${unavailable.message} ${JSON.stringify(unavailable)}`;
      assert.notInclude(serialized, "secret-token");
      assert.notInclude(serialized, "secret-query");

      yield* fileSystem.writeFileString(tokenFile, "  \n");
      const emptyToken = yield* failureOf(store.read("k"));
      assert.instanceOf(emptyToken, CentralObjectStore.CentralObjectStoreError);
      assert.isNull(emptyToken.status);
      assert.strictEqual(requests.length, 3);
    }),
  );

  it.effect("sends no Authorization or generation header without a token file or generation", () =>
    Effect.gen(function* () {
      const { store, requests } = yield* makeHttpStore({
        respond: () => new Response(null, { status: 204 }),
      });
      yield* store.remove("k");
      assert.isUndefined(requests[0]?.headers.authorization);
      assert.isUndefined(requests[0]?.headers["x-t3-generation"]);
    }),
  );

  it.effect("refuses keys with dot segments before sending anything", () =>
    Effect.gen(function* () {
      const { store, requests } = yield* makeHttpStore({
        respond: () => new Response(null, { status: 204 }),
      });
      for (const key of [
        "homes/a/blobs/../../b/manifest.json",
        "homes/a/./lease.json",
        "homes//a",
      ]) {
        const error = yield* failureOf(store.remove(key));
        assert.instanceOf(error, CentralObjectStore.CentralObjectStoreError, key);
      }
      assert.deepStrictEqual(requests, []);
    }),
  );

  it.effect("a transport failure is a store error with no status", () =>
    Effect.gen(function* () {
      const client = HttpClient.make((request) =>
        Effect.fail(
          new HttpClientError.HttpClientError({
            reason: new HttpClientError.TransportError({ request, cause: new Error("reset") }),
          }),
        ),
      );
      const store = yield* buildStore(
        CentralObjectStore.layerHttp({
          baseUrl: new URL("https://store.test/objects?userProject=secret-query"),
          tokenFile: undefined,
          preconditions: "etag",
          generation: undefined,
        }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, client))),
      );
      const error = yield* failureOf(store.write("k", bytes("x"), IfAbsent));
      assert.instanceOf(error, CentralObjectStore.CentralObjectStoreError);
      assert.isNull(error.status);
      assert.notInclude(error.message, "secret-query");
    }),
  );
});

it.live("the undici HTTP backend sends a file body with its Content-Length, not chunked", () =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-central-http-" });
    const file = path.join(directory, "blob.zst");
    yield* fileSystem.writeFile(file, new Uint8Array(70_000).fill(7));
    const seen: Array<{
      readonly length: string | undefined;
      readonly encoding: string | undefined;
      readonly bytes: number;
    }> = [];
    const server = yield* Effect.acquireRelease(
      Effect.callback<NodeHttp.Server>((resume) => {
        const created = NodeHttp.createServer((request, response) => {
          let bytes = 0;
          request.on("data", (chunk: Buffer) => (bytes += chunk.length));
          request.on("end", () => {
            seen.push({
              length: request.headers["content-length"],
              encoding: request.headers["transfer-encoding"],
              bytes,
            });
            response.writeHead(200, { etag: '"1"' }).end();
          });
        });
        created.listen(0, "127.0.0.1", () => resume(Effect.succeed(created)));
      }),
      (created) =>
        Effect.callback<void>((resume) => {
          created.close(() => resume(Effect.void));
        }),
    );
    const address = server.address();
    if (address === null || typeof address === "string") throw new Error("no port");
    const store = yield* buildStore(
      CentralObjectStore.layerHttpUndici({
        baseUrl: new URL(`http://127.0.0.1:${address.port}/v1/t3/objects`),
        tokenFile: undefined,
        preconditions: "etag",
        generation: undefined,
      }),
    );
    yield* store.write("homes/a/blobs/x", { _tag: "File", path: file }, IfAbsent);
    assert.deepStrictEqual(seen, [{ length: "70000", encoding: undefined, bytes: 70_000 }]);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
