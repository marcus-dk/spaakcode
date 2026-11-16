import { assert, it } from "@effect/vitest";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";

import { centralStateConfig } from "./CentralStateConfig.ts";

const read = (env: Record<string, string>) =>
  centralStateConfig.parse(ConfigProvider.fromEnv({ env })).pipe(Effect.result);

const enabled = { T3CODE_CENTRAL_STATE_URL: "file:///tmp/x", T3CODE_CENTRAL_STATE_HOME_KEY: "dev" };

it.effect("reads T3CODE_CENTRAL_STATE_* only when a URL is set", () =>
  Effect.gen(function* () {
    assert.deepStrictEqual(yield* read({}), Result.succeed(Option.none()));
    assert.deepStrictEqual(
      yield* read({ T3CODE_CENTRAL_STATE_HOME_KEY: "../x" }),
      Result.succeed(Option.none()),
    );

    const defaults = yield* read(enabled);
    assert.isTrue(Result.isSuccess(defaults));
    const settings = Option.getOrThrow(Result.getOrThrow(defaults));
    assert.deepStrictEqual(settings.store, { _tag: "Filesystem", root: "/tmp/x" });
    assert.strictEqual(settings.homeKey, "dev");
    assert.strictEqual(settings.claudeHome, undefined);
    assert.strictEqual(settings.codexHome, undefined);
    assert.strictEqual(settings.generation, undefined);
    assert.strictEqual(settings.publicUrl, undefined);
    assert.isFalse(settings.takeover);
    assert.deepStrictEqual(
      [
        settings.debounce,
        settings.minSpacing,
        settings.maxInterval,
        settings.idleInterval,
        settings.transcriptInterval,
        settings.idleExit,
        settings.leaseTtl,
        settings.finalPushTimeout,
      ].map(Duration.toMillis),
      [10_000, 30_000, 120_000, 600_000, 2_000, 0, 60_000, 20_000],
    );

    const http = yield* read({
      T3CODE_CENTRAL_STATE_URL: "https://broker.example/v1/t3/objects",
      T3CODE_CENTRAL_STATE_HOME_KEY: "dev.one_2-x",
      T3CODE_CENTRAL_STATE_TOKEN_FILE: "/run/token",
      T3CODE_CENTRAL_STATE_HTTP_PRECONDITIONS: "gcs",
      T3CODE_CENTRAL_STATE_CLAUDE_HOME: "/claude",
      T3CODE_CENTRAL_STATE_CODEX_HOME: "/codex",
      T3CODE_CENTRAL_STATE_TAKEOVER: "1",
      T3CODE_CENTRAL_STATE_DEBOUNCE: "10s",
      T3CODE_CENTRAL_STATE_MAX_INTERVAL: "2m",
      T3CODE_CENTRAL_STATE_MIN_SPACING: "5s",
      T3CODE_CENTRAL_STATE_PUBLIC_URL: "https://t3-s-abc.sessions.example",
      T3CODE_CENTRAL_STATE_TRANSCRIPT_INTERVAL: "0",
      T3CODE_CENTRAL_STATE_IDLE_EXIT: "30m",
    });
    const httpSettings = Option.getOrThrow(Result.getOrThrow(http));
    assert.deepStrictEqual(httpSettings.store, {
      _tag: "Http",
      baseUrl: new URL("https://broker.example/v1/t3/objects"),
      tokenFile: "/run/token",
      preconditions: "gcs",
    });
    assert.strictEqual(httpSettings.claudeHome, "/claude");
    assert.strictEqual(httpSettings.codexHome, "/codex");
    assert.isTrue(httpSettings.takeover);
    assert.strictEqual(Duration.toMillis(httpSettings.debounce), 10_000);
    assert.strictEqual(Duration.toMillis(httpSettings.maxInterval), 120_000);
    assert.strictEqual(Duration.toMillis(httpSettings.minSpacing), 5_000);
    assert.strictEqual(httpSettings.publicUrl, "https://t3-s-abc.sessions.example");
    assert.strictEqual(Duration.toMillis(httpSettings.transcriptInterval), 0);
    assert.strictEqual(Duration.toMillis(httpSettings.idleExit), 1_800_000);
    const neverIdle = yield* read({ ...enabled, T3CODE_CENTRAL_STATE_IDLE_EXIT: "0" });
    assert.isTrue(Duration.isZero(Option.getOrThrow(Result.getOrThrow(neverIdle)).idleExit));

    // The snapshot loop polls every 2 s, so a shorter spacing is raised to that.
    for (const minSpacing of ["500ms", "0"]) {
      const floored = yield* read({ ...enabled, T3CODE_CENTRAL_STATE_MIN_SPACING: minSpacing });
      assert.strictEqual(
        Duration.toMillis(Option.getOrThrow(Result.getOrThrow(floored)).minSpacing),
        2_000,
      );
    }

    const broker = {
      T3CODE_CENTRAL_STATE_URL: "https://broker.example/v1/t3/objects",
      T3CODE_CENTRAL_STATE_HOME_KEY: "dev",
      T3CODE_CENTRAL_STATE_GENERATION: "3",
    };
    const generation = yield* read(broker);
    assert.strictEqual(Option.getOrThrow(Result.getOrThrow(generation)).generation, 3);

    const rejected = [
      { T3CODE_CENTRAL_STATE_URL: "file:///tmp/x" },
      { ...enabled, T3CODE_CENTRAL_STATE_HOME_KEY: "../x" },
      { ...enabled, T3CODE_CENTRAL_STATE_HOME_KEY: "" },
      { ...enabled, T3CODE_CENTRAL_STATE_HOME_KEY: "k".repeat(129) },
      { ...enabled, T3CODE_CENTRAL_STATE_URL: "ftp://store.example/x" },
      { ...enabled, T3CODE_CENTRAL_STATE_CLAUDE_HOME: "relative/claude" },
      { ...enabled, T3CODE_CENTRAL_STATE_CODEX_HOME: "relative/codex" },
      { ...enabled, T3CODE_CENTRAL_STATE_HTTP_PRECONDITIONS: "s3" },
      { ...enabled, T3CODE_CENTRAL_STATE_LEASE_TTL: "soon" },
      { ...enabled, T3CODE_CENTRAL_STATE_TRANSCRIPT_INTERVAL: "soon" },
      { ...enabled, T3CODE_CENTRAL_STATE_IDLE_EXIT: "later" },
      { ...broker, T3CODE_CENTRAL_STATE_GENERATION: "0" },
      { ...broker, T3CODE_CENTRAL_STATE_GENERATION: "1.5" },
      { ...broker, T3CODE_CENTRAL_STATE_GENERATION: "next" },
      // Only a broker refuses an older generation, so neither store may skip the lease wait.
      { ...enabled, T3CODE_CENTRAL_STATE_GENERATION: "3" },
      { ...broker, T3CODE_CENTRAL_STATE_HTTP_PRECONDITIONS: "gcs" },
    ];
    for (const env of rejected) {
      assert.isTrue(Result.isFailure(yield* read(env)), JSON.stringify(env));
    }
    assert.isTrue(
      Result.isSuccess(yield* read({ ...enabled, T3CODE_CENTRAL_STATE_HOME_KEY: "k".repeat(128) })),
    );
  }),
);
