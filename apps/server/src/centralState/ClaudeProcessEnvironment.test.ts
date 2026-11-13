import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import * as ClaudeProcessEnvironment from "./ClaudeProcessEnvironment.ts";

const STRIPPED = [
  "CLAUDECODE",
  "CLAUDE_CODE_SESSION_ACCESS_TOKEN",
  "CLAUDE_CODE_SESSION_ID",
  "CLAUDE_CODE_SESSION_ATTENDED",
  "CLAUDE_CODE_CHILD_SESSION",
  "CLAUDE_CODE_ENVIRONMENT_KIND",
  "CLAUDE_CODE_USE_CCR_V2",
  "CLAUDE_CODE_WORKER_EPOCH",
  "CLAUDE_CODE_OAUTH_SCOPES",
  "CLAUDE_CODE_ACCOUNT_UUID",
  "CLAUDE_CODE_ORGANIZATION_UUID",
  "CLAUDE_CODE_MESSAGING_SOCKET",
  "CLAUDE_CODE_MESSAGING_TOKEN",
  "CLAUDE_CODE_EXECPATH",
  "CLAUDE_CODE_ENTRYPOINT",
  "CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD",
  "CLAUDE_CODE_ARTIFACT_ASSETS",
  "CLAUDE_CODE_SYNC_PLUGINS",
  "CLAUDE_CODE_SYNC_SKILLS",
  "CLAUDE_CODE_TEE_SDK_STDOUT",
  "CLAUDE_CODE_INCLUDE_PARTIAL_MESSAGES",
  "CLAUDE_CODE_BG_TASKS_REPORT_RUNNING",
  "CLAUDE_ENV_FILE",
  "CLAUDE_EFFORT",
  "CLAUDE_PID",
  "CLAUDE_SESSION_INGRESS_TOKEN_FILE",
  "CLAUDE_STAGE_FILE_ROOT",
  "CLAUDE_AUTOCOMPACT_PCT_OVERRIDE",
  "SESSION_INGRESS_URL",
  "CLAUDE_CODE_REMOTE",
  "CLAUDE_CODE_REMOTE_SESSION_ID",
  "CLAUDE_CODE_AGENT_PROXY_URL",
  "CLAUDE_RUNNER_ID",
  "CCR_ENVIRONMENT",
];
const KEPT = [
  "CLAUDE_CODE_OAUTH_TOKEN",
  "ANTHROPIC_API_KEY",
  "ANTHROPIC_BASE_URL",
  "CLAUDE_CODE_USE_BEDROCK",
  "CLAUDE_CODE_USE_VERTEX",
  "CLAUDE_CODE_MAX_RETRIES",
  "CLAUDE_CONFIG_DIR",
  "HOME",
  "PATH",
];

const environmentWith = (names: ReadonlyArray<string>): NodeJS.ProcessEnv =>
  Object.fromEntries(names.map((name) => [name, `value-of-${name}`]));

it.layer(NodeServices.layer)("ClaudeProcessEnvironment", (it) => {
  it("plans the T3-owned Claude home and strips only the hosting session's variables", () => {
    const rows: ReadonlyArray<{
      readonly env: NodeJS.ProcessEnv;
      readonly unset: ReadonlyArray<string>;
    }> = [
      { env: environmentWith([...STRIPPED, ...KEPT]), unset: [...STRIPPED].sort() },
      { env: environmentWith(KEPT), unset: [] },
      { env: environmentWith(["CLAUDE_PID", "PATH"]), unset: ["CLAUDE_PID"] },
      { env: {}, unset: [] },
    ];
    for (const row of rows) {
      assert.deepStrictEqual(ClaudeProcessEnvironment.plan(row.env, "/home/t3/claude"), {
        claudeHome: "/home/t3/claude",
        unset: row.unset,
      });
    }
  });

  it.effect("applies the plan to the provided environment and creates a private Claude home", () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-claude-env-" });
      const claudeHome = path.join(directory, "base", "claude");
      const environment = environmentWith([
        "CLAUDE_CODE_SESSION_ID",
        "CCR_ENVIRONMENT",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "CLAUDE_CONFIG_DIR",
      ]);

      yield* ClaudeProcessEnvironment.apply(
        ClaudeProcessEnvironment.plan(environment, claudeHome),
      ).pipe(Effect.provideService(HostProcess.Environment, environment));

      assert.deepStrictEqual(environment, {
        CLAUDE_CODE_OAUTH_TOKEN: "value-of-CLAUDE_CODE_OAUTH_TOKEN",
        CLAUDE_CONFIG_DIR: claudeHome,
      });
      const info = yield* fileSystem.stat(claudeHome);
      assert.strictEqual(info.type, "Directory");
      assert.strictEqual(info.mode & 0o777, 0o700);
    }),
  );
});
