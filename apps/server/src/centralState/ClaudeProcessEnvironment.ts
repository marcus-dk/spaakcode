import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";

// Variables that bind a process to the Claude Code remote session hosting T3. Terminals and Claude
// children inherit the process env, and neither may act as that session.
const STRIPPED_NAMES = new Set([
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
]);
const STRIPPED_PREFIXES = [
  "CLAUDE_CODE_REMOTE",
  "CLAUDE_CODE_AGENT_PROXY_",
  "CLAUDE_RUNNER_",
  "CCR_",
];

export interface ClaudeEnvironmentPlan {
  /** Becomes CLAUDE_CONFIG_DIR. */
  readonly claudeHome: string;
  /** Names present in the env that will be removed. */
  readonly unset: ReadonlyArray<string>;
}

/** Pure. */
export const plan = (env: NodeJS.ProcessEnv, claudeHome: string): ClaudeEnvironmentPlan => ({
  claudeHome,
  unset: Object.keys(env)
    .filter(
      (name) =>
        STRIPPED_NAMES.has(name) || STRIPPED_PREFIXES.some((prefix) => name.startsWith(prefix)),
    )
    .sort(),
});

/** Mutates HostProcess.Environment (process.env by default) before any layer reads it. */
export const apply = Effect.fn("ClaudeProcessEnvironment.apply")(function* (
  environmentPlan: ClaudeEnvironmentPlan,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const environment = yield* HostProcess.Environment;
  yield* fileSystem.makeDirectory(environmentPlan.claudeHome, { recursive: true, mode: 0o700 });
  environment.CLAUDE_CONFIG_DIR = environmentPlan.claudeHome;
  for (const name of environmentPlan.unset) delete environment[name];
  if (environmentPlan.unset.length > 0) {
    yield* Effect.logInfo("Removed hosting Claude Code session variables.").pipe(
      Effect.annotateLogs({ removed: environmentPlan.unset }),
    );
  }
});
