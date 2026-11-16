// @effect-diagnostics nodeBuiltinImport:off - file: URLs and absolute paths are parsed while reading config, before any service exists.
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as Config from "effect/Config";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";

import { DurationFromString } from "../cli/config.ts";

export interface CentralStateSettings {
  readonly store:
    | { readonly _tag: "Filesystem"; readonly root: string }
    | {
        readonly _tag: "Http";
        readonly baseUrl: URL;
        readonly tokenFile: string | undefined;
        readonly preconditions: "etag" | "gcs";
      };
  readonly homeKey: string;
  /** Set by the session service; a higher generation of the same home supersedes a lower one. */
  readonly generation: number | undefined;
  /** Where clients reach this server; only logged. */
  readonly publicUrl: string | undefined;
  /** Absolute; undefined means <baseDir>/claude. */
  readonly claudeHome: string | undefined;
  /** Absolute; undefined means <baseDir>/codex. */
  readonly codexHome: string | undefined;
  readonly takeover: boolean;
  readonly debounce: Duration.Duration;
  readonly minSpacing: Duration.Duration;
  readonly maxInterval: Duration.Duration;
  readonly idleInterval: Duration.Duration;
  readonly leaseTtl: Duration.Duration;
  readonly finalPushTimeout: Duration.Duration;
}

export class CentralStateConfigError extends Schema.TaggedError<CentralStateConfigError>()(
  "CentralStateConfigError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Invalid T3CODE_CENTRAL_STATE_* configuration.";
  }
}

const HOME_KEY_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
// The snapshot loop polls every 2 s, so a shorter spacing would change nothing.
const MIN_SPACING_FLOOR = Duration.seconds(2);

const invalid = (message: string) =>
  Effect.fail(
    new Config.ConfigError(new Schema.SchemaError(new SchemaIssue.InvalidValue({ message }))),
  );

const duration = (name: string, fallback: Duration.Duration) =>
  Config.schema(DurationFromString, name).pipe(Config.withDefault(fallback));

const optionalString = (name: string) =>
  Config.String(name).pipe(Config.option, Config.map(Option.getOrUndefined));

const storeOf = (
  value: string,
  http: { readonly tokenFile: string | undefined; readonly preconditions: "etag" | "gcs" },
): Effect.Effect<CentralStateSettings["store"], Config.ConfigError> => {
  const url = URL.parse(value.trim());
  if (url?.protocol === "file:") {
    try {
      return Effect.succeed({ _tag: "Filesystem", root: NodeURL.fileURLToPath(url) });
    } catch {
      return invalid("T3CODE_CENTRAL_STATE_URL must be an absolute file:/// URL.");
    }
  }
  if (url?.protocol === "http:" || url?.protocol === "https:") {
    return Effect.succeed({ _tag: "Http", baseUrl: url, ...http });
  }
  return invalid("T3CODE_CENTRAL_STATE_URL must be a file:///, http:// or https:// URL.");
};

const enabledConfig = (url: string) =>
  Config.all({
    homeKey: Config.schema(
      Schema.String.check(Schema.isPattern(HOME_KEY_PATTERN)),
      "T3CODE_CENTRAL_STATE_HOME_KEY",
    ),
    generation: Config.Int("T3CODE_CENTRAL_STATE_GENERATION").pipe(
      Config.option,
      Config.map(Option.getOrUndefined),
    ),
    publicUrl: optionalString("T3CODE_CENTRAL_STATE_PUBLIC_URL"),
    tokenFile: optionalString("T3CODE_CENTRAL_STATE_TOKEN_FILE"),
    preconditions: Config.Literals(["etag", "gcs"], "T3CODE_CENTRAL_STATE_HTTP_PRECONDITIONS").pipe(
      Config.withDefault("etag" as const),
    ),
    claudeHome: optionalString("T3CODE_CENTRAL_STATE_CLAUDE_HOME"),
    codexHome: optionalString("T3CODE_CENTRAL_STATE_CODEX_HOME"),
    takeover: Config.Boolean("T3CODE_CENTRAL_STATE_TAKEOVER").pipe(Config.withDefault(false)),
    debounce: duration("T3CODE_CENTRAL_STATE_DEBOUNCE", Duration.seconds(10)),
    minSpacing: duration("T3CODE_CENTRAL_STATE_MIN_SPACING", Duration.seconds(30)),
    maxInterval: duration("T3CODE_CENTRAL_STATE_MAX_INTERVAL", Duration.minutes(2)),
    idleInterval: duration("T3CODE_CENTRAL_STATE_IDLE_INTERVAL", Duration.minutes(10)),
    leaseTtl: duration("T3CODE_CENTRAL_STATE_LEASE_TTL", Duration.seconds(60)),
    finalPushTimeout: duration("T3CODE_CENTRAL_STATE_FINAL_PUSH_TIMEOUT", Duration.seconds(20)),
  }).pipe(
    Config.mapEffect(({ tokenFile, preconditions, claudeHome, codexHome, minSpacing, ...rest }) => {
      if (claudeHome !== undefined && !NodePath.isAbsolute(claudeHome)) {
        return invalid("T3CODE_CENTRAL_STATE_CLAUDE_HOME must be an absolute path.");
      }
      if (codexHome !== undefined && !NodePath.isAbsolute(codexHome)) {
        return invalid("T3CODE_CENTRAL_STATE_CODEX_HOME must be an absolute path.");
      }
      if (rest.generation !== undefined && rest.generation < 1) {
        return invalid("T3CODE_CENTRAL_STATE_GENERATION must be a positive integer.");
      }
      return Effect.map(
        storeOf(url, { tokenFile, preconditions }),
        (store): CentralStateSettings => ({
          ...rest,
          store,
          claudeHome,
          codexHome,
          minSpacing: Duration.max(minSpacing, MIN_SPACING_FLOOR),
        }),
      );
    }),
  );

/** None unless T3CODE_CENTRAL_STATE_URL is set; the other variables are read only then. */
export const centralStateConfig: Config.Config<Option.Option<CentralStateSettings>> = Config.String(
  "T3CODE_CENTRAL_STATE_URL",
).pipe(
  Config.option,
  Config.map(Option.filter((url) => url.trim().length > 0)),
  Config.flatMap(
    Option.match({
      onNone: () => Config.succeed(Option.none<CentralStateSettings>()),
      onSome: (url) => enabledConfig(url).pipe(Config.map(Option.some)),
    }),
  ),
);
