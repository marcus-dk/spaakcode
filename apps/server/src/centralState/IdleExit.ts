import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";

const SAMPLE = Duration.seconds(10);
const HEARTBEAT_SPACING_MS = 30_000;
const TCP_TABLES = ["/proc/net/tcp", "/proc/net/tcp6"];
const TCP_ESTABLISHED = "01";

/** Whether a /proc/net/tcp{,6} table holds an established connection to localPort. */
const hasEstablished = (table: string, localPort: number) =>
  table
    .split("\n")
    .slice(1)
    .some((line) => {
      const [, local, , state] = line.trim().split(/\s+/);
      return (
        state === TCP_ESTABLISHED &&
        local !== undefined &&
        Number.parseInt(local.slice(local.lastIndexOf(":") + 1), 16) === localPort
      );
    });

/**
 * auth_sessions records connects but never disconnects, and the live set is only in the server's
 * memory, so the kernel's table is read instead. Any connection counts, idle keep-alives and
 * loopback included. Null off Linux.
 */
const clientsConnected = Effect.fn("IdleExit.clientsConnected")(function* (port: number) {
  // Port 0 asks the OS for one, so the real port is not known here.
  if (port === 0) return null;
  const fileSystem = yield* FileSystem.FileSystem;
  const tables = yield* Effect.forEach(TCP_TABLES, (table) =>
    fileSystem.readFileString(table).pipe(Effect.option),
  );
  if (tables.every(Option.isNone)) return null;
  return tables.some((table) => Option.isSome(table) && hasEstablished(table.value, port));
});

/**
 * Samples activity: busy while a run is unfinished, an effect is due or a socket is open on the
 * server's port, and whenever either signal is unknown. While busy it touches heartbeatPath at most
 * every 30 s. The returned Deferred completes once the server has been idle for idleExit; never
 * when that is zero.
 */
export const start = Effect.fn("IdleExit.start")(function* (input: {
  readonly idleExit: Duration.Duration;
  readonly heartbeatPath: string;
  readonly port: number;
  readonly work: Effect.Effect<boolean | null>;
  readonly homeKey: string;
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const idle = yield* Deferred.make<void>();
  const idleExitMs = Duration.toMillis(input.idleExit);
  let idleSinceMs: number | null = null;
  let touchedAtMs: number | null = null;

  const touch = fileSystem
    .writeFile(input.heartbeatPath, new Uint8Array())
    .pipe(
      Effect.catch((error) =>
        Effect.logWarning("Could not touch the central state activity file.").pipe(
          Effect.annotateLogs({ path: input.heartbeatPath, error: error._tag }),
        ),
      ),
    );

  /** One sample; false ends the sampling. */
  const sample = Effect.gen(function* () {
    const nowMs = Number((yield* Clock.monotonicTimeNanos) / 1_000_000n);
    const work = yield* input.work;
    const clients = yield* clientsConnected(input.port);
    if (work !== false || clients !== false) {
      idleSinceMs = null;
      if (touchedAtMs === null || nowMs - touchedAtMs >= HEARTBEAT_SPACING_MS) {
        touchedAtMs = nowMs;
        yield* touch;
      }
      return true;
    }
    idleSinceMs ??= nowMs;
    if (idleExitMs === 0 || nowMs - idleSinceMs < idleExitMs) return true;
    yield* Effect.logInfo(
      `No unfinished runs and no clients for ${Duration.format(input.idleExit)}; stopping the server (T3CODE_CENTRAL_STATE_IDLE_EXIT).`,
    ).pipe(Effect.annotateLogs({ homeKey: input.homeKey }));
    yield* Deferred.succeed(idle, undefined);
    return false;
  });

  yield* Effect.forkScoped(
    Effect.repeat(sample, { schedule: Schedule.spaced(SAMPLE), while: (keepGoing) => keepGoing }),
  );
  return idle;
});
