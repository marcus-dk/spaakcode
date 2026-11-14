# Central state on ephemeral runners

This page covers running `t3 serve` on machines that do not keep their disk, such as Spaak's
claude-runner pods. T3 still keeps its live database in local SQLite. Central state copies the T3
home to an object store and restores it on the next machine, so that history follows the
developer from pod to pod. The code is in
[`apps/server/src/centralState/`](../../apps/server/src/centralState/). Central state is off
unless `T3CODE_CENTRAL_STATE_URL` is set.

## What it covers

A home is stored as a **state bundle** under `homes/<homeKey>/` in the store:

- `blobs/<sha256>`: content-addressed, zstd-compressed files. They hold database snapshots, state
  files and Claude transcript chunks.
- `manifest.json`: the head. A compare-and-swap write of this file is the only commit point, and
  it also fences out older servers.
- `manifests/<epoch>/<generation>.json`: history copies of the last 10 generations, plus the last
  generation from before each of the 2 most recent migrations.
- `lease.json`: the record of which process serves the home.

The bundle carries:

- the database (`statev2.sqlite`), which holds thread and message history, projections, auth
  sessions, pairing links and scheduled tasks;
- `environment-id`, `anonymous-id`, `settings.json`, `keybindings.json` and `themes/`;
- `secrets/`, without DPoP and cloud replay markers or `.tmp` files, so paired devices keep
  working after a move. It holds every sensitive value entered in T3 settings, not only the auth
  keys: source-control tokens (GitHub, Bitbucket) and sensitive provider-instance environment
  variables such as provider API keys;
- `attachments/`, without pending uploads and `.part` files;
- the Claude transcripts under `<claudeHome>/projects/`, so Claude threads resume natively, and
  the task lists under `<claudeHome>/tasks/` that Claude's task tools read back on resume.

Symbolic links under the T3 home are never followed or synced, so a symlinked `settings.json`,
theme or secret stays behind.

It does **not** carry:

- **Worktrees and unpushed code.** On the next pod, `restoreMissingWorktree` silently recreates a
  missing worktree from the pushed branch or `HEAD`. Push work you want to keep.
- **`refs/t3/*` checkpoint refs.** Full diffs and rollback of restored turns fail.
- `<baseDir>/scratch` and `<baseDir>/projects`.
- Terminal scrollback and logs.
- Codex and the other provider homes.
- The `homePath` of a custom Claude instance. Only the default instance uses the synced Claude
  home.
- Claude's own login in the Claude home (its OAuth or credentials file). Supply it per developer
  with `CLAUDE_CODE_OAUTH_TOKEN`, or with an API key, Bedrock or Vertex. Keys set as provider
  environment variables in T3 settings are in `secrets/` and are synced.
- Other Claude state, such as `plans/`. Plan-mode files mostly repeat what the transcript holds.

## Environment variables

Central state is configured through environment variables only. There are no CLI flags.

| Variable                                  | Default                     | Meaning                                                                                                                                                                 |
| ----------------------------------------- | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `T3CODE_CENTRAL_STATE_URL`                | unset, meaning **disabled** | `file:///abs/dir` selects the filesystem backend. `http(s)://…` selects the HTTP backend; object keys are appended to this base URL. Any other scheme is a config error |
| `T3CODE_CENTRAL_STATE_HOME_KEY`           | required when URL is set    | Per-developer key; one live server per key. Must match `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`                                                                             |
| `T3CODE_CENTRAL_STATE_TOKEN_FILE`         | unset                       | Bearer token file, re-read on every request (HTTP only)                                                                                                                 |
| `T3CODE_CENTRAL_STATE_HTTP_PRECONDITIONS` | `etag`                      | `etag` (the Spaak broker, or another Bearer-token gateway) or `gcs` (GCS XML API, which uses `x-goog-generation`). S3, R2 and MinIO need SigV4 and are not supported    |
| `T3CODE_CENTRAL_STATE_CLAUDE_HOME`        | `<baseDir>/claude`          | Absolute path. T3's Claude children use it as `CLAUDE_CONFIG_DIR`                                                                                                       |
| `T3CODE_CENTRAL_STATE_TAKEOVER`           | `false`                     | Ask a live holder to hand off instead of failing fast                                                                                                                   |
| `T3CODE_CENTRAL_STATE_DEBOUNCE`           | `10s`                       | Quiet time after the last history write before a push                                                                                                                   |
| `T3CODE_CENTRAL_STATE_MAX_INTERVAL`       | `2m`                        | Longest a history-dirty home waits during continuous activity                                                                                                           |
| `T3CODE_CENTRAL_STATE_IDLE_INTERVAL`      | `10m`                       | Cadence for non-history DB writes and the files-only sweep                                                                                                              |
| `T3CODE_CENTRAL_STATE_LEASE_TTL`          | `60s`                       | Lease lifetime. The holder renews every quarter of it and stops itself after a full TTL without a renewal                                                               |
| `T3CODE_CENTRAL_STATE_FINAL_PUSH_TIMEOUT` | `20s`                       | Bound on the shutdown push, which retries within it, and on the wait for a push already running at shutdown                                                             |

When central state is on, T3 sets `CLAUDE_CONFIG_DIR` to the Claude home before the server starts.
It also removes the variables that bind a process to the runner's own Claude Code session
(`CLAUDECODE`, `CLAUDE_CODE_SESSION_*`, `CLAUDE_CODE_REMOTE*`, `CCR_*` and the others in
`ClaudeProcessEnvironment.ts`). Terminals and Claude children inherit the result. It keeps
`CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_*`, `CLAUDE_CODE_USE_BEDROCK`, `CLAUDE_CODE_USE_VERTEX` and
`CLAUDE_CODE_MAX_RETRIES`.

## Required stable paths

Projections store absolute paths, and the loopback cookie name hashes `stateDir`. These paths must
therefore be the same on every machine that serves a home:

- `T3CODE_HOME` (the base dir, and the state dir below it);
- the Claude home;
- the repository checkout paths of every project.

Restore refuses a different `baseDir`, `stateDir` or Claude home before it downloads anything. The
error is `CentralStatePathMismatchError`, and it names the field with its recorded and current
values. To recover, set `T3CODE_HOME` or `T3CODE_CENTRAL_STATE_CLAUDE_HOME` to the recorded value
and start again. Restore does not check checkout paths. Clone each project to the same path,
otherwise its threads point at a missing directory.

## Recovery point

| Situation                                                                                                                                     | Committed history lost                                                                                                                                                  |
| --------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pod dies at least `debounce` (10 s) plus one push duration after activity settles                                                             | 0                                                                                                                                                                       |
| OOM, SIGKILL or node loss during continuous activity                                                                                          | Up to `maxInterval` (2 min) plus one push duration, plus at most the adaptive spacing when pushes are slow                                                              |
| Other non-history DB writes (scheduled-task edits) and settings, keybindings or themes edits, then SIGKILL                                    | Up to `idleInterval` (10 min) plus one push                                                                                                                             |
| Graceful SIGTERM **delivered to the T3 process**, with a pod grace period as in [infra item 3](#infra-contract), when the final push succeeds | 0. If it fails, the lease is kept, so a restart in the same pod keeps the local home; another pod restores the last save                                                |
| Takeover handoff whose final push succeeds within `finalPushTimeout`                                                                          | 0. If it fails, the holder keeps the lease and exits with `CentralStateHandoffUnsavedError`; a requester that then takes over restores the last save                    |
| Store or broker outage                                                                                                                        | Pushes retry. After `leaseTtl` (60 s) with no renewal, T3 stops itself. A restart in the same pod keeps the local home, so nothing is lost once the store answers again |
| This server is superseded (another pod took the lease)                                                                                        | Everything since this server's last commit, by design                                                                                                                   |

Pushes are spaced at least `max(30 s, 20 × the last successful push's duration)` apart, so
snapshot work stays near 5% of one core. On a large database that spacing, not `maxInterval`, sets
the loss window. A failed push only restarts the 30 s floor, so an outage does not stretch it.

Session and pairing-link changes (pairing, revoking a device, redeeming a single-use link) are
pushed on the history cadence, not the idle one. A lossy restore can still undo one made in the
last `debounce` plus a push, which brings a revoked session back to life or lets a redeemed link
work again until it expires. After a move that followed a pod death, revoke again anything you
revoked in the minutes before it.

On runners, kubelet signals PID 1, which is the runner, not a `t3 serve` started from a Bash tool
call. Graceful RPO 0 therefore needs the SIGTERM forward and grace period in the
[infra contract](#infra-contract). Without them, every pod death is a SIGKILL. An idle-shutdown
death still loses no history, because the debounce push has already run.

In-flight runs reappear as "Server restarted" through the normal startup reconcile. With
**Continue threads after restarts** on (the default), a continuation is queued. After a lossy
restore, that continuation can repeat side effects the lost tail already performed, such as a push
or a PR comment. Turn the setting off if that matters to you. A Claude transcript written after
the last push is missing after restore, and the thread continues through the lossy context
handoff.

Clients holding cursors from the lost range are not confused. Each restore gives the database a
sequence floor 10⁹ above any sequence the home has issued. The existing resume guards then send
those clients a full snapshot instead of a delta.

## One server per home

The lease allows one live server per home key. Before it takes the lease, the server holds the local
ownership lock, so a second server on the same machine and `T3CODE_HOME` fails at once with the
usual "already running" error.

A second server elsewhere fails fast, with exit code 78. A supervisor told not to restart on 78
(for example systemd's `RestartPreventExitStatus=78`) then leaves it stopped. The `t3 service`
launcher restarts it once the local ownership lock is free, which it always is when the holder is
on another machine, and the desktop app stops its backend for good; central state is not supported
under either of them.

```text
T3 home <key> is served by <host> (pid <pid>). Stop that server, set
T3CODE_CENTRAL_STATE_TAKEOVER=1 to move it here, or use another T3CODE_CENTRAL_STATE_HOME_KEY.
```

Your choices:

- **Stop the other server.** A clean stop releases the lease, and the next start acquires it at
  once.
- **`T3CODE_CENTRAL_STATE_TAKEOVER=1`.** The new server asks the holder to hand off. The holder
  stops its server, pushes and releases, then exits with `CentralStateHandedOffError` (exit code
  78). Nothing is lost. If the final push fails, the holder keeps the lease and exits with
  `CentralStateHandoffUnsavedError` (exit code 1): its unsaved changes are only on its machine, and
  a requester that takes over once the lease lapses restores the last save. If the holder cannot
  finish within 120 s, the request fails and the requester takes it back, so a holder that sees it
  late (for example during its own restore) keeps serving. A holder that is still renewing is
  never robbed.
- **A second home key.** It gives you a separate history. The two homes never merge.

After an **unclean** stop on another machine, the new server waits until the old lease has gone
unrenewed for 1.5 × `leaseTtl` (90 s), then takes it. A restart in the **same** pod after a crash
skips that wait: same host, same boot id, same `stateDir` and a dead pid prove the holder is gone.

A server that loses its lease stops itself with `CentralStateFencedError`, with no final push and
no release. A server whose lease lapses while it boots stops the same way, before it writes the
head or moves local files.

- `superseded` (exit code 78): another process took the lease or fenced the manifest.
- `expired` (exit code 1, so supervisors restart it): renewals failed for a full `leaseTtl`,
  usually because the store, broker or token was unavailable.

A head write that landed after it looked failed, or a push cut short at shutdown, is not a fence:
the next push finds this server's own commit at the head, adopts it and logs `Adopted an
unconfirmed central state commit of this server.`

## Running on a Spaak runner

spaak-infra owns the start script. It should do the equivalent of this:

```sh
export T3CODE_HOME=/tmp/t3 T3CODE_CENTRAL_STATE_URL=http://session-broker.claude-runners.svc:8080/v1/t3/objects
export T3CODE_CENTRAL_STATE_HOME_KEY=<dev-id> T3CODE_CENTRAL_STATE_TOKEN_FILE=$SESSION_ENV_DIR/token
export T3CODE_TELEMETRY_ENABLED=false T3CODE_HOST_ID=spaak-<dev-id> IS_SANDBOX=1 NODE_OPTIONS=--max-old-space-size=3072
export CLAUDE_CODE_OAUTH_TOKEN=<per-developer>
t3 serve --host 0.0.0.0 --port 3773 & echo $! > /tmp/t3.pid
```

Also:

- Bake [managed policy](../user/managed-policy.md) into the image at `/etc/t3code/policy.json`:
  `{"telemetry": false, "connect": false}`.
- Set `PRETTY_HOSTNAME` in `/etc/machine-info`, so clients show a readable machine name.
- Mint pairing links with
  `t3 auth pairing create --base-url <public url> --base-dir "$T3CODE_HOME"`, and only **after** the
  server has started. CLI database writes made while no server holds the lease can be moved aside
  by a later restore. The URL that `t3 serve` prints names the pod IP, which is useless behind the
  router.
- Pin one T3 image for every developer. A newer image migrates the home, and older images then
  refuse it.

`T3CODE_HOST_ID` keeps [usage](../user/usage.md) history on one host instead of one per pod.
`NODE_OPTIONS` caps the heap; the pod's 16 GiB is shared with Claude Code, MCP servers and dev
servers, and going over ends the pod.

## Infra contract

This repo cannot provide these. spaak-infra must:

1. **Broker endpoint** `/v1/t3/objects/<key>`, in the `etag` dialect:
   - authenticates `Authorization: Bearer <token file contents>`;
   - confines each creator to their own prefix;
   - accepts the path charset `[A-Za-z0-9._/-]` and refuses `..` (T3 never sends an empty, `.` or
     `..` segment);
   - maps `If-None-Match: *` and `If-Match` to GCS `x-goog-if-generation-match` `0` or
     `<generation>`;
   - answers GET with 200 or 404, and PUT with 200, 201 or 204; both carry `ETag` (the quoted
     generation). A precondition failure is 412;
   - accepts unconditional `DELETE` with the same confinement, answering 204 (or 200 or 202), and
     404 only when the object is absent. Never answer 404 for an unsupported method: T3 reads it
     as deleted, and garbage collection would leak storage without a warning;
   - streams PUT bodies up to 5 GiB, which arrive with `Content-Length`, with request timeouts of
     at least 10 min;
   - runs at least 2 replicas. A broker outage longer than `leaseTtl` stops every T3 server.
2. **Token refresher.** Keep `$SESSION_ENV_DIR/token` valid for at least `leaseTtl` + 60 s while T3
   runs, whether or not Claude is active. Today the file is refreshed only when Claude runs a Bash
   command (`/etc/claude/session-env.sh`). The alternative is a long-lived per-home credential from
   the broker. After `leaseTtl` without a valid token, T3 stops itself.
3. **SIGTERM to T3.** A preStop hook or the post-session hook sends SIGTERM to the pid in
   `/tmp/t3.pid` and waits for it, or T3 runs as the pod's PID 1. Shutdown takes the server's own
   stop, up to `finalPushTimeout` waiting for a push already running, up to `finalPushTimeout` for
   the final push and up to 15 s for the release. Set `terminationGracePeriodSeconds >= 75`.
4. **Port and route.** Add a `t3` port to the per-session Service and a `t3-` host prefix to
   sessions-router. The route must pass WebSockets, keep `Host`, set `X-Forwarded-Proto=https`, and
   do no HTTP-level SSO, which breaks CORS, mobile and peer links. A stable per-developer host,
   routed to the lease holder, is strongly recommended. Without one, every client must add each new
   session URL as a route to the saved environment.
5. **Activity.** Count T3 activity as runner activity. Otherwise the idle shutdowns stop T3
   mid-turn.
6. **Bucket.** Use a per-developer, access-controlled, encrypted bucket with a documented
   data-retention process and a GDPR purge path. See [Data governance](#data-governance).
   - Do not enable GCS bucket retention policies, Bucket Lock or S3 Object Lock: `manifest.json`
     and `lease.json` are overwritten in place.
   - No age-based or prefix lifecycle rule may delete or archive anything under `homes/*/`.
     Blobs are deduplicated forever, so a referenced blob, such as the signing key or an old
     transcript chunk, keeps the age of its first upload, and deleting it makes the home
     unrestorable.

## Multi-developer limitations

Ranked by how likely each is to bite a team, worst first.

1. **One live server per developer.** A home key has one lease, and developers routinely run
   several Claude Code sessions, each in its own pod. Only one of them can serve T3 for that key.
   The others fail fast, take over (which stops the first one), or use a second key with a separate
   history. Central state does not merge histories.
2. **No isolation inside a server.** T3 has no user principal. Every paired device gets the
   standard grant, which includes terminals and full-access agents, and the server runs as root on
   a runner. Its file access reaches `/workspace/_sessions/*`, the gcloud token and T3's own
   `secrets/`. Session URLs are reachable by all of tech@, so the pairing token is the only
   boundary. Never share a home key or a pairing link between developers, and do not run a team
   server on a runner.
3. **The pod lives only while its Claude Code session does.** Runners stop after 30 minutes of
   session idle, after 1 hour of idle shutdown, and always after 24 hours. Until infra items 2 and
   5 land, T3 activity does not keep the pod alive and the broker token goes stale when Claude is
   idle, so T3 stops itself mid-turn.
4. **Each move can be a SIGKILL.** Without infra item 3, graceful RPO 0 does not apply, and the
   [recovery point](#recovery-point) table's SIGKILL rows are the expectation.
5. **Credentials are shared across the runner fleet.** The pod's gcloud identity and the session
   Postgres role are the same for every developer. That is why the store must sit behind the
   broker's per-creator confinement (infra item 1), never in a bucket or database the shared
   identity can reach. The session database is also deleted after 72 h unused.
6. **Clients follow the URL, not the developer.** A restored home keeps its environment id and
   auth, so paired devices keep working, but only if they can reach the new pod. Without a stable
   per-developer host, each device must add every new session URL as a route.
7. **Provider coverage is Claude only.** Only `claude` is installed on runners, and only the
   default Claude instance's home is synced. Codex and other providers need installing on each pod,
   and their sessions and logins do not move.
8. **Code does not move.** Worktrees, unpushed commits and `refs/t3/*` stay on the old pod.
   The runner's own checkpoint covers only `/workspace/spaak-technologies/*` repos and never covers
   `refs/t3/*`.
9. **Scheduled and webhook tasks barely work.** Tasks run in-process, in the pod's UTC zone. A
   fixed-time run more than 10 minutes late is marked missed, and session URLs are tailnet-only, so
   external webhooks cannot reach them.
10. **No attribution.** History records `user`, `agent` or `system`, never which developer acted,
    and T3 has no audit log. One home per developer is the only attribution there is.
11. **Version skew.** A newer image migrates the home, and older images then refuse it with
    `CentralStateMigrationRefusedError`. Pin one image for everyone.
12. **No hosted T3 Connect.** The hosted relay can mint full-scope sessions on a linked environment
    and makes the pod reachable from the internet. Keep `"connect": false`. Peer links break when a
    peer's URL changes, so relink them after a move.
13. **Restoring a very large home blocks the server thread.** Restore checks the downloaded
    database with `PRAGMA quick_check` and checkpoints it synchronously, about 2 s per GB. Lease
    renewals wait meanwhile, so a home above roughly 20 GB can let the lease lapse during boot. The
    server then stops with `CentralStateFencedError` (`expired`) before it changes anything; pushes
    skip the check.
14. **Garbage collection is fenced by time, not by a lock.** A holder deletes unreferenced blobs
    only while its lease is valid, and bounds each delete by the time left, because a taker steals
    no sooner. A delete that the network holds back for longer than half a `leaseTtl` can still
    reach the store after a new holder reused that blob.

## Operations

**Read the logs.** At startup the server logs the home key and the store host once. A restore logs:

```text
Restored T3 home <key> generation <g> (written <age> ago by <host>);
at least <n> committed events after it were lost.
```

The second clause appears only when the lease's last reported event head proves a loss. A
self-fence ends the server with `CentralStateFencedError` and its reason. Failed pushes log a
warning with the error tag and retry once the push spacing (at least 30 s) has passed. A store
error during boot is retried for up to 120 s.

**Recover set-aside files.** Restore moves what it replaces aside instead of deleting it:

- `<stateDir>/central-state/aside/<time>-<old epoch>/`, for the database and state files;
- `<claudeHome>/.central-aside/<time>-<old epoch>/`, for Claude transcripts (`projects/`) and task
  lists (`tasks/`).

Only the two newest directories in each location are kept. Older ones, set-aside databases
included, are deleted by later restores, so copy what you need from them soon; do not move them
back while a server runs.

**Roll back a home.** Stop every server for the key first, and check that `lease.json` has
`"holder": null`. Then write a history copy over `manifest.json`:

1. Read `manifests/<epoch>/<generation>.json`, or a generation listed in the head's `pinned`.
2. Give it a **new `epoch` UUID and a new `writeNonce`**. The new epoch makes every server restore
   from it instead of keeping its local database.
3. Set `sequenceFloor` to the larger of its own value and the current head's, so a later restore
   never reuses sequences clients have already seen.
4. Write it with the current head's version as the precondition (`If-Match` on the HTTP backend).

**Downgrade after an upgrade.** Roll back to the newest `pinned` entry whose `migrationHead` the
older image knows. Only the last generation before each of the 2 most recent migrations is pinned.
Everything committed after it is lost.

**Filesystem backend.** `file://` stores keep their CAS index in `<root>/cas.sqlite`, which needs
reliable SQLite locking. Use it for tests and for a single-writer local disk or RWO volume, never
on NFS shared between pods.

## Upstream coupling

Re-check these on every upstream merge. Central state relies on them without changing them.

- The `sqlite_sequence` row for `orchestration_events` and the `event-log-pruned-through` metadata
  row, against the resume guards in [`ws.ts`](../../apps/server/src/ws.ts) (thread resume and shell
  resume) and `canResumeShellFrom` in
  [`ShellStream.ts`](../../apps/server/src/orchestration-v2/ShellStream.ts). The restore floor works
  only while those guards send a snapshot for a cursor below the pruned-through mark.
- `recordedMigrations` and `BUILD_MIGRATION_HEAD` in
  [`databaseMigrationHead.ts`](../../apps/server/src/persistence/databaseMigrationHead.ts), and the
  guard order in
  [`cloud/servicePreflight.ts`](../../apps/server/src/cloud/servicePreflight.ts).
- Attachment and secret file naming: pending attachments and `.part` uploads in
  [`attachmentStore.ts`](../../apps/server/src/attachmentStore.ts), replay markers in
  [`auth/dpop.ts`](../../apps/server/src/auth/dpop.ts) and
  [`auth/replayMarkers.ts`](../../apps/server/src/auth/replayMarkers.ts), and temp files in
  [`auth/ServerSecretStore.ts`](../../apps/server/src/auth/ServerSecretStore.ts). A new file under
  `stateDir` is not synced until the allowlist in `StateFiles.ts` names it.
- `claudeTranscriptState` and `resumeThread` in
  [`ClaudeAdapterV2.ts`](../../apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts). The
  fork changes them so a transcript missing from every project directory fails the resume for a
  thread with T3 run history, which takes the fresh-session handoff. A session id the same runtime
  allocated (a new thread, or a rollback to thread start) is exempt, since Claude has not written
  its transcript yet. After a restart that exemption is gone, so such a thread resumes through the
  handoff. For the same reason the Claude replay testkit (`ClaudeAdapterV2.testkit.ts`) writes a
  transcript for every session a recording resumes.
- Claude's task lists under `<CLAUDE_CONFIG_DIR>/tasks/<session id>/`, which `readClaudeTaskList`
  in the same file reads back.
- `CLAUDE_CONFIG_DIR` resolution and the host session variables in
  [`ClaudeHome.ts`](../../apps/server/src/provider/Drivers/ClaudeHome.ts), and the claude-runner
  variable names stripped in `ClaudeProcessEnvironment.ts`. New runner variables need adding there.
- The `CentralState.supervise(config, { afterServerStop })` step in
  [`cli/server.ts`](../../apps/server/src/cli/server.ts). Settings' Reclaim runs as
  `afterServerStop`: it needs the snapshot loop's database reader closed, and its result is what
  the final push saves.

## Data governance

- History holds full tool output: query results, environment dumps and possibly production
  customer data. The store needs access control at least as strict as production read access.
- Blobs hold plaintext signing keys. The database plus `server-signing-key` is enough to mint any
  session on that home.
- Blobs also hold, in plaintext (only compressed), every source-control token and provider API key
  entered in T3 settings, in every retained and pinned generation.
- Deleting a thread is a soft delete. Its content stays in the database and in every retained
  generation and pinned generation. A purge request needs the whole home deleted from the store.
- Pushes that crash or come from a fenced server, and garbage collection a holder skips when its
  lease may have passed on, leave orphan blobs and history copies. v1 leaves them in place. Only a
  reference-aware sweep may remove them: one that holds the lease, marks every blob the head, the
  history copies and the pinned manifests reference, and deletes the rest. That sweep is future
  work; an age-based lifecycle rule is not a substitute (see infra item 6).
