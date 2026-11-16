# Central state on ephemeral runners

This page covers running `t3 serve` on machines that do not keep their disk, such as Kubernetes
pods. T3 still keeps its live database in local SQLite. Central state copies the T3 home to an
object store and restores it on the next machine. The code is in
[`apps/server/src/centralState/`](../../apps/server/src/centralState/). Central state is off
unless `T3CODE_CENTRAL_STATE_URL` is set.

The operating model is **one home per work session**. The home key is the session's stable id. A
session service outside T3 creates the pods, gives each new pod of a session a strictly higher
generation, and wakes a stopped session on a new pod. A broker in front of the object store rejects
requests from older generations, so a stale pod cannot write. A pod stops itself after a period with
no work and no client, and the next pod restores the home. The pod side and the contract with the
session service and broker are in
[`infra/spaak-runner/README.md`](../../infra/spaak-runner/README.md), which has not run against real
infrastructure.

## What it covers

A home is stored as a **state bundle** under `homes/<homeKey>/` in the store:

- `blobs/<sha256>`: content-addressed, zstd-compressed files. They hold database snapshots, state
  files and Claude and Codex transcript chunks.
- `manifest.json`: the head. A compare-and-swap write of this file is the only commit point, and
  it also fences out older servers.
- `manifests/<epoch>/<generation>.json`: history copies of the last 10 generations, plus the last
  generation from before each of the 2 most recent migrations.
- `lease.json`: the record of which process serves the home.
- `transcript-tail.json` and `tail/<sha256>`: transcript lines shipped since the head. See
  [Transcript fast lane](#transcript-fast-lane).

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
  the task lists under `<claudeHome>/tasks/` that Claude's task tools read back on resume;
- the Codex rollouts under `<codexHome>/sessions/` and `<codexHome>/archived_sessions/`, so Codex
  threads resume natively.

Transcripts and rollouts are uploaded append-only: a push sends only the complete lines added since
the last one.

### Transcript fast lane

Between pushes, every `T3CODE_CENTRAL_STATE_TRANSCRIPT_INTERVAL` (2 s), the server ships the complete
lines appended to `.jsonl` transcripts and rollouts, without a database snapshot. Each shipment
uploads one `tail/<sha256>` chunk per grown file and rewrites `transcript-tail.json`, a small
compare-and-swap index naming the head it extends (epoch and generation) and the chunks past it.
The head stays the commit point for everything else.

- It reads only the appended range of a file, and with nothing appended it writes nothing. When
  pushes stall, a file whose tail reaches 64 chunks is shipped again as one chunk from the head.
- It never runs while a push runs. A push takes everything the tail held into the head, empties
  the index and deletes the chunks.
- Restore appends the tail to the transcripts it restores only when the index extends the head being
  restored. It then carries the tail onto its own head, so a crash before its first push keeps it. A
  tail that extends anything else is logged, emptied and its chunks deleted. A tail chunk that is
  missing or fails its sha256 check restores the home without the tail, with a warning.
- Only the head's holder writes the index. A superseded server's shipment fails and stops it like
  a lost push, and a server whose lease is lost ships nothing.
- Only `.jsonl` lines ship. Task lists, Claude's tool-result files and rewritten transcripts wait
  for the next push, so a restored transcript can name a tool-result file that is missing.

An active session sends about one chunk upload and one index write per interval.

Symbolic links under the T3 home are never followed or synced, so a symlinked `settings.json`,
theme or secret stays behind.

It does **not** carry:

- **Worktrees and unpushed code.** On the next pod, `restoreMissingWorktree` silently recreates a
  missing worktree from the pushed branch or `HEAD`. Push work you want to keep.
- **`refs/t3/*` checkpoint refs.** Full diffs and rollback of restored turns fail.
- `<baseDir>/scratch` and `<baseDir>/projects`.
- Terminal scrollback and logs.
- The other provider homes.
- The `homePath` of a custom Claude instance, and the **CODEX_HOME path** of a custom Codex
  instance. Only instances without one use the synced homes. A Codex **ChatGPT connection**
  (managed setup) does not read `CODEX_HOME` and keeps its sessions in `~/.codex`, so they do not
  move.
- Claude's own login in the Claude home (its OAuth or credentials file). Supply it per owner
  with `CLAUDE_CODE_OAUTH_TOKEN`, or with an API key, Bedrock or Vertex. Keys set as provider
  environment variables in T3 settings are in `secrets/` and are synced.
- Other Claude state, such as `plans/`. Plan-mode files mostly repeat what the transcript holds.
- Codex's login and everything else in the Codex home: `auth.json`, `auth.lock`, `config.toml`,
  `history.jsonl`, `session_index.jsonl`, its state database (`state_*.sqlite`), logs and caches.
  Write `auth.json` into the Codex home on every pod, or supply `OPENAI_API_KEY`. Codex rebuilds
  its state database from the rollouts.

## Environment variables

Central state is configured through environment variables only. There are no CLI flags.

| Variable                                   | Default                     | Meaning                                                                                                                                                                 |
| ------------------------------------------ | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `T3CODE_CENTRAL_STATE_URL`                 | unset, meaning **disabled** | `file:///abs/dir` selects the filesystem backend. `http(s)://…` selects the HTTP backend; object keys are appended to this base URL. Any other scheme is a config error |
| `T3CODE_CENTRAL_STATE_HOME_KEY`            | required when URL is set    | The session id; one live server per key. Must match `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`                                                                                |
| `T3CODE_CENTRAL_STATE_TOKEN_FILE`          | unset                       | Bearer token file, re-read on every request (HTTP only)                                                                                                                 |
| `T3CODE_CENTRAL_STATE_HTTP_PRECONDITIONS`  | `etag`                      | `etag` (the Spaak broker, or another Bearer-token gateway) or `gcs` (GCS XML API, which uses `x-goog-generation`). S3, R2 and MinIO need SigV4 and are not supported    |
| `T3CODE_CENTRAL_STATE_CLAUDE_HOME`         | `<baseDir>/claude`          | Absolute path. T3's Claude children use it as `CLAUDE_CONFIG_DIR`                                                                                                       |
| `T3CODE_CENTRAL_STATE_CODEX_HOME`          | `<baseDir>/codex`           | Absolute path. T3's Codex children and terminals use it as `CODEX_HOME`                                                                                                 |
| `T3CODE_CENTRAL_STATE_TAKEOVER`            | `false`                     | Ask a live holder to hand off instead of failing fast. Not needed with generations                                                                                      |
| `T3CODE_CENTRAL_STATE_GENERATION`          | unset                       | Positive integer the session service gives each new pod of a session. Sent as `x-t3-generation` on every request. See [Generations](#generations)                       |
| `T3CODE_CENTRAL_STATE_PUBLIC_URL`          | unset                       | The URL clients reach this server at. Only logged at startup, as `Serving T3 home <key> at <url>.`; pairing links are unchanged                                         |
| `T3CODE_CENTRAL_STATE_DEBOUNCE`            | `10s`                       | Quiet time after the last history write before a push                                                                                                                   |
| `T3CODE_CENTRAL_STATE_MIN_SPACING`         | `30s`                       | Floor on the time between push starts; values below `2s` are raised to `2s`. See [Recovery point](#recovery-point)                                                      |
| `T3CODE_CENTRAL_STATE_MAX_INTERVAL`        | `2m`                        | Longest a history-dirty home waits during continuous activity                                                                                                           |
| `T3CODE_CENTRAL_STATE_IDLE_INTERVAL`       | `10m`                       | Cadence for non-history DB writes and the files-only sweep                                                                                                              |
| `T3CODE_CENTRAL_STATE_TRANSCRIPT_INTERVAL` | `2s`                        | How often appended transcript and rollout lines ship between pushes. `0` turns the [fast lane](#transcript-fast-lane) off                                               |
| `T3CODE_CENTRAL_STATE_IDLE_EXIT`           | unset, meaning never        | Stop cleanly (exit 0) after this long with no unfinished run and no client. Zero or unset never stops. See [Idle exit](#idle-exit)                                      |
| `T3CODE_CENTRAL_STATE_LEASE_TTL`           | `60s`                       | Lease lifetime. The holder renews every quarter of it and stops itself after a full TTL without a renewal                                                               |
| `T3CODE_CENTRAL_STATE_FINAL_PUSH_TIMEOUT`  | `20s`                       | Bound on the shutdown push, which retries within it, and on the wait for a push already running at shutdown                                                             |

When central state is on, T3 sets `CLAUDE_CONFIG_DIR` to the Claude home and `CODEX_HOME` to the
Codex home before the server starts, replacing any value the runner set, and creates both with mode
`0700`. It also removes the variables that bind a process to the runner's own Claude Code session
(`CLAUDECODE`, `CLAUDE_CODE_SESSION_*`, `CLAUDE_CODE_REMOTE*`, `CCR_*` and the others in
`ProviderProcessEnvironment.ts`). Terminals and provider children inherit the result. It keeps
`CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_*`, `CLAUDE_CODE_USE_BEDROCK`, `CLAUDE_CODE_USE_VERTEX` and
`CLAUDE_CODE_MAX_RETRIES`.

## Required stable paths

Projections store absolute paths, and the loopback cookie name hashes `stateDir`. These paths must
therefore be the same on every machine that serves a home:

- `T3CODE_HOME` (the base dir, and the state dir below it);
- the Claude home;
- the Codex home, once it holds a rollout, because Codex threads are grouped by their home path;
- the repository checkout paths of every project.

Restore refuses a different `baseDir`, `stateDir` or Claude home, and a different Codex home when
the save holds Codex rollouts, before it downloads anything. The error is
`CentralStatePathMismatchError`, and it names the field with its recorded and current values. To
recover, set `T3CODE_HOME`, `T3CODE_CENTRAL_STATE_CLAUDE_HOME` or `T3CODE_CENTRAL_STATE_CODEX_HOME`
to the recorded value and start again. Restore does not check checkout paths. Clone each project to the same path,
otherwise its threads point at a missing directory.

## Recovery point

| Situation                                                                                                                                                                | Committed history lost                                                                                                                                                  |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Pod dies at least `debounce` (10 s) plus one push duration after activity settles                                                                                        | 0                                                                                                                                                                       |
| OOM, SIGKILL or node loss during continuous activity                                                                                                                     | Up to `maxInterval` (2 min) plus one push duration, plus at most the adaptive spacing when pushes are slow                                                              |
| Other non-history DB writes (scheduled-task edits) and settings, keybindings or themes edits, then SIGKILL                                                               | Up to `idleInterval` (10 min) plus one push                                                                                                                             |
| Graceful SIGTERM or [idle exit](#idle-exit) **delivered to the T3 process**, with a pod grace period as in [infra item 3](#infra-contract), when the final push succeeds | 0. If it fails, the lease is kept, so a restart in the same pod keeps the local home; another pod restores the last save                                                |
| Takeover handoff whose final push succeeds within `finalPushTimeout`                                                                                                     | 0. If it fails, the holder keeps the lease and exits with `CentralStateHandoffUnsavedError`; a requester that then takes over restores the last save                    |
| Store or broker outage                                                                                                                                                   | Pushes retry. After `leaseTtl` (60 s) with no renewal, T3 stops itself. A restart in the same pod keeps the local home, so nothing is lost once the store answers again |
| This server is superseded (another pod took the lease)                                                                                                                   | Everything since this server's last commit, by design                                                                                                                   |

Pushes are spaced at least `max(minSpacing, 20 × the last successful push's duration)` apart
(`minSpacing` is 30 s by default), so snapshot work stays near 5% of one core. On a large database
that spacing, not `maxInterval`, sets the loss window. A failed push only restarts the `minSpacing`
floor, so an outage does not stretch it.

Session and pairing-link changes (pairing, revoking a device, redeeming a single-use link) are
pushed on the history cadence, not the idle one. A lossy restore can still undo one made in the
last `debounce` plus a push, which brings a revoked session back to life or lets a redeemed link
work again until it expires. After a move that followed a pod death, revoke again anything you
revoked in the minutes before it.

Kubelet signals PID 1. Graceful RPO 0 therefore needs T3 to be PID 1 or to receive the forwarded
SIGTERM, and a grace period as in the [infra contract](#infra-contract). Without them, every pod
death is a SIGKILL.

In-flight runs reappear as "Server restarted" through the normal startup reconcile. With
**Continue threads after restarts** on (the default), a continuation is queued. After a lossy
restore, that continuation can repeat side effects the lost tail already performed, such as a push
or a PR comment. Turn the setting off if that matters to you.

The [transcript fast lane](#transcript-fast-lane) protects the provider's context, not the thread.
After a hard kill, Claude transcripts and Codex rollouts are restored to within
`transcriptInterval` (2 s) plus one shipment of the kill, so the model resumes knowing what it did. The T3 thread, its
messages and activity still lose up to the push interval above, so the model can remember work the
visible thread does not show. Lines written in the last interval are missing, and a thread whose
transcript is missing entirely, such as one started then, continues through the lossy context
handoff. With the fast lane off, everything written after the last push is missing.

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

- `superseded` (exit code 78): another process took the lease or fenced the manifest, or the
  broker refused this server's generation.
- `expired` (exit code 1, so supervisors restart it): renewals failed for a full `leaseTtl`,
  usually because the store, broker or token was unavailable.

### Generations

With one home per work session, the session service outside T3 creates the pods and gives each new
pod of a session a strictly higher `T3CODE_CENTRAL_STATE_GENERATION`. The lease records it on the
holder, and the manifest's `holder` carries it. When both the holder and the new server have a
generation:

- a holder with a **lower** generation is taken over at once, with no 1.5 × `leaseTtl` wait, because
  the broker already refuses the older pod;
- a holder with a **higher** generation stops the new server at once with `CentralStateFencedError`
  (`superseded`, exit code 78): it is the stale pod.

An equal generation, or none on either side, keeps the behaviour above. Older builds ignore the
field.

A head write that landed after it looked failed, or a push cut short at shutdown, is not a fence:
the next push finds this server's own commit at the head, adopts it and logs `Adopted an
unconfirmed central state commit of this server.`

## Running on a Spaak runner

[`infra/spaak-runner/`](../../infra/spaak-runner/) holds the pod entrypoint, a managed policy and
the contract. In short, the pod sets

```sh
export T3CODE_HOME=/var/lib/t3 T3CODE_CENTRAL_STATE_URL=<session API>/objects
export T3CODE_CENTRAL_STATE_HOME_KEY=<session id> T3CODE_CENTRAL_STATE_GENERATION=<n>
export T3CODE_CENTRAL_STATE_TOKEN_FILE=/run/t3/token
export T3CODE_CENTRAL_STATE_PUBLIC_URL=https://t3-s-<hash>.sessions.<domain>
export T3CODE_CENTRAL_STATE_IDLE_EXIT=30m
exec t3 --host 0.0.0.0 --port 3773 --no-browser <repo>
```

and also:

- Install a [managed policy](../user/managed-policy.md) at `/etc/t3code/policy.json`, root-writable
  only. Turn off `connect` (a tunnel is invisible to idle exit and exposes the pod), telemetry,
  updates and webhook tasks.
- Mint pairing links with `t3 auth pairing create --base-url <public url> --json`, and only
  **after** the server has started. CLI database writes made while no server holds the lease can be
  moved aside by a later restore. The URL that `t3 serve` prints names the pod IP, which is useless
  behind the router.
- Pin one T3 image for every pod of a session. A newer image migrates the home, and older images
  then refuse it.
- Set `T3CODE_HOST_ID` so [usage](../user/usage.md) history stays on one host, and `NODE_OPTIONS`
  to cap the heap; the pod's memory is shared with the agents and anything they run.

### Idle exit

With `T3CODE_CENTRAL_STATE_IDLE_EXIT` set, the server stops itself once both have held for that
long: no run is unfinished in any thread (queued, starting, running, or waiting on an approval or
an answer) and no effect is due, and no TCP connection to the server's port is open. It logs

```text
No unfinished runs and no clients for <duration>; stopping the server (T3CODE_CENTRAL_STATE_IDLE_EXIT).
```

then stops as it does on SIGTERM: the final push, the lease release, exit code 0. If the final
push fails it keeps the lease and exits with `CentralStateIdleUnsavedError` (exit code 1), so a
container restart in the same pod keeps the local home. A session service can treat exit 0 as
sleeping.

- Activity is sampled every 10 s, so the stop comes up to 10 s after the interval.
- Clients are read from the kernel's socket table (`/proc/net/tcp` and `tcp6`), because the
  database records connects but not disconnects. Every established connection counts: browsers and
  mobile clients, loopback connections from provider MCP calls, and keep-alive connections, which
  the server holds for 95 s after their last request. A proxy or health check that keeps a pooled
  connection open keeps the server up; kubelet probes do not. Clients that reach the server
  through T3 Connect arrive on an outbound tunnel and are not seen, so turn T3 Connect off.
- Off Linux, or with `--port 0`, clients cannot be read, and the server never stops for
  idleness. The same holds while the database cannot be read.
- A run stuck in an unfinished state, such as an approval nobody answers, keeps the server up.
  A pending effect that is not yet due (a retry backoff or a resume at a usage-limit reset) does
  not; it runs when the session next starts.

Whether or not idle exit is on, the server touches `<stateDir>/central-state/activity` while it is
busy by this definition, at most every 30 s. Its modification time tells something inside the pod
when the home was last in use, without reading the database. The file is not synced.

## Infra contract

This repo cannot provide these. The session service and broker must, as detailed in
[`infra/spaak-runner/README.md`](../../infra/spaak-runner/README.md):

1. **Broker endpoint** `<session API>/objects/<key>`, in the `etag` dialect:
   - authenticates `Authorization: Bearer <token file contents>`, and confines each session to its
     own prefix: T3 writes only `homes/<session id>/…`;
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
   - runs at least 2 replicas. A broker outage longer than `leaseTtl` stops every T3 server;
   - reads `x-t3-generation` and refuses a request from a generation older than the session's
     current one, on every method, with 409 or 403 and `x-t3-fenced: 1`. T3 never retries that
     answer. The lease renewal and every push treat it as `superseded`, and the server stops
     without a final push or release. Without the header, 409 on PUT is a precondition failure and
     403 is a store error.
2. **Token.** Keep the token file valid for at least `leaseTtl` + 60 s for as long as T3 runs. The
   reference entrypoint refreshes it by pull. After `leaseTtl` without a valid token, T3 stops
   itself.
3. **SIGTERM to T3.** T3 is PID 1 or receives the forwarded signal. Shutdown takes the server's own
   stop, up to `finalPushTimeout` waiting for a push already running, up to `finalPushTimeout` for
   the final push and up to 15 s for the release. Set `terminationGracePeriodSeconds >= 75`.
4. **Generations and restarts.** The session service, not Kubernetes, replaces pods, because each
   replacement needs a higher generation. Exit 0 means idle exit, 78 means superseded or handed
   off, anything else is a crash.
5. **Port and route.** Add a `t3` port to the per-session Service and a `t3-` host prefix to the
   router, one stable host per session. The route must pass WebSockets, keep `Host`, set
   `X-Forwarded-Proto=https`, and do no HTTP-level SSO, which breaks CORS, mobile and peer links.
6. **Activity.** Count T3 activity as session activity, or an outside idle rule stops T3 mid-turn.
7. **Bucket.** Use an access-controlled, encrypted bucket with a documented data-retention process
   and a GDPR purge path. See [Data governance](#data-governance).
   - Delete a session's prefix only when the session ends. T3 never lists the bucket or deletes a
     home.
   - Do not enable GCS bucket retention policies, Bucket Lock or S3 Object Lock: `manifest.json`
     and `lease.json` are overwritten in place.
   - No age-based or prefix lifecycle rule may delete or archive anything under `homes/*/`.
     Blobs are deduplicated forever, so a referenced blob, such as the signing key or an old
     transcript chunk, keeps the age of its first upload, and deleting it makes the home
     unrestorable.

## Limitations

1. **One live server per session.** A session has one lease. A second pod of the same session
   must carry a higher generation, which stops the first. Central state does not merge histories.
2. **No isolation inside a server.** T3 has no user principal. Every paired device gets the
   standard grant, which includes terminals and full-access agents, and the server runs as root in
   the pod. Its file access reaches every credential in the pod. Session URLs are reachable by a
   whole group, so the pairing token is the only boundary. Never share a pairing link between
   people, and do not run a team server in a pod. Hand out pairing links only to the session's
   owner.
3. **Each move can be a SIGKILL.** Eviction and node loss skip the final push, and the
   [recovery point](#recovery-point) table's SIGKILL rows are the expectation.
4. **Credentials are per owner and unprotected in the pod.** Anything in the pod's environment is
   readable by every agent. Provider logins are not synced: the pod supplies them
   (`CLAUDE_CODE_OAUTH_TOKEN`, the Codex `auth.json`, API keys).
5. **Clients follow the URL.** A restored home keeps its environment id and auth, so paired
   devices keep working, but only if they can reach the new pod. Use one stable host per session.
6. **Provider coverage is Claude and Codex.** Only the default Claude and Codex instances' homes
   are synced. Other providers need installing on each pod, and their sessions do not move.
7. **Code does not move.** Worktrees, unpushed commits and `refs/t3/*` stay on the old pod unless
   the pod's workspace checkpoint carries them.
8. **Scheduled and webhook tasks barely work.** Tasks run in-process, in the pod's UTC zone. A
   fixed-time run more than 10 minutes late is marked missed, a sleeping session runs none, and
   pods are not reachable by external webhooks.
9. **No attribution.** History records `user`, `agent` or `system`, never which person acted, and
   T3 has no audit log. One home per session and owner is the only attribution there is.
10. **Version skew.** A newer image migrates the home, and older images then refuse it with
    `CentralStateMigrationRefusedError`. Once a save holds Codex rollouts, its manifest needs
    reader version 2, and images from before Codex sync refuse it with
    `CentralStateUnsupportedManifestError` instead of dropping the rollouts on their next push.
    Pin one image per session.
11. **No hosted T3 Connect.** The hosted relay can mint full-scope sessions on a linked environment
    and makes the pod reachable from the internet. Keep `"connect": false`. Peer links break when a
    peer's URL changes, so relink them after a move.
12. **Restoring a very large home blocks the server thread.** Restore checks the downloaded
    database with `PRAGMA quick_check` and checkpoints it synchronously, about 2 s per GB. Lease
    renewals wait meanwhile, so a home above roughly 20 GB can let the lease lapse during boot. The
    server then stops with `CentralStateFencedError` (`expired`) before it changes anything; pushes
    skip the check.
13. **Garbage collection is fenced by time, not by a lock.** A holder deletes unreferenced blobs
    only while its lease is valid, and bounds each delete by the time left, because a taker steals
    no sooner. A delete that the network holds back for longer than half a `leaseTtl` can still
    reach the store after a new holder reused that blob.

## Operations

**Read the logs.** At startup the server logs the home key, the store host and the generation once,
and `Serving T3 home <key> at <url>.` when `T3CODE_CENTRAL_STATE_PUBLIC_URL` is set. A restore logs:

```text
Restored T3 home <key> generation <g> (written <age> ago by <host>);
at least <n> committed events after it were lost.
```

The second clause appears only when the lease's last reported event head proves a loss. A
self-fence ends the server with `CentralStateFencedError` and its reason. Failed pushes log a
warning with the error tag and retry once the push spacing (at least `minSpacing`) has passed. A store
error during boot is retried for up to 120 s.

**Recover set-aside files.** Restore moves what it replaces aside instead of deleting it:

- `<stateDir>/central-state/aside/<time>-<old epoch>/`, for the database and state files;
- `<claudeHome>/.central-aside/<time>-<old epoch>/`, for Claude transcripts (`projects/`) and task
  lists (`tasks/`);
- `<codexHome>/.central-aside/<time>-<old epoch>/`, for Codex rollouts (`sessions/` and
  `archived_sessions/`).

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
  variable names stripped in `ProviderProcessEnvironment.ts`. New runner variables need adding there.
- Codex's use of `CODEX_HOME` for an instance with no home path, in
  [`CodexDriver.ts`](../../apps/server/src/provider/Drivers/CodexDriver.ts), and its layout:
  `thread/resume` finds a rollout by thread id under `sessions/` or `archived_sessions/` when its
  state database lacks the thread. A rollout missing on resume fails `thread/resume`, and
  `ProviderTurnStartService.ts` then starts a fresh native session with the context handoff.
- The `orchestration_v2_projection_runs.status` final values (`OrchestrationV2RunStatus`) and the
  `orchestration_v2_effect_outbox` status and `available_at` columns, which idle exit reads in
  `DatabaseSnapshot.ts`. A query that stops working disables idle exit, with a warning.
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
- Pushes and transcript shipments that crash or come from a fenced server, and garbage collection a
  holder skips when its lease may have passed on, leave orphan blobs, tail chunks and history
  copies. v1 leaves them in place. Only a reference-aware sweep may remove them: one that holds the
  lease, marks every blob the head, the history copies and the pinned manifests reference, and
  deletes the rest. That sweep is future work; an age-based lifecycle rule is not a substitute (see
  infra item 7).
