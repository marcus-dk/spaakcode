# T3 sessions on Spaak runners (untested reference)

**Nothing in this directory has run against real infrastructure.** It is the pod side and the
contract for running T3 as its own session type on Spaak's runners: one T3 home per work session,
ephemeral pods, a session service that starts them, and a broker in front of the object store. The
T3 side is [central state](../../docs/operations/central-state.md).

| File                  | What it is                                                                               |
| --------------------- | ---------------------------------------------------------------------------------------- |
| `entrypoint.sh`       | Pod entrypoint. Its header lists the variables it reads                                  |
| `policy.example.json` | Managed policy; passes `t3 policy check`. Written to `/etc/t3code/policy.json` if absent |

## The pod

The session service creates one Kubernetes Job per pod, from the existing runner pod template, with:

- command `entrypoint.sh`, ideally under `tini` (Node as PID 1 does not reap orphaned processes
  that agents leave behind); T3 receives SIGTERM directly in either case;
- `T3_SESSION_ID`, `T3_SESSION_GENERATION`, `T3_SESSION_API`, `T3_SESSION_PUBLIC_URL` and, when the
  workspace checkpoint does not restore the repo, `T3_SESSION_REPO_URL` and `T3_SESSION_REPO_BRANCH`;
- the session token in a Secret mounted at `/var/run/t3-session/token`;
- `/run/t3` as an `emptyDir` with `medium: Memory` (tokens and credentials only);
- `terminationGracePeriodSeconds: 120`, `restartPolicy: Never`, `backoffLimit: 0`. The service, not
  Kubernetes, replaces pods, because each replacement needs a new generation;
- a readiness probe on `GET /.well-known/t3/environment` at port 3773. It answers once restore is done;
- the image: the built `t3` server and web bundle, `claude` at `/usr/local/bin/claude`, `codex` at
  `/usr/local/bin/codex`, `git`, `curl` 7.71 or later. All pods of a session must run one T3 image:
  a newer one migrates the home and older ones then refuse it.

The workspace checkpoint (restore before the entrypoint, checkpoint loop beside it) stays the pod
template's job. Repo, `T3CODE_HOME` (`/var/lib/t3`) and Claude and Codex homes below it must be the
same path on every pod of a session.

The entrypoint fetches the per-user environment and the Codex login, configures git, clones the repo
if missing, then `exec`s `t3 --host 0.0.0.0 --port 3773 --no-browser <repo>`. The default command
creates the project for the repo on first start. A background helper refreshes the token, writes the
Codex login back and answers pairing requests. It sets `T3CODE_CENTRAL_STATE_URL`, `_HOME_KEY`,
`_GENERATION`, `_TOKEN_FILE`, `_PUBLIC_URL`, `_IDLE_EXIT` (30 minutes unless `T3_SESSION_IDLE_EXIT`)
and `_CODEX_HOME`; the other variables keep their defaults, see the
[variable table](../../docs/operations/central-state.md#environment-variables). It never sets
`T3CODE_CENTRAL_STATE_TAKEOVER`: a newer generation takes the lease without it.

## Session-scoped API

One base URL per session, `T3_SESSION_API` (for example `https://broker/v1/t3/sessions/<id>`). Every
call carries `Authorization: Bearer <token>`. The token names the session, owner and generation.
Refuse a token whose session is not the one in the path. Whether the broker or the session service
answers each path is the infra owner's choice.

| Method and path                     | Answer                                                                                                                                                                                                                   |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `GET environment`                   | 200 with `NAME=value` lines (no quoting, one line each), or 404 for none. Lines the entrypoint skips: names that are not `[A-Za-z_][A-Za-z0-9_]*`, start with `T3CODE_`, `T3_SESSION_` or `LD_`, or are `PATH` or `HOME` |
| `GET codex-auth.json`               | 200 with the owner's Codex `auth.json`, or 404                                                                                                                                                                           |
| `PUT codex-auth.json`               | Store the rewritten login; 200, 201 or 204                                                                                                                                                                               |
| `POST token`                        | 200 with a new token, only when the caller's generation is the current one; 401 or 403 otherwise                                                                                                                         |
| `GET pairing-requests/next?wait=25` | Long poll. 200 with a request id (`[A-Za-z0-9_-]+`) as plain text, or 204 after `wait` seconds                                                                                                                           |
| `POST pairing-requests/<id>`        | Body is the JSON of `t3 auth pairing create --json`; 204                                                                                                                                                                 |
| `GET`/`PUT`/`DELETE objects/<key>`  | The object store, below                                                                                                                                                                                                  |

Tokens live at least 15 minutes; the helper refreshes every 5 (`T3_SESSION_TOKEN_REFRESH`) and
retries on failure. T3 re-reads the token file on every request. Keep each token valid for at least
`T3CODE_CENTRAL_STATE_LEASE_TTL` (60 s) plus 60 s: without a valid token for a full TTL, T3 stops
itself as `expired` (exit 1).

## Object store

`T3CODE_CENTRAL_STATE_URL` is `$T3_SESSION_API/objects`. T3 writes only keys under
`homes/<session id>/`. Map them to `gs://<bucket>/user_<owner>/t3/<session id>/…` and refuse any
other key. The scope is the session, not the user: a per-user scope would let one session read,
overwrite or fence the owner's other sessions.

What T3 sends, in the default `etag` dialect (`T3CODE_CENTRAL_STATE_HTTP_PRECONDITIONS`):

- `GET`: answer 200 with `ETag`, or 404.
- `PUT` with `If-None-Match: *` (create only) or `If-Match: <etag>`, or neither. Map them to GCS
  `x-goog-if-generation-match: 0` or `<generation>`. Answer 200, 201 or 204 with `ETag`; a failed
  precondition is 412 (409 also works). Bodies up to 5 GiB arrive with `Content-Length`; allow
  request timeouts of at least 10 minutes.
- `DELETE`, unconditional: 200, 202 or 204, and 404 only when the object is absent. Never answer 404
  for an unsupported method: T3 reads it as deleted.
- Key paths use `[A-Za-z0-9._/-]` with no empty, `.` or `..` segment; refuse anything else.

The `gcs` dialect sends `x-goog-if-generation-match` itself and reads `x-goog-generation` instead of
`ETag`.

**Generation fence.** Every request carries `x-t3-generation: <n>`, the pod's
`T3_SESSION_GENERATION`. Refuse a request whose header differs from the token's generation, or whose
generation is lower than the session record's current one, with 403 or 409 and `x-t3-fenced: 1`, on
every method. T3 then stops as superseded (exit 78) without writing again. A pod with a higher
generation takes the lease at once, without the 90 s wait.

**Storage rules.** Delete a session's prefix only when the session service ends it, after the
retention period. T3 never lists the bucket and never deletes a home. No retention policy, Bucket
Lock or age-based lifecycle rule may touch a live home: `manifest.json` and `lease.json` are
overwritten in place, and blobs keep the age of their first upload. The data holds full tool output,
signing keys and tokens typed into T3 settings: encrypt the bucket and restrict access at least as
strictly as production read access.

## Session service

- **Record:** id, owner, repo and branch, state (`running`, `sleeping`, `ended`), current generation,
  public URL, last activity, recent crash count.
- **Create and wake:** raise the generation, issue a token for it, create the Job and a Service with
  a `t3` port (3773).
- **Pod end, by container exit code:**
  - `0`: idle exit after a final save (`T3CODE_CENTRAL_STATE_IDLE_EXIT`), or a SIGTERM that saved.
    Mark `sleeping`.
  - `78`: superseded (a newer generation holds the session, or the broker fenced this one) or
    handed off. Do nothing.
  - anything else, or the pod vanished (eviction, node loss): crash. This includes exit 1 after a
    failed final save, which keeps the lease, and exit 1 after the lease expired. Start a
    replacement with the next generation; it takes the lease at once and restores the last save.
    Stop after 3 crashes in 10 minutes and mark the session failed; keep its home.
- **Open:** wake if sleeping, then pair (below).
- **Activity:** T3 stops itself after the idle interval with no unfinished run and no open TCP
  connection to its port. A router or health check that holds a pooled connection open keeps it up;
  kubelet probes do not. Count T3 as active in the service's own idle rules, or the service stops
  the pod mid-turn. Inside the pod, `<T3CODE_HOME>/userdata/central-state/activity` is touched at
  most every 30 s while T3 is busy.
- **End:** the owner ends the session. Delete the Job, stop issuing tokens, and delete the store
  prefix after the retention period.
- **Identity:** the service must know which individual is calling, to list their sessions and to
  deliver pairing links only to the owner. The tailnet proves group membership only; this is open.

## Pairing

Every `tech@` member can reach every session URL, and a paired client has a root shell in the pod.
The pairing link is the only barrier.

1. The owner clicks Open. The service creates a request id and holds the browser.
2. The pod's helper, once T3 answers its readiness URL, receives the id from
   `GET pairing-requests/next`, runs
   `t3 auth pairing create --base-url $T3_SESSION_PUBLIC_URL --ttl 2m --json --label "session-service <id>"`
   and posts the JSON to `POST pairing-requests/<id>`.
3. The service redirects the owner's browser to `pairUrl` from that JSON, then discards it.

Never log the JSON or the link. Expire unanswered requests after 2 minutes. A paired browser
reconnects after a pod move with no action, because the home keeps its auth state. After a pod
death (not a graceful stop), a revocation made in the last minutes can be undone by the restore;
revoke again.

## Router

Add a `t3-` host prefix to sessions-router, for example `t3-s-<hash>.sessions.internal.spaak.ai`,
routed to the session's Service port 3773. Pass WebSockets, keep `Host`, set
`X-Forwarded-Proto: https`, and add no HTTP-level SSO, which breaks CORS and mobile clients.

## Credentials

All arrive through `GET environment` for the session's owner, are never shared between users and
never go into the policy or the store. Every process in the pod, agents included, can read them.

- **Claude:** `CLAUDE_CODE_OAUTH_TOKEN` from `claude setup-token`. An `ANTHROPIC_API_KEY` in the
  same environment outranks it; send one or the other.
- **Codex:** the owner's `auth.json` through `codex-auth.json`. Codex rewrites it when it refreshes,
  so the helper puts it back when its checksum changes; a refresh in the last moments before a pod
  dies is lost. **Unverified:** whether two parallel sessions with one login invalidate each other's
  copy. Test before relying on it. `OPENAI_API_KEY` through `environment` avoids the question.
- **Git:** a credential that can push the session's repo, as `GH_TOKEN`. The entrypoint points git's
  credential helper at it. A GitHub App installation token expires after an hour and this reference
  does not refresh it; a per-user fine-grained token does not have that problem. Which to use is open.

## Not provided

The session service, the broker endpoints, the router change, the Job template, the image, a warm
pool, a session list page, user identity, the workspace checkpoint, production read access and the
MCP gateway for T3 sessions, per-user profile bundles, and any test of this directory.
