#!/bin/sh
# UNTESTED REFERENCE. Pod entrypoint for a T3 session on Spaak's runners; the contract is README.md.
#
# Required (set on the Job by the session service):
#   T3_SESSION_ID          stable session id; the T3 home key ([A-Za-z0-9][A-Za-z0-9._-]{0,127})
#   T3_SESSION_GENERATION  this pod's generation, an integer >= 1, raised for every new pod
#   T3_SESSION_API         session-scoped base URL, e.g. https://broker/v1/t3/sessions/<id>
#   T3_SESSION_PUBLIC_URL  where browsers reach this pod, e.g. https://t3-s-<hash>.sessions.internal.spaak.ai
# Optional:
#   T3_SESSION_TOKEN_SOURCE   initial bearer token file (default /var/run/t3-session/token)
#   T3_SESSION_REPO_DIR       checkout path, the same for every pod (default /workspace/repo)
#   T3_SESSION_REPO_URL       cloned into T3_SESSION_REPO_DIR when that has no .git
#   T3_SESSION_REPO_BRANCH    branch for that clone
#   T3_SESSION_IDLE_EXIT      passed to T3CODE_CENTRAL_STATE_IDLE_EXIT (default 30m here; T3's own default is never)
#   T3_SESSION_TOKEN_REFRESH  seconds between token refreshes (default 300)
#   T3_SESSION_PORT           T3's port (default 3773)
#   T3_BIN                    the t3 command (default t3)
set -eu

log() { printf '[t3-entrypoint] %s\n' "$*" >&2; }
die() { log "$*"; exit 1; }

: "${T3_SESSION_ID:?}" "${T3_SESSION_GENERATION:?}" "${T3_SESSION_API:?}" "${T3_SESSION_PUBLIC_URL:?}"
case $T3_SESSION_ID in [!A-Za-z0-9]* | *[!A-Za-z0-9._-]*) die "bad T3_SESSION_ID" ;; esac
[ "${#T3_SESSION_ID}" -le 128 ] || die "T3_SESSION_ID is longer than 128"
case $T3_SESSION_GENERATION in '' | *[!0-9]* | 0*) die "bad T3_SESSION_GENERATION" ;; esac

T3_BIN=${T3_BIN:-t3}
PORT=${T3_SESSION_PORT:-3773}
REPO=${T3_SESSION_REPO_DIR:-/workspace/repo}
REFRESH=${T3_SESSION_TOKEN_REFRESH:-300}
API=${T3_SESSION_API%/}
RUN=/run/t3 # secrets only; an emptyDir with medium Memory in the pod template

# Before umask: every user may read the policy, only root may write it.
if [ ! -e /etc/t3code/policy.json ]; then
  mkdir -p /etc/t3code
  chmod 755 /etc/t3code
  cp "$(dirname "$0")/policy.example.json" /etc/t3code/policy.json
  chmod 644 /etc/t3code/policy.json
fi

umask 077
mkdir -p "$RUN"
cp "${T3_SESSION_TOKEN_SOURCE:-/var/run/t3-session/token}" "$RUN/token"

# The token goes to curl through a file, never on a command line.
auth_header() { printf 'Authorization: Bearer %s\n' "$(cat "$RUN/token")" >"$RUN/auth-header"; }
auth_header

# api METHOD PATH [curl args]: body to $RUN/body, HTTP status (000 on transport failure) to stdout.
api() {
  method=$1 path=$2
  shift 2
  curl -sS --connect-timeout 10 --max-time 60 -o "$RUN/body" -w '%{http_code}' \
    -X "$method" -H "@$RUN/auth-header" "$@" "$API/$path"
}

# Per-user environment: NAME=value lines. 404 means none. Names that steer T3 or the loader are refused.
status=$(api GET environment --retry 5 --retry-all-errors) || true
case $status in
  200)
    while IFS= read -r line || [ -n "$line" ]; do
      case $line in '#'*) continue ;; *=*) ;; *) continue ;; esac
      name=${line%%=*}
      case $name in
        '' | [0-9]* | *[!A-Za-z0-9_]* | T3CODE_* | T3_SESSION_* | LD_* | PATH | HOME) log "skipped variable $name" ;;
        *) export "$name=${line#*=}" ;;
      esac
    done <"$RUN/body"
    ;;
  404) ;;
  *) die "environment: HTTP $status" ;;
esac
rm -f "$RUN/body"

T3CODE_HOME=/var/lib/t3
CODEX_HOME=$T3CODE_HOME/codex
mkdir -p "$CODEX_HOME"

# Codex login. Codex rewrites it on refresh, so the helper below writes it back.
status=$(api GET codex-auth.json --retry 5 --retry-all-errors) || true
case $status in
  200) mv "$RUN/body" "$CODEX_HOME/auth.json" ;;
  404) rm -f "$RUN/body" ;;
  *) die "codex-auth.json: HTTP $status" ;;
esac

if [ -n "${GH_TOKEN:-}" ]; then
  # Single quotes on purpose: git's shell expands $GH_TOKEN at call time, so no token is on disk.
  # shellcheck disable=SC2016
  git config --global credential.https://github.com.helper \
    '!f() { test "$1" = get && printf "username=x-access-token\npassword=%s\n" "$GH_TOKEN"; }; f'
fi
if [ ! -d "$REPO/.git" ] && [ -n "${T3_SESSION_REPO_URL:-}" ]; then
  git clone ${T3_SESSION_REPO_BRANCH:+--branch "$T3_SESSION_REPO_BRANCH"} -- "$T3_SESSION_REPO_URL" "$REPO"
fi

export T3CODE_HOME CODEX_HOME
export T3CODE_CENTRAL_STATE_URL="$API/objects"
export T3CODE_CENTRAL_STATE_HOME_KEY="$T3_SESSION_ID"
export T3CODE_CENTRAL_STATE_GENERATION="$T3_SESSION_GENERATION"
export T3CODE_CENTRAL_STATE_TOKEN_FILE="$RUN/token"
export T3CODE_CENTRAL_STATE_PUBLIC_URL="$T3_SESSION_PUBLIC_URL"
export T3CODE_CENTRAL_STATE_IDLE_EXIT="${T3_SESSION_IDLE_EXIT:-30m}"
export T3CODE_CENTRAL_STATE_CODEX_HOME="$CODEX_HOME"
export T3CODE_TELEMETRY_ENABLED=false T3CODE_HOST_ID="spaak-$T3_SESSION_ID" IS_SANDBOX=1
export NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=3072}"

codex_sum() { cksum "$CODEX_HOME/auth.json" 2>/dev/null || true; }

# Refreshes the token, writes the Codex login back and answers pairing requests, all by pull.
helper() {
  set +e
  refreshed=$(date +%s) refreshing=1 pushed=$(codex_sum)
  while :; do
    now=$(date +%s)
    if [ "$refreshing" = 1 ] && [ $((now - refreshed)) -ge "$REFRESH" ]; then
      status=$(api POST token)
      case $status in
        200) [ -s "$RUN/body" ] && mv "$RUN/body" "$RUN/token" && auth_header && refreshed=$now ;;
        401 | 403) refreshing=0 && log "token refresh refused (HTTP $status): a newer generation owns this session" ;;
        *) log "token refresh failed (HTTP $status)" ;;
      esac
    fi
    sum=$(codex_sum)
    if [ -n "$sum" ] && [ "$sum" != "$pushed" ]; then
      status=$(api PUT codex-auth.json -H 'Content-Type: application/json' --data-binary "@$CODEX_HOME/auth.json")
      case $status in 200 | 201 | 204) pushed=$sum ;; *) log "codex login write-back failed (HTTP $status)" ;; esac
    fi
    # Pairing writes T3's database, so only once the server (and its restore) is up.
    if ! curl -fsS -o /dev/null --max-time 5 "http://127.0.0.1:$PORT/.well-known/t3/environment" 2>/dev/null; then
      sleep 2
      continue
    fi
    status=$(api GET 'pairing-requests/next?wait=25')
    case $status in
      200) ;;
      204) continue ;;
      *) log "pairing poll failed (HTTP $status)" && sleep 5 && continue ;;
    esac
    request=$(cat "$RUN/body")
    case $request in '' | *[!A-Za-z0-9_-]*) log "bad pairing request id" && continue ;; esac
    if "$T3_BIN" auth pairing create --base-url "$T3_SESSION_PUBLIC_URL" --ttl 2m --json \
      --label "session-service $request" >"$RUN/pair.json" 2>/dev/null; then
      status=$(api POST "pairing-requests/$request" -H 'Content-Type: application/json' --data-binary "@$RUN/pair.json")
      [ "$status" = 204 ] || [ "$status" = 200 ] || log "pairing answer failed (HTTP $status)"
    else
      log "t3 auth pairing create failed"
    fi
    rm -f "$RUN/pair.json" "$RUN/body"
  done
}
helper &

# T3 replaces this shell, so SIGTERM and the pod's grace period are its own.
exec "$T3_BIN" --host 0.0.0.0 --port "$PORT" --no-browser "$REPO"
