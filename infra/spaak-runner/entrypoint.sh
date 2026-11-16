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

# T3EP_*, so the per-user environment imported below cannot overwrite them.
T3EP_BIN=${T3_BIN:-t3}
T3EP_PORT=${T3_SESSION_PORT:-3773}
T3EP_REPO=${T3_SESSION_REPO_DIR:-/workspace/repo}
T3EP_REFRESH=${T3_SESSION_TOKEN_REFRESH:-300}
T3EP_API=${T3_SESSION_API%/}
T3EP_RUN=/run/t3 # secrets only; an emptyDir with medium Memory in the pod template

# Before umask: every user may read the policy, only root may write it.
if [ ! -e /etc/t3code/policy.json ]; then
  mkdir -p /etc/t3code
  chmod 755 /etc/t3code
  cp "$(dirname "$0")/policy.example.json" /etc/t3code/policy.json
  chmod 644 /etc/t3code/policy.json
fi

umask 077
mkdir -p "$T3EP_RUN"
cp "${T3_SESSION_TOKEN_SOURCE:-/var/run/t3-session/token}" "$T3EP_RUN/token"

# The token goes to curl through a file, never on a command line.
auth_header() { printf 'Authorization: Bearer %s\n' "$(cat "$T3EP_RUN/token")" >"$T3EP_RUN/auth-header"; }
auth_header

# api METHOD PATH [curl args]: body to $T3EP_RUN/body, HTTP status (000 on transport failure) to stdout.
# Every call carries the generation, so the API can refuse a superseded pod as the broker does.
api() {
  method=$1 path=$2
  shift 2
  curl -sS --connect-timeout 10 --max-time 60 -o "$T3EP_RUN/body" -w '%{http_code}' \
    -X "$method" -H "@$T3EP_RUN/auth-header" -H "x-t3-generation: $T3_SESSION_GENERATION" \
    "$@" "$T3EP_API/$path"
}

# Per-user environment: NAME=value lines. 404 means none. Names that steer T3 or the loader are refused.
status=$(api GET environment --retry 5 --retry-all-errors) || true
case $status in
  200)
    while IFS= read -r line || [ -n "$line" ]; do
      case $line in '#'*) continue ;; *=*) ;; *) continue ;; esac
      name=${line%%=*}
      case $name in
        '' | [0-9]* | *[!A-Za-z0-9_]* | T3CODE_* | T3_SESSION_* | T3EP_* | LD_* | PATH | HOME) log "skipped variable $name" ;;
        *) export "$name=${line#*=}" ;;
      esac
    done <"$T3EP_RUN/body"
    ;;
  404) ;;
  *) die "environment: HTTP $status" ;;
esac
rm -f "$T3EP_RUN/body"

T3CODE_HOME=/var/lib/t3
CODEX_HOME=$T3CODE_HOME/codex
mkdir -p "$CODEX_HOME"

# Codex login. Codex rewrites it on refresh, so the helper below writes it back.
status=$(api GET codex-auth.json --retry 5 --retry-all-errors) || true
case $status in
  200) mv "$T3EP_RUN/body" "$CODEX_HOME/auth.json" ;;
  404) rm -f "$T3EP_RUN/body" ;;
  *) die "codex-auth.json: HTTP $status" ;;
esac

if [ -n "${GH_TOKEN:-}" ]; then
  # Single quotes on purpose: git's shell expands $GH_TOKEN at call time, so no token is on disk.
  # shellcheck disable=SC2016
  git config --global credential.https://github.com.helper \
    '!f() { test "$1" = get && printf "username=x-access-token\npassword=%s\n" "$GH_TOKEN"; }; f'
fi
if [ ! -d "$T3EP_REPO/.git" ] && [ -n "${T3_SESSION_REPO_URL:-}" ]; then
  git clone ${T3_SESSION_REPO_BRANCH:+--branch "$T3_SESSION_REPO_BRANCH"} -- "$T3_SESSION_REPO_URL" "$T3EP_REPO"
fi

export T3CODE_HOME CODEX_HOME
export T3CODE_CENTRAL_STATE_URL="$T3EP_API/objects"
export T3CODE_CENTRAL_STATE_HOME_KEY="$T3_SESSION_ID"
export T3CODE_CENTRAL_STATE_GENERATION="$T3_SESSION_GENERATION"
export T3CODE_CENTRAL_STATE_TOKEN_FILE="$T3EP_RUN/token"
export T3CODE_CENTRAL_STATE_PUBLIC_URL="$T3_SESSION_PUBLIC_URL"
export T3CODE_CENTRAL_STATE_IDLE_EXIT="${T3_SESSION_IDLE_EXIT:-30m}"
export T3CODE_CENTRAL_STATE_CODEX_HOME="$CODEX_HOME"
export T3CODE_TELEMETRY_ENABLED=false T3CODE_HOST_ID="spaak-$T3_SESSION_ID" IS_SANDBOX=1
export NODE_OPTIONS="${NODE_OPTIONS:---max-old-space-size=3072}"

codex_sum() { cksum "$CODEX_HOME/auth.json" 2>/dev/null || true; }

# A refused call means a newer generation owns this session: the helper stops, T3 follows on its own.
refused() {
  case $1 in 401 | 403) log "$2 refused (HTTP $1): a newer generation owns this session; helper stops" ;; *) return 1 ;; esac
}

# Refreshes the token, writes the Codex login back and answers pairing requests, all by pull.
helper() {
  set +e
  refreshed=$(date +%s) pushed=$(codex_sum)
  while :; do
    now=$(date +%s)
    if [ $((now - refreshed)) -ge "$T3EP_REFRESH" ]; then
      status=$(api POST token)
      refused "$status" "token refresh" && return
      case $status in
        200) [ -s "$T3EP_RUN/body" ] && mv "$T3EP_RUN/body" "$T3EP_RUN/token" && auth_header && refreshed=$now ;;
        *) log "token refresh failed (HTTP $status)" ;;
      esac
    fi
    sum=$(codex_sum)
    if [ -n "$sum" ] && [ "$sum" != "$pushed" ]; then
      status=$(api PUT codex-auth.json -H 'Content-Type: application/json' --data-binary "@$CODEX_HOME/auth.json")
      refused "$status" "codex login write-back" && return
      case $status in 200 | 201 | 204) pushed=$sum ;; *) log "codex login write-back failed (HTTP $status)" ;; esac
    fi
    # Pairing writes T3's database, so only once the server (and its restore) is up.
    if ! curl -fsS -o /dev/null --max-time 5 "http://127.0.0.1:$T3EP_PORT/.well-known/t3/environment" 2>/dev/null; then
      sleep 2
      continue
    fi
    status=$(api GET 'pairing-requests/next?wait=25')
    refused "$status" "pairing poll" && return
    case $status in
      200) ;;
      204) continue ;;
      *) log "pairing poll failed (HTTP $status)" && sleep 5 && continue ;;
    esac
    request=$(cat "$T3EP_RUN/body")
    case $request in '' | *[!A-Za-z0-9_-]*) log "bad pairing request id" && continue ;; esac
    if "$T3EP_BIN" auth pairing create --base-url "$T3_SESSION_PUBLIC_URL" --ttl 2m --json \
      --label "session-service $request" >"$T3EP_RUN/pair.json" 2>/dev/null; then
      status=$(api POST "pairing-requests/$request" -H 'Content-Type: application/json' --data-binary "@$T3EP_RUN/pair.json")
      rm -f "$T3EP_RUN/pair.json"
      refused "$status" "pairing answer" && return
      [ "$status" = 204 ] || [ "$status" = 200 ] || log "pairing answer failed (HTTP $status)"
    else
      log "t3 auth pairing create failed"
    fi
    rm -f "$T3EP_RUN/pair.json" "$T3EP_RUN/body"
  done
}
helper &

# T3 prints a one-time admin pairing link on start; the container log must never carry it. The
# filter ignores SIGTERM so it outlives T3's final save and ends when T3 closes its stdout.
mkfifo "$T3EP_RUN/stdout"
(
  trap '' TERM INT
  exec sed -u 's/#token=[^[:space:]]*/#token=REDACTED/g'
) <"$T3EP_RUN/stdout" &

# T3 replaces this shell, so SIGTERM and the pod's grace period are its own.
exec "$T3EP_BIN" --host 0.0.0.0 --port "$T3EP_PORT" --no-browser "$T3EP_REPO" >"$T3EP_RUN/stdout"
