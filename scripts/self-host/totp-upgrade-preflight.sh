#!/usr/bin/env sh
set -eu

usage() {
  cat <<'HELP'
Usage: totp-upgrade-preflight.sh -- RELEASE_COMMAND [ARG ...]

Read-only check through an already-running old release; no local Elixir needed.
The command prefix receives: rpc <self-contained preflight code>

Docker:
  sh scripts/self-host/totp-upgrade-preflight.sh -- docker exec OLD_CONTAINER /app/bin/codex_pooler
Kubernetes (pin the intended target explicitly):
  sh scripts/self-host/totp-upgrade-preflight.sh -- kubectl --kubeconfig PRIVATE_CONFIG --context CONTEXT --namespace NAMESPACE exec OLD_POD -c app -- /app/bin/codex_pooler

Exit 0: ready. Exit 2: operator action required. Exit 3: check incomplete.
No keys are generated/exported, no ciphertext is rewritten, and no process is stopped.
The report covers the selected old runtime, not a subsequently changed secret source.
HELP
}

case "${1:-}" in
  --help|-h) usage; exit 0 ;;
  --) shift ;;
  *) usage >&2; exit 3 ;;
esac
[ "$#" -gt 0 ] || { usage >&2; exit 3; }
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
if ! rpc_code=$(cat "$script_dir/totp-upgrade-preflight.exs" 2>/dev/null); then
  printf '%s\n' '{"disposition":"preflight_failed","inventory_complete":false}'
  exit 3
fi

# Transport/remote failures may contain sensitive exception arguments. Report
# only a fixed diagnostic, never the captured error or the evaluated code.
if output=$("$@" rpc "$rpc_code" 2>/dev/null); then
  :
else
  printf '%s\n' '{"disposition":"transport_failed","inventory_complete":false}'
  exit 3
fi

report=
count=0
while IFS= read -r line; do
  case "$line" in
    'TOTP_UPGRADE_PREFLIGHT_V1 '*) report=${line#TOTP_UPGRADE_PREFLIGHT_V1 }; count=$((count + 1)) ;;
  esac
done <<EOF_REPORT
$output
EOF_REPORT

[ "$count" -eq 1 ] || { printf '%s\n' '{"disposition":"preflight_failed","inventory_complete":false}'; exit 3; }
case "$report" in
  *'"disposition":"ready"'*) result=0 ;;
  *'"disposition":"replace_totp_key"'*|*'"disposition":"offline_reencryption_required"'*|*'"disposition":"restore_key_or_investigate"'*|*'"disposition":"inventory_limit_exceeded"'*) result=2 ;;
  *) printf '%s\n' '{"disposition":"preflight_failed","inventory_complete":false}'; exit 3 ;;
esac
printf '%s\n' "$report"
exit "$result"
