#!/usr/bin/env bash
cd "$(dirname "$0")"
node_bin="${NODE:-node}"
pids=()

cleanup() {
  trap '' INT TERM
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$node_bin" entry.js 2 & pids+=("$!")
"$node_bin" entry.js 3 & pids+=("$!")
"$node_bin" entry.js 1 & pids+=("$!")
wait "${pids[2]}"
exit "$?"
