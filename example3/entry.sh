#!/usr/bin/env bash
cd "$(dirname "$0")"
node_bin="${NODE:-node}"
pids=()
server_pid=''

cleanup() {
  trap '' INT TERM
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  if [ -n "$server_pid" ]; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

export MONSTERFT_HTTP_PORT="${MONSTERFT_HTTP_PORT:-43000}"
export MONSTERFT_FACT_URL="http://127.0.0.1:${MONSTERFT_HTTP_PORT}/facts"
"$node_bin" server.js & server_pid="$!"
if ! kill -0 "$server_pid" 2>/dev/null; then
  wait "$server_pid"
  exit 1
fi

"$node_bin" entry.js 1 & pids+=("$!")
"$node_bin" entry.js 2 & pids+=("$!")
"$node_bin" entry.js 3 & pids+=("$!")
wait "${pids[0]}"
exit "$?"
