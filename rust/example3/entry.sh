#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."

cargo build --locked \
  --example example3-entry \
  --example example3-procA \
  --example example3-procB \
  --example example3-procC \
  --example example3-apply \
  --example example3-server || exit $?

target_dir="${CARGO_TARGET_DIR:-target}"
binary="$target_dir/debug/examples/example3-entry"
pids=()
server_pid=''

cleanup() {
  trap '' INT TERM
  for pid in "${pids[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done

  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done

  # Let every Apply child finish before stopping the service it calls.
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
"$target_dir/debug/examples/example3-server" &
server_pid="$!"

sleep 1
if ! kill -0 "$server_pid" 2>/dev/null; then
  wait "$server_pid"
  exit 1
fi

"$binary" 1 &
pids+=("$!")

"$binary" 2 &
pids+=("$!")

"$binary" 3 &
pids+=("$!")

wait "${pids[0]}"
exit "$?"
