#!/usr/bin/env bash
cd "$(dirname "$0")"
node_bin="${NODE:-node}"
pids=()
server_pid=''
runtime_dir="$(mktemp -d /tmp/mft-http-XXXXXX)"

cleanup() {
  trap '' INT TERM
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  if [ -n "$server_pid" ]; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$runtime_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$node_bin" server.js "$runtime_dir/url" & server_pid="$!"
for ((attempt = 0; attempt < 100; attempt++)); do
  [ -s "$runtime_dir/url" ] && break
  if ! kill -0 "$server_pid" 2>/dev/null; then
    wait "$server_pid"
    exit 1
  fi
  sleep 0.05
done
if [ ! -s "$runtime_dir/url" ]; then echo 'HTTP startup timeout' >&2; exit 1; fi
export MONSTERFT_FACT_URL="$(cat "$runtime_dir/url")"

"$node_bin" entry.js 2 & pids+=("$!")
"$node_bin" entry.js 3 & pids+=("$!")
"$node_bin" entry.js 1 & pids+=("$!")
wait "${pids[2]}"
exit "$?"
