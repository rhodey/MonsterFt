#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")/.."

# Compile the parent and every child before launching, including after source edits.
cargo build --locked \
  --example example2-entry \
  --example example2-procA \
  --example example2-procB \
  --example example2-procC \
  --example example2-apply || exit $?

target_dir="${CARGO_TARGET_DIR:-target}"
binary="$target_dir/debug/examples/example2-entry"
pids=()

cleanup() {
  trap '' INT TERM
  for pid in "${pids[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done

  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$binary" 1 &
pids+=("$!")

"$binary" 2 &
pids+=("$!")

"$binary" 3 &
pids+=("$!")

wait "${pids[0]}"
exit "$?"
