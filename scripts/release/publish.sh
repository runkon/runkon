#!/usr/bin/env bash
# Publish workspace crates to crates.io in dependency order.
# Sleeps between publishes to give the crates.io index time to propagate so
# downstream crates can resolve their just-published dependency.
#
# Usage: scripts/release/publish.sh [--dry-run]
#
# Requires: cargo logged in (CARGO_REGISTRY_TOKEN env or ~/.cargo/credentials.toml).
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

dry_run=()
if [[ "${1:-}" == "--dry-run" ]]; then
  dry_run=(--dry-run)
fi

# Dependency-ordered. runkon-runtimes and runkon-flow are leaves; executors
# depends on both; anthropic depends on all three.
crates=(runkon-runtimes runkon-flow runkon-flow-executors runkon-anthropic)

last_idx=$(( ${#crates[@]} - 1 ))
for i in "${!crates[@]}"; do
  crate="${crates[$i]}"
  echo
  echo "=== publishing ${crate} ${dry_run[*]+${dry_run[*]}} ==="
  cargo publish -p "${crate}" ${dry_run[@]+"${dry_run[@]}"}
  if [[ ${#dry_run[@]} -eq 0 && $i -lt $last_idx ]]; then
    echo "waiting 30s for crates.io index to update before next publish..."
    sleep 30
  fi
done
