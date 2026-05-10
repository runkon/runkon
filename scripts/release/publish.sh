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
# depends on both; anthropic depends on all three. runkon-notify is a leaf
# (no intra-workspace deps), placed last.
crates=(runkon-runtimes runkon-flow runkon-flow-executors runkon-anthropic runkon-notify)

version=$(awk -F'"' '/^version = /{print $2; exit}' Cargo.toml)
[[ -n "$version" ]] || { echo "could not parse workspace version from Cargo.toml"; exit 1; }

last_idx=$(( ${#crates[@]} - 1 ))
for i in "${!crates[@]}"; do
  crate="${crates[$i]}"
  echo
  # Idempotence: skip if this version is already on crates.io. The 404 path
  # also covers brand-new crates (the index returns "does not exist").
  if [[ ${#dry_run[@]} -eq 0 ]] && curl -sfo /dev/null \
      "https://crates.io/api/v1/crates/${crate}/${version}"; then
    echo "=== ${crate} ${version} already on crates.io, skipping ==="
    continue
  fi
  echo "=== publishing ${crate} ${dry_run[*]+${dry_run[*]}} ==="
  cargo publish -p "${crate}" ${dry_run[@]+"${dry_run[@]}"}
  if [[ ${#dry_run[@]} -eq 0 && $i -lt $last_idx ]]; then
    echo "waiting 30s for crates.io index to update before next publish..."
    sleep 30
  fi
done
