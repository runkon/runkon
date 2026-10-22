#!/usr/bin/env bash
# Publish the workspace crates to crates.io with `cargo publish --workspace`.
# Cargo packages the crates together, resolves intra-workspace dependencies
# against the local packages (so a dry run works on a version bump), publishes
# in dependency order, and waits for each crate to reach the index before
# publishing the crates that depend on it.
#
# Usage: scripts/release/publish.sh [--dry-run]
#
# Requires: cargo logged in (CARGO_REGISTRY_TOKEN env or ~/.cargo/credentials.toml)
# for a real publish; a dry run needs no credentials.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

dry_run=()
if [[ "${1:-}" == "--dry-run" ]]; then
  dry_run=(--dry-run)
fi

crates=(runkon-runtimes runkon-flow runkon-flow-executors runkon-anthropic runkon-google runkon-notify)

version=$(awk -F'"' '/^version = /{print $2; exit}' Cargo.toml)
[[ -n "$version" ]] || { echo "could not parse workspace version from Cargo.toml"; exit 1; }

# Idempotence: a real publish refuses to start if any crate's version is
# already on crates.io, so a re-run after a partial failure excludes the crates
# that made it. A dry run only warns about those, so it always checks all six.
exclude=()
if [[ ${#dry_run[@]} -eq 0 ]]; then
  for crate in "${crates[@]}"; do
    # Sparse index path for names of 4+ characters: <first 2>/<next 2>/<name>.
    url="https://index.crates.io/${crate:0:2}/${crate:2:2}/${crate}"
    body=$(curl -sS -w '\n%{http_code}' "${url}")
    status=${body##*$'\n'}
    case "${status}" in
      200)
        if grep -q "\"vers\":\"${version}\"" <<<"${body}"; then
          echo "=== ${crate} ${version} already on crates.io, skipping ==="
          exclude+=(--exclude "${crate}")
        fi
        ;;
      404) ;; # never published
      *) echo "crates.io index lookup for ${crate} failed (HTTP ${status})"; exit 1 ;;
    esac
  done
  if [[ ${#exclude[@]} -eq $(( ${#crates[@]} * 2 )) ]]; then
    echo "=== all crates already on crates.io at ${version}, nothing to publish ==="
    exit 0
  fi
fi

args=(--workspace ${dry_run[@]+"${dry_run[@]}"} ${exclude[@]+"${exclude[@]}"})
echo
echo "=== cargo publish ${args[*]} ==="
cargo publish "${args[@]}"
