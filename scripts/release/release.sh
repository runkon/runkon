#!/usr/bin/env bash
# End-to-end release driver. Runs in order:
#   1. tag the workspace crates locally
#   2. push tags to origin
#   3. publish crates to crates.io (dependency order)
#   4. create the GitHub release
#
# Usage: scripts/release/release.sh [<ref>]
#   <ref> defaults to HEAD; pass a commit SHA to tag a specific point.
#
# Set DRY_RUN=1 to skip pushing tags, publishing, and creating the release.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

here=$(dirname "$0")
ref="${1:-HEAD}"

"${here}/tag.sh" "${ref}"

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo
  echo "DRY_RUN=1: stopping after local tag creation"
  "${here}/publish.sh" --dry-run
  exit 0
fi

version=$(awk -F'"' '/^version = /{print $2; exit}' Cargo.toml)
crates=(runkon-flow runkon-flow-executors runkon-runtimes runkon-anthropic runkon-google runkon-notify)
tags=()
for c in "${crates[@]}"; do tags+=("${c}-v${version}"); done

echo
echo "pushing tags: ${tags[*]}"
git push origin "${tags[@]}"

"${here}/publish.sh"
"${here}/gh-release.sh"
