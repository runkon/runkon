#!/usr/bin/env bash
# Tag each workspace crate as `<crate>-v<version>`, where <version> is the
# workspace.package.version from Cargo.toml. Tags point at <ref> (default HEAD).
#
# Usage: scripts/release/tag.sh [<ref>]
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

ref="${1:-HEAD}"
version=$(awk -F'"' '/^version = /{print $2; exit}' Cargo.toml)
[[ -n "$version" ]] || { echo "could not parse workspace version from Cargo.toml"; exit 1; }

crates=(runkon-flow runkon-flow-executors runkon-runtimes runkon-anthropic runkon-notify)
tags_to_push=()

for crate in "${crates[@]}"; do
  tag="${crate}-v${version}"
  if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
    echo "tag ${tag} already exists, skipping"
  else
    git tag -a "${tag}" -m "${crate} ${version}" "${ref}"
    echo "created ${tag} -> $(git rev-parse --short "${ref}")"
  fi
  tags_to_push+=("${tag}")
done

echo
echo "to push: git push origin ${tags_to_push[*]}"
