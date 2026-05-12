#!/usr/bin/env bash
# Create a single GitHub release for the workspace at <version>.
# Anchored on the runkon-flow-v<version> tag (the load-bearing crate).
# Marks pre-release automatically if the version contains a hyphen (e.g. -alpha).
#
# Usage: scripts/release/gh-release.sh
#
# Requires: gh authenticated.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

version=$(awk -F'"' '/^version = /{print $2; exit}' Cargo.toml)
[[ -n "$version" ]] || { echo "could not parse workspace version from Cargo.toml"; exit 1; }

anchor_tag="runkon-flow-v${version}"
if ! git rev-parse -q --verify "refs/tags/${anchor_tag}" >/dev/null; then
  echo "anchor tag ${anchor_tag} not found locally; run scripts/release/tag.sh first"
  exit 1
fi

prerelease_flag=()
if [[ "${version}" == *-* ]]; then
  prerelease_flag=(--prerelease)
fi

# Find the previous runkon-flow-v* tag for changelog range. Fall back to first commit if none.
prev_tag=$(git tag -l 'runkon-flow-v*' --sort=-version:refname | grep -v "^${anchor_tag}$" | head -1 || true)

notes=$(mktemp)
trap 'rm -f "${notes}"' EXIT

cat > "${notes}" <<EOF
## Workspace release: ${version}

Crates published to crates.io:
- [runkon-runtimes ${version}](https://crates.io/crates/runkon-runtimes/${version})
- [runkon-flow ${version}](https://crates.io/crates/runkon-flow/${version})
- [runkon-flow-executors ${version}](https://crates.io/crates/runkon-flow-executors/${version})
- [runkon-anthropic ${version}](https://crates.io/crates/runkon-anthropic/${version})
- [runkon-google ${version}](https://crates.io/crates/runkon-google/${version})
- [runkon-notify ${version}](https://crates.io/crates/runkon-notify/${version})

Per-crate tags pointing at this release commit:
- \`runkon-flow-v${version}\`
- \`runkon-flow-executors-v${version}\`
- \`runkon-runtimes-v${version}\`
- \`runkon-anthropic-v${version}\`
- \`runkon-google-v${version}\`
- \`runkon-notify-v${version}\`

EOF

if [[ -n "${prev_tag}" ]]; then
  {
    echo "## Changes since ${prev_tag}"
    echo
    git log "${prev_tag}..${anchor_tag}" --pretty='format:- %s'
    echo
  } >> "${notes}"
fi

if gh release view "${anchor_tag}" >/dev/null 2>&1; then
  echo "release ${anchor_tag} already exists, updating notes"
  gh release edit "${anchor_tag}" \
    --title "runkon ${version}" \
    --notes-file "${notes}" \
    "${prerelease_flag[@]}"
else
  gh release create "${anchor_tag}" \
    --title "runkon ${version}" \
    --notes-file "${notes}" \
    "${prerelease_flag[@]}"
fi
