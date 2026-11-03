#!/usr/bin/env bash
# Version bump for the release PR.
#
# Usage:
#   scripts/release/bump.sh <version>
#     e.g. scripts/release/bump.sh 0.7.3-alpha
#
# Edits the root Cargo.toml ([workspace.package] version and the version of each
# [workspace.dependencies] path entry) and the `runkon-<crate> = "<version>"` install
# snippet in each crate README.md, then runs `cargo update -w --offline` to refresh
# Cargo.lock. Then it checks:
#   1. Cargo.lock equals the old lockfile with only the `version` line of each workspace
#      package (a [[package]] with no `source`) changed. Any other change, such as a
#      third-party dependency moving, fails the script.
#   2. None of the files the bump owns (root Cargo.toml, README.md files) still mention
#      the old version. The rest of the tree is not searched: `#[deprecated(since = ...)]`
#      attributes and CHANGELOGs legitimately keep old versions.
# The Release workflow's "Verify input version matches Cargo.toml" step covers the other
# direction (the tag matching Cargo.toml).
#
# Refuses to run if the files it edits have uncommitted changes. Other changes are fine,
# so it works alongside changelog.sh in either order. If a check fails the edits are left
# in place for inspection and the undo command is printed.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

usage() {
  echo "usage: $0 <version>   (e.g. 0.7.3-alpha)" >&2
  exit 2
}

[[ $# -eq 1 ]] || usage
new=$1
if [[ ! $new =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "invalid version '$new': expected X.Y.Z or X.Y.Z-suffix (e.g. 0.7.3-alpha)" >&2
  exit 2
fi

old=$(awk '
  /^\[/ { in_pkg = ($0 == "[workspace.package]"); next }
  in_pkg && /^version = "/ { v = $0; sub(/^version = "/, "", v); sub(/".*$/, "", v); print v; exit }
' Cargo.toml)
[[ -n $old ]] || { echo "error: no version found in [workspace.package] of Cargo.toml" >&2; exit 1; }
if [[ $old == "$new" ]]; then
  echo "error: already at $new" >&2
  exit 1
fi

old_re=${old//./\\.}
readmes=()
for f in */README.md; do
  [[ -f $f ]] || continue
  if grep -Eq "^runkon-[A-Za-z0-9_-]+ = \"$old_re\"$" "$f"; then
    readmes+=("$f")
  fi
done
if [[ ${#readmes[@]} -eq 0 ]]; then
  echo "error: no crate README.md has a 'runkon-<crate> = \"$old\"' install snippet" >&2
  exit 1
fi

files=(Cargo.toml Cargo.lock "${readmes[@]}")
if ! git diff --quiet HEAD -- "${files[@]}"; then
  echo "error: uncommitted changes in files this script edits; commit or revert them first:" >&2
  git diff --name-only HEAD -- "${files[@]}" | sed 's/^/  /' >&2
  exit 1
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cp Cargo.lock "$tmp/Cargo.lock.orig"

# Rewrite $1 in place; awk program on stdin-free -f via $2, extra awk vars follow.
rewrite() {
  local file=$1 prog=$2
  awk -v old="$old" -v new="$new" "$prog" "$file" > "$tmp/out"
  cat "$tmp/out" > "$file"   # keeps the file's mode
}

rewrite Cargo.toml '
  /^\[/ { sect = $0 }
  {
    if (sect == "[workspace.package]" && $0 == "version = \"" old "\"") {
      $0 = "version = \"" new "\""
    } else if (sect == "[workspace.dependencies]" && $0 ~ /^runkon-[A-Za-z0-9_-]+ = \{ path = /) {
      pat = "version = \"" old "\""
      i = index($0, pat)
      if (i > 0) $0 = substr($0, 1, i - 1) "version = \"" new "\"" substr($0, i + length(pat))
    }
    print
  }'
if ! grep -qxF "version = \"$new\"" Cargo.toml; then
  echo "error: failed to rewrite [workspace.package] version in Cargo.toml" >&2
  exit 1
fi

for f in "${readmes[@]}"; do
  rewrite "$f" '
    {
      q = " = \"" old "\""
      if ($0 ~ /^runkon-[A-Za-z0-9_-]+ = "/ && length($0) > length(q) && substr($0, length($0) - length(q) + 1) == q) {
        $0 = substr($0, 1, length($0) - length(q)) " = \"" new "\""
      }
      print
    }'
done

undo="git checkout -- ${files[*]}"

if ! cargo update -w --offline > "$tmp/cargo.log" 2>&1; then
  cat "$tmp/cargo.log" >&2
  echo "error: cargo update -w --offline failed (--offline needs the crates.io index cached; any earlier build fills it)" >&2
  echo "undo: $undo" >&2
  exit 1
fi

# Check 1: Cargo.lock == old lockfile with only workspace packages' versions bumped.
awk -v old="$old" -v new="$new" '
  function flush(   i) {
    for (i = 1; i <= n; i++) {
      if (!has_source && buf[i] == "version = \"" old "\"") buf[i] = "version = \"" new "\""
      print buf[i]
    }
    n = 0; has_source = 0
  }
  /^\[\[package\]\]$/ { flush() }
  { buf[++n] = $0; if ($0 ~ /^source = /) has_source = 1 }
  END { flush() }
' "$tmp/Cargo.lock.orig" > "$tmp/Cargo.lock.expected"

if ! diff -u "$tmp/Cargo.lock.expected" Cargo.lock > "$tmp/lock.diff"; then
  cat "$tmp/lock.diff" >&2
  echo "error: Cargo.lock changed beyond the workspace crates' own version lines (diff above, expected -> actual)." >&2
  echo "       Most likely a third-party dependency moved. Packages involved:" >&2
  awk '
    /^[-+ ]name = / { gsub(/^[-+ ]name = "|"$/, ""); last = $0 }
    /^[-+][^-+]/ || /^[-+]$/ { if (last != "" && !(last in seen)) { seen[last] = 1; print "  " last } }
  ' "$tmp/lock.diff" >&2
  echo "undo: $undo" >&2
  exit 1
fi

# Check 2: no stale old version in the files the bump owns.
stale=0
for f in Cargo.toml README.md */README.md; do
  [[ -f $f ]] || continue
  if grep -nF -- "$old" "$f" | sed "s|^|$f:|" | grep .; then
    stale=1
  fi
done >&2
if [[ $stale -ne 0 ]]; then
  echo "error: files above still mention $old" >&2
  echo "undo: $undo" >&2
  exit 1
fi

echo "bumped $old -> $new"
git diff --stat HEAD -- "${files[@]}"
