#!/usr/bin/env bash
# Changelog fragments. Ticket PRs add `<crate>/changelog.d/<issue>.md` instead of editing
# `<crate>/CHANGELOG.md`, so they never conflict. The release PR assembles them.
#
# Usage:
#   scripts/release/changelog.sh --check
#   scripts/release/changelog.sh [--dry-run] <version> <label>
#     e.g. scripts/release/changelog.sh 0.8.0 Breaking
#
# --check validates every fragment (run in CI). Assemble validates, then puts each crate's
# fragments, sorted by issue number, under `## <version> — <label>` above the first `## `
# heading of that crate's CHANGELOG.md, and `git rm`s them. <version> has no pre-release
# suffix (0.8.0, not 0.8.0-alpha). --dry-run prints the sections and changes nothing.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

usage() {
  echo "usage: $0 --check | [--dry-run] <version> <label>" >&2
  exit 2
}

mode=assemble
dry_run=0
args=()
for a in "$@"; do
  case "$a" in
    --check) mode=check ;;
    --dry-run) dry_run=1 ;;
    -*) echo "unknown option: $a" >&2; usage ;;
    *) args+=("$a") ;;
  esac
done

if [[ $mode == check ]]; then
  [[ ${#args[@]} -eq 0 && $dry_run -eq 0 ]] || usage
else
  [[ ${#args[@]} -eq 2 ]] || usage
  version=${args[0]}
  label=${args[1]}
  if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "invalid version '$version': expected X.Y.Z without a pre-release suffix (e.g. 0.8.0, not 0.8.0-alpha)" >&2
    exit 2
  fi
  [[ -n "${label// /}" ]] || { echo "label must not be empty" >&2; exit 2; }
fi

errors=0
err() { echo "error: $*" >&2; errors=$((errors + 1)); }

# Fragment files per crate, validated. Populates `crates` and `frags_<index>` via arrays.
crates=()
crate_frags=()   # newline-separated, numerically sorted fragment paths per crate
total=0

shopt -s nullglob
for dir in */changelog.d; do
  crate=${dir%/changelog.d}
  [[ -d $dir ]] || continue
  names=()
  for f in "$dir"/*  "$dir"/.[!.]*; do
    [[ -e $f ]] || continue
    base=${f##*/}
    if [[ ! -f $f || ! $base =~ ^[0-9]+\.md$ ]]; then
      err "$f: fragment must be a regular file named <issue-number>.md"
      continue
    fi
    names+=("$f")
  done
  [[ ${#names[@]} -gt 0 ]] || continue

  if [[ ! -f $crate/CHANGELOG.md ]]; then
    err "$dir: $crate has no CHANGELOG.md; put the entry in an existing crate's changelog.d, or add a CHANGELOG.md first"
  fi

  for f in "${names[@]}"; do
    first=$(awk 'NF { print; exit }' "$f")
    if [[ -z $first ]]; then
      err "$f: fragment is empty"
    elif [[ $first != "- "* ]]; then
      err "$f: first line must start with '- ' (a markdown bullet)"
    fi
    if grep -q '^#' "$f"; then
      err "$f: fragment must not contain headings (lines starting with '#')"
    fi
  done

  sorted=$(for f in "${names[@]}"; do b=${f##*/}; echo "${b%.md} $f"; done | sort -n | cut -d' ' -f2-)
  crates+=("$crate")
  crate_frags+=("$sorted")
  total=$((total + ${#names[@]}))
done

if [[ $errors -gt 0 ]]; then
  echo "$errors error(s) in changelog fragments" >&2
  exit 1
fi

if [[ $mode == check ]]; then
  echo "$total fragment(s) OK"
  exit 0
fi

[[ $total -gt 0 ]] || { echo "nothing to assemble: no changelog fragments found" >&2; exit 1; }

# Pre-flight on every crate before writing anything.
for crate in "${crates[@]}"; do
  cl=$crate/CHANGELOG.md
  if grep -q "^## ${version//./\\.} " "$cl" || grep -q "^## ${version//./\\.}\$" "$cl"; then
    err "$cl: already has a '## $version' heading"
  fi
  if grep -q '^## Unreleased' "$cl"; then
    err "$cl: has a '## Unreleased' heading; move its text into a fragment in $crate/changelog.d/"
  fi
done
[[ $errors -eq 0 ]] || exit 1

build_section() {
  local first=1 f
  printf '## %s — %s\n\n' "$version" "$label"
  while IFS= read -r f; do
    [[ $first -eq 1 ]] || printf '\n'
    first=0
    # trim leading and trailing blank lines
    awk '
      NF { for (i = 0; i < blanks; i++) print ""; blanks = 0; started = 1; print; next }
      started { blanks++ }
    ' "$f"
  done <<<"$1"
  printf '\n'
}

for i in "${!crates[@]}"; do
  crate=${crates[$i]}
  frags=${crate_frags[$i]}
  cl=$crate/CHANGELOG.md
  section=$(build_section "$frags"; echo x)
  section=${section%x}
  count=$(grep -c . <<<"$frags")

  if [[ $dry_run -eq 1 ]]; then
    echo "=== $crate: $count fragment(s) ==="
    printf '%s' "$section"
    continue
  fi

  tmp=$(mktemp "${TMPDIR:-/tmp}/changelog.XXXXXX")
  secfile=$(mktemp "${TMPDIR:-/tmp}/changelog-sec.XXXXXX")
  printf '%s' "$section" >"$secfile"
  awk -v secfile="$secfile" '
    function emit(   line) { while ((getline line < secfile) > 0) print line; close(secfile); done = 1 }
    !done && /^## / { emit() }
    { print }
    END { if (!done) { print ""; emit() } }
  ' "$cl" >"$tmp"
  mv "$tmp" "$cl"
  rm -f "$secfile"

  while IFS= read -r f; do git rm -q -- "$f"; done <<<"$frags"
  rmdir "$crate/changelog.d" 2>/dev/null || true
  echo "$crate: $count fragment(s) -> ## $version — $label"
done

# Crates with a changelog.d-less tree are untouched; report them.
for cl in */CHANGELOG.md; do
  c=${cl%/CHANGELOG.md}
  found=0
  for x in "${crates[@]}"; do [[ $x == "$c" ]] && found=1; done
  [[ $found -eq 1 ]] || echo "$c: no fragments, unchanged"
done
