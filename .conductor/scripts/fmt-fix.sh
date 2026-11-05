#!/usr/bin/env bash
set -euo pipefail

# Runs `cargo fmt --all` and commits only what rustfmt changed.
#
# Every step that runs before this script (implement, complete-plan,
# address-reviews, lint-fix-impl) commits its own work. Uncommitted changes to
# tracked files here mean that step didn't finish, so fail and name the files
# rather than commit them under a formatting message
# (devinrosen/conductor-ai#3178).
#
# Untracked files only produce a warning: build and test steps can leave
# artifacts that aren't gitignored, and they must not abort an otherwise clean
# run. They are never committed (git add -u below skips them).
#
# Lockfiles are the exception (devinrosen/conductor-ai#3185). Implement agents
# don't run cargo,
# so when a change adds a dependency, the lockfile is first rewritten by a later
# build or verify step. Those changes are committed on their own as
# "chore(deps): update lockfiles" instead of failing the step.
#
# FMT_FIX_ALLOW_FAILURE=1: used before verify. If rustfmt can't format the code
# (usually a parse error), leave it for verify to report instead of failing the
# run. It does not relax the tracked-changes check.

LOCKFILES=(Cargo.lock)
lock_excludes=()
for f in "${LOCKFILES[@]}"; do
  lock_excludes+=(":(exclude)${f}")
done

tracked=$(git status --porcelain --untracked-files=no -- . "${lock_excludes[@]}")
if [ -n "${tracked}" ]; then
  {
    echo "fmt-fix: tracked files have uncommitted changes before cargo fmt ran:"
    echo "${tracked}" | sed 's/^/  /'
    echo "The previous step should have committed these, so it probably ended early."
    echo "Nothing was committed. Resume the run from that step, or commit or discard"
    echo "the changes, then resume from this one."
  } >&2
  exit 1
fi

changed_locks=()
for f in "${LOCKFILES[@]}"; do
  # Assigned first so a git failure aborts under set -e instead of reading as "unchanged".
  lock_status=$(git status --porcelain --untracked-files=no -- "${f}")
  if [ -n "${lock_status}" ]; then
    changed_locks+=("${f}")
  fi
done
if [ "${#changed_locks[@]}" -gt 0 ]; then
  git add -- "${changed_locks[@]}"
  git commit -q -m "chore(deps): update lockfiles" -- "${changed_locks[@]}" >&2
  echo "fmt-fix: committed lockfile updates: ${changed_locks[*]}" >&2
fi

untracked=$(git ls-files --others --exclude-standard)
if [ -n "${untracked}" ]; then
  {
    echo "fmt-fix: warning: untracked files (not committed):"
    echo "${untracked}" | sed 's/^/  /'
    echo "If the previous step created these as part of its change, it didn't commit them."
  } >&2
fi

if ! cargo fmt --all; then
  if [ -n "${FMT_FIX_ALLOW_FAILURE:-}" ]; then
    echo "cargo fmt failed (likely a parse error); leaving it to verify" >&2
    exit 0
  fi
  exit 1
fi

# The tree was clean before rustfmt ran, so every change is rustfmt's. rustfmt
# only rewrites tracked files, so stage those and nothing else.
if ! git diff --quiet; then
  git add -u
  git commit -m "style: cargo fmt"
fi
