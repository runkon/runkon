#!/usr/bin/env bash
# Shared diff range for review-pr's detect-* steps.
# Source this file: source "$(dirname "$0")/lib-diff-range.sh"

# resolve_diff_range
# Reads BASE_BRANCH (required), SCOPE and SINCE (review-pr's scope and since
# inputs). Sets DIFF_ARGS (an array for `git diff`) and DIFF_DESC (a short
# description for the step context).
#
# An incremental review with a usable `since` decides from the commits since
# the last review, the same `git diff <since> HEAD` the reviewers run (devinrosen/conductor-ai#3252).
# Otherwise, including when `since` is missing, unresolved or no longer an
# ancestor of HEAD (rebase, force-push), it falls back to the whole PR.
resolve_diff_range() {
  if [ -z "${BASE_BRANCH:-}" ]; then
    echo "ERROR: BASE_BRANCH env var not set — workflow must run resolve-pr-base first." >&2
    return 1
  fi

  local scope="${SCOPE:-full}" since="${SINCE:-}"
  if [ "$scope" = "incremental" ] && [ -n "$since" ] && [[ "$since" != *"{{"* ]]; then
    if git merge-base --is-ancestor "$since" HEAD 2>/dev/null; then
      DIFF_ARGS=("$since" HEAD)
      DIFF_DESC="commits since ${since:0:8}"
      return 0
    fi
    echo "since ${since:0:8} is not an ancestor of HEAD; using the full PR diff." >&2
  fi

  DIFF_ARGS=("origin/${BASE_BRANCH}...HEAD")
  DIFF_DESC="the PR diff against origin/${BASE_BRANCH}"
}

# changed_files_to FILE
# Writes the NUL-separated names of the files changed in DIFF_ARGS to FILE.
# A failed diff (for example a missing base ref) must fail the step: reading
# it as an empty list would skip every conditional reviewer without any error.
changed_files_to() {
  if ! git diff "${DIFF_ARGS[@]}" --name-only -z > "$1"; then
    echo "ERROR: git diff ${DIFF_ARGS[*]} failed." >&2
    return 1
  fi
}
