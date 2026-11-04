#!/usr/bin/env bash
set -uo pipefail

FAILED_CHECKS=()
FAILURE_DETAILS=""

# Each check's output goes to a log here; failing checks report the exact
# command and an error excerpt in the step context, so lint-fix-impl reruns the
# same command instead of a broader one that may pass (per-crate clippy without
# features can fail where workspace clippy, with unified features, does not).
LOG_DIR=$(mktemp -d)
VALIDATE_HOME=""
cleanup() {
  rm -rf "$LOG_DIR"
  if [ -n "$VALIDATE_HOME" ]; then rm -rf "$VALIDATE_HOME"; fi
}
trap cleanup EXIT

# run_check <name> <command...>
# Runs the command, echoes its output, and on failure records <name>, the
# command (CHECK_DISPLAY overrides how it is shown) and the first error lines.
run_check() {
  local name=$1
  shift
  local log
  log="$LOG_DIR/$(printf '%s' "$name" | tr -c 'A-Za-z0-9_.-' '_').log"
  "$@" >"$log" 2>&1
  local status=$?
  cat "$log"
  if [ "$status" -ne 0 ]; then
    FAILED_CHECKS+=("$name")
    local excerpt
    excerpt=$(grep -E -A 8 '^(error|warning)(\[[A-Z0-9]+\])?:' "$log" | head -60)
    [ -n "$excerpt" ] || excerpt=$(head -40 "$log")
    # rustfmt colours its diff even when writing to a file
    excerpt=$(printf '%s\n' "$excerpt" | perl -pe 's/\e\[[0-9;]*m|\e\(B//g')
    FAILURE_DETAILS+="### ${name}"$'\n'"Command: ${CHECK_DISPLAY:-$*}"$'\n'"${excerpt}"$'\n\n'
  fi
}

# Detect which runkon-* crates have changed files.
# If FEATURE_BASE_BRANCH is set (passed by the workflow), diff against the
# merge-base with that branch so committed changes within the worktree are
# included. Falling back to HEAD only sees uncommitted edits, which means
# scope shrinks the moment the worktree commits.
BASE="${FEATURE_BASE_BRANCH:-}"
if [ -n "$BASE" ]; then
  DIFF_TARGET=$(git merge-base HEAD "origin/$BASE" 2>/dev/null \
              || git merge-base HEAD "$BASE" 2>/dev/null \
              || echo HEAD)
else
  DIFF_TARGET=HEAD
fi

CHANGED_CRATES=$(git diff --name-only "$DIFF_TARGET" | grep '^runkon-' | cut -d/ -f1 | sort -u)

if [ -z "$CHANGED_CRATES" ]; then
  # No crate-level changes detected — fall back to full workspace (matches CI)
  run_check "clippy:workspace" cargo clippy --workspace --all-targets -- -D warnings
else
  # Scope clippy to changed crates only
  for crate in $CHANGED_CRATES; do
    run_check "clippy:$crate" cargo clippy -p "$crate" --all-targets -- -D warnings
  done
fi
run_check "fmt" cargo fmt --all --check

# Validate changed or new .wf files. Requires `conductor` on PATH (the
# runkon repo has no `conductor` bin of its own — this dogfoods conductor-ai).
# A throwaway CONDUCTOR_HOME keeps validation off the real database.
validate_wf() {
  CONDUCTOR_HOME="$VALIDATE_HOME" conductor workflow validate "$1" --path .
}
for f in $(git diff --name-only "$DIFF_TARGET" -- '*.wf') $(git ls-files --others --exclude-standard -- '*.wf'); do
  [ -f "$f" ] || continue
  name=$(basename "$f" .wf)
  [ -n "$VALIDATE_HOME" ] || VALIDATE_HOME=$(mktemp -d)
  CHECK_DISPLAY="conductor workflow validate $name --path ." run_check "wf:$name" validate_wf "$name"
done

if [ ${#FAILED_CHECKS[@]} -gt 0 ]; then
  JOINED=$(IFS=','; echo "${FAILED_CHECKS[*]}")
  CONTEXT="Failing checks: ${JOINED}. Rerun each failing check with the exact command below; a broader command (e.g. workspace clippy) can pass where it fails."$'\n\n'"${FAILURE_DETAILS}"
  # jq escapes the excerpts. If it is missing or fails, still emit the
  # marker (with the check names only) so lint failures never pass silently.
  if ! OUTPUT=$(jq -cn --arg ctx "$CONTEXT" '{markers: ["has_lint_errors"], context: $ctx}'); then
    echo "Warning: jq failed; reporting failing check names without details" >&2
    OUTPUT="{\"markers\": [\"has_lint_errors\"], \"context\": \"Failing checks: ${JOINED}\"}"
  fi
  printf '<<<FLOW_OUTPUT>>>\n%s\n<<<END_FLOW_OUTPUT>>>\n' "$OUTPUT"
else
  cat <<'OUT'
<<<FLOW_OUTPUT>>>
{"markers": [], "context": "All lint checks passed"}
<<<END_FLOW_OUTPUT>>>
OUT
fi
