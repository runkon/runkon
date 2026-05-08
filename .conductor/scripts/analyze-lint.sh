#!/usr/bin/env bash
set -uo pipefail

FAILED_CHECKS=()

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
  cargo clippy --workspace --all-targets -- -D warnings 2>&1 || FAILED_CHECKS+=("clippy:workspace")
else
  # Scope clippy to changed crates only
  for crate in $CHANGED_CRATES; do
    cargo clippy -p "$crate" --all-targets -- -D warnings 2>&1 || FAILED_CHECKS+=("clippy:$crate")
  done
fi
cargo fmt --all --check 2>&1 || FAILED_CHECKS+=("fmt")

# Validate changed or new .wf files. Requires `conductor` on PATH (the
# runkon repo has no `conductor` bin of its own — this dogfoods conductor-ai).
for f in $(git diff --name-only "$DIFF_TARGET" -- '*.wf') $(git ls-files --others --exclude-standard -- '*.wf'); do
  [ -f "$f" ] || continue
  name=$(basename "$f" .wf)
  conductor workflow validate "$name" --path . 2>&1 || FAILED_CHECKS+=("wf:$name")
done

if [ ${#FAILED_CHECKS[@]} -gt 0 ]; then
  JOINED=$(IFS=','; echo "${FAILED_CHECKS[*]}")
  cat <<EOF
<<<FLOW_OUTPUT>>>
{"markers": ["has_lint_errors"], "context": "Failing checks: ${JOINED}"}
<<<END_FLOW_OUTPUT>>>
EOF
else
  cat <<'EOF'
<<<FLOW_OUTPUT>>>
{"markers": [], "context": "All lint checks passed"}
<<<END_FLOW_OUTPUT>>>
EOF
fi
