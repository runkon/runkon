#!/usr/bin/env bash
set -uo pipefail

# Resolve PR number and current state
PR_NUMBER=$(gh pr view --json number -q .number)
PR_STATE=$(gh pr view --json state -q .state)

# Attempt merge only if not already merged.
# Suppress exit code from merge commands — the state re-check below is
# authoritative. This avoids failures from:
#   - "already merged" errors when auto-merge fires before this script runs
#   - git worktree conflicts when --delete-branch tries to checkout main
if [ "${PR_STATE}" != "MERGED" ]; then
  gh pr merge --auto --squash --delete-branch 2>/dev/null \
    || gh pr merge --squash --delete-branch 2>/dev/null \
    || true

  PR_STATE=$(gh pr view --json state -q .state)
fi

if [ "${PR_STATE}" != "MERGED" ]; then
  echo "ERROR: PR #${PR_NUMBER} was not merged" >&2
  exit 1
fi

echo "Merged PR #${PR_NUMBER}"

# Close linked issue if TICKET_NUMBER was provided and is a valid number
if [ -n "${TICKET_NUMBER:-}" ] && [[ "${TICKET_NUMBER}" =~ ^#?[0-9]+$ ]]; then
  ISSUE_NUMBER="${TICKET_NUMBER#\#}"
  gh issue close "${ISSUE_NUMBER}"
  gh issue comment "${ISSUE_NUMBER}" --body "Closed by #${PR_NUMBER} (merged)."
  echo "Closed issue #${ISSUE_NUMBER}"
else
  echo "TICKET_NUMBER not set or invalid — skipping issue close."
fi
