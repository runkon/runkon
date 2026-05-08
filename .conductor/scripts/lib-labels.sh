#!/usr/bin/env bash
# Shared label management helpers for qualification scripts.
# Source this file: source "$(dirname "$0")/lib-labels.sh"

# apply_exclusive_label OWNER REPO ISSUE_NUMBER LABEL LABEL_COLOR LABEL_DESC
# Removes all three qualification labels, then creates and applies LABEL.
apply_exclusive_label() {
  local owner="$1"
  local repo="$2"
  local issue="$3"
  local label="$4"
  local color="$5"
  local desc="$6"

  gh issue edit "$issue" --repo "$owner/$repo" --remove-label "qualified" 2>/dev/null || true
  gh issue edit "$issue" --repo "$owner/$repo" --remove-label "needs-work" 2>/dev/null || true
  gh issue edit "$issue" --repo "$owner/$repo" --remove-label "pending-close" 2>/dev/null || true

  gh label create "$label" --repo "$owner/$repo" --color "$color" --description "$desc" 2>/dev/null || true
  gh issue edit "$issue" --repo "$owner/$repo" --add-label "$label"
}
