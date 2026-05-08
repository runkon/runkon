#!/usr/bin/env bash
set -euo pipefail

PRIOR_OUTPUT="${PRIOR_OUTPUT:-}"
TICKET_URL="${TICKET_URL:-}"

# Parse owner, repo, and issue number from TICKET_URL
# Expected format: https://github.com/<owner>/<repo>/issues/<number>
OWNER=$(echo "$TICKET_URL" | sed -E 's|https://github.com/([^/]+)/([^/]+)/issues/([0-9]+)|\1|')
REPO=$(echo "$TICKET_URL" | sed -E 's|https://github.com/([^/]+)/([^/]+)/issues/([0-9]+)|\2|')
ISSUE_NUMBER=$(echo "$TICKET_URL" | sed -E 's|https://github.com/([^/]+)/([^/]+)/issues/([0-9]+)|\3|')

# Validate that URL parsing succeeded — sed returns the original string if pattern doesn't match
if [ -z "$OWNER" ] || [ -z "$REPO" ] || [ -z "$ISSUE_NUMBER" ] || \
   [ "$OWNER" = "$TICKET_URL" ] || ! echo "$ISSUE_NUMBER" | grep -qE '^[0-9]+$'; then
  echo "ERROR: Could not parse OWNER/REPO/ISSUE_NUMBER from TICKET_URL: ${TICKET_URL}" >&2
  exit 1
fi

emit_output() {
  local markers="$1"
  local context="$2"
  local output
  output=$(jq -n --argjson markers "$markers" --arg context "$context" \
    '{"markers": $markers, "context": $context}')
  printf '<<<FLOW_OUTPUT>>>\n%s\n<<<END_FLOW_OUTPUT>>>\n' "$output"
}

# Detect verdict — check SHOULD CLOSE before NOT READY before READY to avoid substring collisions
if echo "$PRIOR_OUTPUT" | grep -q "SHOULD CLOSE"; then
  TARGET_LABEL="pending-close"
  LABEL_COLOR="d93f0b"
  LABEL_DESC="Ticket is invalid, resolved, or no longer actionable"
  MARKERS='["should_close"]'
  CONTEXT_MSG="Applied 'pending-close' label to ticket #${ISSUE_NUMBER} (SHOULD CLOSE verdict)"
elif echo "$PRIOR_OUTPUT" | grep -q "NOT READY"; then
  TARGET_LABEL="needs-work"
  LABEL_COLOR="e4e669"
  LABEL_DESC="Ticket requires clarification before implementation"
  MARKERS='["has_open_questions"]'
  CONTEXT_MSG="Applied 'needs-work' label to ticket #${ISSUE_NUMBER} (NOT READY verdict)"
elif echo "$PRIOR_OUTPUT" | grep -q "READY"; then
  TARGET_LABEL="qualified"
  LABEL_COLOR="0075ca"
  LABEL_DESC="Ticket is ready for autonomous implementation"
  MARKERS='["ticket_ready"]'
  CONTEXT_MSG="Applied 'qualified' label to ticket #${ISSUE_NUMBER} (READY verdict)"
else
  emit_output '[]' "Could not determine verdict from prior output — no SHOULD CLOSE, NOT READY, or READY found"
  exit 0
fi

# shellcheck source=lib-labels.sh
source "$(dirname "$0")/lib-labels.sh"
apply_exclusive_label "$OWNER" "$REPO" "$ISSUE_NUMBER" "$TARGET_LABEL" "$LABEL_COLOR" "$LABEL_DESC"

emit_output "$MARKERS" "$CONTEXT_MSG"
