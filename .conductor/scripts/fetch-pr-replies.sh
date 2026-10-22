#!/usr/bin/env bash
# fetch-pr-replies.sh — collect PR replies posted since the last conductor review
# Called as a script step before review-aggregator. Env vars: PR_NUMBER
#
# Emits a FLOW_OUTPUT block whose `replies` field (surfaced to the aggregator
# as structured_output) lists issue comments, review bodies and inline review
# comments posted after the most recent conductor review, excluding the
# reviewer identity's own posts. It never reads findings from earlier reviews:
# GitHub returns dismissed reviews too, and triaging stale findings is the trap
# triage-reviews guards against. Only the latest review's timestamp and author
# are used.
#
# Failures are non-fatal: with no replies, every finding stays blocking.
set -euo pipefail

MAX_REPLIES=40
MAX_BODY_CHARS=2000

emit() {
  # $1 = context string, $2 = replies JSON array
  jq -cn --arg context "$1" --argjson replies "$2" \
    '{markers: [], context: $context, replies: $replies}' \
    | { printf '<<<FLOW_OUTPUT>>>\n'; cat; printf '<<<END_FLOW_OUTPUT>>>\n'; }
}

if [ -z "${PR_NUMBER:-}" ] || [[ "${PR_NUMBER}" == *"{{"* ]]; then
  PR_NUMBER=$(gh pr view --json number -q .number 2>/dev/null || true)
fi
if ! [[ "${PR_NUMBER:-}" =~ ^[0-9]+$ ]]; then
  emit "No PR number; no replies fetched." "[]"
  exit 0
fi

OWNER_REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)
if [ -z "${OWNER_REPO}" ]; then
  emit "Could not resolve the repository; no replies fetched." "[]"
  exit 0
fi

if ! REVIEWS=$(gh api --paginate "repos/${OWNER_REPO}/pulls/${PR_NUMBER}/reviews" --jq '.[]' 2>/dev/null | jq -s '.'); then
  emit "Could not fetch reviews for PR #${PR_NUMBER}; no replies fetched." "[]"
  exit 0
fi

# Most recent conductor review: its timestamp bounds the window and its author
# is the reviewer identity whose posts are excluded.
LAST_REVIEW=$(echo "${REVIEWS}" | jq -c '
  [.[] | select((.body // "") | contains("<!-- conductor-review -->"))]
  | sort_by(.submitted_at) | last // empty')
if [ -z "${LAST_REVIEW}" ]; then
  emit "No earlier conductor review on PR #${PR_NUMBER}; no replies to consider." "[]"
  exit 0
fi
SINCE=$(echo "${LAST_REVIEW}" | jq -r '.submitted_at')
REVIEWER_LOGIN=$(echo "${LAST_REVIEW}" | jq -r '.user.login')

if ! ISSUE_COMMENTS=$(gh api --paginate "repos/${OWNER_REPO}/issues/${PR_NUMBER}/comments" --jq '.[]' 2>/dev/null | jq -s '.'); then
  ISSUE_COMMENTS="[]"
fi
if ! INLINE_COMMENTS=$(gh api --paginate "repos/${OWNER_REPO}/pulls/${PR_NUMBER}/comments" --jq '.[]' 2>/dev/null | jq -s '.'); then
  INLINE_COMMENTS="[]"
fi

REPLIES=$(jq -cn \
  --argjson reviews "${REVIEWS}" \
  --argjson issue "${ISSUE_COMMENTS}" \
  --argjson inline "${INLINE_COMMENTS}" \
  --arg since "${SINCE}" \
  --arg reviewer "${REVIEWER_LOGIN}" \
  --argjson max "${MAX_REPLIES}" \
  --argjson maxchars "${MAX_BODY_CHARS}" '
  def trim_body: if length > $maxchars then .[0:$maxchars] + " [truncated]" else . end;
  [
    ($issue[] | {kind: "comment", author: .user.login, author_type: .user.type,
                 at: .created_at, body: (.body // "")}),
    ($reviews[] | select((.body // "") != "")
                | select((.body | contains("<!-- conductor-review -->")) | not)
                | {kind: "review", author: .user.login, author_type: .user.type,
                   at: .submitted_at, body: .body}),
    ($inline[] | {kind: "inline", author: .user.login, author_type: .user.type,
                  at: .created_at, path: .path, line: (.line // .original_line),
                  body: (.body // "")})
  ]
  | map(select(.at > $since and .author != $reviewer))
  | map(.body |= trim_body)
  | sort_by(.at) | .[-$max:]')

COUNT=$(echo "${REPLIES}" | jq 'length')
emit "${COUNT} replies on PR #${PR_NUMBER} since the last conductor review (${SINCE})." "${REPLIES}"
