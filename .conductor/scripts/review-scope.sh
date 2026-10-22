#!/usr/bin/env bash
# review-scope.sh — choose what the next review-pr run in iterate-pr covers
# Called as a script step before each review-pr call. Env vars: PR_NUMBER
#
# Emits `review_scope` ("full" or "incremental") and `review_since` (a commit
# SHA, or "") as FLOW_OUTPUT fields, exposed to later steps as
# {{review_scope}} / {{review_since}}.
#
# - incremental, since <sha>: the most recent conductor review on this PR was
#   submitted on <sha>, and <sha> is still an ancestor of HEAD. The next review
#   covers <sha>..HEAD, i.e. every commit pushed since the last review, however
#   many there are (address-reviews, fmt and lint commits).
# - full: no earlier conductor review, its commit is no longer in HEAD's
#   history (rebase, force-push), or anything here fails. The first review of a
#   PR always covers the whole PR, whatever the commit layout.
set -euo pipefail

emit() {
  # $1 = scope, $2 = since, $3 = context
  jq -cn --arg scope "$1" --arg since "$2" --arg context "$3" \
    '{markers: [], context: $context, review_scope: $scope, review_since: $since}' \
    | { printf '<<<FLOW_OUTPUT>>>\n'; cat; printf '<<<END_FLOW_OUTPUT>>>\n'; }
}

if [ -z "${PR_NUMBER:-}" ] || [[ "${PR_NUMBER}" == *"{{"* ]]; then
  PR_NUMBER=$(gh pr view --json number -q .number 2>/dev/null || true)
fi
if ! [[ "${PR_NUMBER:-}" =~ ^[0-9]+$ ]]; then
  emit full "" "No PR number; reviewing the full PR."
  exit 0
fi

OWNER_REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null || true)
if [ -z "${OWNER_REPO}" ] \
  || ! REVIEWS=$(gh api --paginate "repos/${OWNER_REPO}/pulls/${PR_NUMBER}/reviews" --jq '.[]' 2>/dev/null | jq -s '.'); then
  emit full "" "Could not read earlier reviews; reviewing the full PR."
  exit 0
fi

SINCE=$(echo "${REVIEWS}" | jq -r '
  [.[] | select((.body // "") | contains("<!-- conductor-review -->"))]
  | sort_by(.submitted_at) | last | .commit_id // empty')

if [ -z "${SINCE}" ]; then
  emit full "" "No earlier conductor review on PR #${PR_NUMBER}; reviewing the full PR."
  exit 0
fi
if ! git merge-base --is-ancestor "${SINCE}" HEAD 2>/dev/null; then
  emit full "" "Last reviewed commit ${SINCE:0:8} is not in HEAD's history (rebase or force-push); reviewing the full PR."
  exit 0
fi
if [ "$(git rev-parse HEAD)" = "$(git rev-parse "${SINCE}")" ]; then
  emit full "" "No commits since the last review (${SINCE:0:8}); reviewing the full PR."
  exit 0
fi

COUNT=$(git rev-list --count "${SINCE}..HEAD")
emit incremental "${SINCE}" "Reviewing ${COUNT} commit(s) since the last review (${SINCE:0:8})."
