#!/usr/bin/env bash
set -euo pipefail

base="${FEATURE_BASE_BRANCH:-main}"

current_branch=$(git rev-parse --abbrev-ref HEAD)

# Fail loudly if HEAD is on the base branch — commits landed in the wrong place
if [ "$current_branch" = "$base" ]; then
  echo "ERROR: HEAD is on base branch '$base', not a feature branch — aborting to prevent corrupting base" >&2
  exit 1
fi

# Optional stricter check: verify against workflow-supplied branch name
if [ -n "${WORKTREE_BRANCH:-}" ] && [ "$current_branch" != "$WORKTREE_BRANCH" ]; then
  echo "ERROR: current branch '$current_branch' != expected '$WORKTREE_BRANCH' — aborting" >&2
  exit 1
fi

# Push base branch to origin if it doesn't exist there yet
if ! git ls-remote --exit-code --heads origin "$base" > /dev/null 2>&1; then
  echo "Base branch '$base' not found on origin — pushing it now…"
  git push -u origin "$base"
fi

# Fetch latest base ref for accurate comparison
git fetch origin "$base" --quiet

# Early exit if no commits ahead of base
ahead=$(git rev-list --count "origin/$base..HEAD")
if [ "$ahead" -eq 0 ]; then
  cat <<EOF
<<<FLOW_OUTPUT>>>
{"markers": ["no_changes"], "context": "No commits ahead of $base — nothing to push or PR"}
<<<END_FLOW_OUTPUT>>>
EOF
  exit 0
fi

SKIP_E2E=1 git push -u origin "$current_branch"

# The PR description comes from what the run produced (#3148, #3333): the plan's
# summary and risks, implement's summary of what it changed and what verify ran.
# pr-body.sh builds it from PRIOR_CONTEXTS; without usable contexts, fall back to
# --fill (the commits' bodies), as before.
build_pr_body() {
  bash "$(dirname "$0")/pr-body.sh"
}

# Warn (never fail) when the contexts can't be read, separately from the
# expected case of a workflow with no plan or implement step.
pr_body=""
pr_body_err=$(mktemp)
if ! pr_body=$(build_pr_body 2>"$pr_body_err"); then
  pr_body=""
  if [ -s "$pr_body_err" ]; then
    echo "WARNING: could not parse PRIOR_CONTEXTS ($(head -c 300 "$pr_body_err")); using the commit bodies for the PR description" >&2
  fi
elif [ -z "$pr_body" ]; then
  echo "WARNING: no plan or implement summary in PRIOR_CONTEXTS; using the commit bodies for the PR description" >&2
fi
rm -f "$pr_body_err"

pr_body_file=$(mktemp)
trap 'rm -f "$pr_body_file"' EXIT
if [ -n "$pr_body" ]; then
  printf '%s\n' "$pr_body" > "$pr_body_file"
  pr_args=(--base "$base" --body-file "$pr_body_file")
else
  pr_args=(--fill --base "$base")
fi

# Link the ticket so close-linked-issues.yml closes it on a release-branch merge.
# Only a numeric ID counts (rejects empty, Jira keys, an unsubstituted template).
# The line is added with `gh pr edit` after creating the PR, so it lands on both
# the built body and the --fill one. An existing PR's body is left alone.
ticket_id="${TICKET_SOURCE_ID:-}"
case "$ticket_id" in
  ''|*[!0-9]*) ticket_id="" ;;
esac
created=0
link_warning=""

pr_create_err=$(mktemp)
if pr_url=$(gh pr create "${pr_args[@]}" 2>"$pr_create_err"); then
  created=1 # pr_url already set from stdout
else
  exit_code=$?
  if grep -qi "already exists" "$pr_create_err"; then
    pr_url=$(gh pr view --json url -q .url)
  else
    cat "$pr_create_err" >&2
    rm -f "$pr_create_err"
    exit $exit_code
  fi
fi
rm -f "$pr_create_err"

if [ "$created" -eq 1 ] && [ -n "$ticket_id" ]; then
  if body=$(gh pr view "$pr_url" --json body -q .body); then
    if printf '%s' "$body" | grep -qiE "(close[sd]?|fix(e[sd])?|resolve[sd]?):?[[:space:]]+#${ticket_id}([^0-9]|$)"; then
      : # body already links the ticket
    else
      if [ -n "$body" ]; then
        new_body="$body"$'\n\n'"Closes #${ticket_id}"
      else
        new_body="Closes #${ticket_id}"
      fi
      if ! printf '%s\n' "$new_body" | gh pr edit "$pr_url" --body-file - >/dev/null; then
        link_warning=" (warning: could not add Closes #${ticket_id})"
      fi
    fi
  else
    link_warning=" (warning: could not add Closes #${ticket_id})"
  fi
  if [ -n "$link_warning" ]; then
    echo "WARNING: could not add 'Closes #${ticket_id}' to $pr_url" >&2
  fi
fi

cat <<EOF
<<<FLOW_OUTPUT>>>
{"markers": [], "context": "PR is open at $pr_url$link_warning"}
<<<END_FLOW_OUTPUT>>>
EOF
