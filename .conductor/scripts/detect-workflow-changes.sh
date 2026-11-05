#!/usr/bin/env bash
set -euo pipefail

# BASE_BRANCH is injected by the workflow from the resolve-pr-base step.
# We refuse to silently fall back to a guess — a wrong base produces a
# fabricated diff that contaminates the downstream review. See #2736.
if [ -z "${BASE_BRANCH:-}" ]; then
  echo "ERROR: BASE_BRANCH env var not set — workflow must run resolve-pr-base first." >&2
  exit 1
fi

# Get changed files relative to the PR base branch
# A failed diff (for example a missing base ref) must fail the step. Reading it
# as an empty list would skip every conditional reviewer without any error.
# -z gives NUL-separated, unquoted names.
# Command substitution would drop the NULs, so go through a temp file.
diff_tmp=$(mktemp)
trap 'rm -f "$diff_tmp"' EXIT
if ! git diff "origin/${BASE_BRANCH}...HEAD" --name-only -z > "$diff_tmp"; then
  echo "ERROR: git diff against origin/${BASE_BRANCH} failed." >&2
  exit 1
fi

# Filter for pipeline files: workflows, scripts, schemas, and agent and prompt
# briefs.
workflow_files=()
while IFS= read -r -d '' file; do
  [[ -z "$file" ]] && continue
  case "$file" in
    .conductor/workflows/*|.conductor/scripts/*|.conductor/schemas/*|.conductor/agents/*|.conductor/prompts/*)
      workflow_files+=("$file")
      ;;
  esac
done < "$diff_tmp"

count=${#workflow_files[@]}

# jq escapes every character in the file names; a hand-built JSON string would
# be invalid for names with quotes or backslashes and the engine would drop the
# markers without any error.
emit() {
  echo "<<<FLOW_OUTPUT>>>"
  jq -cn --argjson markers "$1" --arg ctx "$2" '{markers: $markers, context: $ctx}'
  echo "<<<END_FLOW_OUTPUT>>>"
}

if [ "$count" -gt 0 ]; then
  file_list=$(IFS=", "; echo "${workflow_files[*]}")
  emit '["has_workflow_changes"]' "Found ${count} workflow/pipeline file(s) in diff: ${file_list}"
else
  emit '[]' "No workflow/pipeline files in diff"
fi
