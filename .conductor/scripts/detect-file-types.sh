#!/usr/bin/env bash
set -euo pipefail

# BASE_BRANCH, SCOPE and SINCE are injected by the workflow. BASE_BRANCH comes
# from the resolve-pr-base step; there is no silent fallback to a guessed base
# (devinrosen/conductor-ai#2736). An incremental review decides from the commits since the last review.
# shellcheck source=lib-diff-range.sh
source "$(dirname "$0")/lib-diff-range.sh"
resolve_diff_range

# -z gives NUL-separated, unquoted names.
# Command substitution would drop the NULs, so go through a temp file.
diff_tmp=$(mktemp)
trap 'rm -f "$diff_tmp"' EXIT
changed_files_to "$diff_tmp"

# Filter for code files, excluding .conductor/, docs/, .github/, and root-level *.md.
# Shell scripts under .conductor/scripts/ count as code, so the security and
# error-handling reviewers cover them.
code_files=()
while IFS= read -r -d '' file; do
  [[ -z "$file" ]] && continue

  # Pipeline shell scripts run gh and git, so they count as code
  if [[ "$file" == .conductor/scripts/*.sh ]]; then
    code_files+=("$file")
    continue
  fi

  # Exclude specific directories and root-level .md files
  [[ "$file" == .conductor/* ]] && continue
  [[ "$file" == docs/* ]] && continue
  [[ "$file" == .github/* ]] && continue
  [[ "$file" == *.md && "$file" != */* ]] && continue

  # Include only code file extensions
  case "$file" in
    *.rs|*.ts|*.tsx|*.js|*.css|Cargo.toml|Cargo.lock)
      code_files+=("$file")
      ;;
  esac
done < "$diff_tmp"

count=${#code_files[@]}

# jq escapes every character in the file names; a hand-built JSON string would
# be invalid for names with quotes or backslashes and the engine would drop the
# markers without any error.
emit() {
  echo "<<<FLOW_OUTPUT>>>"
  jq -cn --argjson markers "$1" --arg ctx "$2" '{markers: $markers, context: $ctx}'
  echo "<<<END_FLOW_OUTPUT>>>"
}

if [ "$count" -gt 0 ]; then
  file_list=$(IFS=", "; echo "${code_files[*]}")
  emit '["has_code_changes"]' "Found ${count} code file(s) in ${DIFF_DESC}: ${file_list}"
else
  emit '[]' "No code files in ${DIFF_DESC}"
fi
