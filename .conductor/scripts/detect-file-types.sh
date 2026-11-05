#!/usr/bin/env bash
set -euo pipefail

# BASE_BRANCH is injected by the workflow from the resolve-pr-base step.
# We refuse to silently fall back to a guess — a wrong base produces a
# fabricated diff that contaminates every conditional reviewer. See #2736.
if [ -z "${BASE_BRANCH:-}" ]; then
  echo "ERROR: BASE_BRANCH env var not set — workflow must run resolve-pr-base first." >&2
  exit 1
fi

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
  emit '["has_code_changes"]' "Found ${count} code file(s) in diff: ${file_list}"
else
  emit '[]' "No code files in diff"
fi
