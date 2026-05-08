#!/usr/bin/env bash
set -euo pipefail

# BASE_BRANCH is injected by the workflow from the resolve-pr-base step.
# We refuse to silently fall back to a guess — a wrong base produces a
# fabricated diff that contaminates every conditional reviewer. See #2736.
if [ -z "${BASE_BRANCH:-}" ]; then
  echo "ERROR: BASE_BRANCH env var not set — workflow must run resolve-pr-base first." >&2
  exit 1
fi

changed_files=$(git diff "origin/${BASE_BRANCH}...HEAD" --name-only 2>/dev/null || true)

# Filter for code files, excluding .conductor/, docs/, .github/, and root-level *.md
code_files=()
while IFS= read -r file; do
  [[ -z "$file" ]] && continue

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
done <<< "$changed_files"

count=${#code_files[@]}

if [ "$count" -gt 0 ]; then
  file_list=$(IFS=", "; echo "${code_files[*]}")
  cat <<EOF
<<<FLOW_OUTPUT>>>
{"markers": ["has_code_changes"], "context": "Found ${count} code file(s) in diff: ${file_list}"}
<<<END_FLOW_OUTPUT>>>
EOF
else
  cat <<'EOF'
<<<FLOW_OUTPUT>>>
{"markers": [], "context": "No code files in diff"}
<<<END_FLOW_OUTPUT>>>
EOF
fi
