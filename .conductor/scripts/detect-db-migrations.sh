#!/usr/bin/env bash
set -euo pipefail

# Emits has_db_migrations when the diff changes the SQLite schema or the SQL
# that runs against it (#182). runkon has no migrations directory: the schema
# lives in Rust source (CANONICAL_SCHEMA_DDL, create_canonical_schema, the
# migrate_v* functions in runkon-flow/src/persistence_sqlite.rs, and the shared
# column lists in constants.rs). Matching that file by path would run the
# reviewer on nearly every persistence PR, so detection reads the changed
# content instead.
#
# This is a keyword heuristic. A false positive (SQL in a test fixture) costs
# one reviewer run. A miss is possible when a changed line inside a multi-line
# SQL string carries no keyword and its hunk header is an `impl` line.
#
# BASE_BRANCH, SCOPE and SINCE are injected by the workflow (see
# detect-workflow-changes.sh).
# shellcheck source=lib-diff-range.sh
source "$(dirname "$0")/lib-diff-range.sh"
resolve_diff_range

# Schema identifiers, then upper-case SQL (word-bounded, so DEFAULT_TIMEOUT and
# similar constants don't match).
readonly SCHEMA_PATTERN='(CANONICAL_SCHEMA_DDL|SCHEMA_HISTORY_DDL|create_canonical_schema|migrate_v[0-9]+|record_version|runkon_flow_schema_versions|step_roles!|role_check!|ROLE_CHECK|RUN_COLUMNS|STEP_COLUMNS|sqlite_master)'
readonly SQL_PATTERN='\b((CREATE|ALTER|DROP) +(TABLE|INDEX|UNIQUE|TRIGGER|VIEW)|PRAGMA|INSERT( OR [A-Z]+)? +INTO|UPDATE +[a-z_]+ +SET|DELETE +FROM|SELECT|FROM|WHERE|JOIN|SET|REFERENCES|CHECK *\(|ON +CONFLICT|NOT +NULL|PRIMARY +KEY|TEXT|INTEGER|REAL|BLOB)\b'

# -z gives NUL-separated, unquoted names. Command substitution would drop the
# NULs, so go through a temp file.
diff_tmp=$(mktemp)
hunk_tmp=$(mktemp)
trap 'rm -f "$diff_tmp" "$hunk_tmp"' EXIT
changed_files_to "$diff_tmp"

matched=()
while IFS= read -r -d '' file; do
  [[ -z "$file" ]] && continue
  case "$file" in
    runkon-flow/src/*.rs|runkon-flow/src/**/*.rs) ;;
    *) continue ;;
  esac
  # A failed diff must fail the step, so the reviewer is never skipped
  # silently; hence a temp file rather than a pipe.
  if ! git diff -U0 --no-color "${DIFF_ARGS[@]}" -- "$file" > "$hunk_tmp"; then
    echo "ERROR: git diff ${DIFF_ARGS[*]} -- $file failed." >&2
    exit 1
  fi
  # Changed lines, plus hunk headers: git puts the enclosing column-0 line
  # (the CREATE TABLE of a DDL string, an fn, a const) after the second @@,
  # which catches a bare column line edited inside the DDL.
  # The last grep must read all its input (no -q): under pipefail an early
  # exit would SIGPIPE an upstream grep and a real match would read as none.
  # Every stage failing to match is a plain "no match", which is what the
  # `if` wants.
  if grep -E '^([+-]|@@ )' "$hunk_tmp" | grep -vE '^(\+\+\+|---) ' \
      | grep -E "${SCHEMA_PATTERN}|${SQL_PATTERN}" > /dev/null; then
    matched+=("$file")
  fi
done < "$diff_tmp"

# jq escapes every character in the file names; a hand-built JSON string would
# be invalid for names with quotes or backslashes and the engine would drop the
# markers without any error.
emit() {
  echo "<<<FLOW_OUTPUT>>>"
  jq -cn --argjson markers "$1" --arg ctx "$2" '{markers: $markers, context: $ctx}'
  echo "<<<END_FLOW_OUTPUT>>>"
}

count=${#matched[@]}
if [ "$count" -gt 0 ]; then
  file_list=$(IFS=", "; echo "${matched[*]}")
  emit '["has_db_migrations"]' "Found schema/SQL changes in ${count} file(s) in ${DIFF_DESC}: ${file_list}"
else
  emit '[]' "No schema or SQL changes in ${DIFF_DESC}"
fi
