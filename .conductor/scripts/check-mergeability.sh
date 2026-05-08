#!/usr/bin/env bash
set -euo pipefail

max_attempts=3
attempt=0

while [ $attempt -lt $max_attempts ]; do
  json=$(gh pr view --json mergeable,mergeStateStatus 2>/dev/null || echo "")
  mergeable=$(echo "$json" | jq -r '.mergeable // "UNKNOWN"')
  merge_state=$(echo "$json" | jq -r '.mergeStateStatus // "UNKNOWN"')

  if [ "$mergeable" != "UNKNOWN" ]; then
    break
  fi

  attempt=$((attempt + 1))
  if [ $attempt -lt $max_attempts ]; then
    sleep 5
  fi
done

if [ "$mergeable" = "CONFLICTING" ]; then
  cat <<EOF
<<<FLOW_OUTPUT>>>
{"markers": ["has_conflicts"], "context": "PR is CONFLICTING (mergeStateStatus: $merge_state) — rebase needed"}
<<<END_FLOW_OUTPUT>>>
EOF
else
  cat <<EOF
<<<FLOW_OUTPUT>>>
{"markers": [], "context": "PR is mergeable (mergeStateStatus: $merge_state) — no rebase needed"}
<<<END_FLOW_OUTPUT>>>
EOF
fi

exit 0
