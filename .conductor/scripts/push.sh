#!/usr/bin/env bash
set -euo pipefail

push_output=$(git push -u --force-with-lease origin HEAD 2>&1)
echo "$push_output"

branch=$(git rev-parse --abbrev-ref HEAD)
echo "Pushed branch: $branch"

if echo "$push_output" | grep -q "Everything up-to-date"; then
  output='{"markers": []}'
else
  output='{"markers": ["pushed_new_commits"]}'
fi
printf '<<<FLOW_OUTPUT>>>\n%s\n<<<END_FLOW_OUTPUT>>>\n' "$output"
