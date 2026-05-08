#!/usr/bin/env bash
set -euo pipefail

git push -u --force-with-lease origin HEAD
branch=$(git rev-parse --abbrev-ref HEAD)

cat <<EOF
<<<FLOW_OUTPUT>>>
{"markers": ["pulled_new_commits"], "context": "Pushed rebased branch: $branch"}
<<<END_FLOW_OUTPUT>>>
EOF
