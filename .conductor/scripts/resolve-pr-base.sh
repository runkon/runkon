#!/usr/bin/env bash
# resolve-pr-base.sh — resolve the PR's base branch ONCE per workflow run and
# expose it to downstream steps via the engine's variable substitution layer.
#
# Emits a FLOW_OUTPUT block whose `base_branch` field is picked up by
# runkon-flow/src/prompt_builder.rs::build_variable_map and exposed as
# `{{base_branch}}` in subsequent step prompts and env-var bindings.
#
# Why this exists: reviewer agents running `gh pr view` from inside their own
# subprocess routinely cd into a different repo path before the lookup. From
# the wrong cwd, `gh pr view` returns nothing and the silent `|| echo main`
# fallback fabricates a diff against `main`. See #2731 / #2735 / #2736.
set -euo pipefail

BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
if [ -z "${BRANCH}" ] || [ "${BRANCH}" = "HEAD" ]; then
  echo "ERROR: not on a named branch (got '${BRANCH}') — cannot resolve PR base." >&2
  exit 1
fi

# `gh pr list --head <branch>` keys on the explicit branch name and is not
# affected by cwd or the currently checked-out HEAD of an unrelated repo.
BASE_BRANCH=$(gh pr list --head "${BRANCH}" --state open \
                --json baseRefName -q '.[0].baseRefName' 2>/dev/null || true)

if [ -z "${BASE_BRANCH}" ]; then
  echo "ERROR: could not resolve PR base branch for branch '${BRANCH}'." >&2
  echo "       Aborting rather than falling back to 'main' silently — a wrong" >&2
  echo "       base produces a fabricated diff that contaminates every reviewer." >&2
  exit 1
fi

# Build the FLOW_OUTPUT JSON via `jq -n --arg` so any quotes / backslashes /
# control chars in BASE_BRANCH are encoded safely. String interpolation here
# would let a crafted branch name (e.g. one containing `\` or `"`) inject
# extra keys into the parsed FlowOutput.
PAYLOAD=$(jq -nc --arg base "${BASE_BRANCH}" \
  '{markers: ["base_branch_resolved"], context: $base, base_branch: $base}')

printf '<<<FLOW_OUTPUT>>>\n%s\n<<<END_FLOW_OUTPUT>>>\n' "${PAYLOAD}"
