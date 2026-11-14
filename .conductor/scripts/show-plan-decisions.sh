#!/usr/bin/env bash
# show-plan-decisions.sh — list the plan's open decisions before the
# human_review gate after plan (#153, port of devinrosen/conductor-ai#3128). A gate's prompt is fixed text, so the
# questions go in this step's context, where whoever answers the gate reads them.
# Env: PRIOR_OUTPUT — the plan step's structured output (task-plan schema).
# PLAN.md is in the worktree's git directory, so its absolute path also goes in
# the context and is emitted as plan_path (#194, port of conductor-ai#3334).
# Never fails the step: the gate still runs, pointing at PLAN.md if needed.
set -uo pipefail

emit() {
  jq -cn --arg ctx "$1" --arg p "${plan_path}" '{markers: [], context: $ctx, plan_path: $p}' \
    | { printf '<<<FLOW_OUTPUT>>>\n'; cat; printf '<<<END_FLOW_OUTPUT>>>\n'; }
}

if plan_dir=$(git rev-parse --absolute-git-dir 2>/dev/null) && [ -n "$plan_dir" ]; then
  plan_path="${plan_dir}/PLAN.md"
  plan_where="${plan_path}"
else
  plan_path=""
  echo "show-plan-decisions: not in a git repository, can't resolve PLAN.md's path" >&2
  plan_where="the worktree's git directory (run \`git rev-parse --absolute-git-dir\` in the worktree to find it)"
fi
see_plan="See \"Decisions for the user\" in ${plan_where}."

if ! jq_err=$(jq -e 'type == "object"' <<< "${PRIOR_OUTPUT:-}" 2>&1 >/dev/null); then
  echo "show-plan-decisions: the plan's output isn't a JSON object: ${jq_err:-empty PRIOR_OUTPUT}" >&2
  emit "The plan flagged decisions for you, but its output couldn't be read. ${see_plan}"
  exit 0
fi

decisions=$(jq -r '
  (.decisions // []) | to_entries
  | map("\(.key + 1). \(.value.question // "(no question)")\n   Default: \(.value.default // "(none given)")")
  | join("\n")' <<< "${PRIOR_OUTPUT}")

if [ -z "${decisions}" ]; then
  echo "show-plan-decisions: needs_decision was set but the plan's decisions list is empty" >&2
  emit "The plan flagged decisions for you but listed none in its output. ${see_plan}"
  exit 0
fi

emit "The plan needs your decision before implementing:

${decisions}

The full plan is at ${plan_where}.

Approve the gate with your answers as feedback, or approve without feedback to keep the defaults. Reject to stop the run."
