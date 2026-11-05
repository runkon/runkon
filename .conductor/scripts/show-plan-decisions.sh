#!/usr/bin/env bash
# show-plan-decisions.sh — list the plan's open decisions before the
# human_review gate after plan (#153, port of devinrosen/conductor-ai#3128). A gate's prompt is fixed text, so the
# questions go in this step's context, where whoever answers the gate reads them.
# Env: PRIOR_OUTPUT — the plan step's structured output (task-plan schema).
# Never fails the step: the gate still runs, pointing at PLAN.md if needed.
set -uo pipefail

emit() {
  jq -cn --arg ctx "$1" '{markers: [], context: $ctx}' \
    | { printf '<<<FLOW_OUTPUT>>>\n'; cat; printf '<<<END_FLOW_OUTPUT>>>\n'; }
}

plan_path="$(git rev-parse --git-dir 2>/dev/null)/PLAN.md"
see_plan="See \"Decisions for the user\" in ${plan_path}."

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

Approve the gate with your answers as feedback, or approve without feedback to keep the defaults. Reject to stop the run."
