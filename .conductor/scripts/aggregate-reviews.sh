#!/usr/bin/env bash
# aggregate-reviews.sh — combine the review swarm's outputs (#152, port of
# devinrosen/conductor-ai#3064).
#
# Runs as review-pr's `review-aggregator` step, replacing the aggregator agent:
# every output here is determined by the reviewers' outputs, so no model call.
#
# Env: PRIOR_CONTEXTS — the engine's prior_contexts JSON array. Each entry has
#   step, iteration, context, markers, structured_output (a JSON string or null).
#
# - Reviewer entries are the steps named in REVIEWERS below. A reviewer blocks
#   when it has the has_review_issues marker or any critical/warning finding.
#   A reviewer whose output can't be parsed or has no boolean `approved` (the
#   review-findings schema), or that doesn't approve but lists
#   no finding, gets a blocking finding saying so: malformed output is never
#   read as approval.
# - review-reply-check (a small agent that runs only when the PR has replies
#   since the last review) lists findings a reply already answered. Those move
#   to answered_findings, except: Security findings and critical findings,
#   which block until fixed (submit-review enforces Security too), and answers
#   from anyone but a repo owner, member or collaborator or an installed app.
# - Off-diff findings are deduplicated by (file, line), keeping the higher
#   severity, and labelled by reviewer (plus `bug` for critical).
#
# Emits the review-aggregator schema fields (reviewed_by, blocking_findings,
# answered_findings, off_diff_findings, overall_approved, summary) and, since a
# script step can't derive schema markers, the markers themselves: approved,
# has_review_issues, has_blocking_findings, has_warnings.
set -euo pipefail

emit_error() {
  # Fails closed: an aggregation error must not read as approval.
  echo "ERROR: $1" >&2
  jq -cn --arg msg "$1" '{
    markers: ["has_review_issues", "has_blocking_findings", "has_warnings"],
    context: ("Review aggregation failed: " + $msg),
    reviewed_by: "",
    blocking_findings: [{reviewer: "Aggregator", file: "", line: 0, severity: "warning",
      message: ("The review could not be aggregated: " + $msg)}],
    answered_findings: [], off_diff_findings: [], overall_approved: false,
    summary: ("Review aggregation failed: " + $msg)
  }' | { printf '<<<FLOW_OUTPUT>>>\n'; cat; printf '<<<END_FLOW_OUTPUT>>>\n'; }
  exit 1
}

PRIOR_CONTEXTS="${PRIOR_CONTEXTS:-}"
if [ -z "${PRIOR_CONTEXTS}" ] || ! jq -e 'type == "array"' >/dev/null 2>&1 <<< "${PRIOR_CONTEXTS}"; then
  emit_error "PRIOR_CONTEXTS is empty or not a JSON array"
fi

if ! result=$(jq -c '
  # step name -> display name and off-diff labels
  def reviewers: {
    "review-architecture":       {name: "Architecture",       labels: ["refactor"]},
    "review-dry-abstraction":    {name: "DRY Abstraction",    labels: ["refactor"]},
    "review-security":           {name: "Security",           labels: ["security"]},
    "review-performance":        {name: "Performance",        labels: ["enhancement"]},
    "review-error-handling":     {name: "Error Handling",     labels: ["bug"]},
    "review-test-coverage":      {name: "Test Coverage",      labels: ["enhancement"]},
    "review-workflow-semantics": {name: "Workflow Semantics", labels: ["bug"]}
  };
  def sev_rank: if . == "critical" then 2 elif . == "warning" then 1 else 0 end;
  # structured_output first, then the context string, as JSON objects.
  def json_obj: strings | try fromjson catch empty | objects;
  def parsed:
    ((.structured_output // empty) | json_obj)
    // ((.context // empty) | json_obj)
    // null;
  # A reviewer output counts only with a boolean `approved` (review-findings
  # schema); anything else is unreadable and blocks.
  def review_out:
    ([ ((.structured_output // empty) | json_obj), ((.context // empty) | json_obj) ]
     | map(select((.approved | type) == "boolean")) | first) // null;

  . as $all
  | [ $all[] | select(reviewers[.step] != null) ]
    # A retried or repeated reviewer: keep its last entry.
    | group_by(.step) | map(last) as $entries
  | [ $entries[] | . as $e | reviewers[$e.step] as $r | ($e | review_out) as $out
      | {
          step: $e.step, name: $r.name, labels: $r.labels, out: $out,
          marker_blocks: (($e.markers // []) | index("has_review_issues") != null)
        } ] as $revs

  # Findings that block, per reviewer.
  | [ $revs[] | . as $v
      | if $v.out == null then
          [{reviewer: $v.name, file: "", line: 0, severity: "warning",
            message: ("The " + $v.name + " reviewer returned no parseable output, so its review could not be read. Re-run the review.")}]
        else
          ( [ ($v.out.findings // [])[]? | objects
              | select((.severity // "") | IN("critical", "warning"))
              | {reviewer: $v.name, file: ((.file // "") | tostring), line: ((.line // 0) | tonumber? // 0),
                 severity: .severity,
                 message: ((.message // "") + (if (.suggestion // "") != "" then " Suggested fix: " + .suggestion else "" end))} ] ) as $f
          | if ($f | length) == 0 and ($v.marker_blocks or ($v.out.approved == false)) then
              [{reviewer: $v.name, file: "", line: 0, severity: "warning",
                message: ("The " + $v.name + " reviewer did not approve but listed no finding: " + (($v.out.summary // "") | tostring))}]
            else $f end
        end
    ] | add // [] | . as $blocking_all

  # Replies that answered a finding (review-reply-check), matched by reviewer, file and line.
  | ([ $all[] | select(.step == "review-reply-check") ] | last // null | if . == null then [] else (parsed // {} | .answered // []) end) as $answers
  # Only a reply from someone with write access to the repo (owner, member,
  # collaborator) or from an app installed on it (a Bot, such as the pipeline
  # posting triage pushback) can set a finding aside; anyone can comment on a
  # public PR.
  | ([ $all[] | select(.step == "fetch-pr-replies") ] | last // null
     | if . == null then [] else ((parsed // {}) | .replies // []) end
     | [ .[]? | objects
         | select(((.author_association // "") | IN("OWNER", "MEMBER", "COLLABORATOR")) or .author_type == "Bot")
         | .author ] | unique) as $trusted
  # review-reply-check names the reviewer by step name; accept the display name too.
  | [ $blocking_all[] | . as $b
      | ([ reviewers | to_entries[] | select(.value.name == $b.reviewer) | .key ] | first // "") as $b_step
      | ([ $answers[]? | objects
           | select(((.reviewer // "") | IN($b.reviewer, $b_step)) and ((.file // "") | tostring) == $b.file
                    and (((.line // 0) | tonumber? // 0)) == $b.line) ] | first) as $a
      | if $a != null and $b.reviewer != "Security" and $b.severity != "critical"
           and ((($a.answered_by // "") | tostring) | IN($trusted[]))
        then $b + {answered: true, answered_by: (($a.answered_by // "") | tostring), reply: (($a.reply // "") | tostring)}
        else $b + {answered: false} end
    ] as $marked
  | [ $marked[] | select(.answered | not) | del(.answered) ] as $blocking
  | [ $marked[] | select(.answered) | del(.answered) ] as $answered

  # Off-diff findings: dedupe by (file, line), keep the higher severity.
  | [ $revs[] | . as $v | ($v.out.off_diff_findings // [])[]? | objects
      | {file: ((.file // "") | tostring), line: ((.line // 0) | tonumber? // 0),
         severity: (if (.severity // "") == "critical" then "critical" else "warning" end),
         title: ((.title // "") | tostring), message: ((.body // .message // "") | tostring),
         reviewer: $v.name,
         labels: ($v.labels + (if .severity == "critical" then ["bug"] else [] end) | unique)} ]
    | group_by([.file, .line]) | map(max_by(.severity | sev_rank)) as $off_diff

  | ($blocking | length == 0) as $approved
  | {
      markers: (
        (if $approved then ["approved"] else ["has_review_issues"] end)
        + (if ($blocking | length) > 0 then ["has_blocking_findings"] else [] end)
        + (if any($blocking[]; .severity == "warning") then ["has_warnings"] else [] end)),
      reviewed_by: ([ $revs[].name ] | join(", ")),
      blocking_findings: $blocking,
      answered_findings: $answered,
      off_diff_findings: $off_diff,
      overall_approved: $approved,
      summary: (
        if ($revs | length) == 0 then "No reviewer output found."
        elif $approved then "All reviewers approved."
          + (if ($answered | length) > 0 then " \($answered | length) finding(s) were already answered on the PR." else "" end)
        else "\($blocking | length) blocking finding(s) from \([ $blocking[].reviewer ] | unique | join(", "))."
          + (if ($answered | length) > 0 then " \($answered | length) already answered." else "" end)
        end)
    }
  | .context = .summary
' <<< "${PRIOR_CONTEXTS}"); then
  emit_error "jq failed to aggregate the reviewer outputs"
fi

# No reviewer ran at all (every entry missing): never approve an empty review.
if [ "$(jq '[.reviewed_by] | .[0] == ""' <<< "${result}")" = "true" ]; then
  emit_error "no reviewer output was found in prior_contexts"
fi

printf '<<<FLOW_OUTPUT>>>\n%s\n<<<END_FLOW_OUTPUT>>>\n' "${result}"
