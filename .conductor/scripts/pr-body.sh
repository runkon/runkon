#!/usr/bin/env bash
# pr-body.sh — print the PR description built from PRIOR_CONTEXTS (#3148, #3333; runkon #181).
#
# PRIOR_CONTEXTS is the engine's {{prior_contexts}} JSON array of {step, context,
# structured_output, ...}, one entry per completed step in run order (the engine
# appends, so a repeated name keeps every occurrence). The body is:
#   ## Summary         the plan's summary (implement's summary when there is no plan)
#   ## Implementation  the first implement / implement-ticket context (later ones are
#                      verify-loopback fixes), minus sentences claiming the code wasn't
#                      built, tested, compiled or run: implement writes its summary before
#                      verify runs and is told not to run cargo (#3333)
#   ## Verification    what the last verify entry ran, when it passed
#   ## Risks and open questions
# Output is empty (exit 0) when there is no plan or implement summary, so the caller falls
# back to the commit bodies. Exit 1 with no stderr when PRIOR_CONTEXTS is unset, empty or
# itself a bare {{...}} template; any other value goes to jq, which exits non-zero with its
# error on stderr when it can't be parsed.
set -euo pipefail

contexts="${PRIOR_CONTEXTS:-}"
# Exit 1 (no stderr) only when the value is empty or is itself an unsubstituted template
# such as {{prior_contexts}} (#3350). Step outputs inside a real array may mention {{var}}
# freely, so don't scan the JSON for braces.
trimmed="${contexts#"${contexts%%[![:space:]]*}"}"
trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"
tmpl_re='^\{\{[[:alnum:]_.]+\}\}$'
if [ -z "$trimmed" ] || [[ "$trimmed" =~ $tmpl_re ]]; then
  exit 1
fi

printf '%s' "$contexts" | jq -r '
    def trim: gsub("^\\s+|\\s+$"; "");
    # A sentence that says the code was not built/tested/run (#3333). Keep narrow.
    def untested:
      test("(\\bnot|\\bnever|n'"'"'t)\\s+(been\\s+|yet\\s+)?(built|tested|compiled)\\b"; "i")
      or test("\\b(nothing|none of (it|this|these))\\b.*\\b(built|tested|compiled)\\b"; "i")
      or test("(\\bnot|\\bnever|n'"'"'t)\\s+(been\\s+|yet\\s+)?(run|verified)\\b.*\\b(cargo|build|tests?|clippy|fmt)\\b"; "i")
      or test("\\b(cargo|build|tests?|clippy|fmt)\\s+(((was|were|has|have|had|is|are)n'"'"'t|((was|were|has|have|had|is|are)\\s+)?(not|never))\\s+(been\\s+|yet\\s+)?(run|verified))\\b"; "i")
      or test("\\bdid(n'"'"'t| not)\\s+run\\b.*\\b(cargo|build|tests?|clippy|fmt)\\b"; "i")
      or test("^\\s*(not run|untested|not tested|not built)\\s*:"; "i")
      or test("\\bleft (it )?to (the )?verify\\b"; "i")
      or test("\\buncompiled\\b"; "i");
    def scrub:
      split("\n")
      | map( if trim == "" then ""
             else (split("(?<=[.!?])\\s+"; null) | map(select(untested | not)) | join(" ")) end )
      | join("\n")
      | gsub("\n{3,}"; "\n\n") | trim;
    def plan: [.[] | select(.step == "plan" and .structured_output != null)]
              | last | (.structured_output // "null") | (try fromjson catch null);
    def impl: [.[] | select(.step == "implement" or .step == "implement-ticket")]
              | first | (.context // "") | scrub;
    def verify: [.[] | select(.step == "verify")] | last;
    def verification:
      verify as $v
      | if $v == null then ""
        else ($v.structured_output // null | if type == "string" then (try fromjson catch null) else . end) as $o
          | ($v.context // "" | trim) as $c
          | if $o != null and ($o.passed == true) then
              ([($o.checks // [])[] | select(.status == "pass")]) as $ok
              | if ($ok | length) > 0 then
                  "The verify step passed before this PR was opened:\n\n"
                  + ($ok | map("- " + .name + (if ((.summary // "") | length) > 0 then ": " + .summary else "" end)) | join("\n"))
                elif $c != "" then $c else "" end
            elif $o == null and ($c | test("^PASS")) then $c
            else "" end
        end;
    plan as $p | impl as $i | verification as $vf
    | (($p.summary // "") | trim) as $s
    | ($p.risks // [] | map(select(type == "string" and length > 0))) as $r
    | if $s == "" and $i == "" then empty else
        ( if $s != "" then "## Summary\n\n\($s)" else "## Summary\n\n\($i)" end )
        + ( if $s != "" and $i != "" then "\n\n## Implementation\n\n\($i)" else "" end )
        + ( if $vf != "" then "\n\n## Verification\n\n\($vf)" else "" end )
        + ( if ($r | length) > 0 then "\n\n## Risks and open questions\n\n" + ($r | map("- " + .) | join("\n")) else "" end )
      end
  '
