## Off-diff findings

While reviewing, you may encounter issues in unchanged or removed code that are real problems but should NOT block this PR (e.g., pre-existing bugs, tech debt, or design flaws in unmodified files).

For each such finding, populate the `off_diff_findings` field in your FLOW_OUTPUT. Off-diff findings do NOT affect whether this PR gets approved — they are filed as separate GitHub issues.

Only include `critical` or `warning` severity findings in `off_diff_findings`. Suggestion-level findings in off-diff code should be omitted entirely — they will not be filed as GitHub issues.
