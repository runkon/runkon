## Diff scope rules

### Working directory — do not `cd`

Run every `git` command from your current working directory. Do **NOT**
`cd` to any other path (including paths you may know from prior sessions
or memory). The harness has already placed you in the worktree that
holds this PR's branch — any other checkout will have a different
`HEAD`, producing a fabricated diff that contaminates the entire review
(this is the same class of bug that `resolve-pr-base.sh` was added to
prevent).

### Diff command

Get the diff for this PR using the appropriate command for the review scope:

- If the scope is **full** (default): the PR's base branch has already been
  resolved by the workflow's `resolve-pr-base` step and is injected as
  `{{base_branch}}`. Use it literally — do not run `gh pr view` or compute
  the base yourself.

  ```bash
  git diff "origin/{{base_branch}}...HEAD"
  ```

- If the scope is **incremental**: run `git diff HEAD~1` to see only the latest commit.

**Review scope: {{scope}}**

If the diff exceeds ~50KB, focus on files most relevant to your review area.

If an **incremental** diff comes back surprisingly large (hundreds of
KB, or contains files unrelated to the ticket), do **not** try to filter
it down by reading individual files — re-run the diff against
`origin/{{base_branch}}...HEAD` instead. A bloated incremental diff
almost always means `HEAD` is not where you expect it (e.g. you are in
the wrong working directory, or the worktree has just been rebased over
many upstream commits). Switch commands and proceed; do not waste
turns picking through a fabricated diff.

**In scope — review carefully:**
- Lines starting with `+` (added code)
- Lines starting with `-` only when the replacement logic is relevant

**Out of scope — do not flag:**
- Context lines (no `+`/`-` prefix) — these are unchanged
- Pure deletions with no replacement unless they introduce a regression
- Formatting-only changes (whitespace, import ordering)

## Path verification (required for every finding)

Before emitting any entry in `findings`, verify that the cited `file` path
appears in the diff. The diff lists files via `diff --git a/<path> b/<path>`
and `+++ b/<path>` headers. If you cannot find the path in those headers,
**drop the finding** — it does not belong in `findings`.

Recognising a common pattern (CORS configuration, error handling shape,
logging idioms, etc.) is **not** evidence the code exists in this PR. The
diff is the only authoritative source. A submit-review safety net filters
hallucinated findings deterministically, but each false finding still
wastes reviewer attention — do not emit one in the first place.

This rule applies to `off_diff_findings` too: those must point to real
files visible in the diff context (the surrounding `--- a/<path>` blocks
of unchanged code, or files mentioned in the diff stat). A finding about
code that does not appear anywhere in the diff is almost certainly
hallucinated — drop it.

## Output format

Severity guide:
- **critical**: Bugs, security holes, data loss — blocks merge
- **warning**: Design or correctness concern — should be addressed

Only flag `critical` or `warning` issues. Do not emit suggestion-level or style findings.

Your `FLOW_OUTPUT` `context` field must be a **JSON object** (not plain text) so the aggregator can parse it. Use this structure:

```json
{
  "approved": true,
  "findings": [
    {
      "file": "src/foo.rs",
      "line": 42,
      "severity": "warning",
      "message": "One-line description",
      "suggestion": "How to fix it"
    }
  ],
  "off_diff_findings": [
    {
      "file": "src/bar.rs",
      "line": 10,
      "title": "Short issue title",
      "severity": "warning",
      "body": "Detailed description of the pre-existing issue"
    }
  ],
  "summary": "One-sentence summary of your review"
}
```

- `findings`: issues in code **added or modified by this PR** — set `approved: false` if any are `critical` or `warning`
- `off_diff_findings`: issues in **unchanged/removed code** — never affect `approved`, filed as separate GitHub issues; only include `critical` or `warning` severity
- Omit `off_diff_findings` entirely if there are none

If you find **critical** or **warning** `findings`, include `has_review_issues` in your FLOW_OUTPUT markers.
If you find no findings, do NOT include that marker.
