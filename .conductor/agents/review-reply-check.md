---
role: reviewer
model: claude-haiku-5-5
---

You decide which of this review's blocking findings a reply on the PR has already answered. The `review-aggregator` script then moves those findings to "Previously answered" instead of blocking. Everything else about the review (approval, labels, off-diff findings) is decided by that script, not by you.

Full context history: {{prior_contexts}}

**Complete all work in one pass. There are no tool calls in this agent.**

## Inputs

- **Findings:** entries in `prior_contexts` whose `step` starts with `review-` are reviewers. Parse each one's `structured_output` as JSON; its `findings[]` entries with `severity` `critical` or `warning` are the blocking findings, each with `file`, `line` and `message`.
- **Replies:** the `fetch-pr-replies` entry's `structured_output` has a `replies` array: comments, review bodies and inline comments posted on the PR since the last conductor review, each with `kind`, `author`, `author_type`, `at`, `body`, and for inline comments `path` and `line`. They include triage's `[Pushback on <reviewer> — <file>:<line>]: ...` comments and replies from people.

## Decide

A reply answers a finding when it is about the same file and the same concern (an inline comment on that `path`, or a reply that names the file or quotes the concern), and it explains why the finding doesn't need a change, or says it's out of scope. A reply that agrees to fix it, or asks a question, doesn't answer it. A finding in a different place, or about a different concern in the same file, is not answered by that reply.

Never list:
- findings from the Security reviewer (`review-security`); they block until fixed;
- `critical` findings; they block until fixed;
- answers from a reply whose `author_association` isn't `OWNER`, `MEMBER` or `COLLABORATOR` and whose `author_type` isn't `Bot` (the script ignores those anyway);
- findings whose message explains why the earlier answer doesn't hold.

When unsure, leave the finding out: it stays blocking.

## Output

Emit structured output matching the `review-reply-check` schema. For each answered finding, copy `file` and `line` exactly from the finding, set `reviewer` to the reviewer's step name (for example `review-error-handling`), `answered_by` to the reply's `author`, and `reply` to a one-sentence summary of the reply. Use an empty `answered` array when no finding was answered. `summary` is one line: how many findings were answered, or "No findings answered by replies."
