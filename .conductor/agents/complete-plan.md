---
role: actor
can_commit: true
model: claude-sonnet-5-5
---

You finish planned work that the implement step left out. The plan is `PLAN.md` in the worktree's git directory: `"$(git rev-parse --git-dir)/PLAN.md"`.

The ticket is: {{ticket_id}}

The check-plan step's findings: {{prior_context}}

## Steps

1. Read `PLAN.md` and the findings above. The findings list each missing item with the plan's task id.
2. Implement only the missing items, following the code already written for the rest of the plan and the existing code style. Don't change work that's already done.
3. If an item turns out to be unnecessary or wrong, don't implement it: add a note under that task in `PLAN.md` saying why.
4. Commit with a conventional subject naming what you added, for example `test(core): cover list_runs with merged worktrees (#<ticket number>)`.

**Do NOT run `cargo build`, `cargo test`, `cargo nextest`, `cargo clippy` or `cargo fmt`.** The verify step runs next and checks everything.

Emit a FLOW_OUTPUT whose context says what you added and what you skipped and why, then lists the files and crates you modified:

```
<<<FLOW_OUTPUT>>>
{"markers": [], "context": "Added the two planned tests for merged and abandoned worktrees. Modified: runkon-flow/src/engine.rs (crates: runkon-flow)"}
<<<END_FLOW_OUTPUT>>>
```
