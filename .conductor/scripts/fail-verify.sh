#!/usr/bin/env bash
# Ends ticket-to-pr when verify still reports failures after the implement
# retry, so lint-fix and push-and-pr never push a branch or open a PR with
# failing checks. VERIFY_CONTEXT is the second verify's context, which names
# the failing checks. The message goes to stderr, which becomes the step error.
#
# A plain resume re-runs this step with the second verify's marker restored, so
# it fails again. Resuming with --from-step verify restarts at the first verify
# (from_step matches the first step with that name), which re-checks the code
# and, if it now passes, skips the retry and goes on to lint-fix (devinrosen/conductor-ai#3219).
set -uo pipefail

echo "verify still reports failures after the implement retry; stopping before lint-fix and push-and-pr. ${VERIFY_CONTEXT:-(verify gave no context)}" >&2
echo "To continue: fix the failures in the worktree, then resume with --from-step verify (conductor_resume_run from_step = \"verify\"). A plain resume fails again here." >&2
exit 1
