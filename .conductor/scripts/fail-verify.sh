#!/usr/bin/env bash
# Ends ticket-to-pr when verify still reports failures after the implement
# retry, so lint-fix and push-and-pr never push a branch or open a PR with
# failing checks. VERIFY_CONTEXT is the second verify's context, which names
# the failing checks. The message goes to stderr, which becomes the step error.
set -uo pipefail

echo "verify still reports failures after the implement retry; stopping before lint-fix and push-and-pr. ${VERIFY_CONTEXT:-(verify gave no context)}" >&2
exit 1
