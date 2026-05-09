#!/usr/bin/env bash
# Run cargo llvm-cov across the workspace.
#
# Usage: scripts/coverage.sh [extra cargo-llvm-cov args]
#
# Examples:
#   scripts/coverage.sh                       # summary table
#   scripts/coverage.sh --html                # write HTML report under target/llvm-cov
#   scripts/coverage.sh --lcov --output-path lcov.info
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if ! cargo llvm-cov --version >/dev/null 2>&1; then
  cat >&2 <<'EOF'
error: cargo-llvm-cov is not installed.

Install it once with:
  rustup component add llvm-tools-preview
  cargo install cargo-llvm-cov

Then re-run scripts/coverage.sh.
EOF
  exit 1
fi

exec cargo llvm-cov --workspace "$@"
