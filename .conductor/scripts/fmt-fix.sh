#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all
if ! git diff --quiet; then
  git add -A
  git commit -m "style: cargo fmt"
fi
