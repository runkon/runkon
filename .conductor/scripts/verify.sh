#!/usr/bin/env bash
# verify.sh — the cargo checks that gate a PR, decided by exit codes (#151).
#
# Replaces the verify agent (port of devinrosen/conductor-ai#3265). Maps the
# changed files to crates, then runs:
#   cargo build
#   cargo nextest run -p <crate> --no-fail-fast, with each crate's test features
#   cargo clippy -p <crate> --all-targets --all-features -- -D warnings (as CI does)
#   cargo fmt --all --check
# A root Cargo.toml or Cargo.lock change checks every crate. Only .rs files,
# Cargo.toml, Cargo.lock and build.rs count as Rust changes; anything else
# (.wf, .md, .sh, frontend files) skips the checks.
#
# Emits has_failures when any check failed or couldn't run, with a context
# that names the failing checks, test names and the first compiler or clippy
# errors, which implement reads as its prior context. The FLOW_OUTPUT also
# carries `passed` and `checks` ({name, status, summary}) as structured output.
#
# Env: FEATURE_BASE_BRANCH (default main). VERIFY_CRATES overrides the crate
# list (space-separated), for testing.
#
# Call sites: ticket-to-pr.wf and iterate-pr.wf call this as `script verify`
# with FEATURE_BASE_BRANCH = {{feature_base_branch}}, twice each. A change to
# the path or env must be made in all four. A shared
# sub-workflow would hide verify's markers behind the call's name, which the
# callers' `if verify.has_failures` and fail-verify's resume guidance rely on.
set -uo pipefail

base="${FEATURE_BASE_BRANCH:-main}"
all_crates=(runkon-anthropic runkon-flow runkon-flow-executors runkon-google runkon-notify runkon-runtimes runkon-test-support)

# test_features CRATE — the --features a crate's tests need. runkon-flow's
# sqlite feature gates the SQLite persistence backend and its tests (#128),
# as in CI's test job.
test_features() {
  case "$1" in
    runkon-flow) echo "test-utils,sqlite" ;;
    runkon-anthropic|runkon-flow-executors|runkon-google|runkon-notify) echo "test-utils" ;;
    *) echo "" ;;
  esac
}
max_context=6000

checks_json='[]'
failed=0
fail_details=""
pass_names=""

emit() {
  # $1 = markers JSON, $2 = context, $3 = passed (true/false)
  echo "<<<FLOW_OUTPUT>>>"
  jq -cn --argjson markers "$1" --arg ctx "$2" --argjson passed "$3" --argjson checks "${checks_json}" \
    '{markers: $markers, context: $ctx, passed: $passed, checks: $checks}'
  echo "<<<END_FLOW_OUTPUT>>>"
}

# record NAME STATUS SUMMARY [DETAIL]
record() {
  checks_json=$(jq -c --arg n "$1" --arg s "$2" --arg m "$3" '. + [{name: $n, status: $s, summary: $m}]' <<< "${checks_json}")
  case "$2" in
    fail)
      failed=1
      fail_details="${fail_details}
[$1] $3${4:+
$4}"
      ;;
    pass) pass_names="${pass_names:+${pass_names}, }$1" ;;
  esac
}

# --- Which crates changed --------------------------------------------------
# Only a successful diff can skip the checks: if the merge base or the diff
# fails, check every crate, since skipping would hide breakage.
crates=()
if [ -n "${VERIFY_CRATES:-}" ]; then
  read -ra crates <<< "${VERIFY_CRATES}"
elif ! merge_base=$(git merge-base HEAD "origin/${base}" 2>/dev/null || git merge-base HEAD "${base}" 2>/dev/null); then
  echo "WARNING: could not find the merge base with ${base}; checking every crate" >&2
  crates=("${all_crates[@]}")
elif ! changed=$(git diff --name-only "${merge_base}") || ! untracked=$(git ls-files --others --exclude-standard); then
  echo "WARNING: git could not list the changed files; checking every crate" >&2
  crates=("${all_crates[@]}")
else
  rust_files=$(printf '%s\n%s\n' "${changed}" "${untracked}" | grep -E '\.rs$|(^|/)Cargo\.(toml|lock)$|(^|/)build\.rs$' || true)
  if [ -z "${rust_files}" ]; then
    emit '[]' "PASS: no Rust changes; cargo checks skipped." true
    exit 0
  fi
  if printf '%s\n' "${rust_files}" | grep -qE '^Cargo\.(toml|lock)$'; then
    crates=("${all_crates[@]}")
  else
    for crate in "${all_crates[@]}"; do
      if printf '%s\n' "${rust_files}" | grep -q "^${crate}/"; then
        crates+=("${crate}")
      fi
    done
  fi
fi

logs=$(mktemp -d)
trap 'rm -rf "${logs}"' EXIT

# first_errors LOG — the first compiler or clippy errors, else the log's tail.
first_errors() {
  local out
  out=$(grep -E -A10 '^error(\[E[0-9]+\])?:' "$1" | head -n 60)
  [ -n "${out}" ] || out=$(tail -n 30 "$1")
  printf '%s' "${out}"
}

# --- cargo build -------------------------------------------------------------
# The other checks would only repeat a build failure, so they're skipped then.
if cargo build > "${logs}/build.log" 2>&1; then
  record "cargo build" pass "ok"
else
  record "cargo build" fail "build failed" "$(first_errors "${logs}/build.log")"
  for crate in "${crates[@]}"; do
    record "cargo nextest -p ${crate}" skipped "not run: cargo build failed"
    record "cargo clippy -p ${crate}" skipped "not run: cargo build failed"
  done
fi

if [ "${failed}" -eq 0 ]; then
  # --- cargo nextest, per crate ----------------------------------------------
  for crate in "${crates[@]}"; do
    features=()
    f=$(test_features "${crate}")
    [ -n "${f}" ] && features=(--features "${f}")
    log="${logs}/nextest-${crate}.log"
    nextest_status=0
    cargo nextest run -p "${crate}" --no-fail-fast "${features[@]+"${features[@]}"}" > "${log}" 2>&1 || nextest_status=$?
    summary=$(grep -E '^\s*Summary \[' "${log}" | tail -n 1 | sed -E 's/^[[:space:]]*Summary \[[^]]*\][[:space:]]*//')
    if [ "${nextest_status}" -eq 0 ]; then
      record "cargo nextest -p ${crate}" pass "${summary:-passed}"
    else
      failing=$(grep -E '^\s*(FAIL|TIMEOUT|SIGSEGV|SIGABRT) \[' "${log}" | sed -E 's/^[[:space:]]*[A-Z]+ \[[^]]*\][[:space:]]*(\([0-9]+\/[0-9]+\)[[:space:]]*)?//' | sort -u | head -n 20)
      if [ -n "${failing}" ]; then
        panics=$(grep -E "panicked at|assertion .* failed|left: |right: " "${log}" | head -n 20)
        record "cargo nextest -p ${crate}" fail "${summary:-tests failed}" "Failing tests:
${failing}${panics:+
${panics}}"
      else
        # No test failed, so it didn't compile or nextest couldn't run.
        record "cargo nextest -p ${crate}" fail "nextest exited ${nextest_status} without a test result" "$(first_errors "${log}")"
      fi
    fi
  done

  # --- cargo clippy, per crate -----------------------------------------------
  for crate in "${crates[@]}"; do
    log="${logs}/clippy-${crate}.log"
    if cargo clippy -p "${crate}" --all-targets --all-features -- -D warnings > "${log}" 2>&1; then
      record "cargo clippy -p ${crate}" pass "no warnings"
    else
      record "cargo clippy -p ${crate}" fail "clippy reported errors" "$(first_errors "${log}")"
    fi
  done
fi

# --- cargo fmt -----------------------------------------------------------------
# runkon's fmt-fix runs after verify, just before push, so unformatted code
# fails here, as it did with the verify agent. Running fmt-fix before verify
# (as conductor-ai does) is a separate port.
if cargo fmt --all --check -- --color never > "${logs}/fmt.log" 2>&1; then
  record "cargo fmt" pass "ok"
else
  record "cargo fmt" fail "cargo fmt --all --check failed: the code isn't rustfmt-formatted" "$(head -n 30 "${logs}/fmt.log")"
fi

if [ "${failed}" -eq 0 ]; then
  emit '[]' "PASS: ${pass_names}." true
else
  context="FAIL:${fail_details}"
  context="${context:0:${max_context}}"
  [ -n "${pass_names}" ] && context="${context}
PASS: ${pass_names}."
  emit '["has_failures"]' "${context}" false
fi
