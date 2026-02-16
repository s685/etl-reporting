#!/usr/bin/env bash
#------------------------------------------------------------------------------
#  Comprehensive Test Harness for git_auto.sh
#
#  Creates an isolated sandbox git repo, then exercises every workflow (1-9)
#  end-to-end with piped input. Uses assertions to verify outcomes.
#
#  Usage:
#    ./test_git_auto.sh           # Run full suite once
#    ./test_git_auto.sh 2         # Run full suite twice (end-to-end x2)
#    ./test_git_auto.sh 1 quick   # Run once, skip slow workflows (3,4)
#
#  Exit code 0 = all tests passed, non-zero = failures detected.
#------------------------------------------------------------------------------
set -uo pipefail
# Note: not using set -e; we check exit codes manually via assert functions.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_AUTO="${SCRIPT_DIR}/git_auto.sh"
SANDBOX=""
PASS=0
FAIL=0
CURRENT_TEST=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

# ─────────────────────────────────────────────────────────────────────────────
# TEST INFRASTRUCTURE
# ─────────────────────────────────────────────────────────────────────────────

setup_sandbox() {
    SANDBOX=$(mktemp -d /tmp/git_auto_test_XXXXXX)
    cd "$SANDBOX"
    git init --quiet
    git config user.email "test@test.com"
    git config user.name "Test User"
    git config commit.gpgSign false
    git config tag.gpgSign false
    echo "initial" > README.md
    git add README.md
    git commit --no-gpg-sign -m "initial commit" --quiet
    # Rename default branch to main
    git branch -m "$(git branch --show-current)" main 2>/dev/null || true
    # Create base branches
    git branch development
    git branch release/1.0
    # Copy script under test, and gitignore it so it doesn't appear
    # as an untracked file in git status (which corrupts file numbering)
    echo "git_auto.sh" > .gitignore
    git add .gitignore
    git commit --no-gpg-sign -m "add gitignore" --quiet
    cp "$GIT_AUTO" ./git_auto.sh
    chmod +x git_auto.sh
}

teardown_sandbox() {
    if [[ -n "$SANDBOX" ]] && [[ -d "$SANDBOX" ]]; then
        rm -rf "$SANDBOX"
    fi
}

# Run git_auto.sh with piped input inside the sandbox.
# Writes output to a temp file and cats it back. This avoids $() subshell
# cwd issues that can cause silent failures.
# Args: stdin lines as arguments
_RUN_OUTPUT="/tmp/git_auto_test_output.tmp"
run_git_auto() {
    local input=""
    for line in "$@"; do
        input+="${line}"$'\n'
    done
    ( cd "$SANDBOX" && rm -f .git/index.lock && \
      echo "$input" | timeout 15 bash git_auto.sh 2>&1 \
    ) | sed 's/\x1b\[[0-9;]*m//g' > "$_RUN_OUTPUT" 2>&1
    cat "$_RUN_OUTPUT"
}

# Assertion helpers
assert_contains() {
    local output="$1"
    local expected="$2"
    local msg="${3:-}"
    # Use bash pattern matching instead of echo|grep to avoid SIGPIPE with pipefail
    if [[ "$output" == *"$expected"* ]]; then
        PASS=$((PASS + 1))
        echo -e "  ${GREEN}PASS${NC}: $CURRENT_TEST — $msg (contains '$expected')"
    else
        FAIL=$((FAIL + 1))
        echo -e "  ${RED}FAIL${NC}: $CURRENT_TEST — $msg (expected '$expected' not found)"
    fi
}

assert_not_contains() {
    local output="$1"
    local unexpected="$2"
    local msg="${3:-}"
    if [[ "$output" != *"$unexpected"* ]]; then
        PASS=$((PASS + 1))
        echo -e "  ${GREEN}PASS${NC}: $CURRENT_TEST — $msg (correctly absent '$unexpected')"
    else
        FAIL=$((FAIL + 1))
        echo -e "  ${RED}FAIL${NC}: $CURRENT_TEST — $msg (unexpected '$unexpected' found)"
    fi
}

assert_file_exists() {
    local filepath="$1"
    local msg="${2:-}"
    if [[ -f "$SANDBOX/$filepath" ]]; then
        PASS=$((PASS + 1))
        echo -e "  ${GREEN}PASS${NC}: $CURRENT_TEST — $msg (file exists: $filepath)"
    else
        FAIL=$((FAIL + 1))
        echo -e "  ${RED}FAIL${NC}: $CURRENT_TEST — $msg (file missing: $filepath)"
    fi
}

assert_branch() {
    local expected_branch="$1"
    local msg="${2:-}"
    local actual
    actual=$(cd "$SANDBOX" && git branch --show-current 2>/dev/null)
    if [[ "$actual" == "$expected_branch" ]]; then
        PASS=$((PASS + 1))
        echo -e "  ${GREEN}PASS${NC}: $CURRENT_TEST — $msg (on branch '$expected_branch')"
    else
        FAIL=$((FAIL + 1))
        echo -e "  ${RED}FAIL${NC}: $CURRENT_TEST — $msg (expected branch '$expected_branch', got '$actual')"
    fi
}

assert_file_in_commit() {
    local filename="$1"
    local msg="${2:-}"
    local files
    files=$(cd "$SANDBOX" && git show --name-only --format="" HEAD 2>/dev/null)
    if echo "$files" | grep -qF "$filename"; then
        PASS=$((PASS + 1))
        echo -e "  ${GREEN}PASS${NC}: $CURRENT_TEST — $msg ('$filename' in HEAD commit)"
    else
        FAIL=$((FAIL + 1))
        echo -e "  ${RED}FAIL${NC}: $CURRENT_TEST — $msg ('$filename' NOT in HEAD commit)"
    fi
}

assert_file_not_in_commit() {
    local filename="$1"
    local msg="${2:-}"
    local files
    files=$(cd "$SANDBOX" && git show --name-only --format="" HEAD 2>/dev/null)
    if ! echo "$files" | grep -qF "$filename"; then
        PASS=$((PASS + 1))
        echo -e "  ${GREEN}PASS${NC}: $CURRENT_TEST — $msg ('$filename' correctly not in HEAD)"
    else
        FAIL=$((FAIL + 1))
        echo -e "  ${RED}FAIL${NC}: $CURRENT_TEST — $msg ('$filename' unexpectedly in HEAD commit)"
    fi
}

assert_commit_count() {
    local expected="$1"
    local msg="${2:-}"
    local actual
    actual=$(cd "$SANDBOX" && git rev-list --count HEAD 2>/dev/null)
    if [[ "$actual" == "$expected" ]]; then
        PASS=$((PASS + 1))
        echo -e "  ${GREEN}PASS${NC}: $CURRENT_TEST — $msg ($expected commits)"
    else
        FAIL=$((FAIL + 1))
        echo -e "  ${RED}FAIL${NC}: $CURRENT_TEST — $msg (expected $expected commits, got $actual)"
    fi
}

section() {
    echo ""
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}  $1${NC}"
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST CASES
# ─────────────────────────────────────────────────────────────────────────────

test_quit() {
    CURRENT_TEST="Quit"
    local out
    out=$(run_git_auto "q")
    assert_contains "$out" "Goodbye!" "quit message shown"
    assert_contains "$out" "Main Menu" "menu was displayed"
}

test_option6_log_last15() {
    CURRENT_TEST="Option6-Log15"
    local out
    out=$(run_git_auto "6" "1" "" "q")
    assert_contains "$out" "Git Log" "log header shown"
    assert_contains "$out" "initial commit" "initial commit visible"
}

test_option6_log_graph() {
    CURRENT_TEST="Option6-LogGraph"
    local out
    out=$(run_git_auto "6" "2" "" "q")
    assert_contains "$out" "initial commit" "graph shows initial commit"
}

test_option7_setup_accept() {
    CURRENT_TEST="Option7-Accept"
    local out
    out=$(run_git_auto "7" "y" "" "q")
    assert_contains "$out" "pull.rebase = true" "pull.rebase configured"
    assert_contains "$out" "rebase.autoStash = true" "autoStash configured"
    assert_contains "$out" "merge.ff = only" "ff-only configured"
    assert_contains "$out" "Linear history configuration applied" "config applied"
}

test_option7_setup_decline() {
    CURRENT_TEST="Option7-Decline"
    local out
    out=$(run_git_auto "7" "n" "" "q")
    assert_not_contains "$out" "pull.rebase = true" "no config applied"
}

test_option5_stash_list_empty() {
    CURRENT_TEST="Option5-ListEmpty"
    local out
    out=$(run_git_auto "5" "2" "" "q")
    assert_contains "$out" "No stashes found" "empty stash list"
}

test_option5_stash_push_pop() {
    CURRENT_TEST="Option5-PushPop"
    cd "$SANDBOX"
    echo "stash me" > stash_file.txt
    # Stash with message
    local out
    out=$(run_git_auto "5" "1" "test stash msg" "" "q")
    assert_contains "$out" "Changes stashed" "stash push succeeded"
    # Verify stash list
    out=$(run_git_auto "5" "2" "" "q")
    assert_contains "$out" "test stash msg" "stash message visible"
    # Pop stash
    out=$(run_git_auto "5" "4" "" "q")
    assert_contains "$out" "Latest stash popped" "stash pop succeeded"
    assert_file_exists "stash_file.txt" "file restored after pop"
    # Cleanup
    rm -f stash_file.txt
}

test_option8_prune() {
    CURRENT_TEST="Option8-Prune"
    local out
    out=$(run_git_auto "8" "3" "" "q")
    assert_contains "$out" "Branch Cleanup" "cleanup header shown"
    # Prune may report success or no stale branches
    assert_contains "$out" "Goodbye!" "completed without crash"
}

test_option9_no_conflict() {
    CURRENT_TEST="Option9-NoConflict"
    local out
    out=$(run_git_auto "9" "" "q")
    assert_contains "$out" "No rebase or merge in progress" "clean state detected"
}

test_option1_create_feature() {
    CURRENT_TEST="Option1-CreateFeature"
    # Inputs: menu=1, base=1 (main), type=1 (feature/), name=test-branch, stash=n, push question won't show (no remote)
    local out
    out=$(run_git_auto "1" "1" "1" "test-branch" "n" "" "q")
    assert_contains "$out" "Create Feature Branch" "header shown"
    assert_contains "$out" "feature/test-branch" "branch name shown"
    assert_branch "feature/test-branch" "switched to new branch"
}

test_option2_stage_all() {
    CURRENT_TEST="Option2-StageAll"
    cd "$SANDBOX"
    echo "file A" > fileA.txt
    echo "file B" > fileB.txt
    # menu=2, stage=1 (all), type=1 (feat), scope=<empty>, desc, body=n, confirm=y, push skipped (no remote)
    local out
    out=$(run_git_auto "2" "1" "1" "" "stage all test" "n" "y" "n" "" "q")
    assert_contains "$out" "All files staged" "all files staged"
    assert_contains "$out" "Committed successfully" "commit succeeded"
    assert_file_in_commit "fileA.txt" "fileA in commit"
    assert_file_in_commit "fileB.txt" "fileB in commit"
}

test_option2_stage_specific_single() {
    CURRENT_TEST="Option2-StageSpecific-Single"
    cd "$SANDBOX"
    echo "single1" > single1.txt
    echo "single2" > single2.txt
    echo "single3" > single3.txt
    # menu=2, stage=2 (specific), select=2 (only file #2), type=1 (feat), scope=<empty>, desc, body=n, confirm=y
    local out
    out=$(run_git_auto "2" "2" "2" "1" "" "stage single file" "n" "y" "n" "" "q")
    assert_contains "$out" "1 file(s) staged" "exactly 1 file staged"
    assert_contains "$out" "Committed successfully" "commit succeeded"
    # Verify only selected file committed, others remain untracked
    cd "$SANDBOX"
    local uncommitted
    uncommitted=$(git status --porcelain 2>/dev/null)
    assert_contains "$uncommitted" "single" "some files still untracked"
}

test_option2_stage_specific_multiple_comma() {
    CURRENT_TEST="Option2-StageSpecific-MultipleComma"
    cd "$SANDBOX"
    echo "comma1" > comma1.txt
    echo "comma2" > comma2.txt
    echo "comma3" > comma3.txt
    echo "comma4" > comma4.txt
    # menu=2, stage=2 (specific), select=1,3 (files #1 and #3), type=1 (feat), scope=<empty>, desc, body=n, confirm=y
    local out
    out=$(run_git_auto "2" "2" "1,3" "1" "" "stage comma select" "n" "y" "n" "" "q")
    assert_contains "$out" "2 file(s) staged" "exactly 2 files staged"
    assert_contains "$out" "Committed successfully" "commit succeeded"
    # Verify 2 files remain (comma2 and comma4)
    cd "$SANDBOX"
    local remaining
    remaining=$(git ls-files --others --exclude-standard 2>/dev/null | wc -l)
    if [[ "$remaining" -ge 2 ]]; then
        PASS=$((PASS + 1))
        echo -e "  ${GREEN}PASS${NC}: $CURRENT_TEST — unselected files remain unstaged ($remaining)"
    else
        FAIL=$((FAIL + 1))
        echo -e "  ${RED}FAIL${NC}: $CURRENT_TEST — expected 2+ untracked files, got $remaining"
    fi
}

test_option2_stage_specific_range() {
    CURRENT_TEST="Option2-StageSpecific-Range"
    cd "$SANDBOX"
    echo "range1" > range1.txt
    echo "range2" > range2.txt
    echo "range3" > range3.txt
    echo "range4" > range4.txt
    echo "range5" > range5.txt
    # menu=2, stage=2 (specific), select=2-4 (files #2 through #4), type=1 (feat), scope=<empty>, desc, body=n, confirm=y
    local out
    out=$(run_git_auto "2" "2" "2-4" "1" "" "stage range select" "n" "y" "n" "" "q")
    assert_contains "$out" "3 file(s) staged" "exactly 3 files staged via range"
    assert_contains "$out" "Committed successfully" "commit succeeded"
}

test_option2_stage_specific_all_shortcut() {
    CURRENT_TEST="Option2-StageSpecific-AllShortcut"
    cd "$SANDBOX"
    echo "shortcut1" > shortcut1.txt
    echo "shortcut2" > shortcut2.txt
    # menu=2, stage=2 (specific), select=a (all), type=1 (feat), scope=<empty>, desc, body=n, confirm=y
    local out
    out=$(run_git_auto "2" "2" "a" "1" "" "stage all shortcut" "n" "y" "n" "" "q")
    assert_contains "$out" "All files staged" "all shortcut worked"
    assert_contains "$out" "Committed successfully" "commit succeeded"
}

test_option2_stage_already_staged() {
    CURRENT_TEST="Option2-StageAlreadyStaged"
    cd "$SANDBOX"
    echo "pre-staged" > prestaged.txt
    git add prestaged.txt
    # menu=2, stage=3 (already staged), type=6 (chore), scope=<empty>, desc, body=n, confirm=y
    local out
    out=$(run_git_auto "2" "3" "6" "" "pre staged file" "n" "y" "n" "" "q")
    assert_contains "$out" "Using already-staged files" "recognized pre-staged"
    assert_contains "$out" "Committed successfully" "commit succeeded"
    assert_file_in_commit "prestaged.txt" "pre-staged file committed"
}

test_option2_commit_with_scope() {
    CURRENT_TEST="Option2-CommitWithScope"
    cd "$SANDBOX"
    echo "scoped" > scoped.txt
    # menu=2, stage=1 (all), type=2 (fix), scope=JIRA-999, desc, body=n, confirm=y
    local out
    out=$(run_git_auto "2" "1" "2" "JIRA-999" "fix the thing" "n" "y" "n" "" "q")
    assert_contains "$out" "Committed successfully" "commit with scope succeeded"
    cd "$SANDBOX"
    local msg
    msg=$(git log --oneline -1)
    assert_contains "$msg" "fix(JIRA-999)" "scope in commit message"
}

test_option3_sync() {
    CURRENT_TEST="Option3-Sync"
    cd "$SANDBOX"
    # First ensure we're on a feature branch
    git checkout -b feature/sync-test main --quiet 2>/dev/null
    git config "branch.feature/sync-test.base" "main"
    echo "sync content" > sync.txt
    git add sync.txt
    git commit --no-gpg-sign -m "sync test commit" --quiet
    # menu=3, use stored base=y, force-push=n
    local out
    out=$(run_git_auto "3" "y" "n" "" "q")
    assert_contains "$out" "Sync Feature Branch" "sync header shown"
    assert_contains "$out" "Rebase successful" "rebase succeeded"
    assert_branch "feature/sync-test" "still on feature branch"
}

test_option4_merge() {
    CURRENT_TEST="Option4-Merge"
    cd "$SANDBOX"
    # Create feature branch from development
    git checkout development --quiet 2>/dev/null
    git checkout -b feature/merge-e2e --quiet 2>/dev/null
    echo "merge e2e" > merge_e2e.txt
    git add merge_e2e.txt
    git commit --no-gpg-sign -m "merge e2e commit" --quiet
    # menu=4, target=1 (development), push=n, delete=n
    local out
    out=$(run_git_auto "4" "1" "n" "n" "" "q")
    assert_contains "$out" "Fast-forward merge successful" "ff merge succeeded"
    assert_contains "$out" "Linear history" "linear history mentioned"
}

# ─────────────────────────────────────────────────────────────────────────────
# FULL END-TO-END SUITE
# ─────────────────────────────────────────────────────────────────────────────

run_full_suite() {
    local run_num="$1"
    local mode="${2:-full}"

    section "END-TO-END RUN #$run_num ($mode mode)"

    # Fresh sandbox for each run
    teardown_sandbox
    setup_sandbox

    echo ""
    echo -e "${YELLOW}--- Quit ---${NC}"
    test_quit

    echo -e "${YELLOW}--- Option 6: View Log ---${NC}"
    test_option6_log_last15
    test_option6_log_graph

    echo -e "${YELLOW}--- Option 7: Setup Linear History ---${NC}"
    test_option7_setup_accept
    teardown_sandbox; setup_sandbox  # fresh state for decline test
    test_option7_setup_decline

    echo -e "${YELLOW}--- Option 5: Stash Management ---${NC}"
    test_option5_stash_list_empty
    test_option5_stash_push_pop

    echo -e "${YELLOW}--- Option 8: Branch Cleanup ---${NC}"
    test_option8_prune

    echo -e "${YELLOW}--- Option 9: Resolve Conflicts ---${NC}"
    test_option9_no_conflict

    echo -e "${YELLOW}--- Option 1: Create Feature Branch ---${NC}"
    teardown_sandbox; setup_sandbox
    test_option1_create_feature

    echo -e "${YELLOW}--- Option 2: Stage, Commit & Push (Stage ALL) ---${NC}"
    test_option2_stage_all

    echo -e "${YELLOW}--- Option 2: Stage SPECIFIC (single file) ---${NC}"
    test_option2_stage_specific_single

    echo -e "${YELLOW}--- Option 2: Stage SPECIFIC (comma: 1,3) ---${NC}"
    test_option2_stage_specific_multiple_comma

    echo -e "${YELLOW}--- Option 2: Stage SPECIFIC (range: 2-4) ---${NC}"
    test_option2_stage_specific_range

    echo -e "${YELLOW}--- Option 2: Stage SPECIFIC ('a' = all) ---${NC}"
    test_option2_stage_specific_all_shortcut

    echo -e "${YELLOW}--- Option 2: Stage Already-Staged (option 3) ---${NC}"
    test_option2_stage_already_staged

    echo -e "${YELLOW}--- Option 2: Commit with Scope ---${NC}"
    test_option2_commit_with_scope

    if [[ "$mode" == "full" ]]; then
        echo -e "${YELLOW}--- Option 3: Sync Feature Branch ---${NC}"
        teardown_sandbox; setup_sandbox
        test_option3_sync

        echo -e "${YELLOW}--- Option 4: Merge Feature to Base ---${NC}"
        teardown_sandbox; setup_sandbox
        test_option4_merge
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

main() {
    local iterations="${1:-1}"
    local mode="${2:-full}"

    if [[ ! -f "$GIT_AUTO" ]]; then
        echo "Error: git_auto.sh not found at $GIT_AUTO"
        exit 1
    fi

    echo ""
    echo -e "${BOLD}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║        git_auto.sh — Comprehensive Test Suite                ║${NC}"
    echo -e "${BOLD}║        Iterations: $iterations  |  Mode: $mode                        ║${NC}"
    echo -e "${BOLD}╚═══════════════════════════════════════════════════════════════╝${NC}"

    for i in $(seq 1 "$iterations"); do
        run_full_suite "$i" "$mode"
    done

    teardown_sandbox

    # Summary
    section "TEST RESULTS"
    echo ""
    echo -e "  ${GREEN}Passed:${NC} $PASS"
    echo -e "  ${RED}Failed:${NC} $FAIL"
    echo -e "  ${BOLD}Total:${NC}  $((PASS + FAIL))"
    echo ""

    if [[ "$FAIL" -gt 0 ]]; then
        echo -e "  ${RED}${BOLD}SOME TESTS FAILED${NC}"
        echo ""
        return 1
    else
        echo -e "  ${GREEN}${BOLD}ALL TESTS PASSED${NC}"
        echo ""
        return 0
    fi
}

main "$@"
