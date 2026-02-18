#!/usr/bin/env bash
#═══════════════════════════════════════════════════════════════════════════════
#  CHECK PR ACTIVITY — Stale Branch & Missing PR Detector
#═══════════════════════════════════════════════════════════════════════════════
#
#  What it does:
#    1. Scans all remote feature/bugfix/hotfix branches
#    2. Flags branches with no commits for 2+ days
#    3. Flags branches that have NO open PR to development
#    4. Lists team members with no PR activity in the last 2 days
#    5. Outputs a summary report (terminal, or file with --output)
#
#  Requirements:
#    • gh CLI (authenticated)
#    • git
#
#  Usage:
#    chmod +x scripts/check_pr_activity.sh
#    ./scripts/check_pr_activity.sh                  # terminal report
#    ./scripts/check_pr_activity.sh --days 3         # custom threshold
#    ./scripts/check_pr_activity.sh --output report  # save to file
#    ./scripts/check_pr_activity.sh --team user1,user2,user3
#
#═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# DEFAULTS
# ─────────────────────────────────────────────────────────────────────────────
STALE_DAYS=2
OUTPUT_FILE=""
TEAM_MEMBERS=""
REMOTE="origin"
BASE_BRANCH="development"
BRANCH_PREFIXES="feature/|bugfix/|hotfix/"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# ─────────────────────────────────────────────────────────────────────────────
# PARSE ARGUMENTS
# ─────────────────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --days)    STALE_DAYS="$2"; shift 2 ;;
        --output)  OUTPUT_FILE="$2"; shift 2 ;;
        --team)    TEAM_MEMBERS="$2"; shift 2 ;;
        --base)    BASE_BRANCH="$2"; shift 2 ;;
        --help|-h)
            echo "Usage: $0 [--days N] [--output FILE] [--team user1,user2] [--base BRANCH]"
            echo ""
            echo "  --days N          Stale threshold in days (default: 2)"
            echo "  --output FILE     Save report to file (default: terminal only)"
            echo "  --team u1,u2,u3   Comma-separated GitHub usernames to audit"
            echo "  --base BRANCH     Target branch for PRs (default: development)"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ─────────────────────────────────────────────────────────────────────────────
# PREFLIGHT
# ─────────────────────────────────────────────────────────────────────────────
if ! git rev-parse --is-inside-work-tree &>/dev/null; then
    echo -e "${RED}Error: Not inside a git repository.${NC}"
    exit 1
fi

if ! command -v gh &>/dev/null; then
    echo -e "${RED}Error: gh CLI is not installed. Install from https://cli.github.com${NC}"
    exit 1
fi

if ! gh auth status &>/dev/null 2>&1; then
    echo -e "${RED}Error: gh CLI is not authenticated. Run: gh auth login${NC}"
    exit 1
fi

REPO_NAME=$(gh repo view --json nameWithOwner -q '.nameWithOwner' 2>/dev/null || basename "$(git rev-parse --show-toplevel)")

# ─────────────────────────────────────────────────────────────────────────────
# UTILITY
# ─────────────────────────────────────────────────────────────────────────────
NOW_EPOCH=$(date +%s)
THRESHOLD_EPOCH=$((NOW_EPOCH - STALE_DAYS * 86400))
TODAY=$(date '+%Y-%m-%d %H:%M')

# Portable date-to-epoch (works on macOS and Linux)
to_epoch() {
    local datestr="$1"
    if date --version &>/dev/null 2>&1; then
        # GNU date (Linux)
        date -d "$datestr" +%s 2>/dev/null || echo "0"
    else
        # BSD date (macOS)
        date -jf "%Y-%m-%dT%H:%M:%S" "${datestr%%+*}" +%s 2>/dev/null || \
        date -jf "%Y-%m-%d %H:%M:%S %z" "$datestr" +%s 2>/dev/null || echo "0"
    fi
}

days_ago() {
    local epoch="$1"
    local diff=$(( (NOW_EPOCH - epoch) / 86400 ))
    echo "$diff"
}

# Collect output lines for optional file write
REPORT_LINES=()
report() {
    local line="$1"
    REPORT_LINES+=("$line")
    echo -e "$line"
}

report_plain() {
    # Strip ANSI for file output, keep colored for terminal
    local line="$1"
    REPORT_LINES+=("$line")
    echo -e "$line"
}

# ─────────────────────────────────────────────────────────────────────────────
# 1. FETCH LATEST
# ─────────────────────────────────────────────────────────────────────────────
echo -e "${DIM}Fetching latest from ${REMOTE}...${NC}"
git fetch "$REMOTE" --prune --quiet 2>/dev/null || true
echo ""

# ═════════════════════════════════════════════════════════════════════════════
# REPORT HEADER
# ═════════════════════════════════════════════════════════════════════════════
report "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
report "${BOLD}${BLUE}  PR Activity Report — ${REPO_NAME}${NC}"
report "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
report "  ${DIM}Generated: ${TODAY}${NC}"
report "  ${DIM}Stale threshold: ${STALE_DAYS} day(s)${NC}"
report "  ${DIM}Target branch: ${BASE_BRANCH}${NC}"
report ""

# ═════════════════════════════════════════════════════════════════════════════
# 2. STALE BRANCHES (no commits for N+ days)
# ═════════════════════════════════════════════════════════════════════════════
report "${CYAN}───────────────────────────────────────────────────────────────${NC}"
report "${BOLD}${CYAN}  Stale Branches (no commits for ${STALE_DAYS}+ days)${NC}"
report "${CYAN}───────────────────────────────────────────────────────────────${NC}"
report ""

stale_count=0
ok_count=0

while IFS= read -r ref; do
    [[ -z "$ref" ]] && continue

    # Extract branch name (strip remotes/origin/)
    branch="${ref#remotes/${REMOTE}/}"

    # Only check feature/bugfix/hotfix branches
    if ! echo "$branch" | grep -qE "^(${BRANCH_PREFIXES})"; then
        continue
    fi

    # Get last commit date and author
    last_commit_date=$(git log -1 --format='%aI' "$ref" 2>/dev/null || echo "")
    last_commit_author=$(git log -1 --format='%ae' "$ref" 2>/dev/null || echo "unknown")
    last_commit_msg=$(git log -1 --format='%s' "$ref" 2>/dev/null || echo "")

    if [[ -z "$last_commit_date" ]]; then
        continue
    fi

    commit_epoch=$(to_epoch "$last_commit_date")
    age=$(days_ago "$commit_epoch")

    if [[ "$commit_epoch" -lt "$THRESHOLD_EPOCH" ]]; then
        stale_count=$((stale_count + 1))
        report "  ${RED}⚠ STALE${NC}  ${BOLD}${branch}${NC}"
        report "           Last commit: ${age} days ago by ${last_commit_author}"
        report "           Message: ${DIM}${last_commit_msg}${NC}"
        report ""
    else
        ok_count=$((ok_count + 1))
    fi
done < <(git branch -r 2>/dev/null | sed 's|^[* ]*||' | grep -v HEAD)

if [[ "$stale_count" -eq 0 ]]; then
    report "  ${GREEN}✔ No stale branches found.${NC} (${ok_count} active branches)"
fi
report ""

# ═════════════════════════════════════════════════════════════════════════════
# 3. BRANCHES WITHOUT OPEN PRs
# ═════════════════════════════════════════════════════════════════════════════
report "${CYAN}───────────────────────────────────────────────────────────────${NC}"
report "${BOLD}${CYAN}  Branches Without Open PRs${NC}"
report "${CYAN}───────────────────────────────────────────────────────────────${NC}"
report ""

# Get all open PRs (head branch names)
open_pr_branches=$(gh pr list --base "$BASE_BRANCH" --state open --json headRefName -q '.[].headRefName' 2>/dev/null || echo "")

no_pr_count=0

while IFS= read -r ref; do
    [[ -z "$ref" ]] && continue

    branch="${ref#remotes/${REMOTE}/}"

    if ! echo "$branch" | grep -qE "^(${BRANCH_PREFIXES})"; then
        continue
    fi

    # Check if this branch has an open PR
    if ! echo "$open_pr_branches" | grep -qF "$branch"; then
        last_commit_author=$(git log -1 --format='%aN <%ae>' "$ref" 2>/dev/null || echo "unknown")
        last_commit_date=$(git log -1 --format='%aI' "$ref" 2>/dev/null || echo "")
        commit_epoch=$(to_epoch "$last_commit_date")
        age=$(days_ago "$commit_epoch")

        no_pr_count=$((no_pr_count + 1))
        report "  ${YELLOW}⚠ NO PR${NC}  ${BOLD}${branch}${NC}"
        report "           Author: ${last_commit_author}"
        report "           Last commit: ${age} day(s) ago"
        report ""
    fi
done < <(git branch -r 2>/dev/null | sed 's|^[* ]*||' | grep -v HEAD)

if [[ "$no_pr_count" -eq 0 ]]; then
    report "  ${GREEN}✔ All active branches have open PRs.${NC}"
fi
report ""

# ═════════════════════════════════════════════════════════════════════════════
# 4. TEAM MEMBER PR ACTIVITY (last N days)
# ═════════════════════════════════════════════════════════════════════════════
report "${CYAN}───────────────────────────────────────────────────────────────${NC}"
report "${BOLD}${CYAN}  Team Member PR Activity (last ${STALE_DAYS} days)${NC}"
report "${CYAN}───────────────────────────────────────────────────────────────${NC}"
report ""

# Build team list: from --team flag, or auto-detect from recent contributors
team_list=()
if [[ -n "$TEAM_MEMBERS" ]]; then
    IFS=',' read -ra team_list <<< "$TEAM_MEMBERS"
else
    # Auto-detect: unique committers in the last 30 days across all branches
    while IFS= read -r author; do
        [[ -n "$author" ]] && team_list+=("$author")
    done < <(git log --all --since="30 days ago" --format='%aN' 2>/dev/null | sort -u)
fi

if [[ ${#team_list[@]} -eq 0 ]]; then
    report "  ${DIM}No team members detected. Use --team user1,user2 to specify.${NC}"
else
    inactive_count=0

    for member in "${team_list[@]}"; do
        member=$(echo "$member" | xargs)  # trim whitespace

        # Check for PRs created/updated by this member in the last N days
        # gh search uses author for created PRs
        recent_prs=$(gh pr list --base "$BASE_BRANCH" --state all \
            --search "author:${member}" \
            --json number,title,state,updatedAt,headRefName \
            -q "[.[] | select(.updatedAt >= \"$(date -d "-${STALE_DAYS} days" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null || date -v-${STALE_DAYS}d '+%Y-%m-%dT%H:%M:%S' 2>/dev/null)\")] | length" \
            2>/dev/null || echo "0")

        # Fallback: check git log for recent commits by this member
        recent_commits=$(git log --all --author="$member" --since="${STALE_DAYS} days ago" --oneline 2>/dev/null | wc -l | tr -d ' ')

        if [[ "${recent_prs:-0}" -eq 0 && "${recent_commits:-0}" -eq 0 ]]; then
            inactive_count=$((inactive_count + 1))
            # Find their last activity
            last_activity=$(git log --all --author="$member" -1 --format='%ar' 2>/dev/null || echo "unknown")
            report "  ${RED}⚠ INACTIVE${NC}  ${BOLD}${member}${NC}"
            report "              No PRs or commits in the last ${STALE_DAYS} day(s)"
            report "              Last activity: ${last_activity}"
            report ""
        elif [[ "${recent_prs:-0}" -eq 0 ]]; then
            report "  ${YELLOW}⚠ NO PR${NC}     ${BOLD}${member}${NC}"
            report "              ${recent_commits} commit(s) but no PR in the last ${STALE_DAYS} day(s)"
            report ""
        else
            report "  ${GREEN}✔ ACTIVE${NC}    ${BOLD}${member}${NC}"
            report "              ${recent_prs} PR(s), ${recent_commits} commit(s) in the last ${STALE_DAYS} day(s)"
            report ""
        fi
    done

    if [[ "$inactive_count" -eq 0 ]]; then
        report "  ${GREEN}All team members have recent activity.${NC}"
    fi
fi
report ""

# ═════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═════════════════════════════════════════════════════════════════════════════
report "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
report "${BOLD}${BLUE}  Summary${NC}"
report "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
report ""
report "  Stale branches (${STALE_DAYS}+ days):   ${stale_count}"
report "  Branches without PRs:        ${no_pr_count}"
report "  Team members checked:        ${#team_list[@]}"
report ""

if [[ "$stale_count" -gt 0 || "$no_pr_count" -gt 0 ]]; then
    report "  ${YELLOW}Action required — review flagged items above.${NC}"
else
    report "  ${GREEN}✔ All clear. No issues found.${NC}"
fi
report ""

# ═════════════════════════════════════════════════════════════════════════════
# SAVE TO FILE (optional)
# ═════════════════════════════════════════════════════════════════════════════
if [[ -n "$OUTPUT_FILE" ]]; then
    # Strip ANSI codes for file output
    printf '%s\n' "${REPORT_LINES[@]}" | sed 's/\x1B\[[0-9;]*m//g' > "$OUTPUT_FILE"
    echo -e "${GREEN}Report saved to: ${OUTPUT_FILE}${NC}"
fi
