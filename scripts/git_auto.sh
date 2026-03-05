#!/usr/bin/env bash
#═══════════════════════════════════════════════════════════════════════════════
#  GIT AUTOMATION SCRIPT — Interactive Local Git Operations
#═══════════════════════════════════════════════════════════════════════════════
#
#  Features:
#    • Create feature branches from development/release/main (interactive)
#    • Selective file staging or stage all
#    • Commit with conventional commit messages
#    • Push to remote with upstream tracking
#    • Linear history enforcement (rebase workflow on protected branches)
#    • Sync feature branch with latest base branch
#    • Safe branch cleanup after merge
#    • Stash management
#    • Pre-flight checks (clean tree, remote connectivity, branch existence)
#    • Cherry-pick specific files from any source branch into a target branch
#    • Dev → Release workflow: select files, auto-create PR branch, push & surface PR URL
#
#  Linear History Strategy:
#    Protected branches (main, release/*, development) use --ff-only merges.
#    Feature branches rebase onto their base before merge.
#    This ensures a clean, linear commit history with no merge commits.
#
#  Usage:
#    chmod +x git_auto.sh
#    ./git_auto.sh
#
#═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
PROTECTED_BRANCHES=("main" "development")
PROTECTED_PATTERNS=("release/*")  # glob patterns
REMOTE="origin"
FEATURE_PREFIX="feature/"
BUGFIX_PREFIX="bugfix/"
HOTFIX_PREFIX="hotfix/"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m' # No Color

# ─────────────────────────────────────────────────────────────────────────────
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

print_header() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${CYAN}───────────────────────────────────────────────────────────────${NC}"
    echo -e "${BOLD}${CYAN}  $1${NC}"
    echo -e "${CYAN}───────────────────────────────────────────────────────────────${NC}"
}

info()    { echo -e "${GREEN}  ✔ $1${NC}"; }
warn()    { echo -e "${YELLOW}  ⚠ $1${NC}"; }
error()   { echo -e "${RED}  ✖ $1${NC}"; }
prompt()  { echo -e -n "${MAGENTA}  ➤ $1${NC}"; }
dim()     { echo -e "${DIM}    $1${NC}"; }

confirm() {
    local msg="${1:-Continue?}"
    prompt "$msg [y/N]: "
    read -r response || response="n"
    response=$(echo "$response" | tr -d '\r')
    [[ "$response" =~ ^[Yy]$ ]]
}

# Cached paths (set once at startup — avoids repeated git rev-parse)
GIT_DIR=""
TOP_LEVEL=""
REPO_NAME=""

# Check if inside a git repository and cache paths
check_git_repo() {
    if ! git rev-parse --is-inside-work-tree &>/dev/null; then
        error "Not inside a Git repository."
        error "Please navigate to a Git repository and try again."
        exit 1
    fi
    GIT_DIR=$(git rev-parse --git-dir 2>/dev/null)
    TOP_LEVEL=$(git rev-parse --show-toplevel 2>/dev/null)
    REPO_NAME=$(basename "$TOP_LEVEL" 2>/dev/null)
    info "Git repository: $REPO_NAME"
}

# Check remote connectivity
check_remote() {
    if ! git ls-remote --exit-code "$REMOTE" &>/dev/null; then
        warn "Cannot reach remote '$REMOTE'. Push operations will fail."
        return 1
    fi
    info "Remote '$REMOTE' is reachable."
    return 0
}

# Check if branch exists locally
branch_exists_local() {
    git show-ref --verify --quiet "refs/heads/$1" 2>/dev/null
}

# Check if branch exists on remote
branch_exists_remote() {
    git ls-remote --exit-code --heads "$REMOTE" "$1" &>/dev/null
}

# Check if current branch is a protected branch
is_protected_branch() {
    local branch="$1"
    for pb in "${PROTECTED_BRANCHES[@]}"; do
        [[ "$branch" == "$pb" ]] && return 0
    done
    for pattern in "${PROTECTED_PATTERNS[@]}"; do
        # shellcheck disable=SC2254
        case "$branch" in
            $pattern) return 0 ;;
        esac
    done
    return 1
}

# Get current branch name
current_branch() {
    git symbolic-ref --short HEAD 2>/dev/null || echo "DETACHED"
}

# Get stored base branch for current branch (set when creating feature branch)
get_stored_base_branch() {
    local branch
    branch=$(current_branch)
    git config --get "branch.${branch}.base" 2>/dev/null || true
}

# Check if in rebase state (conflict or mid-rebase)
in_rebase_state() {
    [[ -n "$GIT_DIR" ]] && { [[ -d "$GIT_DIR/rebase-merge" ]] || [[ -d "$GIT_DIR/rebase-apply" ]]; }
}

# Check if in merge conflict state
in_merge_state() {
    [[ -n "$GIT_DIR" ]] && [[ -f "$GIT_DIR/MERGE_HEAD" ]]
}

# List conflicted files (unmerged)
get_conflicted_files() {
    git diff --name-only --diff-filter=U 2>/dev/null || true
}

# Check for uncommitted changes
has_uncommitted_changes() {
    ! git diff --quiet 2>/dev/null || ! git diff --cached --quiet 2>/dev/null
}

# Check for untracked files
has_untracked_files() {
    [[ -n $(git ls-files --others --exclude-standard 2>/dev/null) ]]
}

# Fetch latest from remote (silent)
fetch_latest() {
    info "Fetching latest from $REMOTE..."
    git fetch "$REMOTE" --prune --quiet 2>/dev/null || warn "Fetch failed — working offline."
}

# Handle rebase conflict: list files, offer resolve flow
_handle_rebase_conflict() {
    local branch="$1" base="$2"
    local conflicted
    conflicted=$(get_conflicted_files)
    error "Rebase conflict detected!"
    echo ""
    if [[ -n "$conflicted" ]]; then
        echo -e "  ${BOLD}Conflicted files:${NC}"
        echo "$conflicted" | sed 's/^/    /'
        echo ""
    fi
    warn "Steps to resolve:"
    dim "  1. Edit conflicted files and remove <<<<<<<, =======, >>>>>>> markers"
    dim "  2. git add <resolved-files>"
    dim "  3. git rebase --continue"
    dim "  Or: git rebase --abort  to undo and return to previous state"
    echo ""
    if [[ -n "$conflicted" ]] && confirm "Open conflict resolution menu now?"; then
        resolve_conflicts
    fi
}

# Parse git status --porcelain into counts (staged, modified, untracked)
# Output: "staged modified untracked" on one line
_parse_status_counts() {
    local porcelain
    porcelain="${1:-$(git status --porcelain 2>/dev/null)}"
    awk '
        /^\?\?/ { u++ }
        /^[MADRC]/ { s++ }
        /^.[MD ]|^ [MD]/ { if ($0 !~ /^\?\?/) m++ }
        END { print s+0, m+0, u+0 }
    ' <<< "$porcelain"
}


# Display current repo status summary (reuses porcelain if provided)
show_status_summary() {
    local branch staged modified untracked porcelain
    branch=$(current_branch)
    porcelain="${1:-$(git status --porcelain 2>/dev/null)}"
    read -r staged modified untracked <<< "$(_parse_status_counts "$porcelain")"

    echo ""
    echo -e "  ${BOLD}Branch:${NC}    $branch"
    echo -e "  ${BOLD}Staged:${NC}    ${staged:-0} file(s)"
    echo -e "  ${BOLD}Modified:${NC}  ${modified:-0} file(s)"
    echo -e "  ${BOLD}Untracked:${NC} ${untracked:-0} file(s)"
    echo ""
}

# ─────────────────────────────────────────────────────────────────────────────
# CORE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

# ═════════════════════════════════════════════════════════════════════════════
# 1. CREATE FEATURE BRANCH
# ═════════════════════════════════════════════════════════════════════════════
create_feature_branch() {
    print_header "Create Feature Branch"

    # ── Select base branch ──────────────────────────────────────────────
    print_section "Select Base Branch"

    # Build list of available base branches (one git call instead of 4+ ls-remote)
    local base_branches=()
    local display_names=()
    local all_branches
    all_branches=$(git branch -a 2>/dev/null | sed 's|remotes/[^/]*/||;s|^[* ]*||')

    echo "$all_branches" | grep -qE '^main$' && base_branches+=("main") && display_names+=("main")
    echo "$all_branches" | grep -qE '^development$' && base_branches+=("development") && display_names+=("development")
    while IFS= read -r rb; do
        [[ -n "$rb" ]] && base_branches+=("$rb") && display_names+=("$rb")
    done < <(echo "$all_branches" | grep 'release/' | sort -u)

    if [[ ${#base_branches[@]} -eq 0 ]]; then
        error "No base branches found (main, development, release/*)."
        error "Please create at least one base branch first."
        return 1
    fi

    echo ""
    for i in "${!display_names[@]}"; do
        echo -e "    ${BOLD}$((i+1)))${NC} ${display_names[$i]}"
    done
    echo ""

    local selection
    prompt "Select base branch [1-${#base_branches[@]}]: "
    read -r selection || true
    selection=$(echo "${selection:-}" | tr -d '\r')

    if [[ ! "$selection" =~ ^[0-9]+$ ]] || (( selection < 1 || selection > ${#base_branches[@]} )); then
        error "Invalid selection."
        return 1
    fi

    local base_branch="${base_branches[$((selection-1))]}"
    info "Base branch: $base_branch"

    # ── Select branch type ──────────────────────────────────────────────
    print_section "Select Branch Type"
    echo ""
    echo -e "    ${BOLD}1)${NC} feature/   — New feature development"
    echo -e "    ${BOLD}2)${NC} bugfix/    — Bug fix"
    echo -e "    ${BOLD}3)${NC} hotfix/    — Production hotfix"
    echo -e "    ${BOLD}4)${NC} custom     — Custom prefix"
    echo ""

    prompt "Select branch type [1-4]: "
    read -r type_selection || true
    type_selection=$(echo "${type_selection:-}" | tr -d '\r')

    local prefix
    case "$type_selection" in
        1) prefix="$FEATURE_PREFIX" ;;
        2) prefix="$BUGFIX_PREFIX" ;;
        3) prefix="$HOTFIX_PREFIX" ;;
        4)
            prompt "Enter custom prefix (e.g. 'refactor/'): "
            read -r prefix || true
            # Ensure trailing slash
            [[ "$prefix" != */ ]] && prefix="$prefix/"
            ;;
        *)
            error "Invalid selection."
            return 1
            ;;
    esac

    # ── Enter branch name ───────────────────────────────────────────────
    print_section "Enter Branch Name"
    echo ""
    dim "Use lowercase, hyphens, no spaces. Example: add-payment-validation"
    dim "Ticket reference example: JIRA-1234-add-payment-validation"
    echo ""

    prompt "Branch name (without prefix): "
    read -r branch_name || true

    # Sanitize branch name
    branch_name=$(echo "${branch_name:-}" | tr '[:upper:]' '[:lower:]' | tr ' ' '-' | sed 's/[^a-z0-9\-]//g')

    if [[ -z "$branch_name" ]]; then
        error "Branch name cannot be empty."
        return 1
    fi

    local full_branch_name="${prefix}${branch_name}"

    # Check if branch already exists
    if branch_exists_local "$full_branch_name"; then
        error "Branch '$full_branch_name' already exists locally."
        if confirm "Switch to existing branch instead?"; then
            git checkout "$full_branch_name"
            info "Switched to $full_branch_name"
            return 0
        fi
        return 1
    fi

    # ── Create the branch ───────────────────────────────────────────────
    print_section "Creating Branch"

    # Stash any uncommitted work
    local stashed=false
    if has_uncommitted_changes || has_untracked_files; then
        warn "You have uncommitted changes."
        if confirm "Stash changes before switching branches?"; then
            git stash push -m "auto-stash before creating $full_branch_name" --include-untracked
            stashed=true
            info "Changes stashed."
        fi
    fi

    # Ensure base branch is up-to-date
    if branch_exists_local "$base_branch"; then
        git checkout "$base_branch" --quiet
        if check_remote; then
            info "Pulling latest $base_branch from $REMOTE..."
            git pull --rebase "$REMOTE" "$base_branch" --quiet 2>/dev/null || true
        fi
    else
        # Base branch only exists on remote — track it
        info "Checking out $base_branch from $REMOTE..."
        git checkout -b "$base_branch" "$REMOTE/$base_branch" --quiet
    fi

    # Create and switch to new branch
    git checkout -b "$full_branch_name"

    # Store base branch for Sync and conflict workflows (avoids merge conflicts)
    git config "branch.$full_branch_name.base" "$base_branch"
    info "Branch '$full_branch_name' created from '$base_branch' (base stored for Sync)"

    # Push branch to remote and set upstream
    if check_remote; then
        if confirm "Push new branch to remote and set upstream tracking?"; then
            git push -u "$REMOTE" "$full_branch_name"
            info "Branch pushed to $REMOTE with upstream tracking."
        fi
    fi

    # Restore stash if applicable
    if $stashed; then
        if confirm "Restore stashed changes?"; then
            git stash pop
            info "Stashed changes restored."
        else
            warn "Stash preserved. Use 'git stash pop' to restore later."
        fi
    fi

    echo ""
    info "You are now on: $full_branch_name"
    info "Base branch: $base_branch"
    echo ""
}

# ═════════════════════════════════════════════════════════════════════════════
# 2. STAGE, COMMIT & PUSH
# ═════════════════════════════════════════════════════════════════════════════
commit_and_push() {
    print_header "Stage, Commit & Push"

    local branch
    branch=$(current_branch)

    if [[ "$branch" == "DETACHED" ]]; then
        error "You are in a detached HEAD state. Please checkout a branch first."
        return 1
    fi

    info "Current branch: $branch"

    # Single git call for status (replaces 3–4 separate calls)
    local porcelain
    porcelain=$(git status --porcelain 2>/dev/null)

    # ── Show current status ─────────────────────────────────────────────
    show_status_summary "$porcelain"

    # Check if there's anything to commit (parse from porcelain)
    local s m u
    read -r s m u <<< "$(_parse_status_counts "$porcelain")"
    if [[ "${s:-0}" -eq 0 && "${m:-0}" -eq 0 && "${u:-0}" -eq 0 ]]; then
        warn "Nothing to commit. Working tree is clean."
        return 0
    fi

    # ── File Selection ──────────────────────────────────────────────────
    print_section "Select Files to Stage"
    echo ""
    echo -e "    ${BOLD}1)${NC} Stage ALL changed and untracked files"
    echo -e "    ${BOLD}2)${NC} Select SPECIFIC files (shows numbered list next)"
    echo -e "    ${BOLD}3)${NC} Stage only already-staged files (skip staging)"
    echo ""
    dim "  For specific files: choose 2 first, then enter numbers like 1,4 at the next prompt."
    echo ""

    prompt "Choose option [1-3]: "
    read -r stage_choice || true

    # Normalize input (strip CRLF/spaces) so "1", "2", "3" work on Windows
    stage_choice=$(echo "$stage_choice" | tr -d '\r' | tr -d ' ')

    # If user typed file numbers (e.g. 1,4) at first prompt, treat as "select specific"
    local prefill=""
    if [[ ! "$stage_choice" =~ ^[123]$ ]] && [[ "$stage_choice" =~ ^[0-9]+([,\-][0-9]+)*$ ]]; then
        prefill="$stage_choice"
        stage_choice="2"
        dim "  (Interpreted as: select specific files: $prefill)"
        echo ""
    fi

    case "$stage_choice" in
        1)
            git add -A
            info "All files staged."
            ;;
        2)
            _interactive_file_staging "$prefill" "$porcelain"
            ;;
        3)
            if [[ "${s:-0}" -eq 0 ]]; then
                warn "No files are currently staged."
                if confirm "Stage all files instead?"; then
                    git add -A
                    info "All files staged."
                else
                    return 0
                fi
            else
                info "Using already-staged files."
            fi
            ;;
        *)
            error "Invalid selection."
            return 1
            ;;
    esac

    # Verify something is staged
    staged_files=$(git diff --cached --name-only 2>/dev/null)
    if [[ -z "$staged_files" ]]; then
        warn "No files staged. Nothing to commit."
        return 0
    fi

    echo ""
    echo -e "  ${BOLD}Files to be committed:${NC}"
    git diff --cached --name-status | while read -r status file; do
        case "$status" in
            A) echo -e "    ${GREEN}[+] $file${NC}" ;;
            M) echo -e "    ${YELLOW}[~] $file${NC}" ;;
            D) echo -e "    ${RED}[-] $file${NC}" ;;
            R*) echo -e "    ${CYAN}[→] $file${NC}" ;;
            *) echo -e "    [?] $file" ;;
        esac
    done
    echo ""

    # ── Commit Message ──────────────────────────────────────────────────
    print_section "Commit Message"
    echo ""
    dim "Conventional Commits format recommended:"
    dim "  feat:     New feature"
    dim "  fix:      Bug fix"
    dim "  docs:     Documentation changes"
    dim "  refactor: Code restructuring"
    dim "  test:     Adding/updating tests"
    dim "  chore:    Maintenance tasks"
    dim ""
    dim "Example: feat: add payment validation for CDC pipeline"
    dim "Example: fix(JIRA-1234): correct service detail amount calculation"
    echo ""

    echo -e "    ${BOLD}1)${NC} feat       4) refactor"
    echo -e "    ${BOLD}2)${NC} fix        5) test"
    echo -e "    ${BOLD}3)${NC} docs       6) chore"
    echo -e "    ${BOLD}7)${NC} custom (type your own)"
    echo ""

    prompt "Commit type [1-7]: "
    read -r commit_type_choice || true
    commit_type_choice=$(echo "$commit_type_choice" | tr -d '\r\t ')

    local commit_prefix
    case "$commit_type_choice" in
        1) commit_prefix="feat" ;;
        2) commit_prefix="fix" ;;
        3) commit_prefix="docs" ;;
        4) commit_prefix="refactor" ;;
        5) commit_prefix="test" ;;
        6) commit_prefix="chore" ;;
        7)
            prompt "Enter commit type: "
            read -r commit_prefix || true
            ;;
        *)
            error "Invalid selection."
            return 1
            ;;
    esac

    prompt "Scope (optional, e.g. JIRA-1234 — press Enter to skip): "
    read -r commit_scope || true
    commit_scope=$(echo "${commit_scope:-}" | tr -d '\r' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

    prompt "Short description: "
    read -r commit_desc || true
    commit_desc=$(echo "$commit_desc" | tr -d '\r' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

    if [[ -z "${commit_desc:-}" ]]; then
        error "Commit description cannot be empty."
        return 1
    fi

    # Build commit message
    local commit_msg
    if [[ -n "${commit_scope:-}" ]]; then
        commit_msg="${commit_prefix}(${commit_scope}): ${commit_desc}"
    else
        commit_msg="${commit_prefix}: ${commit_desc}"
    fi

    # Optional body
    prompt "Add detailed body? [y/N]: "
    read -r add_body || true
    add_body=$(echo "$add_body" | tr -d '\r')
    local commit_body=""
    if [[ "$add_body" =~ ^[Yy]$ ]]; then
        echo -e "    ${DIM}Enter commit body (empty line to finish):${NC}"
        local body_lines=()
        while IFS= read -r line || true; do
            [[ -z "${line:-}" ]] && break
            body_lines+=("$line")
        done
        commit_body=$(printf '%s\n' "${body_lines[@]}")
    fi

    # ── Confirm and Commit ──────────────────────────────────────────────
    print_section "Confirm Commit"
    echo ""
    echo -e "  ${BOLD}Message:${NC} $commit_msg"
    if [[ -n "$commit_body" ]]; then
        echo -e "  ${BOLD}Body:${NC}"
        echo "$commit_body" | sed 's/^/    /'
    fi
    echo ""

    if ! confirm "Proceed with commit?"; then
        warn "Commit cancelled."
        return 0
    fi

    if [[ -n "$commit_body" ]]; then
        git commit -m "$commit_msg" -m "$commit_body"
    else
        git commit -m "$commit_msg"
    fi

    info "Committed successfully."

    # ── Push ────────────────────────────────────────────────────────────
    print_section "Push to Remote"

    if ! check_remote; then
        warn "Skipping push — remote not reachable."
        return 0
    fi

    # Pre-push: check if remote has new commits (avoids rejection, keeps linear history)
    local upstream
    upstream=$(git rev-parse --abbrev-ref --symbolic-full-name "@{upstream}" 2>/dev/null || echo "")
    if [[ -n "$upstream" ]]; then
        fetch_latest 2>/dev/null || true
        local behind
        behind=$(git rev-list --count "HEAD..@{upstream}" 2>/dev/null || echo "0")
        if [[ "${behind:-0}" -gt 0 ]]; then
            warn "Remote has $behind new commit(s). Push may be rejected."
            warn "Run 'Sync Feature Branch' (option 3) first to rebase, then push."
            if ! confirm "Push anyway? (may fail)"; then
                return 0
            fi
        fi
    fi

    # Check if upstream is set

    if [[ -z "$upstream" ]]; then
        info "No upstream set. Will push and set upstream."
        if confirm "Push '$branch' to $REMOTE and set upstream?"; then
            git push -u "$REMOTE" "$branch"
            info "Pushed with upstream tracking set."
        fi
    else
        if confirm "Push to $upstream?"; then
            # For feature branches, normal push
            # For protected branches, this shouldn't happen directly
            if is_protected_branch "$branch"; then
                warn "You are pushing directly to protected branch '$branch'."
                warn "Consider using merge workflow instead."
                if ! confirm "Are you SURE you want to push directly?"; then
                    warn "Push cancelled."
                    return 0
                fi
            fi
            git push
            info "Pushed successfully."
        fi
    fi

    echo ""
}

# Interactive file staging helper
# Args: 1) pre-filled selection (e.g. "1,4"), 2) optional porcelain (avoids 3 git calls)
_interactive_file_staging() {
    local prefill="${1:-}"
    local porcelain="${2:-}"
    local all_files=()
    local file_statuses=()

    if [[ -n "$porcelain" ]]; then
        # Parse from porcelain (single git call upstream) — order: modified, untracked, staged
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            local path="${line:3}" xy="${line:0:2}"
            # For renames "R  old -> new": store both paths; we use path for display, staging adds new
            [[ "$path" == *" -> "* ]] && path="${path#* -> }"
            if [[ "$xy" == "??" ]]; then
                all_files+=("$path"); file_statuses+=("untracked")
            elif [[ "${xy:1:1}" == "M" ]] || [[ "${xy:1:1}" == "D" ]]; then
                all_files+=("$path"); file_statuses+=("modified")
            elif [[ "${xy:0:1}" =~ [MADRC] ]]; then
                all_files+=("$path"); file_statuses+=("staged")
            else
                all_files+=("$path"); file_statuses+=("modified")
            fi
        done < <(echo "$porcelain")
    else
        while IFS= read -r file; do
            [[ -n "$file" ]] && all_files+=("$file") && file_statuses+=("modified")
        done < <(git diff --name-only 2>/dev/null)
        while IFS= read -r file; do
            [[ -n "$file" ]] && all_files+=("$file") && file_statuses+=("untracked")
        done < <(git ls-files --others --exclude-standard 2>/dev/null)
        while IFS= read -r file; do
            local found=false
            for f in "${all_files[@]}"; do
                [[ "$f" == "$file" ]] && found=true && break
            done
            if ! $found && [[ -n "$file" ]]; then
                all_files+=("$file")
                file_statuses+=("staged")
            fi
        done < <(git diff --cached --name-only 2>/dev/null)
    fi

    if [[ ${#all_files[@]} -eq 0 ]]; then
        warn "No files to stage."
        return
    fi

    echo ""
    echo -e "  ${BOLD}Available files:${NC}"
    for i in "${!all_files[@]}"; do
        local status_color
        case "${file_statuses[$i]}" in
            modified)  status_color="${YELLOW}[modified]${NC}" ;;
            untracked) status_color="${GREEN}[new]${NC}" ;;
            staged)    status_color="${CYAN}[staged]${NC}" ;;
        esac
        echo -e "    ${BOLD}$((i+1)))${NC} ${all_files[$i]}  $status_color"
    done
    echo ""
    dim "Enter file numbers separated by spaces or commas."
    dim "Ranges supported: 1-5  |  All: 'a'  |  Example: 1,3,5-8"
    echo ""
    local file_selection
    if [[ -n "$prefill" ]]; then
        file_selection="$prefill"
        info "Using selection: $file_selection"
    else
        prompt "Files to stage: "
        read -r file_selection || true
    fi

    if [[ "$(echo "${file_selection:-}" | tr -d '\r')" =~ ^[Aa]$ ]]; then
        git add -A
        info "All files staged."
        return
    fi

    # Parse selection (supports: 1,3,5-8 or 1 3 5-8 — spaces or commas)
    # Strip \r (Windows CRLF) so "1,4" from terminal is parsed correctly
    local selected_indices=()
    local normalized
    normalized=$(echo "${file_selection:-}" | tr -d '\r' | tr ',' ' ')
    read -ra parts <<< "$normalized"
    for part in "${parts[@]}"; do
        part=$(echo "$part" | tr -d ' \r')
        if [[ "$part" =~ ^([0-9]+)-([0-9]+)$ ]]; then
            for (( i=BASH_REMATCH[1]; i<=BASH_REMATCH[2]; i++ )); do
                selected_indices+=("$i")
            done
        elif [[ "$part" =~ ^[0-9]+$ ]]; then
            selected_indices+=("$part")
        fi
    done

    # Stage selected files
    local staged_count=0
    for idx in "${selected_indices[@]}"; do
        local file_idx=$((idx - 1))
        if (( file_idx >= 0 && file_idx < ${#all_files[@]} )); then
            git add "${all_files[$file_idx]}"
            info "Staged: ${all_files[$file_idx]}"
            staged_count=$((staged_count + 1))
        else
            warn "Invalid index: $idx (skipped)"
        fi
    done

    info "$staged_count file(s) staged."
}

# ═════════════════════════════════════════════════════════════════════════════
# 3. SYNC FEATURE BRANCH (Rebase onto latest base)
# ═════════════════════════════════════════════════════════════════════════════
sync_feature_branch() {
    print_header "Sync Feature Branch (Rebase onto Base)"

    local branch
    branch=$(current_branch)

    if is_protected_branch "$branch"; then
        error "You are on protected branch '$branch'."
        error "This operation is for feature/bugfix/hotfix branches only."
        return 1
    fi

    if has_uncommitted_changes; then
        warn "You have uncommitted changes. Please commit or stash first."
        if confirm "Auto-stash changes?"; then
            git stash push -m "auto-stash before sync" --include-untracked
        else
            return 1
        fi
    fi

    # Use stored base branch if available (set when creating feature branch)
    local base=""
    local stored_base
    stored_base=$(get_stored_base_branch)
    if [[ -n "$stored_base" ]] && branch_exists_local "$stored_base"; then
        info "Stored base branch: $stored_base"
        if confirm "Use stored base '$stored_base'? (N = select different base)"; then
            base="$stored_base"
        fi
    fi

    if [[ -z "$base" ]]; then
        print_section "Select Base Branch to Sync From"
        local base_branches=()
        for b in "development" "main"; do
            branch_exists_local "$b" && base_branches+=("$b")
        done
        while IFS= read -r rb; do
            [[ -n "$rb" ]] && base_branches+=("$rb")
        done < <(git branch --list 'release/*' 2>/dev/null | sed 's|^[* ]*||')

        if [[ ${#base_branches[@]} -eq 0 ]]; then
            error "No base branches found."
            return 1
        fi

        echo ""
        for i in "${!base_branches[@]}"; do
            echo -e "    ${BOLD}$((i+1)))${NC} ${base_branches[$i]}"
        done
        echo ""

        prompt "Select base branch [1-${#base_branches[@]}]: "
        read -r selection || true
        selection=$(echo "${selection:-}" | tr -d '\r')

        if [[ ! "$selection" =~ ^[0-9]+$ ]] || (( selection < 1 || selection > ${#base_branches[@]} )); then
            error "Invalid selection."
            return 1
        fi

        base="${base_branches[$((selection-1))]}"
        git config "branch.$branch.base" "$base"
    fi

    # Fetch and update base branch
    fetch_latest
    info "Updating $base from $REMOTE..."
    git checkout "$base" --quiet
    git pull --rebase "$REMOTE" "$base" --quiet 2>/dev/null || true

    # Switch back and rebase
    git checkout "$branch" --quiet
    info "Rebasing '$branch' onto '$base'..."

    if git rebase "$base"; then
        info "Rebase successful. '$branch' is now up-to-date with '$base'."

        if confirm "Force-push rebased branch to remote?"; then
            git push --force-with-lease "$REMOTE" "$branch"
            info "Force-pushed with lease (safe force push)."
        fi
    else
        _handle_rebase_conflict "$branch" "$base"
    fi

    # Restore stash if we auto-stashed
    if git stash list | grep -q "auto-stash before sync"; then
        if confirm "Restore auto-stashed changes?"; then
            git stash pop
            info "Stashed changes restored."
        fi
    fi
}

# ═════════════════════════════════════════════════════════════════════════════
# 4. MERGE FEATURE BRANCH (Linear History — ff-only)
# ═════════════════════════════════════════════════════════════════════════════
merge_feature_to_base() {
    print_header "Merge Feature Branch → Base (Linear History)"

    local feature_branch
    feature_branch=$(current_branch)

    if is_protected_branch "$feature_branch"; then
        error "You are on a protected branch. Switch to a feature branch first."
        return 1
    fi

    if has_uncommitted_changes; then
        error "Uncommitted changes detected. Commit or stash before merging."
        return 1
    fi

    # ── Select target base branch ───────────────────────────────────────
    print_section "Select Target Branch to Merge Into"

    local base_branches=()
    for b in "development" "main"; do
        branch_exists_local "$b" && base_branches+=("$b")
    done
    while IFS= read -r rb; do
        [[ -n "$rb" ]] && base_branches+=("$rb")
    done < <(git branch --list 'release/*' 2>/dev/null | sed 's|^[* ]*||')

    if [[ ${#base_branches[@]} -eq 0 ]]; then
        error "No target branches found (development, main, release/*)."
        return 1
    fi

    echo ""
    for i in "${!base_branches[@]}"; do
        echo -e "    ${BOLD}$((i+1)))${NC} ${base_branches[$i]}"
    done
    echo ""

    prompt "Select target branch [1-${#base_branches[@]}]: "
    read -r selection || true
    selection=$(echo "${selection:-}" | tr -d '\r')

    if [[ ! "$selection" =~ ^[0-9]+$ ]] || (( selection < 1 || selection > ${#base_branches[@]} )); then
        error "Invalid selection."
        return 1
    fi

    local target="${base_branches[$((selection-1))]}"

    # ── Rebase feature onto latest target first ─────────────────────────
    print_section "Step 1: Rebase '$feature_branch' onto latest '$target'"

    fetch_latest
    git checkout "$target" --quiet
    git pull --rebase "$REMOTE" "$target" --quiet 2>/dev/null || true
    git checkout "$feature_branch" --quiet

    info "Rebasing '$feature_branch' onto '$target'..."
    if ! git rebase "$target"; then
        _handle_rebase_conflict "$feature_branch" "$target"
        return 1
    fi
    info "Rebase complete. Linear history ensured."

    # ── Fast-forward merge into target ──────────────────────────────────
    print_section "Step 2: Fast-Forward Merge into '$target'"

    git checkout "$target" --quiet

    if git merge --ff-only "$feature_branch"; then
        info "Fast-forward merge successful!"
        info "'$target' now includes all commits from '$feature_branch'."

        # Push
        if confirm "Push '$target' to $REMOTE?"; then
            git push "$REMOTE" "$target"
            info "'$target' pushed to $REMOTE."
        fi

        # Cleanup
        echo ""
        if confirm "Delete feature branch '$feature_branch' (local and remote)?"; then
            git branch -d "$feature_branch"
            info "Local branch deleted."
            if branch_exists_remote "$feature_branch"; then
                git push "$REMOTE" --delete "$feature_branch" 2>/dev/null || true
                info "Remote branch deleted."
            fi
        fi
    else
        error "Fast-forward merge not possible!"
        error "This means '$feature_branch' is not a direct descendant of '$target'."
        warn "Run 'Sync Feature Branch' first to rebase."
        git checkout "$feature_branch" --quiet
        return 1
    fi

    echo ""
    info "Merge complete. Linear history preserved on '$target'."
}

# ═════════════════════════════════════════════════════════════════════════════
# 5. STASH MANAGEMENT
# ═════════════════════════════════════════════════════════════════════════════
stash_management() {
    print_header "Stash Management"

    echo ""
    echo -e "    ${BOLD}1)${NC} Stash current changes"
    echo -e "    ${BOLD}2)${NC} List all stashes"
    echo -e "    ${BOLD}3)${NC} Apply latest stash (keep in stash list)"
    echo -e "    ${BOLD}4)${NC} Pop latest stash (remove from stash list)"
    echo -e "    ${BOLD}5)${NC} Drop a specific stash"
    echo -e "    ${BOLD}6)${NC} Clear all stashes"
    echo ""

    prompt "Selection [1-6]: "
    read -r choice || true
    choice=$(echo "${choice:-}" | tr -d '\r')

    case "$choice" in
        1)
            prompt "Stash message (optional): "
            read -r stash_msg || true
            if [[ -n "${stash_msg:-}" ]]; then
                git stash push -m "$stash_msg" --include-untracked
            else
                git stash push --include-untracked
            fi
            info "Changes stashed."
            ;;
        2)
            echo ""
            local stash_list
            stash_list=$(git stash list 2>/dev/null)
            if [[ -z "$stash_list" ]]; then
                info "No stashes found."
            else
                echo "$stash_list"
            fi
            ;;
        3)
            git stash apply && info "Latest stash applied." || error "No stash to apply."
            ;;
        4)
            git stash pop && info "Latest stash popped." || error "No stash to pop."
            ;;
        5)
            local stash_entries
            stash_entries=$(git stash list 2>/dev/null)
            if [[ -z "$stash_entries" ]]; then
                info "No stashes to drop."
            else
                echo "$stash_entries"
                echo ""
                prompt "Enter stash index to drop (e.g. 0): "
                read -r stash_idx || true
                stash_idx=$(echo "${stash_idx:-}" | tr -d '\r')
                if [[ "$stash_idx" =~ ^[0-9]+$ ]]; then
                    git stash drop "stash@{$stash_idx}" && info "Stash dropped." || error "Failed to drop stash@{$stash_idx}."
                else
                    error "Invalid index: '$stash_idx'. Must be a number."
                fi
            fi
            ;;
        6)
            if confirm "Drop ALL stashes? This cannot be undone."; then
                git stash clear
                info "All stashes cleared."
            fi
            ;;
        *)
            error "Invalid selection."
            ;;
    esac
}

# ═════════════════════════════════════════════════════════════════════════════
# 6. VIEW LOG / HISTORY
# ═════════════════════════════════════════════════════════════════════════════
view_log() {
    print_header "Git Log — Linear History"

    echo ""
    echo -e "    ${BOLD}1)${NC} Last 15 commits (current branch)"
    echo -e "    ${BOLD}2)${NC} Last 30 commits (all branches, graph)"
    echo -e "    ${BOLD}3)${NC} Commits on current branch not in base"
    echo ""

    prompt "Selection [1-3]: "
    read -r choice || true
    choice=$(echo "${choice:-}" | tr -d '\r')

    case "$choice" in
        1)
            echo ""
            git log --oneline --decorate -15
            ;;
        2)
            echo ""
            git log --oneline --graph --decorate --all -30
            ;;
        3)
            prompt "Compare against which base branch? (e.g. development): "
            read -r base || true
            echo ""
            if branch_exists_local "$base"; then
                git log --oneline "$base..HEAD"
            else
                error "Branch '$base' not found."
            fi
            ;;
        *)
            error "Invalid selection."
            ;;
    esac
}

# ═════════════════════════════════════════════════════════════════════════════
# 7. SETUP LINEAR HISTORY (one-time repo config)
# ═════════════════════════════════════════════════════════════════════════════
setup_linear_history() {
    print_header "Setup Linear History (Repository Config)"

    echo ""
    dim "This configures your local Git to enforce linear history:"
    dim "  • Default pull strategy: rebase (not merge)"
    dim "  • Auto-stash on rebase"
    dim "  • Fast-forward only merges on protected branches"
    echo ""

    if ! confirm "Apply these settings to the current repository?"; then
        return 0
    fi

    # Pull with rebase by default (no merge commits on pull)
    git config pull.rebase true
    info "Set pull.rebase = true"

    # Auto-stash before rebase
    git config rebase.autoStash true
    info "Set rebase.autoStash = true"

    # Default merge is ff-only (prevents accidental merge commits)
    git config merge.ff only
    info "Set merge.ff = only (fast-forward only)"

    # Prune stale remote-tracking branches on fetch
    git config fetch.prune true
    info "Set fetch.prune = true"

    # Push default: current branch only
    git config push.default current
    info "Set push.default = current"

    # Autosquash for interactive rebase
    git config rebase.autoSquash true
    info "Set rebase.autoSquash = true"

    echo ""
    info "Linear history configuration applied!"
    echo ""
    dim "Current Git configuration:"
    echo ""
    git config --local --list 2>/dev/null | grep -E "pull\.|rebase\.|merge\.|fetch\.|push\." | sed 's/^/    /'
    echo ""
}

# ═════════════════════════════════════════════════════════════════════════════
# 8. BRANCH CLEANUP
# ═════════════════════════════════════════════════════════════════════════════
branch_cleanup() {
    print_header "Branch Cleanup"

    echo ""
    echo -e "    ${BOLD}1)${NC} Delete merged feature branches (local)"
    echo -e "    ${BOLD}2)${NC} Delete merged feature branches (local + remote)"
    echo -e "    ${BOLD}3)${NC} Prune stale remote-tracking branches"
    echo ""

    prompt "Selection [1-3]: "
    read -r choice || true
    choice=$(echo "${choice:-}" | tr -d '\r')

    case "$choice" in
        1|2)
            # Find branches merged into development or main
            local merged_branches=()
            for base in "development" "main"; do
                if branch_exists_local "$base"; then
                    while IFS= read -r b; do
                        b=$(echo "$b" | tr -d ' *')
                        [[ -z "$b" ]] && continue
                        is_protected_branch "$b" && continue
                        merged_branches+=("$b")
                    done < <(git branch --merged "$base" 2>/dev/null)
                fi
            done

            # Deduplicate
            local unique_branches
            unique_branches=$(printf '%s\n' "${merged_branches[@]}" | sort -u)

            if [[ -z "$unique_branches" ]]; then
                info "No merged feature branches to clean up."
                return 0
            fi

            echo ""
            echo -e "  ${BOLD}Merged branches (safe to delete):${NC}"
            echo "$unique_branches" | sed 's/^/    /'
            echo ""

            if confirm "Delete these branches?"; then
                local current
                current=$(current_branch)
                while IFS= read -r b; do
                    [[ -z "$b" ]] && continue
                    [[ "$b" == "$current" ]] && warn "Skipping current branch '$b'" && continue
                    git branch -d "$b" 2>/dev/null && info "Deleted local: $b"
                    if [[ "$choice" == "2" ]] && branch_exists_remote "$b"; then
                        git push "$REMOTE" --delete "$b" 2>/dev/null && info "Deleted remote: $b"
                    fi
                done <<< "$unique_branches"
            fi
            ;;
        3)
            git fetch --prune
            info "Stale remote-tracking branches pruned."
            ;;
        *)
            error "Invalid selection."
            ;;
    esac
}

# ═════════════════════════════════════════════════════════════════════════════
# 9. RESOLVE REBASE/MERGE CONFLICTS
# ═════════════════════════════════════════════════════════════════════════════
resolve_conflicts() {
    print_header "Resolve Rebase/Merge Conflicts"

    if ! in_rebase_state && ! in_merge_state; then
        info "No rebase or merge in progress. Working tree is clean."
        return 0
    fi

    local conflicted
    conflicted=$(get_conflicted_files)
    local op="rebase"
    in_merge_state && op="merge"

    echo -e "  ${BOLD}Status:${NC} $op in progress"
    if [[ -n "$conflicted" ]]; then
        echo -e "  ${BOLD}Conflicted files:${NC}"
        echo "$conflicted" | sed 's/^/    /'
        echo ""
        echo -e "    ${BOLD}1)${NC} Stage all resolved files and continue"
        echo -e "    ${BOLD}2)${NC} Stage specific file(s)"
        echo -e "    ${BOLD}3)${NC} Show conflict summary (git diff --check)"
        echo -e "    ${BOLD}4)${NC} Abort $op and return to previous state"
        echo ""
        prompt "Selection [1-4]: "
        read -r choice || true
        choice=$(echo "${choice:-}" | tr -d '\r')
        case "$choice" in
            1)
                git add -A
                if in_rebase_state; then
                    if git rebase --continue 2>/dev/null; then
                        info "Rebase completed successfully."
                    else
                        warn "More conflicts. Resolve remaining files and run option 1 again."
                    fi
                else
                    if git -c core.editor=true merge --continue 2>/dev/null; then
                        info "Merge completed successfully."
                    else
                        warn "Fix any issues and run: git merge --continue"
                    fi
                fi
                ;;
            2)
                echo ""
                local i=1
                local files=()
                while IFS= read -r f; do
                    [[ -n "$f" ]] && files+=("$f") && echo -e "    ${BOLD}$i)${NC} $f" && i=$((i + 1))
                done <<< "$conflicted"
                echo ""
                prompt "Enter file number(s) to stage (e.g. 1,3 or 1 3): "
                read -r nums || true
                nums=$(echo "$nums" | tr -d '\r' | tr ',' ' ')
                for n in $nums; do
                    n=$(echo "$n" | tr -d ' \r')
                    if [[ "$n" =~ ^[0-9]+$ ]] && (( n >= 1 && n <= ${#files[@]} )); then
                        git add "${files[$((n-1))]}"
                        info "Staged: ${files[$((n-1))]}"
                    fi
                done
                if in_rebase_state; then
                    prompt "Continue rebase now? [y/N]: "
                    read -r cont || true
                    cont=$(echo "${cont:-}" | tr -d '\r')
                    [[ "$cont" =~ ^[Yy]$ ]] && git rebase --continue 2>/dev/null && info "Rebase continued."
                fi
                ;;
            3)
                echo ""
                git diff --check 2>/dev/null | head -20 || true
                echo ""
                ;;
            4)
                if confirm "Abort $op? All $op progress will be lost."; then
                    if in_rebase_state; then
                        git rebase --abort
                        info "Rebase aborted."
                    else
                        git merge --abort
                        info "Merge aborted."
                    fi
                fi
                ;;
            *)
                error "Invalid selection."
                ;;
        esac
    else
        warn "No conflicted files found, but $op is in progress."
        dim "Run: git add <files> && git ${op} --continue"
        if confirm "Abort $op?"; then
            if in_rebase_state; then git rebase --abort; else git merge --abort; fi
            info "Aborted."
        fi
    fi
}

# ═════════════════════════════════════════════════════════════════════════════
# 10. CHERRY-PICK SPECIFIC FILES FROM SOURCE BRANCH
# ═════════════════════════════════════════════════════════════════════════════
cherry_pick_files() {
    print_header "Cherry-Pick Specific Files from Source Branch"

    local current
    current=$(current_branch)
    local stashed=false
    local switched=false

    # Helper: restore state on early exit (stash + branch)
    _cp_cleanup() {
        if $switched; then
            git checkout "$current" --quiet 2>/dev/null || true
        fi
        if $stashed; then
            git stash pop --quiet 2>/dev/null \
                && info "Auto-stashed changes restored." \
                || warn "Could not restore stash. Use 'git stash pop' manually."
        fi
    }

    # ── Select source branch ─────────────────────────────────────────────
    print_section "Select Source Branch"

    local all_branches=()
    while IFS= read -r b; do
        b=$(echo "$b" | tr -d ' *')
        [[ -z "$b" || "$b" == "$current" ]] && continue
        all_branches+=("$b")
    done < <(git branch -a 2>/dev/null | sed 's|remotes/[^/]*/||;s|^[* ]*||' | sort -u)

    if [[ ${#all_branches[@]} -eq 0 ]]; then
        error "No other branches found to cherry-pick files from."
        return 1
    fi

    echo ""
    for i in "${!all_branches[@]}"; do
        echo -e "    ${BOLD}$((i+1)))${NC} ${all_branches[$i]}"
    done
    echo ""

    prompt "Select source branch [1-${#all_branches[@]}]: "
    read -r selection || true
    selection=$(echo "${selection:-}" | tr -d '\r')

    if [[ ! "$selection" =~ ^[0-9]+$ ]] || (( selection < 1 || selection > ${#all_branches[@]} )); then
        error "Invalid selection."
        return 1
    fi

    local source_branch="${all_branches[$((selection-1))]}"
    info "Source branch: $source_branch"

    # ── Select target branch ─────────────────────────────────────────────
    print_section "Select Target Branch (where files will be applied)"

    local local_branches=()
    while IFS= read -r b; do
        b=$(echo "$b" | tr -d ' *')
        [[ -z "$b" || "$b" == "$source_branch" ]] && continue
        local_branches+=("$b")
    done < <(git branch 2>/dev/null | sed 's|^[* ]*||' | sort -u)

    echo ""
    echo -e "    ${BOLD}1)${NC} Current branch: ${CYAN}$current${NC}  (stay here)"

    local non_current_branches=()
    for b in "${local_branches[@]}"; do
        [[ "$b" == "$current" ]] && continue
        non_current_branches+=("$b")
    done

    for i in "${!non_current_branches[@]}"; do
        echo -e "    ${BOLD}$((i+2)))${NC} ${non_current_branches[$i]}"
    done
    echo ""

    prompt "Select target branch [1-$((${#non_current_branches[@]}+1))]: "
    read -r tsel || true
    tsel=$(echo "${tsel:-1}" | tr -d '\r')

    local target_branch="$current"
    if [[ "$tsel" =~ ^[0-9]+$ ]] && (( tsel >= 2 && tsel <= ${#non_current_branches[@]}+1 )); then
        target_branch="${non_current_branches[$((tsel-2))]}"
    elif [[ ! "$tsel" =~ ^1$ ]]; then
        error "Invalid selection."
        return 1
    fi

    info "Target branch: $target_branch"

    # Switch to target if different from current
    if [[ "$target_branch" != "$current" ]]; then
        if has_uncommitted_changes; then
            warn "Uncommitted changes detected."
            if confirm "Auto-stash before switching to '$target_branch'?"; then
                git stash push -m "auto-stash before cherry-pick-files" --include-untracked
                stashed=true
            else
                error "Cannot switch branches with uncommitted changes."
                return 1
            fi
        fi
        if ! git checkout "$target_branch" --quiet; then
            error "Failed to switch to '$target_branch'."
            _cp_cleanup
            return 1
        fi
        switched=true
        info "Switched to '$target_branch'."
    fi

    # ── List files that differ between source and target ─────────────────
    print_section "Files Differing in '$source_branch' vs '$target_branch'"

    local diff_files=()
    while IFS= read -r f; do
        [[ -n "$f" ]] && diff_files+=("$f")
    done < <(git diff --name-only "$target_branch" "$source_branch" 2>/dev/null)

    if [[ ${#diff_files[@]} -eq 0 ]]; then
        info "No file differences found between '$source_branch' and '$target_branch'."
        _cp_cleanup
        return 0
    fi

    # Pre-fetch diff statuses in one call for performance
    local diff_status_output
    diff_status_output=$(git diff --name-status "$target_branch" "$source_branch" 2>/dev/null)

    echo ""
    echo -e "  ${BOLD}Changed files (source vs target):${NC}"
    for i in "${!diff_files[@]}"; do
        local diff_type
        diff_type=$(echo "$diff_status_output" | awk -v f="${diff_files[$i]}" '($2==f || $3==f){print $1; exit}')
        local status_label
        case "$diff_type" in
            A*)  status_label="${GREEN}[added in source]${NC}" ;;
            D*)  status_label="${RED}[deleted in source]${NC}" ;;
            M*)  status_label="${YELLOW}[modified]${NC}" ;;
            R*)  status_label="${CYAN}[renamed]${NC}" ;;
            *)   status_label="${DIM}[changed]${NC}" ;;
        esac
        echo -e "    ${BOLD}$((i+1)))${NC} ${diff_files[$i]}  $status_label"
    done
    echo ""
    dim "Select files to bring from '$source_branch' into '$target_branch'."
    dim "Ranges: 1-5  |  All: 'a'  |  Example: 1,3,5-8"
    echo ""

    prompt "Files to cherry-pick: "
    read -r file_selection || true
    file_selection=$(echo "${file_selection:-}" | tr -d '\r')

    # Parse selection into array of file paths
    local selected_files=()
    if [[ "$file_selection" =~ ^[Aa]$ ]]; then
        selected_files=("${diff_files[@]}")
    else
        local normalized
        normalized=$(echo "$file_selection" | tr ',' ' ')
        read -ra parts <<< "$normalized"
        for part in "${parts[@]}"; do
            part=$(echo "$part" | tr -d ' \r')
            if [[ "$part" =~ ^([0-9]+)-([0-9]+)$ ]]; then
                for (( idx=BASH_REMATCH[1]; idx<=BASH_REMATCH[2]; idx++ )); do
                    local fidx=$((idx - 1))
                    (( fidx >= 0 && fidx < ${#diff_files[@]} )) && selected_files+=("${diff_files[$fidx]}")
                done
            elif [[ "$part" =~ ^[0-9]+$ ]]; then
                local fidx=$((part - 1))
                (( fidx >= 0 && fidx < ${#diff_files[@]} )) && selected_files+=("${diff_files[$fidx]}")
            fi
        done
    fi

    if [[ ${#selected_files[@]} -eq 0 ]]; then
        warn "No valid files selected. Aborting."
        _cp_cleanup
        return 0
    fi

    echo ""
    echo -e "  ${BOLD}Files to bring from '${source_branch}':${NC}"
    for f in "${selected_files[@]}"; do
        echo -e "    ${GREEN}→${NC} $f"
    done
    echo ""

    if ! confirm "Apply these ${#selected_files[@]} file(s) from '$source_branch' into '$target_branch'?"; then
        warn "Cherry-pick files cancelled."
        _cp_cleanup
        return 0
    fi

    # ── Apply the selected files ──────────────────────────────────────────
    local applied=0 failed=0
    for f in "${selected_files[@]}"; do
        local ftype
        ftype=$(echo "$diff_status_output" | awk -v fl="$f" '($2==fl || $3==fl){print $1; exit}')
        if [[ "$ftype" == D* ]]; then
            # File deleted in source — remove it from target
            if [[ -f "$f" ]]; then
                git rm "$f" --quiet \
                    && info "Removed (deleted in source): $f" \
                    && applied=$((applied+1)) \
                    || { warn "Failed to remove: $f"; failed=$((failed+1)); }
            else
                warn "Already absent locally: $f"
            fi
        else
            if git checkout "$source_branch" -- "$f" 2>/dev/null; then
                info "Applied: $f"
                applied=$((applied+1))
            else
                warn "Failed to apply: $f"
                failed=$((failed+1))
            fi
        fi
    done

    echo ""
    info "$applied file(s) staged from '$source_branch'."
    [[ $failed -gt 0 ]] && warn "$failed file(s) could not be applied."

    echo ""
    echo -e "  ${BOLD}Staged changes:${NC}"
    git diff --cached --stat 2>/dev/null | sed 's/^/    /' || true
    echo ""

    # ── Optional commit ───────────────────────────────────────────────────
    if confirm "Commit these cherry-picked files now?"; then
        local default_msg="chore: cherry-pick files from ${source_branch} into ${target_branch}"
        echo ""
        echo -e "  ${BOLD}Default message:${NC} $default_msg"
        prompt "Use default message? [Y/n]: "
        read -r use_default || true
        use_default=$(echo "${use_default:-y}" | tr -d '\r')

        local commit_msg="$default_msg"
        if [[ "$use_default" =~ ^[Nn]$ ]]; then
            prompt "Enter commit message: "
            read -r commit_msg || true
            commit_msg=$(echo "$commit_msg" | tr -d '\r' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            [[ -z "$commit_msg" ]] && commit_msg="$default_msg"
        fi

        if git commit -m "$commit_msg"; then
            info "Committed: $commit_msg"
        else
            error "Commit failed."
            return 1
        fi

        # ── Optional push ─────────────────────────────────────────────────
        if check_remote && confirm "Push '$target_branch' to $REMOTE?"; then
            local upstream
            upstream=$(git rev-parse --abbrev-ref --symbolic-full-name "@{upstream}" 2>/dev/null || echo "")
            if [[ -z "$upstream" ]]; then
                git push -u "$REMOTE" "$target_branch"
            else
                git push
            fi
            info "Pushed '$target_branch' to $REMOTE."
        fi
    else
        warn "Files staged but not committed. Use 'Stage, Commit & Push' (option 2) when ready."
    fi

    echo ""
    info "Cherry-pick files complete. Current branch: $(current_branch)"
}

# ═════════════════════════════════════════════════════════════════════════════
# 11. DEVELOPMENT → RELEASE: CHERRY-PICK FILES & RAISE PR
# ═════════════════════════════════════════════════════════════════════════════
dev_to_release_pr() {
    print_header "Dev → Release: Cherry-Pick Files & Raise PR"

    local original_branch
    original_branch=$(current_branch)
    local stashed=false

    # Helper: restore stash on the correct branch
    _dtr_restore_stash() {
        if $stashed; then
            git stash pop --quiet 2>/dev/null \
                && info "Auto-stashed changes restored." \
                || warn "Could not restore stash. Use 'git stash pop' manually."
        fi
    }

    # ── Resolve development branch (development or develop) ─────────────
    local dev_branch=""
    for candidate in "development" "develop"; do
        if branch_exists_local "$candidate" || branch_exists_remote "$candidate"; then
            dev_branch="$candidate"
            break
        fi
    done

    if [[ -z "$dev_branch" ]]; then
        error "No development branch found (tried: development, develop)."
        error "Please ensure your development branch exists locally or on $REMOTE."
        return 1
    fi
    info "Development branch: $dev_branch"

    # Make sure development is available locally and up-to-date
    if ! branch_exists_local "$dev_branch"; then
        info "Fetching '$dev_branch' from $REMOTE..."
        git fetch "$REMOTE" "$dev_branch":"$dev_branch" --quiet 2>/dev/null \
            || { error "Could not fetch '$dev_branch' from $REMOTE."; return 1; }
    else
        info "Updating '$dev_branch' from $REMOTE..."
        git fetch "$REMOTE" "$dev_branch" --quiet 2>/dev/null || warn "Fetch failed — using local copy."
        # Fast-forward local branch to match remote (safe: won't discard local-only commits)
        if git merge-base --is-ancestor "$dev_branch" "refs/remotes/$REMOTE/$dev_branch" 2>/dev/null; then
            git branch -f "$dev_branch" "refs/remotes/$REMOTE/$dev_branch" 2>/dev/null || true
        else
            warn "'$dev_branch' has local commits not on $REMOTE — using local version."
        fi
    fi

    # ── Resolve release branch (always named 'release') ──────────────────
    local release_branch="release"

    if ! branch_exists_local "$release_branch" && ! branch_exists_remote "$release_branch"; then
        error "No 'release' branch found locally or on $REMOTE."
        error "Please create the 'release' branch first."
        return 1
    fi
    info "Release branch: $release_branch"

    # Ensure release branch exists locally and is up-to-date
    if ! branch_exists_local "$release_branch"; then
        info "Fetching '$release_branch' from $REMOTE..."
        git fetch "$REMOTE" "$release_branch":"$release_branch" --quiet 2>/dev/null \
            || { error "Could not fetch '$release_branch'."; return 1; }
    else
        git fetch "$REMOTE" "$release_branch" --quiet 2>/dev/null || warn "Fetch failed — using local copy."
        # Fast-forward local branch to match remote (safe: won't discard local-only commits)
        if git merge-base --is-ancestor "$release_branch" "refs/remotes/$REMOTE/$release_branch" 2>/dev/null; then
            git branch -f "$release_branch" "refs/remotes/$REMOTE/$release_branch" 2>/dev/null || true
        else
            warn "'$release_branch' has local commits not on $REMOTE — using local version."
        fi
    fi

    # ── Show files differing between dev and release ──────────────────────
    print_section "Files Differing: '$dev_branch' vs '$release_branch'"

    local diff_files=()
    while IFS= read -r f; do
        [[ -n "$f" ]] && diff_files+=("$f")
    done < <(git diff --name-only "$release_branch" "$dev_branch" 2>/dev/null)

    if [[ ${#diff_files[@]} -eq 0 ]]; then
        info "No differences found between '$dev_branch' and '$release_branch'."
        return 0
    fi

    # Single call for all statuses
    local diff_status_output
    diff_status_output=$(git diff --name-status "$release_branch" "$dev_branch" 2>/dev/null)

    echo ""
    echo -e "  ${BOLD}Files changed in '$dev_branch' compared to '$release_branch':${NC}"
    for i in "${!diff_files[@]}"; do
        local dt
        dt=$(echo "$diff_status_output" | awk -v f="${diff_files[$i]}" '($2==f || $3==f){print $1; exit}')
        local lbl
        case "$dt" in
            A*)  lbl="${GREEN}[new in dev]${NC}" ;;
            D*)  lbl="${RED}[deleted in dev]${NC}" ;;
            M*)  lbl="${YELLOW}[modified]${NC}" ;;
            R*)  lbl="${CYAN}[renamed]${NC}" ;;
            *)   lbl="${DIM}[changed]${NC}" ;;
        esac
        echo -e "    ${BOLD}$((i+1)))${NC} ${diff_files[$i]}  $lbl"
    done
    echo ""
    dim "Select files to promote from '$dev_branch' to '$release_branch'."
    dim "Ranges: 1-5  |  All: 'a'  |  Example: 1,3,5-8"
    echo ""

    prompt "Files to cherry-pick: "
    read -r file_selection || true
    file_selection=$(echo "${file_selection:-}" | tr -d '\r')

    # Parse selection
    local selected_files=()
    if [[ "$file_selection" =~ ^[Aa]$ ]]; then
        selected_files=("${diff_files[@]}")
    else
        local normalized
        normalized=$(echo "$file_selection" | tr ',' ' ')
        read -ra parts <<< "$normalized"
        for part in "${parts[@]}"; do
            part=$(echo "$part" | tr -d ' \r')
            if [[ "$part" =~ ^([0-9]+)-([0-9]+)$ ]]; then
                for (( idx=BASH_REMATCH[1]; idx<=BASH_REMATCH[2]; idx++ )); do
                    local fidx=$((idx - 1))
                    (( fidx >= 0 && fidx < ${#diff_files[@]} )) && selected_files+=("${diff_files[$fidx]}")
                done
            elif [[ "$part" =~ ^[0-9]+$ ]]; then
                local fidx=$((part - 1))
                (( fidx >= 0 && fidx < ${#diff_files[@]} )) && selected_files+=("${diff_files[$fidx]}")
            fi
        done
    fi

    if [[ ${#selected_files[@]} -eq 0 ]]; then
        warn "No valid files selected. Aborting."
        return 0
    fi

    echo ""
    echo -e "  ${BOLD}Files selected from '$dev_branch':${NC}"
    for f in "${selected_files[@]}"; do
        echo -e "    ${GREEN}→${NC} $f"
    done
    echo ""

    # ── Name the custom PR branch ─────────────────────────────────────────
    print_section "Name the PR Branch"

    local timestamp
    timestamp=$(date '+%Y%m%d-%H%M%S' 2>/dev/null || echo "$(date '+%s')")
    local default_branch_name="cherry-pick/dev-to-${release_branch//\//-}-${timestamp}"
    # Sanitize: allow alphanum, slashes, dots, hyphens, underscores; strip leading dash
    default_branch_name=$(echo "$default_branch_name" | tr ' ' '-' | tr -cd 'a-zA-Z0-9/_.-' | sed 's/^-*//')

    echo ""
    echo -e "  ${BOLD}Default branch name:${NC} $default_branch_name"
    prompt "Use default? [Y/n] or type a custom name: "
    read -r branch_input || true
    branch_input=$(echo "${branch_input:-}" | tr -d '\r' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

    local pr_branch="$default_branch_name"
    if [[ -n "$branch_input" && ! "$branch_input" =~ ^[Yy]$ ]]; then
        pr_branch=$(echo "$branch_input" | tr ' ' '-' | tr -cd 'a-zA-Z0-9/_.-' | sed 's/^-*//')
    fi

    if [[ -z "$pr_branch" ]]; then
        error "Branch name cannot be empty."
        return 1
    fi

    if branch_exists_local "$pr_branch"; then
        error "Branch '$pr_branch' already exists locally."
        if confirm "Delete it and recreate from '$release_branch'?"; then
            git branch -D "$pr_branch"
        else
            return 1
        fi
    fi

    info "PR branch: $pr_branch"

    # ── Stash if needed ───────────────────────────────────────────────────
    if has_uncommitted_changes; then
        warn "You have uncommitted changes."
        if confirm "Auto-stash before switching?"; then
            git stash push -m "auto-stash before dev-to-release-pr" --include-untracked
            stashed=true
        else
            error "Cannot proceed with uncommitted changes."
            return 1
        fi
    fi

    # ── Create PR branch off release ──────────────────────────────────────
    print_section "Creating '$pr_branch' from '$release_branch'"

    if ! git checkout "$release_branch" --quiet; then
        error "Failed to switch to '$release_branch'."
        _dtr_restore_stash
        return 1
    fi
    if ! git checkout -b "$pr_branch"; then
        error "Failed to create branch '$pr_branch'."
        git checkout "$original_branch" --quiet 2>/dev/null || true
        _dtr_restore_stash
        return 1
    fi
    info "Branch '$pr_branch' created from '$release_branch'."

    # ── Apply selected files from dev ─────────────────────────────────────
    print_section "Applying Files from '$dev_branch'"

    local applied=0 failed=0
    for f in "${selected_files[@]}"; do
        local ftype
        ftype=$(echo "$diff_status_output" | awk -v fl="$f" '($2==fl || $3==fl){print $1; exit}')
        if [[ "$ftype" == D* ]]; then
            if [[ -f "$f" ]]; then
                git rm "$f" --quiet \
                    && info "Removed (deleted in dev): $f" \
                    && applied=$((applied+1)) \
                    || { warn "Failed to remove: $f"; failed=$((failed+1)); }
            else
                warn "Already absent: $f"
            fi
        else
            if git checkout "$dev_branch" -- "$f" 2>/dev/null; then
                info "Applied: $f"
                applied=$((applied+1))
            else
                warn "Failed to apply: $f"
                failed=$((failed+1))
            fi
        fi
    done

    echo ""
    info "$applied file(s) applied from '$dev_branch'."
    [[ $failed -gt 0 ]] && warn "$failed file(s) could not be applied."

    # Verify something is staged
    if git diff --cached --quiet 2>/dev/null; then
        warn "No staged changes after applying files."
        git checkout "$release_branch" --quiet 2>/dev/null || true
        git branch -D "$pr_branch" 2>/dev/null || true
        git checkout "$original_branch" --quiet 2>/dev/null || true
        _dtr_restore_stash
        return 1
    fi

    echo ""
    echo -e "  ${BOLD}Staged changes:${NC}"
    git diff --cached --stat 2>/dev/null | sed 's/^/    /' || true
    echo ""

    # ── Commit ────────────────────────────────────────────────────────────
    print_section "Commit"

    local default_commit_msg="chore: cherry-pick [${#selected_files[@]} file(s)] from ${dev_branch} → ${release_branch}"

    echo -e "  ${BOLD}Default:${NC} $default_commit_msg"
    prompt "Use default message? [Y/n]: "
    read -r use_default || true
    use_default=$(echo "${use_default:-y}" | tr -d '\r')

    local commit_msg="$default_commit_msg"
    if [[ "$use_default" =~ ^[Nn]$ ]]; then
        prompt "Enter commit message: "
        read -r commit_msg || true
        commit_msg=$(echo "$commit_msg" | tr -d '\r' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        [[ -z "$commit_msg" ]] && commit_msg="$default_commit_msg"
    fi

    if ! git commit -m "$commit_msg"; then
        error "Commit failed."
        return 1
    fi
    info "Committed."

    # ── Push & show PR URL ────────────────────────────────────────────────
    print_section "Push & Raise PR"

    if ! check_remote; then
        warn "Remote unreachable. Push manually with:"
        echo -e "    ${DIM}git push -u $REMOTE $pr_branch${NC}"
        warn "You are on branch '$pr_branch'. Switch back with: git checkout $original_branch"
        if $stashed; then
            warn "Your stashed changes are still saved. Restore with: git stash pop"
        fi
        return 0
    fi

    echo ""
    info "Pushing '$pr_branch' to $REMOTE..."
    local push_output
    # Capture both stdout and stderr; git push PR URL comes on stderr
    push_output=$(git push -u "$REMOTE" "$pr_branch" 2>&1)
    local push_exit=$?
    echo "$push_output"

    if [[ $push_exit -ne 0 ]]; then
        warn "Push failed. Try manually: git push -u $REMOTE $pr_branch"
    else
        info "Pushed '$pr_branch' to $REMOTE."

        # Strip ANSI codes before extracting URL
        local clean_output
        clean_output=$(echo "$push_output" | sed 's/\x1b\[[0-9;]*m//g')

        # Extract PR URL printed by the remote (GitHub / Gitea / GitLab all print one)
        local pr_url
        pr_url=$(echo "$clean_output" | grep -oE 'https?://[^ ]+/(pull|merge_requests|compare)[^ ]*' | head -1)

        # Fallback: build a compare URL from the remote URL
        if [[ -z "$pr_url" ]]; then
            local remote_url
            remote_url=$(git remote get-url "$REMOTE" 2>/dev/null \
                | sed 's|://[^@]*@|://|'   \
                | sed 's|\.git$||')
            # Gitea / GitHub compare URL pattern
            pr_url="${remote_url}/compare/${release_branch}...${pr_branch}"
        fi

        echo ""
        echo -e "${BOLD}${GREEN}  ✔ PR branch ready!${NC}"
        echo ""
        echo -e "  ${BOLD}Source:${NC}  $pr_branch"
        echo -e "  ${BOLD}Target:${NC}  $release_branch"
        echo -e "  ${BOLD}Files:${NC}   ${#selected_files[@]} file(s) cherry-picked from $dev_branch"
        echo ""
        echo -e "  ${BOLD}${CYAN}Open this URL to create your Pull Request:${NC}"
        echo -e "    ${CYAN}$pr_url${NC}"
        echo ""
    fi

    # Restore stash on the original branch (not on pr_branch)
    if $stashed; then
        git checkout "$original_branch" --quiet 2>/dev/null || true
        if confirm "Restore auto-stashed changes (on '$original_branch')?"; then
            if git stash pop; then
                info "Stash restored on '$original_branch'."
            else
                warn "Stash pop failed. Your changes are still in stash."
            fi
        else
            warn "Stash preserved. Use 'git stash pop' to restore later."
        fi
        # Switch back to pr_branch so the user sees the result
        git checkout "$pr_branch" --quiet 2>/dev/null || true
    fi

    echo ""
    info "Done. Current branch: $(current_branch)"
}

# ─────────────────────────────────────────────────────────────────────────────
# MAIN MENU
# ─────────────────────────────────────────────────────────────────────────────
main_menu() {
    while true; do
        print_header "Git Automation — Main Menu"

        local branch
        branch=$(current_branch)
        echo -e "  ${BOLD}Repository:${NC} ${REPO_NAME:-$(basename "$(git rev-parse --show-toplevel 2>/dev/null)")}"
        echo -e "  ${BOLD}Branch:${NC}     $branch"
        if is_protected_branch "$branch"; then
            echo -e "  ${BOLD}Type:${NC}       ${RED}PROTECTED${NC}"
        else
            echo -e "  ${BOLD}Type:${NC}       ${GREEN}feature/work${NC}"
        fi
        if in_rebase_state || in_merge_state; then
            echo -e "  ${BOLD}Conflict:${NC}   ${YELLOW}REBASE/MERGE IN PROGRESS — resolve conflicts (option 9)${NC}"
        fi
        show_status_summary

        echo -e "    ${BOLD}1)${NC} Create Feature Branch"
        echo -e "    ${BOLD}2)${NC} Stage, Commit & Push"
        echo -e "    ${BOLD}3)${NC} Sync Feature Branch (rebase onto base)"
        echo -e "    ${BOLD}4)${NC} Merge Feature → Base (fast-forward, linear history)"
        echo -e "    ${BOLD}5)${NC} Stash Management"
        echo -e "    ${BOLD}6)${NC} View Log / History"
        echo -e "    ${BOLD}7)${NC} Setup Linear History Config (one-time)"
        echo -e "    ${BOLD}8)${NC} Branch Cleanup"
        echo -e "    ${BOLD}9)${NC} Resolve Rebase/Merge Conflicts"
        echo -e "    ${BOLD}10)${NC} Cherry-Pick Specific Files from Branch"
        echo -e "    ${BOLD}11)${NC} Dev → Release: Cherry-Pick Files & Raise PR"
        echo -e "    ${BOLD}q)${NC} Quit"
        echo ""

        prompt "Selection: "
        read -r choice || true
        choice=$(echo "${choice:-}" | tr -d '\r')

        case "$choice" in
            1) create_feature_branch ;;
            2) commit_and_push ;;
            3) sync_feature_branch ;;
            4) merge_feature_to_base ;;
            5) stash_management ;;
            6) view_log ;;
            7) setup_linear_history ;;
            8) branch_cleanup ;;
            9) resolve_conflicts ;;
            10) cherry_pick_files ;;
            11) dev_to_release_pr ;;
            q|Q) echo ""; info "Goodbye!"; echo ""; exit 0 ;;
            *) error "Invalid selection. Try again." ;;
        esac

        echo ""
        prompt "Press Enter to return to main menu..."
        read -r _ || true
    done
}

# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
check_git_repo
main_menu
