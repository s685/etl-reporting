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
    read -r response
    [[ "$response" =~ ^[Yy]$ ]]
}

# Check if inside a git repository
check_git_repo() {
    if ! git rev-parse --is-inside-work-tree &>/dev/null; then
        error "Not inside a Git repository."
        error "Please navigate to a Git repository and try again."
        exit 1
    fi
    info "Git repository: $(basename "$(git rev-parse --show-toplevel)")"
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
    [[ -d "$(git rev-parse --git-dir)/rebase-merge" ]] || [[ -d "$(git rev-parse --git-dir)/rebase-apply" ]]
}

# Check if in merge conflict state
in_merge_state() {
    [[ -f "$(git rev-parse --git-dir)/MERGE_HEAD" ]]
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

# Display current repo status summary (single git call for speed)
show_status_summary() {
    local branch staged modified untracked
    branch=$(current_branch)
    read -r staged modified untracked <<< "$(git status --porcelain 2>/dev/null | awk '
        /^\?\?/ { u++ }
        /^[MADRC]/ { s++ }
        /^.[MD ]|^ [MD]/ { if ($0 !~ /^\?\?/) m++ }
        END { print s+0, m+0, u+0 }
    ')"

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

    # Build list of available base branches
    local base_branches=()
    local display_names=()

    if branch_exists_local "main" || branch_exists_remote "main"; then
        base_branches+=("main")
        display_names+=("main")
    fi
    if branch_exists_local "development" || branch_exists_remote "development"; then
        base_branches+=("development")
        display_names+=("development")
    fi

    # Add release branches (local and remote)
    while IFS= read -r rb; do
        [[ -n "$rb" ]] && base_branches+=("$rb") && display_names+=("$rb")
    done < <(git branch -a --list '*release/*' 2>/dev/null \
             | sed 's|remotes/origin/||;s|^[* ]*||' | sort -u)

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
    read -r selection

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
    read -r type_selection

    local prefix
    case "$type_selection" in
        1) prefix="$FEATURE_PREFIX" ;;
        2) prefix="$BUGFIX_PREFIX" ;;
        3) prefix="$HOTFIX_PREFIX" ;;
        4)
            prompt "Enter custom prefix (e.g. 'refactor/'): "
            read -r prefix
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
    read -r branch_name

    # Sanitize branch name
    branch_name=$(echo "$branch_name" | tr '[:upper:]' '[:lower:]' | tr ' ' '-' | sed 's/[^a-z0-9\-]//g')

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

    # ── Show current status ─────────────────────────────────────────────
    show_status_summary

    # Check if there's anything to commit
    local modified_files untracked_files staged_files
    staged_files=$(git diff --cached --name-only 2>/dev/null)
    modified_files=$(git diff --name-only 2>/dev/null)
    untracked_files=$(git ls-files --others --exclude-standard 2>/dev/null)

    if [[ -z "$modified_files" && -z "$untracked_files" && -z "$staged_files" ]]; then
        warn "Nothing to commit. Working tree is clean."
        return 0
    fi

    # ── File Selection ──────────────────────────────────────────────────
    print_section "Select Files to Stage"
    echo ""
    echo -e "    ${BOLD}1)${NC} Stage ALL changed and untracked files"
    echo -e "    ${BOLD}2)${NC} Select SPECIFIC files interactively"
    echo -e "    ${BOLD}3)${NC} Stage only already-staged files (skip staging)"
    echo ""

    prompt "Selection [1-3]: "
    read -r stage_choice

    case "$stage_choice" in
        1)
            git add -A
            info "All files staged."
            ;;
        2)
            _interactive_file_staging
            ;;
        3)
            if [[ -z "$staged_files" ]]; then
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
    read -r commit_type_choice

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
            read -r commit_prefix
            ;;
        *)
            error "Invalid selection."
            return 1
            ;;
    esac

    prompt "Scope (optional, e.g. JIRA-1234 — press Enter to skip): "
    read -r commit_scope

    prompt "Short description: "
    read -r commit_desc

    if [[ -z "$commit_desc" ]]; then
        error "Commit description cannot be empty."
        return 1
    fi

    # Build commit message
    local commit_msg
    if [[ -n "$commit_scope" ]]; then
        commit_msg="${commit_prefix}(${commit_scope}): ${commit_desc}"
    else
        commit_msg="${commit_prefix}: ${commit_desc}"
    fi

    # Optional body
    prompt "Add detailed body? [y/N]: "
    read -r add_body
    local commit_body=""
    if [[ "$add_body" =~ ^[Yy]$ ]]; then
        echo -e "    ${DIM}Enter commit body (press Ctrl+D or empty line to finish):${NC}"
        local body_lines=()
        while IFS= read -r line; do
            [[ -z "$line" ]] && break
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
_interactive_file_staging() {
    # Combine all changed/untracked files
    local all_files=()
    local file_statuses=()

    while IFS= read -r file; do
        [[ -n "$file" ]] && all_files+=("$file") && file_statuses+=("modified")
    done < <(git diff --name-only 2>/dev/null)

    while IFS= read -r file; do
        [[ -n "$file" ]] && all_files+=("$file") && file_statuses+=("untracked")
    done < <(git ls-files --others --exclude-standard 2>/dev/null)

    # Also show already-staged files
    while IFS= read -r file; do
        # Avoid duplicates
        local found=false
        for f in "${all_files[@]}"; do
            [[ "$f" == "$file" ]] && found=true && break
        done
        if ! $found && [[ -n "$file" ]]; then
            all_files+=("$file")
            file_statuses+=("staged")
        fi
    done < <(git diff --cached --name-only 2>/dev/null)

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
    prompt "Files to stage: "
    read -r file_selection

    if [[ "$file_selection" =~ ^[Aa]$ ]]; then
        git add -A
        info "All files staged."
        return
    fi

    # Parse selection (supports: 1,3,5-8)
    local selected_indices=()
    IFS=',' read -ra parts <<< "$file_selection"
    for part in "${parts[@]}"; do
        part=$(echo "$part" | tr -d ' ')
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
            ((staged_count++))
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
        read -r selection

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

    echo ""
    for i in "${!base_branches[@]}"; do
        echo -e "    ${BOLD}$((i+1)))${NC} ${base_branches[$i]}"
    done
    echo ""

    prompt "Select target branch [1-${#base_branches[@]}]: "
    read -r selection

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
    read -r choice

    case "$choice" in
        1)
            prompt "Stash message (optional): "
            read -r stash_msg
            if [[ -n "$stash_msg" ]]; then
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
            git stash list
            echo ""
            prompt "Enter stash index to drop (e.g. 0): "
            read -r stash_idx
            git stash drop "stash@{$stash_idx}" && info "Stash dropped." || error "Invalid stash index."
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
    read -r choice

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
            read -r base
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
    read -r choice

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
                while IFS= read -r b; do
                    [[ -z "$b" ]] && continue
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
        read -r choice
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
                    [[ -n "$f" ]] && files+=("$f") && echo -e "    ${BOLD}$i)${NC} $f" && ((i++))
                done <<< "$conflicted"
                echo ""
                prompt "Enter file number(s) to stage (e.g. 1 3): "
                read -r nums
                for n in $nums; do
                    if [[ "$n" =~ ^[0-9]+$ ]] && (( n >= 1 && n <= ${#files[@]} )); then
                        git add "${files[$((n-1))]}"
                        info "Staged: ${files[$((n-1))]}"
                    fi
                done
                if in_rebase_state; then
                    prompt "Continue rebase now? [y/N]: "
                    read -r cont
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

# ─────────────────────────────────────────────────────────────────────────────
# MAIN MENU
# ─────────────────────────────────────────────────────────────────────────────
main_menu() {
    while true; do
        print_header "Git Automation — Main Menu"

        local branch
        branch=$(current_branch)
        echo -e "  ${BOLD}Repository:${NC} $(basename "$(git rev-parse --show-toplevel)" 2>/dev/null)"
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
        echo -e "    ${BOLD}q)${NC} Quit"
        echo ""

        prompt "Selection: "
        read -r choice

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
            q|Q) echo ""; info "Goodbye!"; echo ""; exit 0 ;;
            *) error "Invalid selection. Try again." ;;
        esac

        echo ""
        prompt "Press Enter to return to main menu..."
        read -r
    done
}

# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
check_git_repo
main_menu
