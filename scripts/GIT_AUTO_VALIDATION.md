# Git Auto Script — Production Validation Checklist

## Workflow-by-Workflow Validation

### 1. Create Feature Branch
- [x] Base branch list: `git branch -a` with sed for any remote (not just origin)
- [x] main/development/release/* detected from local + remote
- [x] Input normalization: selection, type_selection strip `\r` (Windows CRLF)
- [x] Branch name sanitization: lowercase, hyphens, alphanumeric
- [x] Stash before switch, restore after
- [x] Base stored in config for Sync
- [x] Push with upstream tracking optional

### 2. Stage, Commit & Push
- [x] Single `git status --porcelain` for status (no redundant git calls)
- [x] Options 1/2/3; direct file numbers (e.g. 1,4) at first prompt
- [x] Input normalization: stage_choice, commit_type_choice, add_body strip `\r`
- [x] _interactive_file_staging: parses porcelain, supports ranges (1-5), "a" for all
- [x] Rename handling: path "old -> new" uses new path for git add
- [x] Verify staged before commit; confirm before push
- [x] Pre-push: fetch, check behind, warn if remote ahead

### 3. Sync Feature Branch
- [x] Protected branch check (main, development, release/*)
- [x] Auto-stash if uncommitted changes
- [x] Stored base or interactive selection
- [x] Rebase onto base, force-push with lease
- [x] Restore stash if auto-stashed
- [x] Input normalization: selection strip `\r`

### 4. Merge Feature → Base
- [x] Protected branch check
- [x] Empty base_branches check (returns error)
- [x] Rebase first, then ff-merge
- [x] Optional push and branch cleanup
- [x] Input normalization: selection strip `\r`

### 5. Stash Management
- [x] 6 options: stash, list, apply, pop, drop, clear
- [x] Input normalization: choice strip `\r`

### 6. View Log
- [x] 3 options: 15 commits, 30 graph, commits not in base
- [x] Input normalization: choice strip `\r`

### 7. Setup Linear History
- [x] Config: pull.rebase, rebase.autoStash, merge.ff, fetch.prune, push.default
- [x] Uses confirm() which strips `\r`

### 8. Branch Cleanup
- [x] Merged branches from development/main
- [x] Skips current branch when deleting
- [x] Option 2: delete local + remote
- [x] Input normalization: choice strip `\r`

### 9. Resolve Conflicts
- [x] Options: stage all, stage specific, diff --check, abort
- [x] Stage specific: supports "1,4" or "1 4", strips `\r`
- [x] Continue rebase prompt strips `\r`
- [x] Input normalization: choice strip `\r`

## Cross-Cutting
- [x] GIT_DIR, TOP_LEVEL, REPO_NAME cached at startup
- [x] confirm() strips `\r`
- [x] Main menu choice strips `\r`
- [x] set -euo pipefail (fail-fast)
