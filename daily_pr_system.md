# Daily Pull Request System — Standard Operating Procedure

## Purpose

Ensure every team member commits and submits a pull request (PR) to the `development` branch at the end of each working day. This practice provides visibility into progress, reduces integration risk, and enables early feedback.

All git operations are handled through the team's shared automation script: **`scripts/git_auto.sh`**.

---

## Scope

Applies to all engineers contributing to this repository.

---

## One-Time Setup (Do This First)

Run this once when you first clone the repository.

```bash
chmod +x scripts/git_auto.sh
```

Then run the script and select **option 7 — Setup Linear History Config**:

```bash
./scripts/git_auto.sh
# → Select 7) Setup Linear History Config (one-time)
```

This configures your local git to:
- Pull with rebase (no merge commits)
- Auto-stash on rebase
- Fast-forward only merges
- Auto-prune stale remote branches

---

## How to Run the Script

From the repo root:

```bash
./scripts/git_auto.sh
```

The interactive menu will appear with the current branch, file status summary, and all available options.

---

## Script Menu Reference

| Option | Name | When to Use |
|--------|------|-------------|
| **1** | Create Feature Branch | Start of a new task/ticket |
| **2** | Stage, Commit & Push | During the day and end-of-day push |
| **3** | Sync Feature Branch | Start of day, or before merging |
| **4** | Merge Feature → Base | When a task is fully complete |
| **5** | Stash Management | Temporarily save uncommitted work |
| **6** | View Log / History | Review commit history |
| **7** | Setup Linear History | One-time repo config (first run only) |
| **8** | Branch Cleanup | After merges, to delete stale branches |
| **9** | Resolve Conflicts | If a rebase conflict is detected |

---

## Branch Naming Convention

The script enforces this automatically when using option 1.

```
<type>/<ticket-id>-<short-description>
```

| Type | Use Case |
|------|----------|
| `feature/` | New functionality |
| `bugfix/` | Bug fixes |
| `hotfix/` | Production hotfixes |
| Custom | Any other prefix (script prompts you) |

**Example:** `feature/ETL-142-add-revenue-report`

---

## Daily Workflow

### Start of Day

**If starting a new task:**

```bash
./scripts/git_auto.sh
# → Select 1) Create Feature Branch
# → Select base: development
# → Select type: feature / bugfix / hotfix / custom
# → Enter branch name (e.g. ETL-123-fix-null-dates)
```

The script will:
- Pull the latest `development`
- Create your branch from it
- Push it to remote with upstream tracking set

**If resuming yesterday's work:**

```bash
./scripts/git_auto.sh
# → Select 3) Sync Feature Branch (rebase onto base)
```

This rebases your branch onto the latest `development` so you start with the freshest code and no drift.

---

### During the Day

Commit regularly — small, focused commits are easier to review.

```bash
./scripts/git_auto.sh
# → Select 2) Stage, Commit & Push
```

The script will walk you through:
1. **File staging** — stage all, select specific files by number, or use already-staged files
2. **Commit type** — pick from `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, or custom
3. **Scope** — optional ticket reference (e.g. `ETL-123`)
4. **Description** — short summary of the change
5. **Optional body** — more detail if needed
6. **Push** — confirms remote is reachable, checks if you are behind, then pushes

---

### End of Day — Mandatory Steps

| Step | Action | Script Option |
|------|--------|---------------|
| 1 | Commit all meaningful work | Option **2** |
| 2 | Push your branch to remote | Included in option **2** |
| 3 | Open a PR to `development` | GitHub / `gh` CLI (see below) |
| 4 | Notify the team | Post PR link in team channel |

**If your work is incomplete**, still push and open a **Draft PR**:

```bash
gh pr create --draft --base development \
  --title "WIP: <description>" \
  --body "End-of-day checkpoint. Work continues tomorrow."
```

Never leave code only on your local machine overnight.

---

## Pull Request Template

When opening a PR (draft or ready), use this body:

```markdown
## Summary
- Brief description of what changed and why.

## Changes
- [ ] List of specific changes made today

## Status
- [ ] Complete and ready for review
- [ ] Work in progress (Draft PR — daily checkpoint)

## Testing
- [ ] Ran unit tests locally (`pytest`)
- [ ] Ran linting (`flake8` / `ruff`)
- [ ] Verified against sample data (if applicable)

## Notes for Reviewer
- Any context, blockers, or areas needing attention.
```

---

## Handling Common Scenarios

### Merge Conflicts

If a conflict is detected during a sync or merge, the script will alert you automatically. Select the conflict resolution menu:

```bash
./scripts/git_auto.sh
# → Select 9) Resolve Rebase/Merge Conflicts
```

The script will:
- List all conflicted files
- Guide you through staging resolved files
- Continue or abort the rebase/merge interactively

Never manually force-push to resolve conflicts without the team lead's approval.

---

### Nothing to Commit Today

If no code was written (meetings, planning, research, code review):
- No PR is needed.
- Post a brief status update in the team channel.
- No empty commits.

---

### Urgent Hotfix

```bash
./scripts/git_auto.sh
# → Select 1) Create Feature Branch
# → Select base: development (or main if critical)
# → Select type: hotfix
# → Enter name: ETL-XXX-short-description
```

After committing:
```bash
gh pr create --base development --label "urgent" \
  --title "hotfix: <description>"
```

Notify the team lead immediately.

---

### Multi-Day Feature

- **Day 1**: Create branch (option 1), commit progress (option 2), open Draft PR.
- **Day 2+**: Sync branch (option 3) at start of day, commit (option 2) at end of day. The existing Draft PR updates automatically with each push.
- **Final day**: Mark PR as "Ready for Review" on GitHub and request reviewers.

---

### When Your Branch is Behind Remote

The script detects this automatically before pushing (option 2) and warns you. If you are behind:

```bash
./scripts/git_auto.sh
# → Select 3) Sync Feature Branch
```

Then re-run option 2 to push.

---

### Stashing Work Temporarily

If you need to switch context mid-task without committing:

```bash
./scripts/git_auto.sh
# → Select 5) Stash Management
# → Select 1) Stash current changes
```

Restore with option 4 (pop) when you return.

---

### After a Task is Merged

Clean up old branches once your PR is merged:

```bash
./scripts/git_auto.sh
# → Select 8) Branch Cleanup
# → Select 2) Delete merged feature branches (local + remote)
```

---

## Rules and Safeguards

### Do

- Run `git_auto.sh` for all git operations — it enforces linear history and safe push behaviour.
- Push at least once per working day, even if the work is partial.
- Open Draft PRs for incomplete work.
- Sync (option 3) before pushing if you have been on the branch for more than one day.
- Keep PRs focused — one task per branch.

### Do Not

- **Never** push directly to `development` or `main` — branch protection rules block this.
- **Never** use `--force` push unless the team lead explicitly approves it. The script uses `--force-with-lease` (safer) when force-pushing rebased branches.
- **Never** commit secrets, credentials, `.env` files, or binary data files.
- **Never** merge your own PR without at least one approval (except Draft PR daily checkpoints).
- **Never** bypass the script to run raw `git merge` on protected branches — this breaks linear history.

---

## Review Expectations

| Role | Responsibility | SLA |
|------|----------------|-----|
| Author | Open PR by end of day, respond to feedback next morning | Same day |
| Reviewer | Review assigned PRs | Within 24 hrs |
| Lead | Monitor for missing daily PRs, stale branches | Daily |

---

## Monitoring and Compliance

The team lead checks daily:

1. Every active team member has at least one PR or Draft PR opened/updated that day.
2. No branches with zero activity for 3+ days (flagged for review).
3. No direct pushes to `development` (branch protection enforces this).

### Recommended Branch Protection Rules (GitHub)

- Require pull request before merging to `development`
- Require at least 1 approval
- Require status checks to pass (CI / linting / tests)
- Disallow force pushes
- Disallow branch deletion for `development` and `main`

---

## End-of-Day Checklist

```
[ ] Run git_auto.sh → option 2 (Stage, Commit & Push)
[ ] All changes committed with clear conventional messages
[ ] Branch pushed to remote
[ ] PR opened (or Draft PR if work is incomplete)
[ ] PR description filled out
[ ] No secrets or sensitive data in the commits
[ ] Merge conflicts resolved if any (option 9)
[ ] Team notified via channel with PR link
```

---

## Escalation

If you are blocked and cannot commit or push:

1. Notify the team lead immediately with the reason.
2. Document the blocker in the team channel.
3. Resume as soon as the blocker is resolved — do not let code sit locally for more than one full day.

---

*This SOP is effective immediately. For script issues or improvements, raise a PR against `scripts/git_auto.sh`.*
