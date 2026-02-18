# Daily Pull Request System — Standard Operating Procedure

## Purpose

Ensure every team member commits and submits a pull request (PR) to the `development` branch at the end of each working day. This practice provides visibility into progress, reduces integration risk, and enables early feedback.

---

## Scope

Applies to all engineers contributing to this repository.

---

## Branch Naming Convention

```
<type>/<ticket-id>-<short-description>
```

| Type       | Use Case                          |
|------------|-----------------------------------|
| `feature/` | New functionality                 |
| `fix/`     | Bug fixes                         |
| `chore/`   | Maintenance, config, dependencies |
| `refactor/`| Code restructuring                |
| `docs/`    | Documentation-only changes        |

**Example:** `feature/ETL-142-add-revenue-report`

---

## Daily Workflow

### 1. Start of Day

```bash
# Pull latest development branch
git checkout development
git pull origin development

# Create or switch to your feature branch
git checkout -b feature/ETL-XXX-description
# or if resuming work:
git checkout feature/ETL-XXX-description
git merge development
```

### 2. During the Day

- Commit frequently with clear messages.
- Each commit should be small and focused on a single logical change.

```bash
git add <specific-files>
git commit -m "feat(etl): add null check to revenue pipeline"
```

### 3. End of Day — Mandatory Steps

| Step | Action | Command |
|------|--------|---------|
| 1 | Save all work | Commit all meaningful changes |
| 2 | Push your branch | `git push -u origin <your-branch>` |
| 3 | Open a PR to `development` | Use the PR template below |
| 4 | Notify the team | Post PR link in the team channel |

**If your work is incomplete**, still open the PR as a **Draft PR**:

```bash
gh pr create --draft --base development --title "WIP: <description>" --body "End-of-day checkpoint. Work continues tomorrow."
```

---

## Commit Message Format

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short summary>

<optional body — explain WHY, not WHAT>
```

**Types:** `feat`, `fix`, `refactor`, `chore`, `docs`, `test`, `ci`

**Examples:**
```
feat(reports): add monthly aggregation query
fix(loader): handle null dates in staging table
chore(deps): upgrade snowflake-connector to 3.5.0
test(transforms): add coverage for currency conversion
```

---

## Pull Request Template

When opening a PR, use this structure:

```markdown
## Summary
- Brief description of what changed and why.

## Changes
- [ ] List of specific changes made today

## Status
- [ ] Complete and ready for review
- [ ] Work in progress (Draft PR)

## Testing
- [ ] Ran unit tests locally (`pytest`)
- [ ] Ran linting (`flake8` / `ruff`)
- [ ] Verified against sample data (if applicable)

## Notes for Reviewer
- Any context, blockers, or areas needing attention.
```

---

## Rules and Safeguards

### Do

- Push at least once per working day, even if the work is partial.
- Open Draft PRs for incomplete work — never leave code only on your local machine.
- Resolve merge conflicts on your feature branch, not on `development`.
- Keep PRs focused. If a task spans multiple days, each daily PR should be reviewable on its own.
- Pull from `development` before pushing to minimize conflicts.

### Do Not

- **Never** push directly to `development` or `main`.
- **Never** use `--force` push unless explicitly approved by the lead.
- **Never** commit secrets, credentials, `.env` files, or large data files.
- **Never** merge your own PR without at least one approval (unless it is a Draft PR checkpoint).
- **Never** skip the daily PR — if you wrote code today, it goes up today.

---

## Handling Common Scenarios

### Merge Conflicts

```bash
git checkout your-branch
git pull origin development
# Resolve conflicts in your editor
git add <resolved-files>
git commit -m "chore: resolve merge conflicts with development"
git push
```

### Nothing to Commit

If no code was written (meetings, planning, research), post a brief status update in the team channel instead. No empty PRs needed.

### Urgent Hotfix

```bash
git checkout development
git pull origin development
git checkout -b fix/ETL-XXX-hotfix-description
# Make fix, test, commit
git push -u origin fix/ETL-XXX-hotfix-description
# Open PR with "URGENT" label
gh pr create --base development --label "urgent"
```

### Multi-Day Feature

- Day 1: Open Draft PR with initial progress.
- Day 2+: Push new commits to the same branch. The PR updates automatically.
- Final Day: Mark PR as "Ready for Review" and request reviewers.

---

## Review Expectations

| Role       | Responsibility                                      | SLA          |
|------------|-----------------------------------------------------|--------------|
| Author     | Open PR by end of day, respond to feedback next day  | Same day     |
| Reviewer   | Review assigned PRs                                  | Within 24 hrs|
| Lead       | Monitor dashboard for missing daily PRs              | Daily        |

---

## Monitoring and Compliance

The team lead will check the following daily:

1. **Every active team member** has at least one PR or Draft PR opened/updated that day.
2. **No stale branches** — branches with no activity for 3+ days will be flagged.
3. **No direct pushes** to `development` — branch protection rules enforce this.

### Recommended Branch Protection Rules (GitHub)

- Require pull request before merging to `development`
- Require at least 1 approval
- Require status checks to pass (CI/linting/tests)
- Disallow force pushes
- Disallow branch deletion for `development` and `main`

---

## Quick Reference Checklist (End of Day)

```
[ ] All changes committed with clear messages
[ ] Branch pushed to remote
[ ] PR opened (or Draft PR if work is incomplete)
[ ] PR description filled out using the template
[ ] No secrets or sensitive data in the commits
[ ] Merge conflicts resolved (if any)
[ ] Team notified via channel
```

---

## Escalation

If you are blocked and cannot commit or open a PR:

1. Notify the team lead immediately with the reason.
2. Document the blocker in the team channel.
3. Resume the process as soon as the blocker is resolved.

---

*This SOP is effective immediately. Questions or suggested improvements should be raised with the team lead.*
