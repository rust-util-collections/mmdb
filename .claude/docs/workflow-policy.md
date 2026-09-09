# Reliability Review and Atomic Commit Policy

Shared workflow rules for `/x-review`, `/x-commit`, `/x-fix`, `/x-overhaul`. Skills must
not weaken it. See also `pragmatic-engineering.md`.

**Hard rules:** user-invoked only · local commits only (never push) · no history
rewrite · one independent issue per commit.

## Task framing and reporting

These workflows review the reliability of a local embedded key-value storage
engine. Include that context in agent handoffs. Ask agents to return neutral,
concrete engineering descriptions: operation/input, relevant state or ordering,
expected behavior, observed behavior, existing checks, and a regression test.

Use the same language in progress updates, findings, and commit descriptions.
For example, describe an "invalid stored length", "interrupted WAL append",
"recovery omits a completed record", or "reverse seek skips an available key".
Describe the actual local operation and result. Do not invent actors, intentions,
or external targets for an ordinary storage-engine test.

Keep technical meaning and evidence intact. Exact API names, Rust `unsafe`,
`ErrorKind::Corruption`, checksum details, and data-loss severity must remain
accurate; neutral wording must never conceal a failure or weaken validation.
This applies equally to task titles, agent prompts, test descriptions, tool-call
summaries, registry entries, and final reports. Use the user's language for
conversation; preserve identifiers and code verbatim.

### Review-agent handoffs

Prefer a fresh, narrowly scoped agent context (`fork_turns="none"` when that
option is available). Supply the repository path, assigned files, applicable
shared guides, current baseline, and the concrete review question. Summarize
relevant verified facts instead of forwarding the entire conversation. Use the
handoff and response templates in `review-core.md`; agents read those guides
before reviewing their assigned files.

A new session derives scope and status from the current checkout and
`docs/audit.md`, not from another session's completion claims. A failed or
incomplete agent run does not establish coverage: record the remaining work
and complete it locally or report the actual limitation. Do not speculate
about a tool/service failure or reproduce unrelated service notices in handoffs.

## 1. Preflight

Before mutate/commit:

1. Record `git status --short`, branch, `HEAD`.
2. Separate staged / unstaged / untracked baseline.
3. Stop on merge/rebase/cherry-pick or detached HEAD unless the user resolves it.
4. Define this invocation’s owned files/hunks; baseline stays with its author.
5. **Commit workflows:** freeze owned paths (+ planned units) before review edits.
   Stage only freeze set + this invocation’s fix/format paths — never paths that
   appeared later from concurrent work.

Dirty tree OK; clear ownership required.

## 2. Preserve existing work

- No `stash` / `clean` / `checkout --` / `restore` / destructive `reset` to fake a clean tree.
- Never touch unrelated baseline (revert, overwrite, stage, commit).
- If a needed fix overlaps baseline and cannot be separated safely → stop and report.
- Review agents read-only. Parallelism: investigation/validation only. Edits and
  commits on one tree: sequential.

## 3. Atomic commit units

One issue / root cause / behavior change → one commit.

- Bundle only its tests, public docs, and audit update.
- Multiple symptoms only if same root cause.
- No drive-by cleanup, format churn, or refactors.
- Stage exact paths/hunks (`git add -A` forbidden). Inspect `git diff --cached` before every commit.
- New commits only — no amend, rebase, history rewrite, or force-push. No remote push.

## 4. Validation and failure

- Smallest relevant checks per unit.
- Dirty-tree validation covers everything present. If other units can interfere,
  validate `HEAD` + only the candidate in a disposable worktree (no stash);
  remove it after.
- Full-repo gate once after the last behavior change.
- Unit-caused failure → fix before commit. Pre-existing failure → report with evidence, never claim success.
- Same failure repeats with no progress → stop and report.

## 5. Finding dispositions

| state | meaning |
|-------|---------|
| Open | confirmed, actionable |
| Won't Fix | confirmed; a complete fix currently has disproportionate cost or regression risk |
| Rejected | material concern ruled out by code or test evidence (not a severity). Skip routine noise. |

Evidence only — no dates or “last reviewed” markers.
