# Reliability Review and Atomic Commit Policy

Shared workflow rules for `/x-review`, `/x-commit`, `/x-fix`, `/x-overhaul`. Skills must
not weaken it. See also `pragmatic-engineering.md`.

**Hard rules:** user-invoked only · local commits only (never push) · no history
rewrite · one independent issue per commit · version bump and tag only in
`/x-overhaul`, once.

## Scope

`/x-review` and `/x-overhaul` take one optional scope and nothing else. That text
is the user argument (`$ARGUMENTS` where the harness substitutes it; otherwise
the words after the command). Reject unknown input. Do not guess. Do not accept
`--fix`.

| Input | Scope |
|-------|-------|
| *(empty)* | Latest commit |
| `N` | Last N commits, N a positive integer |
| `staged` | `git diff --cached` |
| `worktree` | Staged + unstaged + untracked |
| `all` | Full repository |
| `<rev>` | One commit. `git rev-parse --verify <rev>` must succeed |
| `<rev1>..<rev2>` | Two-dot range. Verify both revs. Not three-dot |

Historical scope: only defects still present at HEAD. Empty is never the full
repository. Full repository requires `all`.

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
- Never touch unrelated baseline (revert, overwrite, stage, commit, or format).
- If a needed fix overlaps baseline and cannot be separated safely → stop and report.
- Review agents read-only. Parallelism: investigation/validation only. Edits and
  commits on one tree: sequential.
- Do not create a worktree to hide a dirty tree.

## 3. Atomic commit units

One issue / root cause / behavior change → one commit.

- Bundle only its tests, public docs, and audit update.
- Multiple symptoms only if same root cause.
- No drive-by cleanup, format churn, or refactors.
- Stage exact paths/hunks (`git add -A` forbidden). Inspect `git diff --cached` before every commit.
- New commits only — no amend, rebase, history rewrite, or force-push. No remote push.
- Do not commit a registry inventory ahead of the fixes. Each fix commit carries
  that finding's audit hunk. Remaining registry-only edits are one later commit.

## 4. Validation and failure

- Smallest relevant checks per unit. Do not run `tests/scale_profile.rs` or `--release`.
- Dirty-tree validation covers everything present. If unrelated files can change
  the result and the test command cannot exclude them, stop and report.
- Final gate once after the last behavior change: the cargo commands in `commit-protocol.md`.
- Agent-chosen checks use `cargo`, not `make`. If the user names a `make` target, run that target.
- Unit-caused failure → fix before commit. Pre-existing failure → report with evidence, never claim success.
- Same failure repeats with no progress → stop and report.
- A gate failure after a commit → new commit, then re-run. Do not amend.

## 5. Finding dispositions

| state | meaning |
|-------|---------|
| Open | confirmed, actionable |
| Won't Fix | confirmed defect; a complete fix has disproportionate cost or regression risk. Fix-time only. Not a feature request or documented contract. |
| Rejected | material claim ruled out by code or tests (not a severity). Skip routine noise. |
| omit | feature request, documented contract, style |

`/x-review` records Open and Rejected. It does not add Won't Fix. Do not
reclassify an entry to empty Open. Forms and severities: `review-core.md`.
Evidence only — no dates or “last reviewed” markers.
