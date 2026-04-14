# Self-Reviewing Commit for MMDB

You are performing a self-reviewing commit: review all uncommitted changes, fix every issue found, format, and commit.

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` — bug pattern reference.
2. Read `.claude/docs/review-core.md` — review methodology.
3. Read `.claude/docs/false-positive-guide.md` — consult before reporting any finding.

## Execution Protocol

### Task 1: Deep Self-Review

1. Run `git diff HEAD` to collect all uncommitted changes.
2. If the diff is empty, report "nothing to commit" and stop.
3. Identify ALL affected subsystems by mapping changed files:
   - `src/db.rs` → write-path, read-path
   - `src/memtable/` → memtable
   - `src/wal/` → WAL
   - `src/sst/` → SST
   - `src/iterator/` → iterator
   - `src/compaction/` → compaction
   - `src/manifest/` → manifest
   - `src/cache/` → cache
   - `src/types.rs` → data-encoding
   - `src/options.rs` → configuration
4. For EACH affected subsystem, read the corresponding pattern file from `.claude/docs/patterns/`.
5. Perform the full regression analysis from review-core.md:
   - **Classify** each change (concurrency, resource lifecycle, unsafe, control flow, encoding, etc.)
   - **Invariant check** — verify all invariants from review-core.md
   - **Boundary conditions** — check edge cases
   - **Failure paths** — analyze error handling
   - **Concurrency** — verify lock ordering, shared state safety
6. Check cross-cutting concerns:
   - **Crash safety** — WAL durability, fsync ordering
   - **Performance** — hot path overhead
   - **API compatibility** — observable behavior changes
7. Enforce code style rules:
   - No `#[allow(...)]` — fix warnings at the source
   - Prefer imports over inline paths (3+ uses)
   - Grouped imports with common prefixes
   - Doc-code alignment for public API changes
8. Audit any added/modified `unsafe` blocks.
9. Cross-reference every finding with `false-positive-guide.md` — only retain findings with **concrete evidence**.

### Task 2: Fix All Findings

For EVERY finding from Task 1 (CRITICAL, HIGH, MEDIUM, or LOW):

1. Fix the issue completely — no TODOs, no "fix later", no partial fixes.
2. After all fixes are applied, re-run `git diff HEAD` and repeat Task 1 analysis on the new diff.
3. If new findings emerge from the fixes, fix those too. Iterate until clean.
4. Report the final list of fixes applied.

### Task 3: Format

1. Run `make fmt` to apply code formatting.

### Task 4: Bump Patch Version — MANDATORY

**You MUST complete every step below before proceeding to Task 5. Do NOT skip this task.**

1. Run `git diff HEAD --name-only` — if it lists any `.rs` file, a version bump is required. Skip this task ONLY if every changed file is a non-code file (`.md`, `.toml` version-only, etc.).
2. Read `Cargo.toml` to get the current `version = "X.Y.Z"` line.
3. Compute `NEW = X.Y.(Z+1)` (e.g., `3.2.0` → `3.2.1`).
4. Update `Cargo.toml` — `version = "NEW"`.
5. **Verify**: grep the file for the NEW version string — it must match. If mismatch, fix it before continuing.

### Task 5: Commit

1. Run `git diff HEAD --stat` and `git log -5 --oneline` to understand scope and commit style.
2. Draft a commit message:
   - Follow the repo's existing commit message style (type prefix: `fix:`, `feat:`, `style:`, `refactor:`, etc.)
   - Summarize the "why" not the "what" — keep it concise (1-2 sentences for the subject)
   - Add a body with key details if the change spans multiple subsystems
3. Stage the relevant files with `git add` (specific files, not `-A`).
4. Commit using a HEREDOC — **do NOT include any co-author line**:

```
git commit -m "$(cat <<'EOF'
<commit message here>
EOF
)"
```

5. Run `git status` to verify the commit succeeded.

## Output Format

```
## Self-Review Commit Summary

**Reviewed**: <number of files changed>
**Subsystems**: <list>
**Findings**: <N found, N fixed> (or "0 — clean")
**Commit**: <short hash> <subject line>
```
