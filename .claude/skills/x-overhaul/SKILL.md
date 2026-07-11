---
name: x-overhaul
description: Audit the full MMDB repository, resolve every finding safely, and create atomic local commits. Use only when the user explicitly invokes /x-overhaul.
disable-model-invocation: true
---

# Full MMDB Audit-Fix-Commit Pipeline

Audit the complete repository, give every confirmed finding an explicit
disposition, fix actionable findings, and create local commits. Never push.

## Setup

1. Read `.claude/docs/workflow-policy.md` and run its preflight.
2. Read `.claude/skills/x-review/SKILL.md`,
   `.claude/skills/x-fix/SKILL.md`, and
   `.claude/docs/commit-protocol.md`.
3. Record the commit-protocol invocation ledger before any mutation.

## Phase 1: Full review

Follow `/x-review all` without `--fix`:

1. Build a tracked-file coverage ledger for source, tests, benches, build/CI
   configuration, public docs, and `.claude/`.
2. Use read-only agents with disjoint subsystem ownership; every Rust source
   file must be accounted for exactly once in the depth pass.
3. Run focused cross-subsystem and completeness passes only for gaps the depth
   pass cannot cover.
4. Critically verify and deduplicate candidates.
5. Update `docs/audit.md`, including full re-evaluation of all existing `Open`,
   `Won't Fix`, and `Rejected` entries. Add no timestamps.
6. If the registry changed, validate and commit that review inventory as one
   documentation-only unit before fixes begin. This snapshot may list multiple
   findings; it is the review result, not a batched fix commit.

## Phase 2: Resolve findings

Follow the complete `/x-fix` protocol:

1. Process findings sequentially by severity.
2. Resolve each finding safely and completely, or record a justified
   `Won't Fix`/`Rejected` disposition.
3. Enforce one independent finding/root cause per validated commit.
4. Keep all mutating work sequential. Parallel agents may investigate or
   validate only.
5. Re-review changed files and process any new finding through the same loop.

The goal is not a cosmetic zero count at any cost. Correctness wins over an
over-eager architectural change; every remaining non-open entry must have
current, concrete reasoning.

## Phase 3: Final gate and version

Run the commit protocol's final repository gate. Fix any regression in a new
focused commit. If Rust source changed, bump the patch version exactly once for
the whole pipeline and commit that release metadata separately as directed by
the protocol.

If nothing changed, do not create an empty commit or version bump.

## Output

Report coverage, finding dispositions, validations, every commit hash/subject,
the version result, and any baseline changes left untouched.
