# MMDB Project Instructions

MMDB is a local embedded key-value storage engine. Review and describe its
correctness, persistence, concurrency, and resource behavior using concrete
operations and expected/observed results.

Read [CLAUDE.md](CLAUDE.md) for architecture, build commands, and coding
conventions. The shared AI workflow is
[workflow-policy.md](.claude/docs/workflow-policy.md); it defines neutral
reporting language, scoped agent handoffs, and worktree/commit rules.

Project workflows are user-invoked from `.claude/skills/`:

- `x-review`: inspect changes or the full repository and record findings.
- `x-fix`: resolve recorded findings with validated local commits.
- `x-commit`: review and commit the intended worktree changes.
- `x-overhaul`: full review, resolution, validation, and local release steps.

When invoked, read that workflow's `SKILL.md`. For delegated reviews, use the
handoff and finding templates in
[review-core.md](.claude/docs/review-core.md) and provide only the relevant
repository context. Preserve technical identifiers, evidence, and severity.
