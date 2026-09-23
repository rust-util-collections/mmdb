# MMDB Project Instructions

MMDB is a local embedded key-value storage engine. Review and describe its
correctness, persistence, concurrency, and resource behavior using concrete
operations and expected/observed results.

Read [CLAUDE.md](CLAUDE.md) for architecture, build commands, and coding
conventions. The shared AI workflow is
[workflow-policy.md](.claude/docs/workflow-policy.md); it defines neutral
reporting language, scoped agent handoffs, and worktree/commit rules.

Project workflows are user-invoked from `.claude/skills/`:

- `x-review`: inspect a scope (default: latest commit) and record findings. Does not commit.
- `x-fix`: resolve recorded findings with validated local commits. Does not release.
- `x-commit`: review and commit the intended worktree changes. Does not release.
- `x-overhaul`: same scopes as `x-review` (`all` for the full repo), resolve in-scope findings, then one local release if `src/` changed.

When invoked, read that workflow's `SKILL.md`. For delegated reviews, use the
handoff and finding templates in
[review-core.md](.claude/docs/review-core.md) and provide only the relevant
repository context. Preserve technical identifiers, evidence, and severity.
