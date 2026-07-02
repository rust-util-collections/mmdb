# Commit Protocol

Canonical validate → version-bump → commit procedure. All commands (`/x-commit`, `/x-fix`, `/x-overhaul`) reference this — do not duplicate these steps elsewhere.

## Step 1: Validate — MANDATORY before any commit

1. Run `make fmt` to apply formatting.
2. Run `make lint` (clippy with `-D warnings`, matches CI). Fix every warning at the source — `#[allow(...)]` is forbidden.
3. Run tests scoped to the change:
   - Only doc/comment changes → skip tests.
   - Changes confined to one subsystem → run its unit tests (e.g., `cargo test memtable`, `cargo test wal`) **plus** `cargo test --test integration`.
   - Changes to `src/db.rs`, write path, compaction, manifest, or anything cross-cutting → `cargo test` (full debug suite).
   - Changes touching crash safety (WAL, flush, MANIFEST) → also `cargo test --test crash_recovery`.
4. If any step fails, fix and restart Step 1. Never commit with failing fmt/lint/tests.

## Step 2: Bump Patch Version

1. Run `git diff HEAD --name-only` — if any `.rs` file changed, a version bump is REQUIRED. Skip only if every changed file is non-code (`.md`, config, etc.).
2. Read the current `version = "X.Y.Z"` in `Cargo.toml`, set it to `X.Y.(Z+1)`.
3. **Verify**: grep `Cargo.toml` for the new version string before continuing.

## Step 3: Commit

1. Run `git diff HEAD --stat` and `git log -5 --oneline` to understand scope and existing commit style.
2. Draft the message:
   - Type prefix matching repo history: `fix:`, `feat:`, `style:`, `refactor:`, `chore:`, etc.
   - Subject summarizes the "why", 1-2 sentences max.
   - Body with key details only if the change spans multiple subsystems.
3. Stage specific files with `git add <files>` — never `git add -A`.
4. Commit using a HEREDOC — **do NOT include any co-author or "Generated with" line**:

```
git commit -m "$(cat <<'EOF'
<commit message here>
EOF
)"
```

5. Run `git status` to verify the commit succeeded and nothing intended was left unstaged.
