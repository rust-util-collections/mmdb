.PHONY: all fmt lint check test ci bench clean update publish

all: fmt lint test

fmt:
	cargo fmt
	cargo fmt --manifest-path tools/v4-export/Cargo.toml

lint:
	cargo clippy --lib -- -D warnings
	cargo clippy --all-targets -- -D warnings
	cargo clippy --all-targets --features bench-metrics -- -D warnings
	cargo clippy --manifest-path tools/v4-export/Cargo.toml --all-targets -- -D warnings
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps

check:
	cargo check --all-targets
	cargo check --benches --features bench-metrics
	cargo check --manifest-path tools/v4-export/Cargo.toml --all-targets

test:
	cargo test
	cargo test --release
	cargo test --lib --features bench-metrics
	cargo test --manifest-path tools/v4-export/Cargo.toml

# Same gate as .github/workflows/ci.yml. No release suite, no scale_profile.
ci:
	cargo fmt --all -- --check
	cargo fmt --manifest-path tools/v4-export/Cargo.toml --all -- --check
	$(MAKE) check
	$(MAKE) lint
	cargo test --lib
	cargo test --lib --features bench-metrics
	cargo test --doc
	cargo test --test crash_recovery
	cargo test --test e2e_scenarios
	cargo test --test integration
	cargo test --test proptest_db
	cargo test --test bidi_debug
	cargo test --test lazy_delete
	cargo test --test shared_cache
	cargo test --test read_only
	CI=1 cargo test --manifest-path tools/v4-export/Cargo.toml

bench:
	cargo bench

clean:
	cargo clean
	cargo clean --manifest-path tools/v4-export/Cargo.toml

update:
	cargo update

publish:
	cargo publish
