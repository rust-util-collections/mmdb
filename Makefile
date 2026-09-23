.PHONY: all fmt lint check test ci bench clean update publish

all: fmt lint test

fmt:
	cargo fmt

lint:
	cargo clippy --all-targets -- -D warnings

check:
	cargo check
	cargo check --tests

test:
	cargo test
	cargo test --release

# Same gate as .github/workflows/ci.yml. No release suite, no scale_profile.
ci:
	cargo fmt --all -- --check
	$(MAKE) lint
	cargo test --lib
	cargo test --test crash_recovery
	cargo test --test e2e_scenarios
	cargo test --test integration
	cargo test --test proptest_db
	cargo test --test bidi_debug
	cargo test --test lazy_delete
	cargo test --test shared_cache
	cargo test --test read_only

bench:
	cargo bench

clean:
	cargo clean

update:
	cargo update

publish:
	cargo publish
