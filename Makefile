.PHONY: all fmt lint check test ci bench clean update publish

CARGO_TEST = sh tools/with-test-fds.sh cargo test

all: fmt lint test

fmt:
	cargo fmt

lint:
	cargo clippy --lib -- -D warnings
	cargo clippy --lib --features bench-metrics -- -D warnings
	cargo clippy --all-targets -- -D warnings
	cargo clippy --all-targets --features bench-metrics -- -D warnings
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --features bench-metrics

check:
	cargo check --all-targets
	cargo check --benches --features bench-metrics

test:
	$(CARGO_TEST)
	$(CARGO_TEST) --release
	$(CARGO_TEST) --lib --features bench-metrics

# Same gate as .github/workflows/ci.yml. No release suite, no scale_profile.
ci:
	cargo fmt --all -- --check
	$(MAKE) check
	$(MAKE) lint
	$(CARGO_TEST) --lib
	$(CARGO_TEST) --lib --features bench-metrics
	$(CARGO_TEST) --doc
	$(CARGO_TEST) --test crash_recovery
	$(CARGO_TEST) --test e2e_scenarios
	$(CARGO_TEST) --test integration
	$(CARGO_TEST) --test proptest_db
	$(CARGO_TEST) --test bidi_debug
	$(CARGO_TEST) --test lazy_delete
	$(CARGO_TEST) --test shared_cache
	$(CARGO_TEST) --test grouped_reads
	$(CARGO_TEST) --test read_only
	$(CARGO_TEST) --test cold_read_and_options

bench:
	cargo bench

clean:
	cargo clean

update:
	cargo update

publish:
	cargo publish
