.PHONY: all fmt lint check test clean

all: fmt lint check test

fmt:
	cargo fmt

lint:
	cargo clippy --all-targets -- -D warnings
	cargo clippy --all-targets --tests -- -D warnings

check:
	cargo check
	cargo check --tests

test:
	cargo test
	cargo test --release

clean:
	cargo clean
