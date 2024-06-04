all: fmt lintall testall

export CARGO_NET_GIT_FETCH_WITH_CLI = true

lint:
	cargo clippy --workspace
	cargo check --workspace --tests
	cargo check --workspace --benches

lintall: lint
	cargo clippy --workspace --no-default-features --features "parity_backend,compress,msgpack_codec"
	cargo check --workspace --tests --no-default-features --features "parity_backend,json_codec"

lintmusl:
	cargo clippy --workspace --target x86_64-unknown-linux-musl \
		--no-default-features \
		--features "parity_backend,msgpack_codec"

test:
	- rm -rf ~/.mmdb /tmp/.mmdb /tmp/mmdb_testing $(MMDB_BASE_DIR)
	cargo test --workspace --release --tests -- --test-threads=1
	- rm -rf ~/.mmdb /tmp/.mmdb /tmp/mmdb_testing $(MMDB_BASE_DIR)
	cargo test --workspace --tests -- --test-threads=1

testall: test
	- rm -rf ~/.mmdb /tmp/.mmdb /tmp/mmdb_testing $(MMDB_BASE_DIR)
	cargo test --workspace --tests \
		--no-default-features \
		--features "parity_backend,msgpack_codec" \
		-- --test-threads=1 #--nocapture

testmusl:
	- rm -rf ~/.mmdb /tmp/.mmdb /tmp/mmdb_testing $(MMDB_BASE_DIR)
	cargo test --workspace --target x86_64-unknown-linux-musl --release --tests \
		--no-default-features \
		--features "parity_backend,msgpack_codec" \
		-- --test-threads=1 #--nocapture

bench:
	- rm -rf ~/.mmdb /tmp/.mmdb /tmp/mmdb_testing $(MMDB_BASE_DIR)
	cargo bench --workspace --no-default-features --features "parity_backend,msgpack_codec"
	du -sh ~/.mmdb
	- rm -rf ~/.mmdb /tmp/.mmdb /tmp/mmdb_testing $(MMDB_BASE_DIR)
	cargo bench --workspace --no-default-features --features "parity_backend,compress,msgpack_codec"
	du -sh ~/.mmdb
	- rm -rf ~/.mmdb /tmp/.mmdb /tmp/mmdb_testing $(MMDB_BASE_DIR)
	cargo bench --workspace
	du -sh ~/.mmdb
	- rm -rf ~/.mmdb /tmp/.mmdb /tmp/mmdb_testing $(MMDB_BASE_DIR)
	cargo bench --workspace --features "compress"
	du -sh ~/.mmdb

benchmusl:
	- rm -rf ~/.mmdb /tmp/.mmdb /tmp/mmdb_testing $(MMDB_BASE_DIR)
	cargo bench --workspace --target x86_64-unknown-linux-musl \
		--no-default-features --features "parity_backend,msgpack_codec"
	du -sh ~/.mmdb

fmt:
	cargo +nightly fmt

fmtall:
	bash scripts/fmt.sh

update:
	cargo update

clean:
	cargo clean

clean_all: clean
	git stash
	git clean -fdx

doc:
	cargo doc --open
