# Developer entry points. `make check` is the gate every milestone must pass on
# macOS and in Linux (`make linux-check`, which runs inside deploy/dev/Dockerfile).

DOCKER_RUN = docker run --rm -v "$(CURDIR)":/src -v chimaera-target:/target \
	-v chimaera-cargo:/usr/local/cargo/registry chimaera-dev:latest

.PHONY: check fmt-check clippy test proptest-deep linux-image linux-check

check: fmt-check clippy test

fmt-check:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

# Deeper property run of the shard state machine (default is 512 histories).
proptest-deep:
	PROPTEST_CASES=20000 cargo test -p chimaera-core invariants_hold

linux-image:
	docker build -q -t chimaera-dev:latest deploy/dev

linux-check: linux-image
	$(DOCKER_RUN) make check
