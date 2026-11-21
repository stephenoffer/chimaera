# Developer entry points. `make check` is the gate every milestone must pass on
# macOS and in Linux (`make linux-check`, which runs inside deploy/dev/Dockerfile).

DOCKER_RUN = docker run --rm -v "$(CURDIR)":/src -v chimaera-target:/target \
	-v chimaera-cargo:/usr/local/cargo/registry chimaera-dev:latest

.PHONY: check fmt-check clippy test pytest proptest-deep linux-image linux-check linux-pytest

check: fmt-check clippy test pytest

fmt-check:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

pytest:
	cargo build -q -p chimaera-peer
	VIRTUAL_ENV=$(CURDIR)/.venv .venv/bin/maturin develop -q
	.venv/bin/python -m pytest python/tests -q

# Deeper property run of the shard state machine (default is 512 histories).
proptest-deep:
	PROPTEST_CASES=20000 cargo test -p chimaera-core invariants_hold

linux-image:
	docker build -q -t chimaera-dev:latest deploy/dev

linux-check: linux-image
	$(DOCKER_RUN) bash -c "cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace"

# Python suite on Linux, with cgroup delegation for isolated accounting.
linux-pytest: linux-image
	docker run --rm --privileged --cgroupns=private -v "$(CURDIR)":/src -v chimaera-target:/target \
		-v chimaera-cargo:/usr/local/cargo/registry chimaera-dev:latest deploy/dev/linux-test.sh

# Launcher gates: each builds the runtime image (deploy/images) and runs a
# real cluster in Docker. kind-test also needs kind and kubectl.
.PHONY: kind-test slurm-test ssh-test

kind-test:
	deploy/k8s/kind-test.sh

slurm-test:
	deploy/slurm/slurm-test.sh

ssh-test:
	deploy/compose/ssh-test.sh
