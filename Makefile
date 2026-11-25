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

# A wheel that carries the peer binary, so `pip install` gives a working
# `chimaera` with no Rust toolchain. Both halves are built with one
# CHIMAERA_BUILD_ID, and deploy/dev/wheel-check.py refuses the wheel, before
# it reaches dist/, unless they report the same release, peer level, wire
# version and build id. wheel-smoke installs it in a fresh venv for each
# interpreter in PYTHONS and runs a task, an actor and a 64 MiB result;
# wheel-linux does both for the manylinux_2_28 wheel of this machine's CPU,
# in Docker, on CPython 3.11 to 3.14. .github/workflows/wheels.yml runs them
# for every supported OS and CPU.
TARGET ?= $(or $(CARGO_TARGET_DIR),target)
BUILD_ID := $(or $(CHIMAERA_BUILD_ID),$(shell date -u +%Y%m%d%H%M%S)-$(shell git rev-parse --short=12 HEAD 2>/dev/null || echo src))
PYTHONS ?= $(CURDIR)/.venv/bin/python
MATURIN_ARGS ?=
.PHONY: wheel wheel-smoke wheel-linux
wheel:
	CHIMAERA_BUILD_ID=$(BUILD_ID) cargo build --release --locked -p chimaera-peer
	mkdir -p python/chimaera/_bin
	install -m 755 $(TARGET)/release/chimaera python/chimaera/_bin/chimaera
	rm -rf $(TARGET)/wheel
	CHIMAERA_BUILD_ID=$(BUILD_ID) .venv/bin/maturin build --release --locked $(MATURIN_ARGS) --out $(TARGET)/wheel; \
		status=$$?; rm -rf python/chimaera/_bin; exit $$status  # in a checkout, the peer comes from target/
	.venv/bin/python deploy/dev/wheel-check.py $(TARGET)/wheel/chimaera-*.whl
	mkdir -p dist && mv $(TARGET)/wheel/chimaera-*.whl dist/

wheel-smoke: wheel
	deploy/dev/wheel-smoke.sh $(PYTHONS)

wheel-linux:
	docker run --rm --shm-size=2g -e CHIMAERA_BUILD_ID=$(BUILD_ID) -v "$(CURDIR)":/src \
		quay.io/pypa/manylinux_2_28_$(shell uname -m | sed s/arm64/aarch64/) /src/deploy/dev/wheel-linux.sh

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
# kind-mutant-test passes only if kind-test fails on an image whose render
# moves the peers' volume (the integration lane's mutation proof).
.PHONY: kind-test kind-mutant-test slurm-test ssh-test

kind-test:
	deploy/k8s/kind-test.sh

kind-mutant-test:
	deploy/k8s/kind-mutant-test.sh

slurm-test:
	deploy/slurm/slurm-test.sh

ssh-test:
	deploy/compose/ssh-test.sh

# FileSink on an object store: MinIO in Docker, conditional puts and lost replies.
.PHONY: s3-test
s3-test:
	cargo build -q -p chimaera-peer
	CHIMAERA_BIN=$(abspath $(TARGET))/debug/chimaera deploy/dev/s3-test.sh

# The quick lane's pinned regression run (bench/evals/regress.py): three local
# release peers run fixed workloads at a fixed size and seed, checked against
# this machine's baseline in bench/evals/baselines, which regress-record writes.
# .github/workflows/check.yml runs `make check` and `make regress`;
# integration.yml runs the Linux, launcher and hardware lanes.
.PHONY: regress regress-record
regress regress-record:
	cargo build -q --release -p chimaera-peer
	VIRTUAL_ENV=$(CURDIR)/.venv .venv/bin/maturin develop -q --release
	CHIMAERA_BIN=$(abspath $(TARGET))/release/chimaera .venv/bin/python bench/evals/regress.py \
		$(if $(filter regress-record,$@),--record)

# The release's qualification manifest (bench/qualification): each claim the
# README and docs/qualification.md make, with its tier, raw artifacts and
# replay command, the lanes recorded under dist/qualification/lanes, pending
# gates and tested limits; signed with the SSH key QUALIFICATION_KEY. Record a
# lane here with `.venv/bin/python bench/qualification/build_manifest.py lane
# quick -- make check`; collect the CI jobs' records a run at a time with
# `gh run download <run-id> -p 'lane-*' -D dist/qualification/lanes/<run-id>`.
# RELEASE=<tag> uploads the signed manifest and qualification.tar.gz (the
# manifest and every lane record and log) to that GitHub release.
.PHONY: qualification
qualification:
	.venv/bin/python bench/qualification/build_manifest.py build $(if $(QUALIFICATION_KEY),--sign $(QUALIFICATION_KEY))
	if [ -n "$(RELEASE)" ]; then COPYFILE_DISABLE=1 tar -czf dist/qualification.tar.gz dist/qualification && \
		gh release upload --clobber "$(RELEASE)" dist/qualification.tar.gz dist/qualification/manifest.json \
		dist/qualification/manifest.json.sig; fi
