# Linux build-and-test image for Chimaera (used by `make linux-check` and the
# cluster tests). Same toolchain as rust-toolchain.toml.
FROM rust:1.94.1-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler python3 python3-venv python3-pip iproute2 procps \
 && rm -rf /var/lib/apt/lists/*
RUN rustup component add rustfmt clippy
ENV CARGO_TARGET_DIR=/target
WORKDIR /src
