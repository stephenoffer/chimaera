# The Chimaera runtime image: `chimaera` on PATH and the chimaera wheel
# installed on Python 3.12, running as uid 1000. Every peer and every driver
# in a cluster should run this image (or one built FROM it), so executors
# unpickle calls with the driver's Python version.
#
# Build from the repository with deploy/images/build.sh [TAG], which sends
# only the sources the build needs as the context.

FROM rust:1.94.1-bookworm AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler python3-venv \
 && rm -rf /var/lib/apt/lists/*
RUN python3 -m venv /venv && /venv/bin/pip install --no-cache-dir "maturin>=1.9,<2"
WORKDIR /src
COPY . .
# No debug info, and the target directory is removed in the same layer, so
# the build stage stays small.
ENV CARGO_PROFILE_RELEASE_DEBUG=0
RUN cargo build --release --locked -p chimaera-peer \
 && /venv/bin/maturin build --release --locked --out /out \
 && cp target/release/chimaera /out/ \
 && rm -rf target

FROM python:3.12-slim-bookworm
COPY --from=build /out/chimaera /usr/local/bin/chimaera
# Peers install a remote driver's uv.lock with it (python/chimaera/_project.py).
COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /usr/local/bin/uv
# The wheel stays in the image for derived images and shared installs.
COPY --from=build /out/*.whl /opt/chimaera/
RUN pip install --no-cache-dir /opt/chimaera/*.whl \
 && useradd --uid 1000 --user-group --create-home chimaera
USER 1000:1000
WORKDIR /home/chimaera
