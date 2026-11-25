#!/usr/bin/env bash
# Gate: FileSink on an object store. Starts MinIO in Docker and runs
# python/tests/test_s3_store.py against it: conditional puts, commits whose
# replies are lost and sent again, a part replaced before its commit, and a
# write through the workers. Needs docker and a built peer (CHIMAERA_BIN, as
# for the other Python tests); removes its container.
set -euo pipefail
cd "$(dirname "$0")/../.."
# MinIO stopped publishing images to Docker Hub; this is Bitnami's last build
# of its 2025-04-22 release, which has the conditional puts a commit needs.
image=${S3_TEST_IMAGE:-bitnamilegacy/minio:2025.4.22}
port=${S3_TEST_PORT:-19000}
name=chimaera-s3-test-$$
docker run -d --rm --name "$name" -p "127.0.0.1:$port:9000" -e MINIO_ROOT_USER=chimaera \
  -e MINIO_ROOT_PASSWORD=chimaera-secret -e MINIO_DEFAULT_BUCKETS=chimaera-test "$image" >/dev/null
trap 'docker stop "$name" >/dev/null 2>&1 || true' EXIT
# The image makes the bucket with a server of its own, then starts the one we use.
for _ in $(seq 60); do
  docker logs "$name" 2>&1 | grep -q "MinIO setup finished" &&
    curl -sf "http://127.0.0.1:$port/minio/health/ready" >/dev/null && break
  sleep 1
done
export AWS_ACCESS_KEY_ID=chimaera AWS_SECRET_ACCESS_KEY=chimaera-secret
export CHIMAERA_TEST_S3_ENDPOINT="http://127.0.0.1:$port" CHIMAERA_TEST_S3_BUCKET=chimaera-test
"${PYTHON:-.venv/bin/python}" -m pytest python/tests/test_s3_store.py -q "$@"
