#!/usr/bin/env bash
# Qualification: FileSink writes an Arrow table three times a container's
# memory limit to Parquet a batch at a time, with every row and the table's
# schema, while the same table handed over as Python rows is killed for
# memory. Needs docker; installs pyarrow in throwaway containers of the
# runtime image (CHIMAERA_IMAGE names one already built from this tree).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
image=${CHIMAERA_IMAGE:-chimaera:dev}
[ -n "${CHIMAERA_IMAGE:-}" ] || "$here/../images/build.sh" "$image"

check() {
  docker run --rm -i -u root --memory 512m --memory-swap 512m -e PIP_DISABLE_PIP_VERSION_CHECK=1 --entrypoint sh "$image" -c \
    'pip install -q --root-user-action=ignore pyarrow >/dev/null && mkdir -p /work && python3 - "$1"' sh "$1" <<'EOF'
import os, sys
import pyarrow as pa, pyarrow.ipc as ipc, pyarrow.parquet as pq
from chimaera import ext

limit, batch = int(open("/sys/fs/cgroup/memory.max").read()), 65536
src, schema, n = "/work/big.arrow", pa.schema([("id", pa.int64()), ("blob", pa.binary())]), 0
with ipc.new_file(src, schema) as f:  # a batch at a time, to three times the limit
    while os.path.getsize(src) < 3 * limit:
        f.write_batch(pa.record_batch([pa.array(range(n, n + batch), pa.int64()), [os.urandom(200) for _ in range(batch)]], schema))
        n += batch
table = ipc.open_file(pa.memory_map(src)).read_all()  # mapped from the file, not read into memory
sink = ext.FileSink("/work/out", "parquet").begin("20260101T000000000001Z-a")
if sys.argv[1] == "batches":
    receipt = sink.write_batches(0, table.to_reader(max_chunksize=batch))
else:
    receipt = sink.write(0, table.to_pylist())
written = pq.ParquetFile(receipt["path"])
assert receipt["rows"] == written.metadata.num_rows == n and written.schema_arrow.equals(schema)
print(f"{n} rows, {os.path.getsize(src) >> 20} MiB of Arrow in {limit >> 20} MiB: Arrow's pool peaked at",
      f"{pa.default_memory_pool().max_memory() >> 20} MiB")
EOF
}

check batches
if check rows; then
  echo "the row path fit in memory too, so this check shows nothing" >&2
  exit 1
fi
echo "ok: batches fit, rows were killed"
