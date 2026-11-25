"""This process's private memory, read in a few microseconds: what a
shuffle's gather checks against its ceiling between buckets (see
``_shuffle._Ceiling``). It is what the peer charges an executor for
(budget.rs): on Linux its anonymous resident pages, on macOS its physical
footprint. Neither counts pages mapped from object files, which the peer
counts once as object bytes, and the footprint leaves out freed pages the
allocator keeps for reuse. None on other systems."""

from __future__ import annotations

import ctypes
import os
import sys

_PAGE = os.sysconf("SC_PAGE_SIZE")


def _linux() -> int | None:
    with open("/proc/self/statm", "rb") as f:
        fields = f.read().split()
    return (int(fields[1]) - int(fields[2])) * _PAGE  # resident less shared (file and tmpfs): RssAnon


def _darwin() -> int | None:
    usage = (ctypes.c_uint64 * 12)()  # rusage_info_v0: a 16-byte uuid, then ten counters
    if _libc.proc_pid_rusage(os.getpid(), 0, usage) != 0:  # 0: RUSAGE_INFO_V0
        return None
    return int(usage[9])  # ri_phys_footprint


def _none() -> int | None:
    return None


if sys.platform.startswith("linux"):
    resident = _linux
elif sys.platform == "darwin":
    _libc = ctypes.CDLL(None)
    _libc.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    resident = _darwin
else:
    resident = _none
