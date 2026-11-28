"""Arrow's large buffers in the shared-memory arena (Linux).

A NumPy array of 1 MiB or more lives in the arena (_objects), so storing it
links its file instead of copying it. Arrow allocates from a memory pool of
its own, so a table read from Parquet, a computed column or a string column
was copied into the store by every put. install() makes Arrow's default pool
one that takes allocations of the arena's minimum and up from the arena and
leaves the rest to the pool Arrow had; such buffers are then stored by link.

pyarrow takes no memory pool written in Python, so the pool is a few lines of
C++, built against the installed pyarrow the first time a host needs it (with
the C++ compiler in $CXX, else c++) and cached per pyarrow version. Without a
compiler, Arrow's buffers are copied as before.
"""

from __future__ import annotations

import ctypes
import glob
import hashlib
import importlib.abc
import os
import subprocess
import sys
import threading
from typing import Any

from . import _native

SOURCE = r"""
#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <string>

struct PoolApi {  // objects.rs's PoolApi
  size_t min;
  void* (*alloc)(size_t);
  int (*resize)(void*, size_t, void**);
  bool (*free)(void*);
};

class ArenaPool final : public arrow::MemoryPool {
 public:
  ArenaPool(const PoolApi* api, arrow::MemoryPool* rest) : api_(api), rest_(rest) {}

  arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
    if (size >= static_cast<int64_t>(api_->min) && alignment <= 4096) {
      if (void* p = api_->alloc(static_cast<size_t>(size))) {  // page-aligned
        *out = static_cast<uint8_t*>(p);
        mine_ += size;
        total_ += size;
        count_ += 1;
        return arrow::Status::OK();
      }
    }
    return rest_->Allocate(size, alignment, out);
  }

  arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t** ptr) override {
    void* moved = nullptr;
    int r = api_->resize(*ptr, static_cast<size_t>(new_size), &moved);
    if (r > 0) {
      *ptr = static_cast<uint8_t*>(moved);
      mine_ += new_size - old_size;
      return arrow::Status::OK();
    }
    if (r < 0 && new_size < static_cast<int64_t>(api_->min)) {
      return rest_->Reallocate(old_size, new_size, alignment, ptr);
    }
    uint8_t* fresh = nullptr;  // between the arena and the other pool: move
    ARROW_RETURN_NOT_OK(Allocate(new_size, alignment, &fresh));
    std::memcpy(fresh, *ptr, static_cast<size_t>(std::min(old_size, new_size)));
    Free(*ptr, old_size, alignment);
    *ptr = fresh;
    return arrow::Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
    if (api_->free(buffer)) {
      mine_ -= size;
    } else {
      rest_->Free(buffer, size, alignment);
    }
  }

  int64_t bytes_allocated() const override { return mine_ + rest_->bytes_allocated(); }
  int64_t total_bytes_allocated() const override { return total_ + rest_->total_bytes_allocated(); }
  int64_t num_allocations() const override { return count_ + rest_->num_allocations(); }
  std::string backend_name() const override { return "chimaera-arena+" + rest_->backend_name(); }

 private:
  const PoolApi* api_;
  arrow::MemoryPool* rest_;
  std::atomic<int64_t> mine_{0}, total_{0}, count_{0};
};

extern "C" void* chimaera_arrow_pool(const PoolApi* api) {
  return new ArenaPool(api, arrow::default_memory_pool());  // lives as long as the process
}
"""

_state: dict[str, Any] = {"pool": None, "building": False}
_lock = threading.Lock()
_build_lock = threading.Lock()  # one build per process: the temporary files are named by pid


def _cache_dir() -> str:
    base = os.environ.get("XDG_CACHE_HOME") or os.path.join(os.path.expanduser("~"), ".cache")
    return os.path.join(base, "chimaera", "arrow-pool")


def _library(pa: Any) -> str | None:
    """The pool's library for this pyarrow: cached, or built now (None when
    it cannot be built here)."""
    libdir = pa.get_library_dirs()[0]
    found = sorted(glob.glob(os.path.join(libdir, "libarrow.so.*")), key=len)
    if not found:
        return None
    libarrow = found[0]
    with open(libarrow, "rb") as f:  # built with libstdc++'s C++11 ABI, or the old one
        abi = 1 if b"B5cxx11" in f.read() else 0
    key = hashlib.sha256(f"{SOURCE}{pa.__version__}{abi}{os.uname().machine}".encode()).hexdigest()[:16]
    out = os.path.join(_cache_dir(), f"pool-{pa.__version__}-{key}.so")
    with _build_lock:
        return out if os.path.exists(out) else _build(pa, out, libarrow, libdir, abi)


def _build(pa: Any, out: str, libarrow: str, libdir: str, abi: int) -> str | None:
    os.makedirs(_cache_dir(), exist_ok=True)
    src = f"{out}.{os.getpid()}.cc"
    tmp = f"{out}.{os.getpid()}.tmp"
    with open(src, "w") as f:
        f.write(SOURCE)
    cmd = [os.environ.get("CXX", "c++"), "-std=c++20", "-O2", "-fPIC", "-shared", f"-D_GLIBCXX_USE_CXX11_ABI={abi}",
           f"-I{pa.get_include()}", src, libarrow, f"-Wl,-rpath,{libdir}", "-o", tmp]
    try:
        built = subprocess.run(cmd, capture_output=True, timeout=300, check=False).returncode == 0
    except (OSError, subprocess.TimeoutExpired):  # no compiler here
        built = False
    finally:
        os.unlink(src)
    if not built:
        if os.path.exists(tmp):
            os.unlink(tmp)
        return None
    os.replace(tmp, out)  # whole or absent, for processes building it at once
    return out


def install() -> bool:
    """Make Arrow's default pool take its large allocations from the arena,
    if pyarrow is loaded, the arena is installed and the pool's library is
    (or can be built) here. True when the pool is Arrow's default now."""
    pa = sys.modules.get("pyarrow")
    if pa is None:
        return False
    with _lock:
        if _state["pool"] is not None:
            return True
    api = _native.arena_pool_api()
    if not api:
        return False
    path = _library(pa)
    if path is None:
        return False
    make = ctypes.CDLL(path).chimaera_arrow_pool
    make.restype, make.argtypes = ctypes.c_void_p, [ctypes.c_void_p]
    capsule = pa.lib.__pyx_capi__["box_memory_pool"]  # pyarrow's C API: wrap an arrow::MemoryPool*
    name = ctypes.pythonapi.PyCapsule_GetName
    name.restype, name.argtypes = ctypes.c_char_p, [ctypes.py_object]
    pointer = ctypes.pythonapi.PyCapsule_GetPointer
    pointer.restype, pointer.argtypes = ctypes.c_void_p, [ctypes.py_object, ctypes.c_char_p]
    box = ctypes.PYFUNCTYPE(ctypes.py_object, ctypes.c_void_p)(pointer(capsule, name(capsule)))
    with _lock:
        if _state["pool"] is None:
            _state["pool"] = box(make(api))
            pa.set_memory_pool(_state["pool"])
    return True


def install_soon() -> None:
    """install() now if pyarrow is loaded (building the library in the
    background when it is not cached), else once `import pyarrow` finishes."""
    if "pyarrow" not in sys.modules:
        if not any(isinstance(f, _PyarrowHook) for f in sys.meta_path):
            sys.meta_path.insert(0, _PyarrowHook())
        return
    with _lock:
        if _state["pool"] is not None or _state["building"]:
            return
        _state["building"] = True
    threading.Thread(target=_install_quietly, name="chimaera-arrow-pool", daemon=True).start()


def _install_quietly() -> None:
    try:
        install()
    except Exception:  # noqa: BLE001 -- the pool is an optimization; Arrow's buffers still copy
        pass
    finally:
        with _lock:
            _state["building"] = False


class _PyarrowHook(importlib.abc.MetaPathFinder):
    """Calls install_soon() right after `import pyarrow` finishes."""

    def find_spec(self, fullname: str, path: Any, target: Any = None) -> Any:
        if fullname != "pyarrow":
            return None
        sys.meta_path.remove(self)
        spec = None
        for finder in sys.meta_path:  # whoever would have found it without us
            find = getattr(finder, "find_spec", None)
            spec = find(fullname, path, target) if find is not None else None
            if spec is not None:
                break
        if spec is None or spec.loader is None:
            return spec
        loader = spec.loader
        original = loader.exec_module

        def exec_module(module: Any) -> None:
            original(module)
            install_soon()

        loader.exec_module = exec_module  # type: ignore[method-assign]
        return spec
