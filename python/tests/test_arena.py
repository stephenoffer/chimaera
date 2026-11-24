"""The shared-memory arena (Linux, objects in tmpfs): Python's own large
blocks (bytes, bytearray, lists) live in files of the object store, so a large
`bytes` is stored by link like a NumPy array, and fork still gives the child a
private copy of everything. Where there is no arena (other systems, objects
on disk) the same values are copied, and every check but the linking holds."""

from __future__ import annotations

import gc
import os
import sys
import time

import chimaera as ch
from chimaera import _native, _objects
from chimaera._runtime import Runtime


@ch.remote
def size_and_tail(b: bytes) -> tuple[int, bytes]:
    return len(b), bytes(b[-16:])


@ch.remote
def length_and_last(s: str) -> tuple[int, str]:
    return len(s), s[-1]


def _arena() -> bool:
    """Whether this process has the arena, checked against where it must."""
    ch.put(0)  # starts the runtime, which installs the arena where it can
    rt = Runtime.get()
    expected = (
        sys.platform.startswith("linux")
        and os.environ.get("CHIMAERA_ARENA", "1") != "0"
        and _objects._in_memory(rt.object_dir)
    )
    assert bool(_objects._state["installed"]) == expected, f"arena installed: {_objects._state}, expected {expected}"
    return expected


def _live() -> int:
    return _native.arena_stats()[0]


def _child_ok(status: int) -> bool:
    return os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0


def test_a_large_bytes_value_is_stored_by_link_again_and_again():
    arena = _arena()
    before = _live()
    b = os.urandom(8 << 20)
    claims = _native.arena_stats()[3]
    f = ch.put(b)
    g = ch.put(b)  # its reference count changed since: the data did not
    if arena:
        assert _live() == before + 1, "a large bytes value is allocated in the arena"
        assert _native.arena_stats()[3] == claims + 2, "both puts linked the bytes instead of copying them"
    assert ch.get(f) == b and ch.get(g) == b
    assert size_and_tail.remote(f).result(timeout=60) == (len(b), b[-16:])


def test_a_large_str_is_encoded_once_and_comes_back_exactly():
    from chimaera import _serde

    arena = _arena()
    for s, linked in [
        ("é" * (1 << 20) + "ascii" + "\ud800", 0),  # 2 MiB of UTF-8, and a lone surrogate pickle allows
        ("x" * (8 << 20) + "\ud800", 1),
    ]:
        assert _serde.dumps_parts(s)[2] == 1, "the encoded string travels out of band, not in the pickle"
        claims = _native.arena_stats()[3]
        f = ch.put(s)
        if arena:
            # Below 8 MiB the encoded bytes are written from the heap, one copy
            # with no arena pages to fault in; from 8 MiB they are linked.
            assert _native.arena_stats()[3] == claims + linked, "written below 8 MiB, linked from 8 MiB"
        assert ch.get(f) == s
        assert length_and_last.remote(s).result(timeout=60) == (len(s), "\ud800")


def test_a_str_encoded_on_the_heap_is_what_pickle_would_encode():
    for s in ["", "ascii", "é" * 1000, "\ud800", "a\udfffb" * 100, "x" * (2 << 20)]:
        assert _native.encode_utf8_heap(s) == s.encode("utf-8", "surrogatepass")


def test_a_read_into_the_heap_is_the_file_s_bytes_and_a_short_file_gives_fewer(tmp_path):
    from chimaera import _serde

    data = os.urandom(3 << 20)  # 1 to 8 MiB: read into the heap (_native.pread_heap)
    (tmp_path / "blob").write_bytes(data)
    fd = os.open(tmp_path / "blob", os.O_RDONLY)
    try:
        assert _serde._pread(fd, len(data), 0) == data
        assert _serde._pread(fd, 2 << 20, 1 << 20) == data[1 << 20 :]
        assert len(_serde._pread(fd, 2 << 20, 2 << 20)) < 2 << 20, "past the end: fewer bytes, never garbage"
    finally:
        os.close(fd)


def test_a_large_buffer_keeps_its_contents_as_it_grows_and_shrinks():
    _arena()
    a = bytearray(os.urandom(2 << 20))  # in the arena, resized in place there
    head = bytes(a[:4096])
    a.extend(b"x" * (6 << 20))
    assert len(a) == 8 << 20 and a[:4096] == head and a[-1:] == b"x"
    del a[1 << 20 :]
    assert len(a) == 1 << 20 and a[:4096] == head
    stored = bytes(a)
    assert ch.get(ch.put(stored)) == stored


def test_a_forked_child_does_not_share_reference_counts_with_its_parent():
    # A large bytes object's header (its reference count) is in the arena
    # file. Shared between parent and child, the parent dropping references
    # after the fork would change the child's count: the child would free the
    # object while still holding it.
    arena = _arena()
    before = _live()
    b = os.urandom(4 << 20)
    if arena:
        assert _live() == before + 1
    held = [b] * 1000
    pid = os.fork()
    if pid == 0:
        ok = False
        try:
            time.sleep(0.5)  # the parent drops its 1000 references meanwhile
            ok = len(held) == 1000 and sys.getrefcount(b) >= 1001
            del held
            gc.collect()
            ok = ok and len(b) == 4 << 20
        finally:
            os._exit(0 if ok else 3)
    del held
    gc.collect()
    _, status = os.waitpid(pid, 0)
    assert _child_ok(status), f"the child saw its parent's reference counts: {status}"


def test_a_forked_child_sees_the_parents_writes_made_before_the_fork():
    # A value stored and then changed by its owner: the owner's pages are
    # private from the store, and the child must see the owner's current
    # data, not what was stored.
    _arena()
    a = bytearray(2 << 20)
    f = ch.put(a)  # links `a`'s file where there is an arena
    a[:4] = b"new!"
    pid = os.fork()
    if pid == 0:
        os._exit(0 if bytes(a[:4]) == b"new!" else 3)
    _, status = os.waitpid(pid, 0)
    assert _child_ok(status), "the child saw the stored bytes, not the parent's"
    assert bytes(ch.get(f)[:4]) == bytes(4), "the stored value kept what was put"
