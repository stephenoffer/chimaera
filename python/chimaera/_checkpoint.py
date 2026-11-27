"""Checkpoints a restarted gang resumes from: ``ch.dist.Checkpoints``.

    ckpts = ch.dist.Checkpoints("bert-7", "/shared/ckpt")    # or an ext.S3Store

    @ch.remote(cpus=2)
    def train():
        g = ch.context().gang
        ckpt = ckpts.latest()                       # the same answer on every rank
        step = ckpt.step if ckpt else 0
        ...
        with ckpts.save(step, cursor=loader.state()) as d:
            if g.rank == 0:
                torch.save(model.state_dict(), d / "model.pt")

A save is a collective. Each rank writes into a directory of its own; when
the block ends, every rank seals its files (syncs, sizes and hashes them, and
uploads them to an object store), rank 0 checks it can see them all, writes
the checkpoint's manifest, and only then moves the committed pointer to it.
The pointer is a key in a replicated store (``ckpt/<name>``) that also holds
the manifest's hash; it is moved by compare-and-set and fenced by
generation. Once a restarted generation has claimed it, no rank of the
generation it replaced can commit, and a newer run under the same name takes
it from an older one for good. So every generation restores a checkpoint
that every rank of some generation finished writing.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import math
import os
import pathlib
import re
import secrets
import shutil
import tempfile
import time
import uuid
from collections.abc import Callable
from typing import Any, TypeVar

from ._errors import ChimaeraError, LeaseLost, VersionConflict

T = TypeVar("T")

GANGS = 256  # gangs whose generation a pointer remembers, for fencing
CLAIMS = 1024  # claims whose save directories a pointer remembers, for pruning
TRIES = 6  # puts whose reply is lost before one gives up
MIRROR = "LATEST.json"  # the last commit, beside the checkpoints: read when the store has no pointer
_STEP_DIR = re.compile(r"(\d{10,})-([0-9a-f]{8})-n\d+")
_ID = re.compile(r"[0-9a-f]{32}")
_DRIVER = secrets.token_hex(8)  # this process as a writer, when it saves outside any call
_SAVES: dict[tuple[str, str], int] = {}  # saves this process made under each (name, claim nonce)


class CheckpointError(ChimaeraError):
    """A checkpoint's files or manifest are missing or changed, rank 0 could
    not see a rank's files, or a rank failed while saving: every rank of the
    save raises it. It is not an infrastructure failure, so a gang does not
    restart over it."""


def _who() -> tuple[str, int, int, int, Any]:
    """(writer, generation, rank, world, group): a gang rank's gang and
    generation, a plain call's task and attempt, or this driver process."""
    from ._context import context

    ctx = context()
    if ctx is None:
        return f"driver-{_DRIVER}", 1, 0, 1, None
    if ctx.gang is not None:
        g = ctx.gang
        return g.id, g.generation, g.rank, g.size, g
    return ctx.task.hex(), ctx.attempt, 0, 1, None


def _agree(group: Any, rank: int, fn: Callable[[], T]) -> T:
    """`fn`'s result on every rank, computed once on rank 0; or every rank
    raises, rank 0 its own error and the others ``LeaseLost`` or
    ``CheckpointError`` saying what it was."""
    if rank != 0:
        ok, value = group.broadcast()
        if ok:
            return value
        lost, why = value
        raise (LeaseLost if lost else CheckpointError)(f"rank 0: {why}")
    try:
        value = fn()
    except Exception as e:
        if group is not None:
            group.broadcast((False, (isinstance(e, LeaseLost), f"{type(e).__name__}: {e}")))
        raise
    if group is not None:
        group.broadcast((True, value))
    return value


def _retrying(fn: Callable[[], T]) -> T:
    """`fn`, tried again (TRIES in all) while its reply is lost: whatever
    it does must be safe to repeat."""
    for failures in range(TRIES):
        try:
            return fn()
        except (ConnectionError, TimeoutError):
            if failures + 1 == TRIES:
                raise
            time.sleep(min(2.0, 0.1 * 2**failures))
    raise AssertionError("unreachable")


class Checkpoint:
    """A committed checkpoint: its ``step``, ``meta``, the ``world`` (ranks)
    that wrote it, and each rank's files and cursor."""

    def __init__(self, files: Any, dirname: str, manifest: dict[str, Any], rank: int, world: int) -> None:
        self._where, self._dirname = files, dirname
        self.dir = files.where(dirname)
        self.id: str = manifest["id"]
        self.step: int = manifest["step"]
        self.world: int = manifest["world"]
        self.meta: dict[str, Any] = manifest["meta"]
        self._files: list[dict[str, list[Any]]] = manifest["files"]
        self._cursors: list[Any] = manifest["cursors"]
        self._rank, self._now = rank, world
        self._checked: set[tuple[int, str]] = set()

    def __repr__(self) -> str:
        return f"<Checkpoint step {self.step} of {self.world} rank(s) in {self.dir}>"

    @property
    def cursor(self) -> Any:
        """The cursor this rank saved (``save(cursor=...)``). ValueError if a
        different number of ranks saved it: their cursors split the input
        another way."""
        if self._now != self.world:
            raise ValueError(
                f"checkpoint at step {self.step} was saved by {self.world} rank(s) and this gang has {self._now}: "
                "a cursor resumes only the same number of ranks"
            )
        return self._cursors[self._rank]

    def path(self, name: str, rank: int | None = None) -> pathlib.Path:
        """The file `name` that `rank` (default: this rank) wrote in its save
        directory, as a local path (from an object store, downloaded first),
        checked against the size and SHA-256 it was saved with the first
        time it is asked for. ``CheckpointError`` if it is missing, changed,
        or that rank saved no such file."""
        r = self._rank if rank is None else rank
        if not 0 <= r < self.world:
            raise ValueError(f"rank {r} is not one of the {self.world} rank(s) that saved this checkpoint")
        saved = self._files[r].get(name)
        if saved is None or name.startswith("/") or ".." in name.split("/"):
            have = ", ".join(sorted(self._files[r])[:10]) or "none"
            raise CheckpointError(f"checkpoint at step {self.step}: rank {r} saved no file {name!r} (it saved: {have})")
        where = self._where.where(self._dirname, r, name)
        path = self._where.local(self._dirname, r, name, self.id)
        if (r, name) not in self._checked:
            from .ext import _digest

            try:
                found = list(_digest(path)) if path is not None else None
            except FileNotFoundError:
                found = None
            if found is None:
                raise CheckpointError(f"checkpoint at step {self.step}: {where} is gone")
            if found != saved:
                self._where.forget(path)
                raise CheckpointError(
                    f"checkpoint at step {self.step}: {where} changed since it was saved ({found[0]} bytes, "
                    f"sha256 {found[1][:12]}; saved {saved[0]} bytes, sha256 {saved[1][:12]})"
                )
            self._checked.add((r, name))
        assert path is not None
        return pathlib.Path(path)


class Checkpoints:
    """The checkpoints of one training job, by `name`, kept under
    ``root/<name>/``. `root` is a directory every rank can read and write
    (one host's disk only for a gang that fits on one host), or an
    ``ext.ObjectStore`` such as ``ext.S3Store``, which keys them under
    ``<name>/``. `keep` committed checkpoints stay in a directory; older
    ones are deleted after each commit (an object store keeps them all: it
    has no listing or deletion here).

    Inside a gang, ``latest()`` and ``save()`` are collectives: call them on
    every rank, in the same order. Outside one (a plain call, or the
    driver), they act alone, and a retried call's attempt is its generation.
    """

    def __init__(self, name: str, root: Any, *, keep: int = 2) -> None:
        from .ext import ObjectStore

        if not isinstance(name, str) or not name or name in (".", "..") or "/" in name or os.sep in name:
            raise ValueError(f"a checkpoint name is a non-empty directory name, got {name!r}")
        if isinstance(keep, bool) or not isinstance(keep, int) or keep < 1:
            raise ValueError(f"keep must be an integer >= 1, got {keep!r}")
        self.name, self.root, self.keep = name, root, keep
        self._files: _Disk | _Bucket = _Bucket(root, name) if isinstance(root, ObjectStore) else _Disk(root, name)
        self.directory = self._files.where()
        self._claim: tuple[str, int, str] | None = None  # (writer, generation, nonce) this process claimed

    def __repr__(self) -> str:
        return f"<Checkpoints {self.name!r} in {self.directory}>"

    def __reduce__(self) -> tuple[Any, ...]:
        return (_rebuild, (self.name, self.root, self.keep))

    def latest(self, *, claim: bool | None = None) -> Checkpoint | None:
        """The last committed checkpoint, or None.

        In a gang (or with ``claim=True``) this first claims the checkpoints
        for this generation, so no rank of the generation it replaced can
        commit any more; it raises ``LeaseLost`` if a later generation, or a
        newer run under this name, claimed them. Elsewhere (the driver, or a
        call that only reads them) it reads without claiming, so a running
        job keeps them; a call that trains claims them at its first save."""
        writer, generation, rank, world, group = _who()
        claiming = group is not None if claim is None else claim

        def decide() -> tuple[str | None, str | None, dict[str, Any] | None]:
            if claiming:
                record = self._take(writer, generation)
                nonce = record["writer"][2]
            else:
                record, nonce = self._store().get("pointer") or {}, None
                if record.get("id") is None:
                    record = self._mirrored()
            return nonce, record.get("dir"), self._manifest(record)

        nonce, dirname, manifest = _agree(group, rank, decide)
        if nonce is not None:
            self._claim = (writer, generation, nonce)
        if manifest is None or dirname is None:
            return None
        return Checkpoint(self._files, dirname, manifest, rank, world)

    def save(self, step: int, *, cursor: Any = None, meta: dict[str, Any] | None = None) -> Saving:
        """``with ckpts.save(step) as d:`` gives this rank's directory `d` (a
        ``pathlib.Path``) to write files into; when the block ends, the
        checkpoint commits, and ``.checkpoint`` on the object ``save``
        returned holds it. `cursor` (this rank's input position) and `meta`
        must be JSON."""
        if isinstance(step, bool) or not isinstance(step, int) or step < 0:
            raise ValueError(f"step must be an integer >= 0, got {step!r}")
        meta = {} if meta is None else meta
        for what, value in (("cursor", cursor), ("meta", meta)):
            try:
                json.dumps(value)
            except (TypeError, ValueError) as e:
                raise TypeError(f"a checkpoint's {what} must be JSON: {e}") from None
        return Saving(self, step, cursor, meta)

    # ---- the pointer -----------------------------------------------------------

    def _store(self) -> Any:
        from ._store import Store

        return Store(f"ckpt/{self.name}")

    def _nonce(self, writer: str, generation: int, rank: int, group: Any) -> str:
        """The nonce of this generation's claim, claiming once if needed."""
        if self._claim is None or self._claim[:2] != (writer, generation):
            nonce = _agree(group, rank, lambda: self._take(writer, generation)["writer"][2])
            self._claim = (writer, generation, nonce)
        return self._claim[2]

    def _take(self, writer: str, generation: int) -> dict[str, Any]:
        """Claim the pointer for `writer`'s `generation` (rank 0), unless it
        holds it already; return the record. ``LeaseLost`` if a later
        generation of its gang claimed it, or a newer run took it over."""
        store = self._store()
        while True:
            record, version = store.get_versioned("pointer")
            record = dict(record) if record else {"id": None, "dir": None, "step": None, "world": None, "seq": 0}
            held = record.get("writer")
            if held is not None and held[:2] == [writer, generation]:
                return record
            gens, claims = dict(record.get("gens", {})), list(record.get("claims", []))
            if gens.get(writer, -math.inf) > generation:
                why = "a newer run took them over" if gens[writer] == math.inf else f"generation {gens[writer]} did"
                raise LeaseLost(f"checkpoints {self.name!r}: generation {generation} of {writer} can't claim them: {why}")
            if held is not None and held[0] != writer:
                gens.pop(held[0], None)
                gens[held[0]] = math.inf  # taken over: that run never writes here again (and is remembered longest)
            gens.pop(writer, None)
            gens[writer] = generation
            for old in list(gens)[: max(0, len(gens) - GANGS)]:
                del gens[old]
            if record["id"] is None:
                record.update(self._mirrored())  # a pointer lost with its cluster: the files say what was last
            nonce = secrets.token_hex(4)
            record.update(writer=[writer, generation, nonce], gens=gens, claims=(claims + [nonce])[-CLAIMS:])
            if _put_pointer(store, record, version):
                return record

    def _commit(self, writer: list[Any], manifest: dict[str, Any], sha256: str, dirname: str) -> dict[str, Any]:
        """Move the pointer to `manifest` (rank 0) while `writer` still holds
        it; the record as this commit put it. A commit whose reply was lost
        is found committed."""
        store, put = self._store(), None
        while True:
            record, version = store.get_versioned("pointer")
            if record is not None and record["id"] == manifest["id"]:
                # Committed: by the put whose reply was lost. That put's record, not this one, whose
                # claims may list a claim made since, whose save directory pruning must spare.
                return put if put is not None else {**record, "claims": []}
            if record is None or record.get("writer", [None, None])[:2] != writer[:2]:  # the nonce only names dirs
                now = "nobody" if record is None else f"generation {record['writer'][1]} of {record['writer'][0]}"
                raise LeaseLost(
                    f"checkpoints {self.name!r}: step {manifest['step']} was not committed: {now} holds them "
                    f"now, not generation {writer[1]} of {writer[0]}"
                )
            put = {**record, "id": manifest["id"], "dir": dirname, "step": manifest["step"],
                   "world": manifest["world"], "manifest_sha256": sha256, "seq": record["seq"] + 1}  # fmt: skip
            if _put_pointer(store, put, version):
                return put

    def _manifest(self, record: dict[str, Any]) -> dict[str, Any] | None:
        """The manifest `record` names, checked against the hash it holds."""
        dirname = record.get("dir")
        if dirname is None:
            return None
        body = self._files.manifest(dirname)
        if body is None:
            raise CheckpointError(
                f"checkpoints {self.name!r}: the committed checkpoint {dirname} has no manifest in "
                f"{self.directory}: its files were removed, or this root is not where it was saved"
            )
        sha = hashlib.sha256(body).hexdigest()
        try:
            manifest = json.loads(body)
        except ValueError:
            manifest = None
        if (sha != record.get("manifest_sha256") or not isinstance(manifest, dict)
                or manifest.get("id") != record.get("id") or not _ID.fullmatch(str(record.get("id")))):  # fmt: skip
            raise CheckpointError(
                f"checkpoints {self.name!r}: the manifest of {dirname} is not the one committed "
                f"(sha256 {sha[:12]}, committed {str(record.get('manifest_sha256'))[:12]})"
            )
        return manifest

    def _mirrored(self) -> dict[str, Any]:
        """The last commit as the mirror beside the checkpoints records it, if
        its manifest is still there."""
        mirror = self._files.mirror()
        if not mirror or not isinstance(mirror.get("dir"), str) or not _STEP_DIR.fullmatch(mirror["dir"]):
            return {}
        if self._files.manifest(mirror["dir"]) is None:
            return {}
        return {k: mirror.get(k) for k in _MIRRORED}


_MIRRORED = ("id", "dir", "step", "world", "seq", "manifest_sha256")


def _put_pointer(store: Any, record: dict[str, Any], version: int) -> bool:
    """Compare-and-set the pointer: True once it is put; False to read it
    again and decide again, because it changed since it was read. A put whose
    reply was lost is repeated, and the version says whether it landed."""
    try:
        _retrying(lambda: store.put("pointer", record, if_version=version))
        return True
    except VersionConflict:
        return False  # changed since read, or our own earlier try landed: the next read says which


def _rebuild(name: str, root: Any, keep: int) -> Checkpoints:
    return Checkpoints(name, root, keep=keep)


class Saving:
    """One ``Checkpoints.save``: entering it gives this rank's directory, and
    leaving it commits. ``checkpoint`` is the committed checkpoint after."""

    def __init__(self, ckpts: Checkpoints, step: int, cursor: Any, meta: dict[str, Any]) -> None:
        self.ckpts, self.step, self.cursor, self.meta = ckpts, step, cursor, meta
        self.checkpoint: Checkpoint | None = None
        self.mine: str | None = None

    def __enter__(self) -> pathlib.Path:
        self.who = _who()
        writer, generation, rank, _, group = self.who
        nonce = self.ckpts._nonce(writer, generation, rank, group)
        counted = (self.ckpts.name, nonce)  # per process: another Checkpoints of this name counts on
        self.dirname = f"{self.step:010d}-{nonce}-n{_SAVES.get(counted, 0)}"
        _SAVES[counted] = _SAVES.get(counted, 0) + 1
        try:
            self.mine = self.ckpts._files.stage(self.dirname, rank)
        except Exception as e:
            with contextlib.suppress(Exception):  # tell the others, so none waits out the collective timeout
                self._exchange({}, f"rank {rank} could not make its save directory: {e}")
            raise
        return pathlib.Path(self.mine)

    def __exit__(self, kind: type[BaseException] | None, error: BaseException | None, tb: Any) -> bool:
        assert self.mine is not None
        rank, group = self.who[2], self.who[4]
        try:
            if error is not None and (group is None or not isinstance(error, Exception)):
                return False  # nothing to tell other ranks: the save stays uncommitted, and pruning removes it
            files, failed, sealing = {}, "", None
            if error is None:
                try:
                    files = _seal(self.mine)
                    self.ckpts._files.publish(self.dirname, rank, self.mine, files)
                except Exception as e:
                    sealing, failed = e, f"rank {rank} could not save its files: {type(e).__name__}: {e}"
            else:
                failed = f"rank {rank} raised {type(error).__name__}: {error}"
            try:
                manifest = self._exchange(files, failed)
            except Exception:
                # This rank's own failure goes up, not what the others made of it.
                if error is not None:
                    return False
                if sealing is not None:
                    raise sealing from None
                raise
            self.checkpoint = Checkpoint(self.ckpts._files, self.dirname, manifest, rank, self.who[3])
            return False
        finally:
            self.ckpts._files.discard(self.mine)

    def _exchange(self, files: dict[str, list[Any]], failed: str) -> dict[str, Any]:
        """Every rank's files to rank 0, which commits them; the manifest on
        every rank, or every rank raises. Every rank sends, even one that
        failed, and reads every part, as the object plane needs of a part
        too large to send inline."""
        writer, generation, rank, world, group = self.who
        sent = (files, self.cursor, failed)
        parts = [sent] if group is None else group.all_gather(sent)
        assert self.ckpts._claim is not None
        claim = [writer, generation, self.ckpts._claim[2]]
        return _agree(group, rank, lambda: self._commit(claim, parts))

    def _commit(self, writer: list[Any], parts: list[tuple[dict[str, list[Any]], Any, str]]) -> dict[str, Any]:
        """Rank 0: check every rank's files are there, write the manifest,
        and move the pointer to it."""
        failed = [why for _, _, why in parts if why]
        if failed:
            raise CheckpointError(f"step {self.step} was not saved: " + "; ".join(failed))
        where = self.ckpts._files
        for r, (files, _, _) in enumerate(parts):
            for name, (size, _) in files.items():
                seen = where.size(self.dirname, r, name)
                if seen != size:
                    raise CheckpointError(
                        f"step {self.step} was not saved: rank 0 can't see rank {r}'s {where.where(self.dirname, r, name)} "
                        f"as written ({size} bytes; it sees {seen}). Checkpoints need a root every rank shares"
                    )
        parent = self.ckpts._store().get("pointer") or {}
        manifest = {
            "name": self.ckpts.name, "id": uuid.uuid4().hex, "step": self.step, "world": len(parts),
            "writer": writer[:2], "parent": parent.get("id"), "parent_dir": parent.get("dir"),
            "files": [p[0] for p in parts], "cursors": [p[1] for p in parts], "meta": self.meta, "created": time.time(),
        }  # fmt: skip
        body = json.dumps(manifest).encode()
        where.put_manifest(self.dirname, body)
        record = self.ckpts._commit(writer, manifest, hashlib.sha256(body).hexdigest(), self.dirname)
        with contextlib.suppress(OSError):  # both tidy up: the commit stands without them
            where.put_mirror(record)
            where.prune(record, self.ckpts.keep)
        return manifest


# ---- where the files go ------------------------------------------------------------


class _Disk:
    """Checkpoints in a directory: ranks write in place, rank 0 sees their
    files through the shared filesystem, and old saves are deleted."""

    def __init__(self, root: str | os.PathLike[str], name: str) -> None:
        self.directory = os.path.join(os.fspath(root), name)

    def where(self, dirname: str = "", rank: int | None = None, name: str = "") -> str:
        parts = [self.directory, dirname, "" if rank is None else f"rank-{rank:05d}", name]
        return os.path.join(*[p for p in parts if p])

    def stage(self, dirname: str, rank: int) -> str:
        os.makedirs(self.where(dirname), exist_ok=True)
        mine = self.where(dirname, rank)
        os.mkdir(mine)  # never an existing one: its directory name is this save's alone
        return mine

    def publish(self, dirname: str, rank: int, path: str, files: dict[str, list[Any]]) -> None:
        pass  # written in place

    def discard(self, path: str) -> None:
        pass  # the save itself: an uncommitted one is pruned later

    def size(self, dirname: str, rank: int, name: str) -> int | None:
        try:
            return os.stat(self.where(dirname, rank, name)).st_size
        except FileNotFoundError:
            return None

    def local(self, dirname: str, rank: int, name: str, checkpoint: str) -> str | None:
        return self.where(dirname, rank, name)

    def forget(self, path: str) -> None:
        pass  # the file itself: nothing cached to drop

    def manifest(self, dirname: str) -> bytes | None:
        try:
            with open(self.where(dirname, None, "manifest.json"), "rb") as f:
                return f.read()
        except (FileNotFoundError, NotADirectoryError):
            return None

    def put_manifest(self, dirname: str, body: bytes) -> None:
        _write(self.where(dirname, None, "manifest.json"), body)  # syncs the save dir, so its rank dirs
        _sync_dir(self.directory)  # and the save dir's own entry

    def mirror(self) -> dict[str, Any] | None:
        return _read_json(self.where(MIRROR))

    def put_mirror(self, record: dict[str, Any]) -> None:
        """Record the commit beside the checkpoints, unless a later one is."""
        if (self.mirror() or {}).get("seq", -1) < record["seq"]:
            _write(self.where(MIRROR), json.dumps({k: record.get(k) for k in _MIRRORED}).encode())

    def prune(self, record: dict[str, Any], keep: int) -> None:
        """Delete save directories other than the newest `keep` commits (the
        pointer's and its ancestors) and the mirror's. Only directories of
        claims this record lists go: a claim made since (a newer generation
        or run, which may be writing now) is not among them."""
        kept: set[str] = set()
        dirname = record["dir"]
        while dirname and len(kept) < keep:
            kept.add(dirname)
            body = self.manifest(dirname)
            dirname = (_loads(body) or {}).get("parent_dir") if body else None
        kept.add((self.mirror() or {}).get("dir", ""))
        claims = set(record.get("claims", []))
        with os.scandir(self.directory) as entries:
            for entry in entries:
                found = _STEP_DIR.fullmatch(entry.name)
                if found and found[2] in claims and entry.name not in kept and entry.is_dir(follow_symlinks=False):
                    shutil.rmtree(entry.path, ignore_errors=True)


class _Bucket:
    """Checkpoints in an object store, keyed ``<name>/<save>/rank-NNNNN/...``:
    ranks write into a local staging directory and upload on leaving the
    block, with puts that never replace an object; restores download into a
    local cache that holds one checkpoint per name. Nothing is deleted from
    the store."""

    def __init__(self, store: Any, name: str) -> None:
        self.store, self.name = store, name
        self.cache = os.path.join(tempfile.gettempdir(), "chimaera-checkpoints", name)

    def where(self, dirname: str = "", rank: int | None = None, name: str = "") -> str:
        parts = [self.name, dirname, "" if rank is None else f"rank-{rank:05d}", name]
        return "/".join(p for p in parts if p)

    def stage(self, dirname: str, rank: int) -> str:
        return tempfile.mkdtemp(prefix=f"chimaera-ckpt-{dirname}-r{rank}-")

    def publish(self, dirname: str, rank: int, path: str, files: dict[str, list[Any]]) -> None:
        for name, (size, sha) in files.items():
            self._create(self.where(dirname, rank, name), os.path.join(path, name), sha, size)

    def discard(self, path: str) -> None:
        shutil.rmtree(path, ignore_errors=True)  # the local staging directory, uploaded or not

    def _create(self, key: str, source: str | bytes, sha256: str, size: int) -> None:
        """Put `source` (a file's path, or bytes) under `key`, which must not
        exist; one an earlier try whose reply was lost created (the key is
        this save's alone) counts."""

        def put() -> None:
            if isinstance(source, bytes):
                self.store.put(key, source, sha256=sha256, if_version="")
            else:
                with open(source, "rb") as f:
                    self.store.put(key, f, sha256=sha256, if_version="")

        try:
            _retrying(put)
        except VersionConflict:
            found = self.store.head(key)
            if found is None or found[0] != size:
                raise CheckpointError(f"{key} exists already with {found and found[0]} bytes, not {size}") from None

    def size(self, dirname: str, rank: int, name: str) -> int | None:
        found = _retrying(lambda: self.store.head(self.where(dirname, rank, name)))
        return None if found is None else found[0]

    def local(self, dirname: str, rank: int, name: str, checkpoint: str) -> str | None:
        """A local copy of the file (`checkpoint` is its id, checked to be
        one): downloaded once, into a cache that keeps this name's newest
        checkpoint only."""
        if not _ID.fullmatch(checkpoint):
            raise CheckpointError(f"checkpoints {self.name!r}: {checkpoint!r} is not a checkpoint id")
        path = os.path.join(self.cache, checkpoint, f"rank-{rank:05d}", name)
        if not os.path.exists(path):
            got = _retrying(lambda: self.store.get(self.where(dirname, rank, name)))
            if got is None:
                return None
            with contextlib.suppress(FileNotFoundError), os.scandir(self.cache) as entries:
                for entry in entries:  # other checkpoints of this name: superseded by this one
                    if entry.name != checkpoint and _ID.fullmatch(entry.name):
                        shutil.rmtree(entry.path, ignore_errors=True)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = f"{path}.{secrets.token_hex(4)}"
            with open(tmp, "wb") as f:
                f.write(got[0])
            os.replace(tmp, path)
        return path

    def forget(self, path: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)  # a bad download: fetch it again next time

    def manifest(self, dirname: str) -> bytes | None:
        got = _retrying(lambda: self.store.get(self.where(dirname, None, "manifest.json")))
        return None if got is None else got[0]

    def put_manifest(self, dirname: str, body: bytes) -> None:
        self._create(self.where(dirname, None, "manifest.json"), body, hashlib.sha256(body).hexdigest(), len(body))

    def mirror(self) -> dict[str, Any] | None:
        got = _retrying(lambda: self.store.get(self.where(MIRROR)))
        return None if got is None else _loads(got[0])

    def put_mirror(self, record: dict[str, Any]) -> None:
        """Record the commit beside the checkpoints, unless a later one is:
        each put is conditional on the version read."""
        key, body = self.where(MIRROR), json.dumps({k: record.get(k) for k in _MIRRORED}).encode()
        while True:
            got = _retrying(lambda: self.store.get(key))
            if got is not None and (_loads(got[0]) or {}).get("seq", -1) >= record["seq"]:
                return
            try:
                _retrying(lambda: self.store.put(key, body, if_version="" if got is None else got[1]))
                return
            except VersionConflict:
                continue

    def prune(self, record: dict[str, Any], keep: int) -> None:
        pass  # an ObjectStore here has no listing or deletion


def _seal(directory: str) -> dict[str, list[Any]]:
    """Every file under `directory`, synced to disk: name -> [size, sha256]."""
    files: dict[str, list[Any]] = {}
    for base, _, names in os.walk(directory):
        for name in names:
            path = os.path.join(base, name)
            with open(path, "rb") as f:
                os.fsync(f.fileno())
                files[os.path.relpath(path, directory).replace(os.sep, "/")] = [
                    os.fstat(f.fileno()).st_size,
                    hashlib.file_digest(f, "sha256").hexdigest(),
                ]
        _sync_dir(base)
    return files


def _sync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _loads(data: bytes) -> dict[str, Any] | None:
    try:
        value = json.loads(data)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def _read_json(path: str) -> dict[str, Any] | None:
    try:
        with open(path, "rb") as f:
            return _loads(f.read())
    except (FileNotFoundError, NotADirectoryError):
        return None


def _write(path: str, body: bytes) -> None:
    """Replace `path` with `body`, synced: readers see the old file or the
    new one, and a crash after this returns keeps the new one."""
    directory = os.path.dirname(path)
    tmp = os.path.join(directory, f".{os.path.basename(path)}.{secrets.token_hex(4)}")
    with open(tmp, "wb") as f:
        f.write(body)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _sync_dir(directory)
