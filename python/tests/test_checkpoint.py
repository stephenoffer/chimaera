"""ch.dist.Checkpoints: a committed pointer fenced by generation, files checked
on restore, pruning by lineage, and gangs that resume after losing a rank."""

from __future__ import annotations

import json
import os
import time
import uuid

import pytest

import chimaera as ch
from chimaera import _checkpoint, _store


def named() -> str:
    return f"t-{uuid.uuid4().hex[:8]}"


def acting(monkeypatch, writer: str, generation: int) -> None:
    """Save and restore as `generation` of gang `writer`, a single rank."""
    monkeypatch.setattr(_checkpoint, "_who", lambda: (writer, generation, 0, 1, None))


def write(ckpts, step: int, data: bytes = b"x", cursor=None):
    saving = ckpts.save(step, cursor=cursor)
    with saving as d:
        (d / "state.bin").write_bytes(data)
    return saving.checkpoint


def pointer(name: str) -> dict:
    return _store.Store(f"ckpt/{name}").get("pointer")


def test_a_save_commits_its_files_cursor_and_meta_and_latest_returns_them(tmp_path):
    ckpts = ch.dist.Checkpoints(named(), tmp_path)
    assert ckpts.latest() is None
    saving = ckpts.save(3, cursor={"epoch": 1, "positions": [[0, 5]]}, meta={"loss": 0.25})
    with saving as d:
        (d / "model.bin").write_bytes(b"weights")
        (d / "sub").mkdir()
        (d / "sub" / "opt.bin").write_bytes(b"optimizer")
    got = ch.dist.Checkpoints(ckpts.name, tmp_path).latest()
    assert (got.id, got.step, got.world, got.meta) == (saving.checkpoint.id, 3, 1, {"loss": 0.25})
    assert got.cursor == {"epoch": 1, "positions": [[0, 5]]}
    assert got.path("model.bin").read_bytes() == b"weights"
    assert got.path("sub/opt.bin", rank=0).read_bytes() == b"optimizer"
    with pytest.raises(ch.dist.CheckpointError, match=r"saved no file 'other.bin' \(it saved: model.bin, sub/opt.bin\)"):
        got.path("other.bin")


def test_a_replaced_generation_can_neither_commit_nor_claim_and_a_newer_run_takes_over_for_good(tmp_path, monkeypatch):
    name = named()
    a1, late, a2, b = (ch.dist.Checkpoints(name, tmp_path) for _ in range(4))
    acting(monkeypatch, "A", 1)
    write(a1, 1)
    acting(monkeypatch, "A", 2)
    assert a2.latest(claim=True).step == 1  # generation 2 restores, and so claims
    acting(monkeypatch, "A", 1)
    with pytest.raises(ch.errors.LeaseLost, match="generation 2 of A holds them now, not generation 1 of A"):
        write(a1, 2)  # generation 1 claimed before: its commit is refused
    with pytest.raises(ch.errors.LeaseLost, match="generation 2 did"):
        late.latest(claim=True)  # and it can't claim again
    assert pointer(name)["step"] == 1
    acting(monkeypatch, "B", 1)
    assert b.latest(claim=True).step == 1  # a newer run under the name takes them over
    write(b, 5)
    for generation in (2, 3):  # the run it took them from never gets them back, at any generation
        acting(monkeypatch, "A", generation)
        with pytest.raises(ch.errors.LeaseLost, match="a newer run took them over"):
            ch.dist.Checkpoints(name, tmp_path).latest(claim=True)
    acting(monkeypatch, "A", 2)
    with pytest.raises(ch.errors.LeaseLost, match="generation 1 of B holds them now"):
        write(a2, 6)  # nor can the generation that held them when B took over commit
    acting(monkeypatch, "B", 1)
    assert ch.dist.Checkpoints(name, tmp_path).latest().step == 5


def test_a_commit_whose_reply_was_lost_is_found_committed_once(tmp_path, monkeypatch):
    ckpts = ch.dist.Checkpoints(named(), tmp_path)
    ckpts.latest(claim=True)  # claims: the next put of the pointer is the commit
    put, lost = _store.Store.put, []

    def lossy(self, key, value, **kw):
        version = put(self, key, value, **kw)
        if self.namespace == f"ckpt/{ckpts.name}" and not lost:
            lost.append(version)
            raise ConnectionError("the reply to this put was lost")
        return version

    monkeypatch.setattr(_store.Store, "put", lossy)
    saved = write(ckpts, 7)
    assert lost, "the commit's put lost its reply"
    assert (pointer(ckpts.name)["id"], pointer(ckpts.name)["seq"]) == (saved.id, 1), "one commit, found on retry"


def test_a_changed_or_missing_file_is_refused_on_restore(tmp_path):
    ckpts = ch.dist.Checkpoints(named(), tmp_path)
    saving = ckpts.save(1)
    with saving as d:
        (d / "a.bin").write_bytes(b"aaaa")
        (d / "b.bin").write_bytes(b"bbbb")
    mine = os.path.join(saving.checkpoint.dir, "rank-00000")
    with open(os.path.join(mine, "a.bin"), "wb") as f:
        f.write(b"AAAA")  # same size, other bytes
    os.unlink(os.path.join(mine, "b.bin"))
    got = ckpts.latest()
    with pytest.raises(ch.dist.CheckpointError, match=r"a.bin changed since it was saved \(4 bytes, sha256 "):
        got.path("a.bin")
    with pytest.raises(ch.dist.CheckpointError, match=r"b.bin is gone"):
        got.path("b.bin")


def test_pruning_keeps_the_newest_commits_and_spares_directories_of_claims_it_does_not_know(tmp_path, monkeypatch):
    name = named()
    acting(monkeypatch, "A", 1)
    first = ch.dist.Checkpoints(name, tmp_path, keep=2)
    write(first, 1)
    with pytest.raises(RuntimeError, match="died mid-save"), first.save(2) as d:
        (d / "half.bin").write_bytes(b"h")
        raise RuntimeError("died mid-save")  # an uncommitted save, as a crashed generation leaves one
    foreign = tmp_path / name / "0000000099-ffffffff-n0"  # another claim's save, perhaps still being written
    (foreign / "rank-00000").mkdir(parents=True)
    acting(monkeypatch, "A", 2)
    second = ch.dist.Checkpoints(name, tmp_path, keep=2)
    assert second.latest().step == 1
    third, fourth = write(second, 3), write(second, 4)
    kept = sorted(os.listdir(tmp_path / name))
    expect = sorted([os.path.basename(third.dir), os.path.basename(fourth.dir), foreign.name, _checkpoint.MIRROR])
    assert kept == expect, "steps 1 and 2 (committed and abandoned) are gone; 3, 4 and the unknown claim's stay"
    assert json.loads((tmp_path / name / _checkpoint.MIRROR).read_text())["dir"] == os.path.basename(fourth.dir)


def test_a_pointer_lost_with_its_cluster_is_found_from_the_mirror_beside_the_checkpoints(tmp_path):
    ckpts = ch.dist.Checkpoints(named(), tmp_path)
    write(ckpts, 2, b"two")
    _store.Store(f"ckpt/{ckpts.name}").delete("pointer")
    assert ch.dist.Checkpoints(ckpts.name, tmp_path).latest().step == 2, "a read finds it too"
    got = ch.dist.Checkpoints(ckpts.name, tmp_path).latest(claim=True)
    assert (got.step, got.path("state.bin").read_bytes()) == (2, b"two")
    write(ckpts, 3)
    assert (pointer(ckpts.name)["step"], pointer(ckpts.name)["seq"]) == (3, 2), "commits count on from the mirror"


def test_bad_arguments_are_refused_before_anything_is_written(tmp_path):
    for name in ("", "a/b", "..", 3):
        with pytest.raises(ValueError, match="non-empty directory name"):
            ch.dist.Checkpoints(name, tmp_path)
    with pytest.raises(ValueError, match="keep must be"):
        ch.dist.Checkpoints("x", tmp_path, keep=0)
    ckpts = ch.dist.Checkpoints(named(), tmp_path)
    with pytest.raises(TypeError, match="cursor must be JSON"):
        ckpts.save(1, cursor=object())
    with pytest.raises(ValueError, match="step must be"):
        ckpts.save(-1)
    assert not os.path.exists(ckpts.directory)


def test_reading_the_latest_checkpoint_outside_a_gang_leaves_it_with_the_job(tmp_path, monkeypatch):
    name = named()
    acting(monkeypatch, "trainer", 1)
    trainer = ch.dist.Checkpoints(name, tmp_path)
    write(trainer, 1)
    acting(monkeypatch, "evaluator", 1)
    assert ch.dist.Checkpoints(name, tmp_path).latest().step == 1  # reads, claims nothing
    acting(monkeypatch, "trainer", 1)
    write(trainer, 2)  # the job still commits
    acting(monkeypatch, "evaluator", 1)
    ch.dist.Checkpoints(name, tmp_path).latest(claim=True)  # this one takes them over
    acting(monkeypatch, "trainer", 1)
    with pytest.raises(ch.errors.LeaseLost):
        write(trainer, 3)


@pytest.mark.parametrize("where", ["directory", "object store"])
def test_two_checkpoints_objects_of_one_writer_never_share_a_save(tmp_path, where):
    from test_data import Bucket

    root, name = (str(tmp_path) if where == "directory" else Bucket()), named()
    write(ch.dist.Checkpoints(name, root), 5, b"a5a5")
    write(ch.dist.Checkpoints(name, root), 5, b"B5B5")  # same writer, same step, same size
    assert ch.dist.Checkpoints(name, root).latest().path("state.bin").read_bytes() == b"B5B5"


def test_a_manifest_changed_after_its_commit_is_refused(tmp_path):
    ckpts = ch.dist.Checkpoints(named(), tmp_path)
    saved = write(ckpts, 1)
    path = os.path.join(saved.dir, "manifest.json")
    with open(path) as f:
        manifest = json.load(f)
    manifest["id"] = "/somewhere/else"
    with open(path, "w") as f:
        json.dump(manifest, f)
    with pytest.raises(ch.dist.CheckpointError, match="is not the one committed"):
        ckpts.latest()


def test_a_commit_found_after_a_lost_reply_spares_the_save_of_a_claim_made_meanwhile(tmp_path, monkeypatch):
    name = named()
    acting(monkeypatch, "A", 1)
    ckpts = ch.dist.Checkpoints(name, tmp_path, keep=1)
    write(ckpts, 1)
    put, newer = _store.Store.put, []

    def racing(self, key, value, **kw):
        version = put(self, key, value, **kw)
        if self.namespace == f"ckpt/{name}" and value.get("step") == 2 and not newer:
            newer.append(None)
            acting(monkeypatch, "A", 2)  # generation 2 claims after the commit landed, and starts a save
            later = ch.dist.Checkpoints(name, tmp_path)
            later.latest(claim=True)
            newer[0] = tmp_path / name / f"0000000009-{later._claim[2]}-n0" / "rank-00000"
            newer[0].mkdir(parents=True)
            acting(monkeypatch, "A", 1)
            raise ConnectionError("the reply to the commit was lost")
        return version

    monkeypatch.setattr(_store.Store, "put", racing)
    write(ckpts, 2)  # found committed on retry, then prunes with keep=1
    assert newer[0].is_dir(), "the newer claim's save in progress is not pruned"


# ---- gangs ---------------------------------------------------------------------


@ch.remote(cpus=0.5)
def count_to_four(name: str, root: str, crash: str):
    """Add 1..4 into a running total, a checkpoint per step, each rank saving
    its own total. Generation 1 dies once while saving step 3, where `crash`
    says. Returns (generation, the step it restored, its total)."""
    g = ch.context().gang
    ckpts = ch.dist.Checkpoints(name, root)
    ckpt = ckpts.latest()
    restored = ckpt.step if ckpt else 0
    total = json.loads(ckpt.path("total.json").read_text()) if ckpt else 0
    if g.generation == 1 and g.rank == 0 and crash in ("before commit", "after commit"):
        commit = ckpts._commit

        def dying(writer, manifest, *rest):
            if manifest["step"] == 3:
                if crash == "after commit":
                    commit(writer, manifest, *rest)
                os._exit(1)
            return commit(writer, manifest, *rest)

        ckpts._commit = dying
    for step in range(restored + 1, 5):
        total += step
        with ckpts.save(step, meta={"rank0_total": total}) as d:
            (d / "total.json").write_text(json.dumps(total))
            if crash == "inside" and g.generation == 1 and g.rank == 1 and step == 3:
                os._exit(1)  # rank 1 dies with its files half there
    return g.generation, restored, total


@pytest.mark.parametrize(("crash", "restored"), [("inside", 2), ("before commit", 2), ("after commit", 3)])
def test_a_gang_that_loses_a_rank_while_saving_restores_the_last_committed_step(tmp_path, crash, restored):
    name = named()
    gang = count_to_four.options(ranks=2, collective_timeout=30).gang(name, str(tmp_path), crash)
    assert gang.result(timeout=240) == [(2, restored, 10)] * 2
    assert pointer(name)["step"] == 4 and pointer(name)["writer"][1] == 2


@ch.remote(cpus=0.5)
class Saver:
    def __init__(self, name: str, root: str) -> None:
        self.ckpts = ch.dist.Checkpoints(name, root)

    def save(self, fails: int):
        g = ch.context().gang
        start = time.monotonic()
        try:
            with self.ckpts.save(1) as d:
                (d / "x").write_text("x")
                if g.rank == fails:
                    raise ValueError(f"rank {g.rank} broke")
        except Exception as e:
            return type(e).__name__, str(e), time.monotonic() - start
        return "saved", "", time.monotonic() - start

    def save_unstaged(self, fails: int):
        g = ch.context().gang
        if g.rank == fails:

            def full(dirname, rank):
                raise OSError("no space left on device")

            self.ckpts._files.stage = full
        start = time.monotonic()
        try:
            with self.ckpts.save(2) as d:
                (d / "x").write_text("x")
        except Exception as e:
            return type(e).__name__, str(e), time.monotonic() - start
        return "saved", "", time.monotonic() - start


@ch.remote(cpus=0.5)
def save_large_parts(name: str, root: str) -> list:
    ckpts = ch.dist.Checkpoints(name, root)
    with ckpts.save(1, cursor={"pad": "x" * 100_000}) as d:  # each rank's part is too large to send inline
        for i in range(50):
            (d / f"shard-{i}").write_text(str(i))
    got = ckpts.latest()
    return [len(got.cursor["pad"]), got.path("shard-49").read_text()]


def test_a_save_whose_parts_go_through_the_object_plane_commits(tmp_path):
    out = save_large_parts.options(ranks=2, collective_timeout=20, max_attempts=1).gang(named(), str(tmp_path))
    assert out.result(timeout=120) == [[100_000, "49"]] * 2


def test_a_rank_that_raises_while_saving_makes_every_rank_raise_at_once_and_nothing_commits(tmp_path):
    name = named()
    with Saver.options(ranks=3, collective_timeout=60).gang(name, str(tmp_path)) as g:
        out = g.save.remote(1).result(timeout=120)
    assert out[1][:2] == ("ValueError", "rank 1 broke"), out
    for r in (0, 2):
        assert out[r][0] == "CheckpointError" and "rank 1 raised ValueError: rank 1 broke" in out[r][1], out
    assert all(waited < 20 for *_, waited in out), f"nobody waits out collective_timeout: {out}"
    assert pointer(name)["id"] is None


def test_checkpoints_in_an_object_store_upload_each_file_once_and_check_downloads(tmp_path, monkeypatch):
    import tempfile

    from test_data import Bucket

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))  # staging and the download cache
    bucket, name = Bucket(), named()
    ckpts = ch.dist.Checkpoints(name, bucket)
    ckpts.latest(claim=True)  # claims, which fixes the save's key
    dirname = f"0000000004-{ckpts._claim[2]}-n0"
    shard, manifest = f"{name}/{dirname}/rank-00000/model.bin", f"{name}/{dirname}/manifest.json"
    bucket.lose = {shard, manifest}  # both puts take effect and lose their reply
    saving = ckpts.save(4, cursor={"at": 4})
    with saving as d:
        (d / "model.bin").write_bytes(b"weights")
    assert not d.exists(), "the staging directory goes once uploaded"
    assert (bucket.puts[shard], bucket.puts[manifest]) == (1, 1), "a put whose reply was lost is not repeated"
    assert json.loads(bucket.objects[f"{name}/{_checkpoint.MIRROR}"][0])["dir"] == dirname

    got = ch.dist.Checkpoints(name, bucket).latest()
    assert (got.step, got.cursor, got.path("model.bin").read_bytes()) == (4, {"at": 4}, b"weights")
    cached = got.path("model.bin")
    os.unlink(cached)
    good = bucket.objects[shard]
    bucket.objects[shard] = (b"WEIGHTS", good[1])  # changed in the store
    again = ch.dist.Checkpoints(name, bucket).latest()
    with pytest.raises(ch.dist.CheckpointError, match="model.bin changed since it was saved"):
        again.path("model.bin")
    assert not cached.exists(), "a bad download is dropped, to be fetched again"
    bucket.objects[shard] = good
    assert ch.dist.Checkpoints(name, bucket).latest().path("model.bin").read_bytes() == b"weights"


def test_a_rank_that_cannot_make_its_save_directory_fails_the_save_on_every_rank_at_once(tmp_path):
    with Saver.options(ranks=2, collective_timeout=60).gang(named(), str(tmp_path)) as g:
        out = g.save_unstaged.remote(1).result(timeout=120)
    assert out[1][:2] == ("OSError", "no space left on device"), out
    assert out[0][0] == "CheckpointError" and "rank 1 could not make its save directory" in out[0][1], out
    assert all(waited < 20 for *_, waited in out), f"nobody waits out collective_timeout: {out}"


def test_an_object_store_save_that_fails_leaves_no_staging_directory(tmp_path, monkeypatch):
    import tempfile

    from test_data import Bucket

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    ckpts = ch.dist.Checkpoints(named(), Bucket())
    with pytest.raises(RuntimeError, match="broke"), ckpts.save(1) as d:
        (d / "x").write_text("x")
        raise RuntimeError("broke")
    assert not list(tmp_path.glob("chimaera-ckpt-*")), "the local staging directory is gone"
