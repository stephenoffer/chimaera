import datetime as dt

import pytest

import chimaera as ch
from chimaera import _units
from chimaera._options import Layer, resolve, validate


def test_sizes_need_a_byte_unit():
    assert _units.parse_bytes("32 GiB") == 32 << 30
    assert _units.parse_bytes("2GB") == 2_000_000_000
    assert _units.parse_bytes(1024) == 1024
    with pytest.raises(ValueError, match="byte unit"):
        _units.parse_bytes("512m")
    with pytest.raises(ValueError, match="whole number"):
        _units.parse_bytes("1.5 B")
    with pytest.raises(TypeError):
        _units.parse_bytes(True)


def test_durations():
    assert _units.parse_duration("250ms") == 0.25
    assert _units.parse_duration("5m") == 300
    assert _units.parse_duration(dt.timedelta(seconds=2)) == 2
    with pytest.raises(ValueError):
        _units.parse_duration("-1s")


def test_unknown_option_suggests_a_name():
    with pytest.raises(TypeError, match="did you mean 'memory'"):
        @ch.remote(memroy="1 GiB")
        def f():
            pass


def test_option_in_the_wrong_place_is_rejected():
    @ch.remote
    def f():
        pass

    with pytest.raises(TypeError, match="applies to pools"):
        f.options(replicas=3)
    with pytest.raises(TypeError, match="only be set on the decorator"):
        f.options(workflow="durable")


def test_precedence_view_over_scope_over_decorator():
    @ch.remote(cpus=1, memory="1 GiB")
    def f():
        pass

    with ch.options(cpus=2, memory="2 GiB"):
        with ch.options(cpus=3):
            eff = f.options(cpus=4).policy()
            assert eff.get("cpus") == 4
            assert ".options at" in eff.sources["cpus"]
            eff = f.policy()
            assert eff.get("cpus") == 3
            assert eff.get("memory") == 2 << 30
            assert "with ch.options" in eff.sources["memory"]
    assert f.policy().get("cpus") == 1


def test_resolved_options_follow_scopes_entered_after_an_earlier_call():
    """Options are resolved once and reused while scopes are unchanged; a scope
    entered or left must still take effect on the next call."""
    from chimaera._remote import Actor

    @ch.remote(cpus=1)
    def f():
        pass

    @ch.remote
    class A:
        @ch.method(priority=2)
        def m(self):
            pass

    handle = Actor(None, b"a" * 16, b"c" * 16, A._cls, A.policy())  # type: ignore[arg-type]
    assert f.policy().get("cpus") == 1
    assert handle.m._policy().get("deadline") is None
    with ch.options(cpus=5, deadline="30s"):
        assert f.policy().get("cpus") == 5
        assert handle.m._policy().get("deadline") is not None
        assert handle.m._policy().get("priority") == 2
    assert f.policy().get("cpus") == 1
    assert handle.m._policy().get("deadline") is None
    assert f.policy() is f.policy(), "unchanged scopes reuse the resolved options"


def test_scopes_do_not_leak_across_threads():
    import threading

    @ch.remote
    def f():
        pass

    seen = []
    with ch.options(cpus=7):
        t = threading.Thread(target=lambda: seen.append(f.policy().get("cpus")))
        t.start()
        t.join()
    assert seen == [None]


def test_effects_only_tighten():
    @ch.remote(effects="idempotent")
    def f():
        pass

    assert f.options(effects="external").policy().get("effects") == "external"
    with pytest.raises(ch.OptionsError, match="loosen"):
        f.options(effects="none").policy()


def test_external_effects_start_at_most_once():
    @ch.remote(effects="external")
    def pay():
        pass

    assert pay.policy().get("max_attempts") == 1
    with pytest.raises(ch.OptionsError, match="at most once"):
        pay.options(max_attempts=3).policy()


def test_validate_names_the_source():
    with pytest.raises(ch.OptionsError, match=r"cpus.*\(here\)"):
        validate({"cpus": -1}, kind="view", target="function", source="here")
    eff = resolve((), Layer({}, "d", "decorator"))
    assert eff.get("max_attempts") == 3 and eff.sources["max_attempts"] == "default"
