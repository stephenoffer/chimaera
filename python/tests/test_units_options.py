import datetime as dt
import math
import os
import time

import pytest

import chimaera as ch
from chimaera import _direct, _units
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


def test_precedence_view_over_decorator_over_scope():
    @ch.remote(cpus=1, memory="1 GiB")
    def f():
        pass

    with ch.defaults(cpus=2, deadline="30s"):
        with ch.defaults(cpus=3, priority=5):
            eff = f.options(cpus=4).policy()
            assert eff.get("cpus") == 4
            assert ".options at" in eff.sources["cpus"]
            eff = f.policy()
            assert eff.get("cpus") == 1, "a block fills in only what the decorator leaves unset"
            assert eff.get("memory") == 1 << 30
            assert eff.get("priority") == 5 and "with ch.defaults" in eff.sources["priority"]
            assert eff.get("deadline") is not None
    assert f.policy().get("priority") is None


def test_a_block_never_reaches_chimaeras_own_helpers_nor_names_actors():
    from chimaera._coord import _QueueHost

    with ch.defaults(cpus=8, max_attempts=5):
        assert _QueueHost.policy().get("cpus") != 8
    with pytest.raises(ch.OptionsError, match="names one actor"):
        ch.defaults(name="shared")
    with pytest.warns(UserWarning, match="applies to nothing"):
        ch.defaults(deadline="1s")  # not used with `with`


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
    assert handle.m.policy().get("deadline") is None
    with ch.defaults(priority=5, deadline="30s"):
        assert f.policy().get("priority") == 5
        assert handle.m.policy().get("deadline") is not None
        assert handle.m.policy().get("priority") == 2
    assert f.policy().get("priority") is None
    assert handle.m.policy().get("deadline") is None
    assert f.policy() is f.policy(), "unchanged scopes reuse the resolved options"


def test_scopes_do_not_leak_across_threads():
    import threading

    @ch.remote
    def f():
        pass

    seen = []
    with ch.defaults(cpus=7):
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


def test_a_durability_its_value_cannot_have_fails_before_it_is_stored():
    @ch.remote(durability="replayable", effects="external")
    def charge():
        pass

    with pytest.raises(ch.OptionsError, match="runs once"):
        charge.remote()

    @ch.remote(durability="replayable")
    class Box:
        pass

    with pytest.raises(ch.OptionsError, match="actor"):
        Box.remote()
    with pytest.raises(ch.OptionsError, match="replayable"):
        ch.put(1, durability="replayable")  # type: ignore[arg-type]


def test_a_retained_value_needs_another_peer_and_an_ephemeral_one_does_not():
    @ch.remote(durability="retained")
    def big():
        return b"x" * (1 << 20)

    # The test peer runs alone: there is no other peer to hold a copy.
    with pytest.raises(ch.Infeasible, match="runs alone"):
        big.remote().result(timeout=120)
    with pytest.raises(ch.Infeasible, match="runs alone"):
        ch.put(b"x" * (1 << 20), durability="retained")
    assert len(big.options(durability="ephemeral").remote().result(timeout=120)) == 1 << 20
    assert len(ch.put(b"x" * (1 << 20), durability="ephemeral").result()) == 1 << 20


def test_a_retained_actor_result_goes_through_the_peer_and_needs_another():
    @ch.remote
    class Maker:
        def big(self):
            return b"x" * (1 << 20)

    m = Maker.remote()
    m.big.remote().result(timeout=120)
    path = _direct.socket_path(_direct.socket_dir(ch.runs.current_run()._rt), m._actor)
    deadline = time.monotonic() + 10
    while not os.path.exists(path) and time.monotonic() < deadline:
        time.sleep(0.01)
    before = _direct._counter["direct"]
    m.big.remote().result(timeout=120)
    assert _direct._counter["direct"] == before + 1, "a default call goes straight to the actor"
    with pytest.raises(ch.Infeasible, match="runs alone"):
        m.big.options(durability="retained").remote().result(timeout=120)
    assert _direct._counter["direct"] == before + 1, "a retained one goes through the peer"


def test_validate_names_the_source():
    with pytest.raises(ch.OptionsError, match=r"cpus.*\(here\)"):
        validate({"cpus": -1}, kind="view", target="function", source="here")
    eff = resolve((), Layer({}, "d", "decorator"))
    assert eff.get("max_attempts") == 3 and eff.sources["max_attempts"] == "default"


def test_the_options_type_names_every_option():
    from chimaera._options import FIELDS, Options

    assert set(Options.__annotations__) == set(FIELDS)


def test_byte_sizes_are_exact_and_fit_the_wire():
    assert _units.parse_bytes("9007199254740993 B") == 9007199254740993, "a float would lose the last byte"
    assert _units.parse_bytes("1.5 KiB") == 1536
    assert _units.parse_bytes(2**64 - 1) == 2**64 - 1
    with pytest.raises(ValueError, match=r"memory: '1.0000000000000001 KB' is not a whole number of bytes"):
        _units.parse_bytes("1.0000000000000001 KB", "memory")
    for big in (2**64, "18446744073709551616 B", "16777216 TiB", "99999999999999999999999 TiB"):
        with pytest.raises(ValueError, match="memory: .* is more than 16 EiB"):
            _units.parse_bytes(big, "memory")

    @ch.remote
    def f():
        pass

    for name in ("memory", "memory_limit", "max_in_flight_bytes"):
        with pytest.raises(ch.OptionsError, match=rf"^{name}: .*16 EiB.*\(\.options at test_units_options"):
            f.options(**{name: "20000000 TiB"})
        with pytest.raises(ch.OptionsError, match=rf"^{name}: .*whole number of bytes"):
            f.options(**{name: "0.5 B"})
    # Counts travel as 32-bit integers; a larger one used to be cut down silently.
    for name in ("concurrency", "max_attempts", "max_in_flight", "returns"):
        with pytest.raises(ch.OptionsError, match=rf"^{name}: 4294967296 is more than 4294967295"):
            f.options(**{name: 2**32})
    with pytest.raises(ch.OptionsError, match="^priority: expected an integer from"):
        f.options(priority=2**31)
    assert f.options(concurrency=2**32 - 1, priority=-(2**31)).policy().native["priority"] == -(2**31)


def test_an_actors_lanes_together_fit_the_wire():
    # The peer gets an actor's default lane plus its groups and method lanes as
    # one 32-bit count; a sum past it used to wrap, to 1 call at a time.
    @ch.remote(concurrency=2**32 - 3)
    class Wide:
        def __init__(self):
            self.concurrency = ch.context().policy["concurrency"]

        @ch.method(concurrency=2)
        def side(self):
            return self.concurrency

    actor = Wide.remote()
    assert actor.side.remote().result(timeout=120) == 2**32 - 1
    actor.close()
    with pytest.raises(
        ch.OptionsError, match=r"^concurrency=4294967294 \(\.options at .*\) plus .*\(side=2\) is 4294967296, more than"
    ):
        Wide.options(concurrency=2**32 - 2).remote()

    @ch.remote(groups={"io": 2**32 - 1})
    class Grouped:
        pass

    with pytest.raises(ch.OptionsError, match=r"^concurrency=1 \(default\) plus .*\(io=4294967295\) is 4294967296"):
        Grouped.remote()

    @ch.remote
    class Batched:
        @ch.method(batch=ch.Batch(max_size=2**31))
        def embed(self, xs):
            return xs

    with pytest.raises(ch.OptionsError, match=r"\(embed=4294967296\) is 4294967297"):
        Batched.remote()

    # Under the options, the native layer refuses such a count rather than wrapping it.
    from chimaera._remote import build_call
    from chimaera._runtime import CALL_FUNCTION, Runtime

    @ch.remote
    def noop():
        pass

    rt = Runtime.get()
    spec, _ = build_call(rt, noop._fn, (), {}, kind=CALL_FUNCTION, eff=noop.policy())
    with pytest.raises(ValueError, match="^concurrency: 4294967296 is more than 4294967295"):
        rt.client.submit([{**spec, "policy": {**spec["policy"], "concurrency": 2**32}}])


@pytest.mark.parametrize(
    "bad",
    [math.inf, -math.inf, math.nan, "9" * 400 + "s", 10**400, 1e300, dt.timedelta.max],
    ids=["inf", "-inf", "nan", "400 digits", "10**400", "1e300", "timedelta.max"],
)
def test_durations_and_numbers_must_be_finite_when_set(bad):
    @ch.remote
    def f():
        pass

    for name in ("deadline", "latency", "cache"):
        with pytest.raises(ch.OptionsError, match=rf"^{name}: "):
            f.options(**{name: bad})
    with pytest.raises(ValueError, match="max_wait"):
        ch.Batch(max_wait=bad)
    if isinstance(bad, (int, float)) and not math.isfinite(_units.as_float(bad)):  # 1e300 is a fine amount
        numbers = {"cpus": bad, "gpus": bad, "resources": {"tpu": bad}, "budget": {"cpu_seconds": bad}}
        for name, value in numbers.items():
            with pytest.raises(ch.OptionsError, match=rf"^{name}"):
                f.options(**{name: value})


def test_long_durations_and_far_deadlines_are_refused_before_they_reach_a_peer():
    @ch.remote
    def f():
        pass

    # The hybrid clock that deadlines and expiries run on ends in 2527.
    with pytest.raises(ch.OptionsError, match="between 1970 and the year 2527"):
        f.options(deadline=dt.datetime(3000, 1, 1, tzinfo=dt.timezone.utc))
    with pytest.raises(ch.OptionsError, match="between 1970 and the year 2527"):
        f.options(deadline=dt.datetime.max)
    with pytest.raises(ch.OptionsError, match="longer than 100 years"):
        f.options(deadline=dt.timedelta(days=365 * 101))
    assert f.options(deadline=dt.timedelta(days=365 * 99)).policy().get("deadline") == 365 * 99 * 86400
    # A budget is compute time, not a wait: it may be longer.
    assert f.options(budget={"cpu_seconds": "1000000h"}).policy().get("budget") == {"cpu_seconds": 3.6e9}


def test_contradictions_name_both_options_and_where_each_was_set():
    @ch.remote(memory_limit="1 GiB")
    def f():
        pass

    limit_below = r"memory_limit=1073741824 \(@ch.remote on .*f\) is less than memory="
    with pytest.raises(ch.OptionsError, match=limit_below + r"2147483648 \(\.options at"):
        f.options(memory="2 GiB").policy()
    with ch.defaults(memory="4 GiB"):
        with pytest.raises(ch.OptionsError, match=limit_below + r"4294967296 \(with ch.defaults at"):
            f.policy()
    with pytest.raises(ch.OptionsError, match=r"less than memory=.* \(@ch.remote"):  # one layer: when it is set

        @ch.remote(memory="2 GiB", memory_limit="1 GiB")
        def g():
            pass

    with pytest.raises(ch.OptionsError, match=r"less than memory=2147483648"):
        f.options(memory=ch.auto("2 GiB")).policy()
    assert f.options(memory="512 MiB").policy().get("memory") == 512 << 20
    with pytest.raises(ch.OptionsError, match=r"min <= initial <= max, but min='3 GiB' is more than max='2 GiB' \("):
        f.options(memory=ch.auto(min="3 GiB", max="2 GiB"))
    with pytest.raises(ch.OptionsError, match=r"initial=8 is more than max=4"):
        f.options(cpus=ch.auto(8, max=4))
    with pytest.raises(ch.OptionsError, match=r"cpus \(ch.auto min\): expected a positive number, got nan"):
        f.options(cpus=ch.auto(min=math.nan))
    with pytest.raises(ch.OptionsError, match=r"gpus=0 \(\.options at .*\) contradicts device='cuda' \(@ch.remote on"):

        @ch.remote(device="cuda")
        def h():
            pass

        h.options(gpus=0).policy()


@ch.remote(memory="64 MiB")
def describe_here() -> str:
    return ch.context().describe_policy()


@ch.remote(priority=1)
class Describer:
    @ch.method(max_attempts=2)
    def here(self) -> str:
        return ch.context().describe_policy()


def test_the_policy_is_read_only_and_a_method_has_one_too():
    import pickle

    eff = describe_here.options(deadline="30s").policy()
    with pytest.raises(TypeError):
        eff.values["cpus"] = 8  # type: ignore[index]
    with pytest.raises(TypeError):
        eff.sources["cpus"] = "here"  # type: ignore[index]
    assert pickle.loads(pickle.dumps(eff)).describe() == eff.describe()
    # So are the mappings options hold, which every call made with them shares.
    require = {"zone": "a"}
    nested = describe_here.options(
        budget={"cpu_seconds": 60}, resources={"x": 1}, placement=ch.dist.Placement(require=require)
    ).policy()
    require["zone"] = "b"  # the caller's dict is copied when the option is set
    held = [nested.values["budget"], nested.values["resources"], nested.values["placement"].require]
    for mapping in [*held, Laned.policy().values["groups"]]:
        with pytest.raises(TypeError, match="read-only"):
            mapping["zone"] = "c"
        with pytest.raises(TypeError, match="read-only"):
            mapping.update(zone="c")
    assert nested.get("placement").require == {"zone": "a"} and nested.native["budget_cpu_seconds"] == 60.0
    assert pickle.loads(pickle.dumps(nested)).describe() == nested.describe()
    assert eff.sources["deadline"].startswith(".options at test_units_options.py:")
    # What each call carries: the options not left at their defaults.
    assert eff.said == (f"deadline=30.0 ({eff.sources['deadline']})", "memory=67108864 (@ch.remote on describe_here)")
    from chimaera._remote import Actor

    handle = Actor(None, b"a" * 16, b"c" * 16, Describer._cls, Describer.policy())  # type: ignore[arg-type]
    eff = handle.here.options(deadline="5s").policy()
    assert eff.get("max_attempts") == 2 and eff.sources["max_attempts"] == "@ch.method on here"
    assert eff.get("priority") == 1 and eff.sources["priority"] == "@ch.remote on Describer"
    assert eff.sources["effects"] == "default", "an actor's own sources show through its methods"


def test_a_call_describes_its_policy_as_its_caller_does():
    view = describe_here.options(deadline="60s")
    with ch.defaults(priority=3, max_in_flight=4, explain=True):
        with ch.defaults(priority=5):
            here = view.policy().describe()
            assert "(with ch.defaults at" in here and "(.options at" in here and "(@ch.remote on" in here
            assert view.remote().result(timeout=120) == here
            actor = Describer.remote()
            method = actor.here.options(deadline="60s")
            here = method.policy().describe()
            assert "(@ch.method on here)" in here and "(with ch.defaults at" in here
            assert method.remote().result(timeout=120) == here
            actor.close()
    explained = describe_here.options(explain=True)
    assert explained.remote().result(timeout=120) == explained.policy().describe()
    # Off by default: the call carries no words, and says how to ask for them.
    assert "explained" not in describe_here.policy().native and "explained" not in view.policy().native
    with pytest.raises(ch.OptionsError, match=r"without explain=True"):
        describe_here.remote().result(timeout=120)


@ch.remote(groups={"io": 2}, concurrency=3, explain=True)
class Laned:
    def __init__(self) -> None:
        self.made = ch.context().describe_policy()

    def made_with(self) -> tuple[str, str]:
        return self.made, ch.context().describe_policy()

    @ch.method(concurrency=2)
    def own_lane(self) -> str:
        return ch.context().describe_policy()


def test_an_actor_with_lanes_describes_its_options_as_set():
    # Its groups and method lanes add to the calls the peer sends it at once
    # (3 + 2 + 2), not to the options its handle, methods and calls describe.
    actor = Laned.remote()
    made, method = actor.made_with.remote().result(timeout=120)
    assert made == Laned.policy().describe() and "concurrency=3 (@ch.remote on Laned)" in made
    assert method == actor.made_with.policy().describe() and "concurrency=3 (@ch.remote on Laned)" in method
    assert actor.own_lane.remote().result(timeout=120) == actor.own_lane.policy().describe()
    assert "concurrency=2 (@ch.method on own_lane)" in actor.own_lane.policy().describe()
    assert "explain=True (@ch.remote on Laned)" in method, "the decorator's explain=True reaches its methods"
    actor.close()
