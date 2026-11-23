"""Call options: validation, scopes and precedence.

Soft settings resolve in this order: the call view (``fn.options(...)``), then
the decorator, then ``with ch.defaults(...)`` blocks from innermost to
outermost (a block fills in only what the call and its decorator leave unset,
and never applies to Chimaera's own internal calls), then the learned profile
(applied by the peer), then defaults.

``effects`` is a lattice (none < idempotent < external): a view or scope can
only make a call stricter; only the decorator can choose a weaker contract.
Every option is validated when it is set, so a typo fails at the line that
wrote it, with the field name and a suggestion.
"""

from __future__ import annotations

import contextvars
import datetime as _dt
import difflib
import functools
import inspect
import sys
from dataclasses import dataclass, field
from typing import Any, Iterator, Literal, Mapping, TypedDict, Unpack

from . import _units
from ._errors import OptionsError

EFFECTS = ("none", "idempotent", "external")


@dataclass(frozen=True)
class Placement:
    """Hard placement predicates (``require``) and soft preferences (``prefer``)."""

    require: Mapping[str, str] = field(default_factory=dict)
    prefer: tuple[str, ...] = ()


@dataclass(frozen=True)
class State:
    """Durable actor state: ``@ch.remote(state=ch.durable.State())`` on a class.

    After every method call that returns, the instance's attributes (or what
    ``__getstate__`` returns) are committed with the call's command id before
    the reply goes out. A restarted instance resumes from the last committed
    state instead of running ``__init__`` again. A call that raises commits
    nothing. Repeating a call with ``.options(command_id=...)`` returns the
    first reply; the same id with other arguments raises ``CommandConflict``.
    State is stored inline, at most 1 MiB pickled.
    """


def _state(name: str, v: Any) -> State:
    if not isinstance(v, State):
        raise ValueError(f"{name}: expected ch.durable.State(), got {v!r}")
    return v


def _command_id(name: str, v: Any) -> str:
    if not isinstance(v, str) or not v:
        raise ValueError(f"{name}: expected a non-empty string, got {v!r}")
    return v


@dataclass(frozen=True)
class Auto:
    """A value the runtime learns, clipped to ``[min, max]``."""

    initial: Any = None
    min: Any = None
    max: Any = None


def auto(initial: Any = None, *, min: Any = None, max: Any = None) -> Auto:  # noqa: A002
    return Auto(initial, min, max)


def _pos_float(name: str, v: Any) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0:
        raise ValueError(f"{name}: expected a positive number, got {v!r}")
    return float(v)


def _nonneg_float(name: str, v: Any) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
        raise ValueError(f"{name}: expected a non-negative number, got {v!r}")
    return float(v)


def _pos_int(name: str, v: Any) -> int:
    if isinstance(v, bool) or not isinstance(v, int) or v < 1:
        raise ValueError(f"{name}: expected an integer >= 1, got {v!r}")
    return v


def _choice(choices: tuple[str, ...]):
    def check(name: str, v: Any) -> str:
        if v not in choices:
            raise ValueError(f"{name}: expected one of {', '.join(map(repr, choices))}, got {v!r}")
        return v

    return check


def _bool(name: str, v: Any) -> bool:
    if not isinstance(v, bool):
        raise ValueError(f"{name}: expected True or False, got {v!r}")
    return v


def _int(name: str, v: Any) -> int:
    if isinstance(v, bool) or not isinstance(v, int):
        raise ValueError(f"{name}: expected an integer, got {v!r}")
    return v


def _groups(name: str, v: Any) -> dict[str, int]:
    if not isinstance(v, Mapping) or not v:
        raise ValueError(f"{name}: expected a mapping of group name to concurrency, e.g. {{'io': 8}}")
    return {str(k): _pos_int(f"{name}[{k!r}]", n) for k, n in v.items()}


def _str(name: str, v: Any) -> str:
    if not isinstance(v, str):
        raise ValueError(f"{name}: expected a string, got {v!r}")
    return v


def _or_auto(check):
    def wrapped(name: str, v: Any) -> Any:
        if isinstance(v, Auto) or v == "auto":
            return v if isinstance(v, Auto) else Auto()
        return check(name, v)

    return wrapped


def _deadline(name: str, v: Any) -> Any:
    if isinstance(v, _dt.datetime):
        return v
    return _units.parse_duration(v, name)


def _latency(name: str, v: Any) -> Any:
    if v == "interactive":
        return v
    return _units.parse_duration(v, name)


def _resources(name: str, v: Any) -> dict[str, float]:
    if not isinstance(v, Mapping):
        raise ValueError(f"{name}: expected a mapping of resource name to amount")
    return {str(k): _nonneg_float(f"{name}[{k!r}]", x) for k, x in v.items()}


def _within(name: str, v: Any) -> Any:
    from ._reserve import Bundle

    if not isinstance(v, Bundle):
        raise ValueError(f"{name}: expected a bundle of a reservation, res[i] from ch.dist.reserve(...), got {v!r}")
    return v


BUDGET_KEYS = ("cpu_seconds", "gpu_seconds")


def _budget(name: str, v: Any) -> dict[str, float]:
    """``{"cpu_seconds": 3600, "gpu_seconds": "8h"}``: numbers of seconds or durations."""
    if not isinstance(v, Mapping) or not v:
        raise ValueError(f"{name}: expected a mapping with {' and/or '.join(BUDGET_KEYS)}, got {v!r}")
    unknown = [k for k in v if k not in BUDGET_KEYS]
    if unknown:
        raise ValueError(f"{name}: unknown {unknown[0]!r}; a budget limits {' and '.join(BUDGET_KEYS)}")
    out = {}
    for k, x in v.items():
        secs = _units.parse_duration(x, f"{name}[{k!r}]") if isinstance(x, str) else x
        out[k] = _pos_float(f"{name}[{k!r}]", secs)
    return out


def _budget_fields(eff: Effective) -> dict[str, Any]:
    """The policy's budget: this call's own (it is the root), or the enclosing
    call's, inherited with its root. Budgets do not nest."""
    from ._context import context

    ctx = context()
    outer = ctx.policy if ctx is not None and isinstance(ctx.policy, Mapping) else {}
    inherited = (outer.get("budget_cpu_seconds") or 0) > 0 or (outer.get("budget_gpu_seconds") or 0) > 0
    own = eff.get("budget")
    if own and inherited:
        raise OptionsError(
            "budget: this call runs inside a call that has a budget, which already covers it; budgets do not nest"
        )
    if own:
        return {
            "budget_cpu_seconds": own.get("cpu_seconds", 0.0),
            "budget_gpu_seconds": own.get("gpu_seconds", 0.0),
            "budget_root": b"",
        }
    if inherited:
        assert ctx is not None
        return {
            "budget_cpu_seconds": outer.get("budget_cpu_seconds") or 0.0,
            "budget_gpu_seconds": outer.get("budget_gpu_seconds") or 0.0,
            "budget_root": outer.get("budget_root") or (ctx.run + ctx.task),
        }
    return {}


def _placement(name: str, v: Any) -> Placement:
    if not isinstance(v, Placement):
        raise ValueError(f"{name}: expected ch.dist.Placement(require=..., prefer=...), got {v!r}")
    return v


def _exceptions(name: str, v: Any) -> tuple[type[BaseException], ...]:
    items = v if isinstance(v, tuple) else (v,)
    for e in items:
        if not (isinstance(e, type) and issubclass(e, BaseException)):
            raise ValueError(f"{name}: expected exception types, got {e!r}")
    return items


CACHE_TTL = 7 * 86400.0  # what cache=True keeps a result for, in seconds


def _cache_ttl(name: str, v: Any) -> float:
    """cache=True keeps results 7 days, False turns caching off, else a time to live."""
    if v is True or v is False:
        return CACHE_TTL if v else 0.0
    seconds = _units.parse_duration(v, name)
    if seconds <= 0:
        raise ValueError(f"{name}= is True, False or a time to live, not {v!r}")
    return seconds


def _batch_on_method(name: str, v: Any) -> Any:
    raise ValueError(f"{name}= batches one actor method's calls: put it on the method, @ch.method(batch=ch.Batch(...))")


# name -> (validator, where it may be set). Where: "any", "actor", "pool",
# "decorator", "call" (one call's view), "function" (a function's decorator or views).
FIELDS: dict[str, tuple[Any, str]] = {
    "cpus": (_or_auto(_pos_float), "any"),
    "memory": (_or_auto(lambda n, v: _units.parse_bytes(v, n)), "any"),
    "memory_limit": (lambda n, v: _units.parse_bytes(v, n), "any"),
    "gpus": (_nonneg_float, "any"),
    "device": (_str, "any"),
    "resources": (_resources, "any"),
    "placement": (_placement, "any"),
    "within": (_within, "any"),
    "accounting": (_choice(("auto", "process", "isolated")), "any"),
    "effects": (_choice(EFFECTS), "any"),
    "max_attempts": (_pos_int, "any"),
    "retry_exceptions": (_exceptions, "any"),
    "deadline": (_deadline, "any"),
    "concurrency": (_or_auto(_pos_int), "any"),
    "max_in_flight": (_pos_int, "any"),
    "returns": (_pos_int, "any"),
    "priority": (_int, "any"),
    "budget": (_budget, "any"),
    "max_in_flight_bytes": (lambda n, v: _units.parse_bytes(v, n), "any"),
    "latency": (_latency, "any"),
    "adaptive": (_choice(("on", "observe", "frozen")), "any"),
    "deterministic": (_bool, "any"),
    "cache": (_cache_ttl, "function"),
    "replicas": (_or_auto(_pos_int), "pool"),
    "ranks": (_or_auto(_pos_int), "any"),
    "name": (_str, "actor"),
    "detach": (_bool, "actor"),
    "restart": (_choice(("never", "fresh", "checkpoint")), "actor"),
    "max_restarts": (_pos_int, "actor"),
    "workflow": (_choice(("durable",)), "decorator"),
    "state": (_state, "decorator"),
    "command_id": (_command_id, "call"),
    "batch": (_batch_on_method, "decorator"),
    "groups": (_groups, "decorator"),
}

class Options(TypedDict, total=False):
    """Every option, for type checkers: ``fn.options(cpu=2)`` is an error in
    an editor as it is at run time. Kept equal to ``FIELDS`` by a test."""

    cpus: float | Auto | Literal["auto"]
    memory: int | str | Auto | Literal["auto"]
    memory_limit: int | str
    gpus: float
    device: str
    resources: Mapping[str, float]
    placement: Placement
    within: Any  # a reservation's bundle, res[i]
    accounting: Literal["auto", "process", "isolated"]
    effects: Literal["none", "idempotent", "external"]
    max_attempts: int
    retry_exceptions: type[BaseException] | tuple[type[BaseException], ...]
    deadline: str | float | _dt.datetime
    concurrency: int | Auto | Literal["auto"]
    max_in_flight: int
    returns: int
    priority: int
    budget: Mapping[str, float | str]
    max_in_flight_bytes: int | str
    latency: str | float
    adaptive: Literal["on", "observe", "frozen"]
    deterministic: bool
    cache: bool | str | float | _dt.timedelta
    replicas: int | Auto | Literal["auto"]
    ranks: int | Auto | Literal["auto"]
    name: str
    detach: bool
    restart: Literal["never", "fresh", "checkpoint"]
    max_restarts: int
    workflow: Literal["durable"]
    state: State
    command_id: str
    batch: Any
    groups: Mapping[str, int]


# Options that name one actor: a block of calls cannot share them.
IDENTITY = ("name", "detach")

DEFAULTS: dict[str, Any] = {
    "effects": "none",
    "max_attempts": 3,
    "accounting": "auto",
    "adaptive": "on",
    "deterministic": True,
    "restart": "never",
}


def _caller(depth: int) -> str:
    f = sys._getframe(depth)
    return f"{f.f_code.co_filename.rsplit('/', 1)[-1]}:{f.f_lineno}"


@dataclass(frozen=True)
class Layer:
    values: Mapping[str, Any]
    source: str
    kind: str  # "view" | "scope" | "decorator"
    internal: bool = False  # Chimaera's own decorator: `with ch.defaults` blocks do not apply


def validate(options: Mapping[str, Any], *, kind: str, target: str, source: str) -> Layer:
    """Validate a mapping of options set at `source` for a `target` ("function"|"actor"|"pool")."""
    out: dict[str, Any] = {}
    for name, value in options.items():
        if name not in FIELDS:
            close = difflib.get_close_matches(name, FIELDS, n=1)
            hint = f"; did you mean {close[0]!r}?" if close else ""
            raise OptionsError(f"unknown option {name!r} ({source}){hint}")
        check, where = FIELDS[name]
        if where == "decorator" and kind != "decorator":
            raise OptionsError(f"option {name!r} can only be set on the decorator ({source})")
        if where == "call" and (kind != "view" or target != "function"):
            raise OptionsError(f"option {name!r} names one call; set it with .options(...) on that call ({source})")
        if name in IDENTITY and kind == "scope":
            raise OptionsError(f"option {name!r} names one actor; set it on that actor's .options(...) ({source})")
        if where == "function" and target != "function":
            raise OptionsError(
                f"option {name!r} is set per function, on its decorator or .options(...), not on {target}s: "
                f"it must not reach functions that are not deterministic ({source})"
            )
        if where == "actor" and target not in ("actor", "pool", "scope"):
            raise OptionsError(f"option {name!r} applies to actors, not to {target}s ({source})")
        if where == "pool" and target not in ("pool", "actor", "scope"):
            raise OptionsError(f"option {name!r} applies to pools; use Class.options(replicas=...).pool(...) ({source})")
        try:
            out[name] = check(name, value)
        except (ValueError, TypeError) as e:
            raise OptionsError(f"{e} ({source})") from None
    return Layer(out, source, kind)


_scopes: contextvars.ContextVar[tuple[Layer, ...]] = contextvars.ContextVar("chimaera_scopes", default=())


class _Scope:
    def __init__(self, layer: Layer) -> None:
        self.layer = layer
        self.token: contextvars.Token[tuple[Layer, ...]] | None = None
        self.entered = False

    def __enter__(self) -> None:
        self.entered = True
        self.token = _scopes.set(_scopes.get() + (self.layer,))

    def __exit__(self, *exc: object) -> None:
        assert self.token is not None
        _scopes.reset(self.token)

    def __del__(self) -> None:
        if not self.entered:
            import warnings

            warnings.warn(
                f"{self.layer.source} applies to nothing: use it as `with ch.defaults(...):` around the calls",
                stacklevel=2,
            )

    async def __aenter__(self) -> None:
        self.__enter__()

    async def __aexit__(self, *exc: object) -> None:
        self.__exit__()


def defaults(**opts: Unpack[Options]) -> _Scope:
    """``with ch.defaults(...)``: options for the calls made inside the block,
    where neither the call nor its decorator sets them."""
    return _Scope(validate(opts, kind="scope", target="scope", source=f"with ch.defaults at {_caller(2)}"))


@dataclass(frozen=True)
class Effective:
    values: Mapping[str, Any]
    sources: Mapping[str, str]

    def get(self, name: str, default: Any = None) -> Any:
        return self.values.get(name, default)

    def describe(self) -> str:
        return ", ".join(f"{k}={v!r} ({self.sources[k]})" for k, v in sorted(self.values.items()))

    @functools.cached_property
    def native(self) -> dict[str, Any]:
        """`policy_dict(self)`, computed once: every call submitted with these
        options sends the same policy. Callers copy it before adding to it."""
        return policy_dict(self)


def resolve_cached(holder: Any, view: tuple[Layer, ...], decorator: Layer) -> Effective:
    """`resolve(view, decorator)`, reused while the active ``with ch.defaults``
    scopes are unchanged. The result is kept on `holder._resolved`; a scope
    entered or left makes a new tuple, which misses the cache."""
    scopes = _scopes.get()
    cached = holder._resolved
    if cached is not None and cached[0] is scopes:
        return cached[1]
    eff = resolve(view, decorator)
    holder._resolved = (scopes, eff)
    return eff


@functools.lru_cache(maxsize=4096)
def _signature(fn: Any) -> inspect.Signature:
    return inspect.signature(fn)


def signature_of(fn: Any) -> inspect.Signature:
    """`inspect.signature(fn)`, computed once per callable: every `.remote()`
    checks its arguments against it."""
    try:
        return _signature(fn)
    except TypeError:  # unhashable callable, or one inspect cannot read
        return inspect.signature(fn)


def resolve(view: tuple[Layer, ...], decorator: Layer) -> Effective:
    """Merge layers by precedence; `view` is ordered most specific first."""
    scopes = () if decorator.internal else tuple(reversed(_scopes.get()))
    layers = [*view, decorator, *scopes]
    values: dict[str, Any] = {}
    sources: dict[str, str] = {}
    for layer in layers:
        for k, v in layer.values.items():
            if k not in values:
                values[k] = v
                sources[k] = layer.source
    for k, v in DEFAULTS.items():
        if k not in values:
            values[k] = v
            sources[k] = "default"
    # effects: views and scopes may only tighten the decorator's contract.
    base = decorator.values.get("effects", "none")
    strictest = max(
        [base] + [layer.values["effects"] for layer in layers if "effects" in layer.values], key=EFFECTS.index
    )
    if strictest != values["effects"]:
        loosened = sources["effects"]
        raise OptionsError(
            f"effects={values['effects']!r} ({loosened}) would loosen effects={strictest!r} declared elsewhere; "
            "only the decorator can choose a weaker effect contract"
        )
    if values["effects"] == "external":
        if values["max_attempts"] > 1 and sources["max_attempts"] != "default":
            raise OptionsError(
                f"max_attempts={values['max_attempts']} ({sources['max_attempts']}) conflicts with "
                f"effects='external' ({sources['effects']}): an external effect starts at most once"
            )
        values["max_attempts"] = 1
    return Effective(values, sources)


def policy_dict(eff: Effective) -> dict[str, Any]:
    """The subset of options the peer needs, in the native layer's shape."""
    def num(name: str) -> Any:
        v = eff.get(name)
        if isinstance(v, Auto):
            return v.initial or 0
        return v or 0

    def bound(name: str, which: str) -> Any:
        """A ch.auto(min=, max=) bound on a learned value; 0 = none."""
        v = eff.get(name)
        x = getattr(v, which, None) if isinstance(v, Auto) else None
        if x is None:
            return 0
        return _units.parse_bytes(x, f"{name} {which}") if name == "memory" else _pos_float(f"{name} {which}", x)

    placement: Placement | None = eff.get("placement")
    latency = eff.get("latency")
    return {
        "cpus_min": float(bound("cpus", "min")),
        "cpus_max": float(bound("cpus", "max")),
        "memory_min": int(bound("memory", "min")),
        "memory_max": int(bound("memory", "max")),
        "cpus": float(num("cpus")),
        "memory": int(num("memory")),
        "memory_limit": int(eff.get("memory_limit") or 0),
        "gpus": float(eff.get("gpus") or 0),
        "device": eff.get("device") or "",
        "resources": dict(eff.get("resources") or {}),
        "accounting": eff.get("accounting"),
        "adaptive": eff.get("adaptive"),
        "concurrency": int(num("concurrency")),
        "range_size": 0,  # unused by the peer, which learns batching itself
        "interactive": latency == "interactive",
        "deterministic": bool(eff.get("deterministic")),
        "require": dict(placement.require) if placement else {},
        "prefer": list(placement.prefer) if placement else [],
        "source": eff.sources.get("cpus", ""),
        "priority": int(eff.get("priority") or 0),
        "max_in_flight": int(eff.get("max_in_flight") or 0),
        **_budget_fields(eff),
    }


def scope_layers() -> Iterator[Layer]:
    return iter(_scopes.get())
