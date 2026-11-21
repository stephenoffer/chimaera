"""Call options: validation, scopes and precedence.

Soft settings resolve in this order: the call view (``fn.options(...)``), then
``with ch.options(...)`` scopes from innermost to outermost, then the
decorator, then the learned profile (applied by the peer), then defaults.

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
from typing import Any, Iterator, Mapping

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
    """Durable actor state: ``@ch.remote(state=ch.State())`` on a class.

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
        raise ValueError(f"{name}: expected ch.State(), got {v!r}")
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
        raise ValueError(f"{name}: expected a bundle of a reservation, res[i] from ch.reserve(...), got {v!r}")
    return v


def _placement(name: str, v: Any) -> Placement:
    if not isinstance(v, Placement):
        raise ValueError(f"{name}: expected ch.Placement(require=..., prefer=...), got {v!r}")
    return v


def _exceptions(name: str, v: Any) -> tuple[type[BaseException], ...]:
    items = v if isinstance(v, tuple) else (v,)
    for e in items:
        if not (isinstance(e, type) and issubclass(e, BaseException)):
            raise ValueError(f"{name}: expected exception types, got {e!r}")
    return items


def _batch_on_method(name: str, v: Any) -> Any:
    raise ValueError(f"{name}= batches one actor method's calls: put it on the method, @ch.method(batch=ch.Batch(...))")


# name -> (validator, where it may be set). Where: "any", "actor", "pool", "decorator".
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
    "prefetch": (_or_auto(_pos_int), "any"),
    "range_size": (_or_auto(_pos_int), "any"),
    "max_in_flight": (_pos_int, "any"),
    "returns": (_pos_int, "any"),
    "priority": (_int, "any"),
    "max_in_flight_bytes": (lambda n, v: _units.parse_bytes(v, n), "any"),
    "latency": (_latency, "any"),
    "adaptive": (_choice(("on", "observe", "frozen")), "any"),
    "splittable": (_bool, "any"),
    "deterministic": (_bool, "any"),
    "replicas": (_or_auto(_pos_int), "pool"),
    "ranks": (_or_auto(_pos_int), "any"),
    "key": (_str, "actor"),
    "lifetime": (_choice(("run", "detached")), "actor"),
    "restart": (_choice(("never", "fresh", "checkpoint")), "actor"),
    "max_restarts": (_pos_int, "actor"),
    "workflow": (_choice(("durable",)), "decorator"),
    "state": (_state, "decorator"),
    "command_id": (_command_id, "call"),
    "batch": (_batch_on_method, "decorator"),
    "groups": (_groups, "decorator"),
}

DEFAULTS: dict[str, Any] = {
    "effects": "none",
    "max_attempts": 3,
    "accounting": "auto",
    "adaptive": "on",
    "deterministic": True,
    "restart": "never",
    "lifetime": "run",
}


def _caller(depth: int) -> str:
    f = sys._getframe(depth)
    return f"{f.f_code.co_filename.rsplit('/', 1)[-1]}:{f.f_lineno}"


@dataclass(frozen=True)
class Layer:
    values: Mapping[str, Any]
    source: str
    kind: str  # "view" | "scope" | "decorator"


def validate(options: Mapping[str, Any], *, kind: str, target: str, source: str) -> Layer:
    """Validate a mapping of options set at `source` for a `target` ("function"|"actor"|"pool")."""
    out: dict[str, Any] = {}
    for name, value in options.items():
        if name not in FIELDS:
            close = difflib.get_close_matches(name, FIELDS, n=1)
            hint = f"; did you mean {close[0]!r}?" if close else ""
            raise TypeError(f"unknown option {name!r} ({source}){hint}")
        check, where = FIELDS[name]
        if where == "decorator" and kind != "decorator":
            raise TypeError(f"option {name!r} can only be set on the decorator ({source})")
        if where == "call" and (kind != "view" or target != "function"):
            raise TypeError(f"option {name!r} names one call; set it with .options(...) on that call ({source})")
        if where == "actor" and target not in ("actor", "pool", "scope"):
            raise TypeError(f"option {name!r} applies to actors, not to {target}s ({source})")
        if where == "pool" and target not in ("pool", "actor", "scope"):
            raise TypeError(f"option {name!r} applies to pools; use Class.options(replicas=...).pool(...) ({source})")
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

    def __enter__(self) -> None:
        self.token = _scopes.set(_scopes.get() + (self.layer,))

    def __exit__(self, *exc: object) -> None:
        assert self.token is not None
        _scopes.reset(self.token)

    async def __aenter__(self) -> None:
        self.__enter__()

    async def __aexit__(self, *exc: object) -> None:
        self.__exit__()


def options(**opts: Any) -> _Scope:
    """``with ch.options(...)``: defaults for calls submitted inside the block."""
    return _Scope(validate(opts, kind="scope", target="scope", source=f"with ch.options at {_caller(2)}"))


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
    """`resolve(view, decorator)`, reused while the active ``with ch.options``
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
    layers = list(view) + list(reversed(_scopes.get())) + [decorator]
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
        "range_size": int(num("range_size")),
        "interactive": latency == "interactive",
        "deterministic": bool(eff.get("deterministic")),
        "require": dict(placement.require) if placement else {},
        "prefer": list(placement.prefer) if placement else [],
        "source": eff.sources.get("cpus", ""),
        "priority": int(eff.get("priority") or 0),
        "max_in_flight": int(eff.get("max_in_flight") or 0),
    }


def scope_layers() -> Iterator[Layer]:
    return iter(_scopes.get())
