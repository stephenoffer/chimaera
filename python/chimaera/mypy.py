"""mypy plugin: types for ``@ch.remote`` classes and their actor handles.

Enable it with ``plugins = chimaera.mypy`` in the ``[mypy]`` section of your
config. Without it mypy reports ``"Counter" has no attribute "remote"``: it
ignores what a class decorator returns. With it:

- ``Counter.remote(...)`` checks the arguments against ``Counter.__init__``
  and returns ``Actor[Counter]``;
- ``handle.method.remote(...)`` checks them against ``Counter.method`` and
  returns ``Future[<its return type>]``, and a misspelled method is an error.

Pyright needs no plugin: it applies the decorator's return type, so handles
are ``Actor[Counter]`` and their methods accept any arguments.
"""

from __future__ import annotations

from collections.abc import Callable

from mypy.nodes import ARG_STAR, ARG_STAR2, Argument, Decorator, Var
from mypy.plugin import AttributeContext, ClassDefContext, Plugin
from mypy.plugins.common import add_method_to_class
from mypy.types import AnyType, CallableType, Instance, Parameters, Type, TypeOfAny, get_proper_type

_REMOTE = "chimaera._remote.remote"
_ACTOR = "chimaera._remote.Actor"


def _remote_class(ctx: ClassDefContext) -> bool:
    """Give a ``@ch.remote`` class ``remote(<__init__'s arguments>) -> Actor[C]``."""
    actor = ctx.api.lookup_fully_qualified_or_none(_ACTOR)
    if actor is None or actor.node is None:
        return False
    info = ctx.cls.info
    ret = Instance(actor.node, [Instance(info, [AnyType(TypeOfAny.special_form)] * len(info.defn.type_vars))])  # type: ignore[arg-type]
    init = info.get_method("__init__")
    sig = get_proper_type(init.type) if init is not None and init.type is not None else None
    if isinstance(sig, CallableType):
        params = zip(sig.arg_names[1:], sig.arg_types[1:], sig.arg_kinds[1:])
        args = [Argument(Var(name or f"_{i}"), t, None, kind) for i, (name, t, kind) in enumerate(params)]
    else:  # no annotated __init__: any arguments
        any_t = AnyType(TypeOfAny.unannotated)
        args = [Argument(Var("args"), any_t, None, ARG_STAR), Argument(Var("kwargs"), any_t, None, ARG_STAR2)]
    add_method_to_class(ctx.api, ctx.cls, "remote", args, ret, is_classmethod=True)
    return True


def _handle_method(ctx: AttributeContext) -> Type:
    """``handle.name`` on ``Actor[C]``: ``RemoteMethod`` with ``C.name``'s parameters and result."""
    handle = get_proper_type(ctx.type)
    name = getattr(ctx.context, "name", None)
    if not isinstance(handle, Instance) or not handle.args or name is None:
        return ctx.default_attr_type
    if handle.type.get(name) is not None:  # the handle's own attributes (close, id)
        return ctx.default_attr_type
    cls = get_proper_type(handle.args[0])
    if not isinstance(cls, Instance):
        return ctx.default_attr_type
    method = cls.type.get_method(name)
    if method is None:
        ctx.api.fail(f'"{cls.type.name}" has no method "{name}"', ctx.context)
        return ctx.default_attr_type
    t = method.func.type if isinstance(method, Decorator) else method.type
    sig = get_proper_type(t) if t is not None else None
    remote_method = get_proper_type(ctx.default_attr_type)
    if not isinstance(sig, CallableType) or not isinstance(remote_method, Instance):
        return ctx.default_attr_type  # unannotated: any arguments, any result
    params = Parameters(sig.arg_types[1:], sig.arg_kinds[1:], sig.arg_names[1:])
    return Instance(remote_method.type, [params, sig.ret_type])


class ChimaeraPlugin(Plugin):
    def get_class_decorator_hook_2(self, fullname: str) -> Callable[[ClassDefContext], bool] | None:
        return _remote_class if fullname == _REMOTE else None

    def get_attribute_hook(self, fullname: str) -> Callable[[AttributeContext], Type] | None:
        return _handle_method if fullname.startswith(_ACTOR + ".") else None


def plugin(version: str) -> type[Plugin]:
    return ChimaeraPlugin
