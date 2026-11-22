"""Types for annotations: what ``@ch.remote`` makes, handles, options, and
the ``TaskContext`` that ``ch.context()`` returns. For mypy, also enable the
plugin: ``plugins = chimaera.mypy``."""

from ._context import TaskContext
from ._future import Outcome, Stream
from ._options import Options
from ._pool import Pool
from ._remote import Actor, ActorClass, RemoteFunction, RemoteMethod

__all__ = ["Actor", "ActorClass", "Options", "Outcome", "Pool", "RemoteFunction", "RemoteMethod", "Stream", "TaskContext"]
