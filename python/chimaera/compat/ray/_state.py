"""Shim state set by ``ray.init``: whether it ran, and the namespace for named actors."""

from __future__ import annotations

initialized = False
namespace: str | None = None
