"""Base class for Smart Connect server modules (plugins)."""

from __future__ import annotations

import logging
from abc import ABC
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import FastAPI
    from device_manager import DeviceManager


class BaseModule(ABC):
    """Abstract base for a server module.

    Lifecycle:
        register_routes(app)   — called once at startup to mount endpoints
        on_start(dm)           — called when the module is enabled
        on_stop()              — called when the module is disabled
    """

    name: str = "unnamed"
    description: str = ""

    def __init__(self) -> None:
        self.enabled: bool = False
        self._device_manager: DeviceManager | None = None
        self.logger = logging.getLogger(f"smart_connect.mod.{self.name}")

    @property
    def device_manager(self) -> DeviceManager:
        if self._device_manager is None:
            raise RuntimeError("Module not started")
        return self._device_manager

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def on_start(self, device_manager: DeviceManager) -> None:
        """Called when the module is enabled. Override to set up tasks."""
        self._device_manager = device_manager
        self.enabled = True

    async def on_stop(self) -> None:
        """Called when the module is disabled. Override to clean up."""
        self.enabled = False

    def register_routes(self, app: FastAPI) -> None:
        """Override to add custom FastAPI routes for this module."""

    # ── Status / Config ──────────────────────────────────────────────────

    def get_status(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "enabled": self.enabled,
        }

    def get_config(self) -> dict[str, Any]:
        """Return current config. Override in subclass."""
        return {}

    def set_config(self, config: dict[str, Any]) -> None:
        """Apply new config. Override in subclass."""
