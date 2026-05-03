"""Module manager — registers, starts / stops server modules."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import FastAPI
    from device_manager import DeviceManager

from modules.base import BaseModule

logger = logging.getLogger("smart_connect.modules")


class ModuleManager:
    def __init__(self) -> None:
        self._modules: dict[str, BaseModule] = {}
        self._device_manager: DeviceManager | None = None

    # ── Registration ─────────────────────────────────────────────────────

    def register(self, module: BaseModule) -> None:
        self._modules[module.name] = module
        logger.info("Registered module: %s — %s", module.name, module.description)

    def get(self, name: str) -> BaseModule | None:
        return self._modules.get(name)

    def all_modules(self) -> list[BaseModule]:
        return list(self._modules.values())

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def start_all(self, device_manager: DeviceManager) -> None:
        self._device_manager = device_manager
        for mod in self._modules.values():
            try:
                await mod.on_start(device_manager)
                logger.info("Started module: %s", mod.name)
            except Exception as e:
                logger.error("Failed to start module %s: %s", mod.name, e)

    async def stop_all(self) -> None:
        for mod in self._modules.values():
            try:
                await mod.on_stop()
            except Exception as e:
                logger.error("Failed to stop module %s: %s", mod.name, e)

    async def enable_module(self, name: str) -> bool:
        mod = self._modules.get(name)
        if mod is None or self._device_manager is None:
            return False
        if not mod.enabled:
            await mod.on_start(self._device_manager)
        return True

    async def disable_module(self, name: str) -> bool:
        mod = self._modules.get(name)
        if mod is None:
            return False
        if mod.enabled:
            await mod.on_stop()
        return True

    # ── Routes ───────────────────────────────────────────────────────────

    def register_routes(self, app: FastAPI) -> None:
        """Let every module mount its own endpoints."""
        for mod in self._modules.values():
            mod.register_routes(app)

    # ── Status ───────────────────────────────────────────────────────────

    def get_status(self) -> list[dict[str, Any]]:
        return [mod.get_status() for mod in self._modules.values()]
