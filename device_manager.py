"""Manages TinyTuya BulbDevice instances with cached state and push notifications."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import tinytuya

from config import BulbConfig, BULBS

logger = logging.getLogger("smart_connect.devices")


@dataclass
class BulbState:
    """Cached snapshot of a single bulb's datapoints."""
    on: bool = False
    mode: str | None = None          # "white" | "colour" | "scene" | "music"
    brightness: int | None = None    # DP22: 10..1000
    color_temp: int | None = None    # DP23: 0..1000
    hsv_hex: str | None = None       # DP24: "hhhhssssvvvv"
    rgb: list[int] | None = None     # decoded from hsv_hex
    online: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "on": self.on,
            "mode": self.mode,
            "brightness": self.brightness,
            "color_temp": self.color_temp,
            "hsv_hex": self.hsv_hex,
            "rgb": self.rgb,
            "online": self.online,
        }

    @staticmethod
    def from_dps(dps: dict[str, Any]) -> BulbState:
        hsv_hex = dps.get("24")
        rgb = _tuya_hsv_to_rgb(hsv_hex) if hsv_hex else None
        return BulbState(
            on=bool(dps.get("20", False)),
            mode=dps.get("21"),
            brightness=dps.get("22"),
            color_temp=dps.get("23"),
            hsv_hex=hsv_hex,
            rgb=rgb,
            online=True,
        )

    def merge_dps(self, dps: dict[str, Any]) -> None:
        """Merge partial DPS update into current state (in-place)."""
        if "20" in dps:
            self.on = bool(dps["20"])
        if "21" in dps:
            self.mode = dps["21"]
        if "22" in dps:
            self.brightness = dps["22"]
        if "23" in dps:
            self.color_temp = dps["23"]
        if "24" in dps:
            self.hsv_hex = dps["24"]
            self.rgb = _tuya_hsv_to_rgb(dps["24"])
        self.online = True


class ManagedBulb:
    """Wraps a TinyTuya BulbDevice with cached state."""

    def __init__(self, cfg: BulbConfig, on_state_change: Callable[[str, dict], None] | None = None):
        self.cfg = cfg
        self.dev = tinytuya.BulbDevice(
            dev_id=cfg.dev_id,
            address=cfg.address,
            local_key=cfg.local_key,
            version=cfg.version,
        )
        self.dev.set_socketPersistent(False)
        self.dev.set_socketTimeout(5)
        self.dev.set_socketRetryLimit(2)
        self.state = BulbState()
        self._lock = threading.Lock()
        self.on_state_change = on_state_change
        self.last_manual_interaction = 0.0

    @property
    def dev_id(self) -> str:
        return self.cfg.dev_id

    @property
    def name(self) -> str:
        return self.cfg.name

    def _notify(self) -> None:
        if self.on_state_change:
            self.on_state_change(self.dev_id, self.state.to_dict())

    def fetch_state(self) -> BulbState:
        """Query device for current DPS; updates and returns cached state."""
        with self._lock:
            try:
                data = self.dev.status()
                if data and "dps" in data:
                    self.state.merge_dps(data["dps"])
                    self._notify()
                else:
                    # Often means device didn't respond or timed out
                    if isinstance(data, dict) and ("Error" in data or "Err" in data):
                        self.state.online = False
                        self._notify()
            except Exception as e:
                logger.error("[%s] fetch_state error: %s", self.name, e)
                self.state.online = False
                self._notify()
            return self.state

    def mark_manual_interaction(self) -> None:
        self.last_manual_interaction = time.time()

    def turn_on(self, is_manual: bool = True) -> BulbState:
        with self._lock:
            if is_manual: self.mark_manual_interaction()
            res = self.dev.turn_on()
            if isinstance(res, dict) and ("Error" in res or "Err" in res):
                logger.error("[%s] turn_on failed: %s", self.name, res)
                self.state.online = False
            else:
                self.state.on = True
                self.state.online = True
            self._notify()
        return self.state

    def turn_off(self, is_manual: bool = True) -> BulbState:
        with self._lock:
            if is_manual: self.mark_manual_interaction()
            res = self.dev.turn_off()
            if isinstance(res, dict) and ("Error" in res or "Err" in res):
                logger.error("[%s] turn_off failed: %s", self.name, res)
                self.state.online = False
            else:
                self.state.on = False
                self.state.online = True
            self._notify()
        return self.state

    def set_hsv(self, h: float, s: float, v: float, is_manual: bool = True) -> BulbState:
        """Set HSV colour. h: 0-360, s: 0-1, v: 0-1."""
        with self._lock:
            if is_manual: self.mark_manual_interaction()
            hsv_hex = _hsv_to_tuya_hex(h, s, v)
            res = self.dev.set_multiple_values(
                {"20": True, "21": "colour", "24": hsv_hex},
                nowait=False, # Wait for response to ensure it didn't fail
            )
            if isinstance(res, dict) and ("Error" in res or "Err" in res):
                logger.error("[%s] set_hsv failed: %s", self.name, res)
                self.state.online = False
            else:
                self.state.on = True
                self.state.mode = "colour"
                self.state.hsv_hex = hsv_hex
                self.state.rgb = _tuya_hsv_to_rgb(hsv_hex)
                self.state.online = True
            self._notify()
        return self.state

    def set_white(self, brightness: int = 1000, color_temp: int = 500, is_manual: bool = True) -> BulbState:
        with self._lock:
            if is_manual: self.mark_manual_interaction()
            brightness = max(10, min(1000, brightness))
            color_temp = max(0, min(1000, color_temp))
            payload = {
                "20": True,
                "21": "white",
                "22": brightness,
                "23": color_temp,
            }
            res = self.dev.set_multiple_values(payload, nowait=False)
            if isinstance(res, dict) and ("Error" in res or "Err" in res):
                logger.error("[%s] set_white failed: %s", self.name, res)
                self.state.online = False
            else:
                self.state.on = True
                self.state.mode = "white"
                self.state.brightness = brightness
                self.state.color_temp = color_temp
                self.state.online = True
            self._notify()
        return self.state

    def set_brightness(self, value: int, is_manual: bool = True) -> BulbState:
        with self._lock:
            if is_manual: self.mark_manual_interaction()
            value = max(10, min(1000, value))
            res = self.dev.set_value("22", value, nowait=False)
            if isinstance(res, dict) and ("Error" in res or "Err" in res):
                logger.error("[%s] set_brightness failed: %s", self.name, res)
                self.state.online = False
            else:
                self.state.brightness = value
                self.state.online = True
            self._notify()
        return self.state

    def set_colour_hex(self, hsv_hex: str, nowait: bool = True, is_manual: bool = True) -> BulbState:
        """Atomic colour set from raw Tuya HSV hex (DP 24)."""
        with self._lock:
            if is_manual: self.mark_manual_interaction()
            payload = {"20": True, "21": "colour", "24": hsv_hex}
            res = self.dev.set_multiple_values(payload, nowait=nowait)
            if not nowait and isinstance(res, dict) and ("Error" in res or "Err" in res):
                logger.error("[%s] set_colour_hex failed: %s", self.name, res)
                self.state.online = False
            else:
                self.state.on = True
                self.state.mode = "colour"
                self.state.hsv_hex = hsv_hex
                self.state.rgb = _tuya_hsv_to_rgb(hsv_hex)
                self.state.online = True
            self._notify()
        return self.state

    def apply_snapshot(self, snap: dict[str, Any], nowait: bool = False, is_manual: bool = True) -> BulbState:
        """Restore this bulb to a previously captured `state.to_dict()` snapshot."""
        with self._lock:
            if is_manual: self.mark_manual_interaction()
            want_on = bool(snap.get("on", False))
            mode = snap.get("mode")
            payload: dict[str, Any] = {"20": want_on}

            if mode == "white":
                brightness = snap.get("brightness")
                color_temp = snap.get("color_temp")
                payload["21"] = "white"
                if brightness is not None:
                    brightness = max(10, min(1000, brightness))
                    payload["22"] = brightness
                    self.state.brightness = brightness
                if color_temp is not None:
                    color_temp = max(0, min(1000, color_temp))
                    payload["23"] = color_temp
                    self.state.color_temp = color_temp
                self.state.mode = "white"
            elif mode == "colour":
                hsv_hex = snap.get("hsv_hex")
                payload["21"] = "colour"
                if hsv_hex:
                    payload["24"] = hsv_hex
                    self.state.hsv_hex = hsv_hex
                    self.state.rgb = _tuya_hsv_to_rgb(hsv_hex)
                self.state.mode = "colour"

            self.dev.set_multiple_values(payload, nowait=nowait)
            self.state.on = want_on
            self.state.online = True
            self._notify()
            return self.state

    def reconnect(self) -> None:
        """Drop the persistent socket; tinytuya reopens it on next command."""
        with self._lock:
            try:
                close = getattr(self.dev, "close", None)
                if callable(close):
                    close()
                else:
                    sock = getattr(self.dev, "socket", None)
                    if sock is not None:
                        try:
                            sock.close()
                        except Exception:
                            pass
                        self.dev.socket = None
            except Exception as e:
                logger.debug("[%s] reconnect/close ignored: %s", self.name, e)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dev_id": self.cfg.dev_id,
            "name": self.cfg.name,
            "address": self.cfg.address,
            "state": self.state.to_dict(),
        }


class DeviceManager:
    """Singleton managing all bulb devices."""

    def __init__(self) -> None:
        self._bulbs: dict[str, ManagedBulb] = {}
        self._ws_callbacks: list[Callable[[str, dict], None]] = []
        self._poll_task: asyncio.Task | None = None
        # Last state dict we pushed for each dev_id — avoids re-serializing
        # twice per poll tick just to diff old vs new.
        self._last_pushed: dict[str, dict] = {}

    def init_devices(self, configs: list[BulbConfig] | None = None) -> None:
        cfgs = configs or BULBS
        for cfg in cfgs:
            mb = ManagedBulb(cfg, on_state_change=self._on_bulb_state_change)
            self._bulbs[cfg.dev_id] = mb
            logger.info("Registered device: %s (%s @ %s)", cfg.name, cfg.dev_id, cfg.address)

    def _on_bulb_state_change(self, dev_id: str, state: dict) -> None:
        """Callback from ManagedBulb when its internal state is updated."""
        # Only notify if the state actually changed compared to what we last pushed.
        if self._last_pushed.get(dev_id) != state:
            self._last_pushed[dev_id] = state
            self._notify_ws(dev_id, state)

    def get_bulb(self, dev_id: str) -> ManagedBulb | None:
        return self._bulbs.get(dev_id)

    def all_bulbs(self) -> list[ManagedBulb]:
        return list(self._bulbs.values())

    def register_ws_callback(self, cb: Callable[[str, dict], None]) -> None:
        self._ws_callbacks.append(cb)

    def unregister_ws_callback(self, cb: Callable[[str, dict], None]) -> None:
        self._ws_callbacks.remove(cb)

    def _notify_ws(self, dev_id: str, state: dict) -> None:
        for cb in self._ws_callbacks:
            try:
                cb(dev_id, state)
            except Exception as e:
                logger.error("WS callback error: %s", e)

    async def start_polling(self, interval: float = 15.0) -> None:
        """Background task: periodically fetch state from all devices in parallel."""
        logger.info("Starting device polling (interval=%.1fs)", interval)
        while True:
            bulbs = list(self._bulbs.values())
            # fetch_state now calls _on_bulb_state_change which calls _notify_ws
            await asyncio.gather(
                *[asyncio.to_thread(mb.fetch_state) for mb in bulbs],
                return_exceptions=True,
            )
            await asyncio.sleep(interval)

    async def initial_fetch(self) -> None:
        """Fetch state from all devices on startup."""
        await asyncio.gather(
            *[asyncio.to_thread(mb.fetch_state) for mb in self._bulbs.values()],
            return_exceptions=True,
        )
        logger.info("Initial fetch complete for %d devices", len(self._bulbs))


# Singleton
device_manager = DeviceManager()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _hsv_to_tuya_hex(h: float, s: float, v: float) -> str:
    """Encode (h 0-360, s 0-1, v 0-1) as Tuya HSV hex 'hhhhssssvvvv'."""
    hi = max(0, min(360, round(h)))
    si = max(0, min(1000, round(s * 1000)))
    vi = max(0, min(1000, round(v * 1000)))
    return f"{hi:04x}{si:04x}{vi:04x}"


def _tuya_hsv_parse(hsv: str) -> tuple[float, float, float] | None:
    """Parse 'hhhhssssvvvv' → (h 0-360, s 0-1, v 0-1)."""
    if len(hsv) < 12:
        return None
    try:
        h = int(hsv[0:4], 16)
        s = int(hsv[4:8], 16) / 1000.0
        v = int(hsv[8:12], 16) / 1000.0
        return (min(360, h), min(1.0, s), min(1.0, v))
    except ValueError:
        return None


def _tuya_hsv_to_rgb(hsv_hex: str) -> list[int] | None:
    """Parse Tuya HSV hex → [r, g, b] 0-255."""
    parsed = _tuya_hsv_parse(hsv_hex)
    if parsed is None:
        return None
    h, s, v = parsed
    c = v * s
    hh = h / 60.0
    x = c * (1 - abs((hh % 2) - 1))
    if hh < 1:
        r1, g1, b1 = c, x, 0.0
    elif hh < 2:
        r1, g1, b1 = x, c, 0.0
    elif hh < 3:
        r1, g1, b1 = 0.0, c, x
    elif hh < 4:
        r1, g1, b1 = 0.0, x, c
    elif hh < 5:
        r1, g1, b1 = x, 0.0, c
    else:
        r1, g1, b1 = c, 0.0, x
    m = v - c
    return [
        max(0, min(255, round((r1 + m) * 255))),
        max(0, min(255, round((g1 + m) * 255))),
        max(0, min(255, round((b1 + m) * 255))),
    ]
