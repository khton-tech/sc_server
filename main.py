"""Smart Connect — FastAPI server for Tuya bulb control."""

from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from starlette.types import Scope

from device_manager import device_manager
from modules.manager import ModuleManager
from modules.lol_live import LoLLiveModule
from modules.clap_listen import ClapListenModule

# Path to Flutter web build output.
# When running standalone from /server, 'app' is usually not available.
WEB_BUILD_DIR = Path(__file__).resolve().parent.parent / "app" / "build" / "web"
if not WEB_BUILD_DIR.is_dir():
    # Fallback/Development check
    WEB_BUILD_DIR = Path(__file__).resolve().parent / "web_build"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-3s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("smart_connect.api")


# ── Global loop reference for thread-safe callbacks ─────────────────────────
_main_loop: asyncio.AbstractEventLoop | None = None


def _on_device_state_change(dev_id: str, state: dict) -> None:
    """Called by DeviceManager when a bulb state is updated.

    This may be called from a background thread (e.g. within a ThreadPoolExecutor),
    so we use _main_loop.call_soon_threadsafe to schedule the broadcast.
    """
    if _main_loop is not None and _main_loop.is_running():
        _main_loop.call_soon_threadsafe(
            lambda: asyncio.create_task(_broadcast_state(dev_id, state))
        )


# Register the callback so all state changes (REST or Modules) push to WS.
device_manager.register_ws_callback(_on_device_state_change)


# ── Lifespan ─────────────────────────────────────────────────────────────────

# ── Module manager ───────────────────────────────────────────────────────────

module_manager = ModuleManager()
module_manager.register(LoLLiveModule())
module_manager.register(ClapListenModule())


def _register_module_routes(app_: FastAPI) -> None:
    module_manager.register_routes(app_)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown hooks."""
    global _main_loop
    _main_loop = asyncio.get_running_loop()

    # Size the default executor for concurrent blocking Tuya calls
    # bulb op (fetch_state, set_*, turn_*) is sync and runs via to_thread.
    # A typical LoL flash queues `len(bulbs) * flash_count` operations in
    # quick succession — the Python default (min(32, cpu+4)) is fine but
    # we set an explicit floor to keep headroom predictable.
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="sc-worker")
    loop.set_default_executor(executor)

    device_manager.init_devices()
    logger.info("Fetching initial device states…")
    await device_manager.initial_fetch()
    poll_task = asyncio.create_task(device_manager.start_polling(interval=15.0))
    # Start modules
    await module_manager.start_all(device_manager)
    logger.info("Server ready — %d devices, %d modules",
                len(device_manager.all_bulbs()), len(module_manager.all_modules()))
    yield
    await module_manager.stop_all()
    poll_task.cancel()
    for mb in device_manager.all_bulbs():
        try:
            mb.dev.close()
        except Exception:
            pass
    executor.shutdown(wait=False, cancel_futures=True)
    logger.info("Server shut down")


app = FastAPI(
    title="Smart Connect API",
    version="1.0.0",
    lifespan=lifespan,
)

# Allow Flutter web app (served from any origin during dev) to call the API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Gzip responses >1KB — big win for the Flutter web bundle and list payloads.
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Let each module mount its own routes (e.g. /api/modules/lol_live/players).
_register_module_routes(app)


# ── Pydantic models ─────────────────────────────────────────────────────────

class BulbStateResponse(BaseModel):
    on: bool = False
    mode: str | None = None
    brightness: int | None = None
    color_temp: int | None = None
    hsv_hex: str | None = None
    rgb: list[int] | None = None
    online: bool = False


class BulbResponse(BaseModel):
    dev_id: str
    name: str
    address: str
    state: BulbStateResponse


class HsvRequest(BaseModel):
    h: float  # 0-360
    s: float  # 0-1
    v: float  # 0-1


class WhiteRequest(BaseModel):
    brightness: int = 1000  # 10-1000
    color_temp: int = 500   # 0-1000


class BrightnessRequest(BaseModel):
    value: int  # 10-1000


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_bulb(dev_id: str):
    mb = device_manager.get_bulb(dev_id)
    if mb is None:
        raise HTTPException(status_code=404, detail=f"Device {dev_id} not found")
    return mb


def _bulb_response(mb) -> dict[str, Any]:
    return {
        "dev_id": mb.dev_id,
        "name": mb.name,
        "address": mb.cfg.address,
        "state": mb.state.to_dict(),
    }


# ── REST endpoints ───────────────────────────────────────────────────────────

@app.get("/api/bulbs", response_model=list[BulbResponse])
async def list_bulbs():
    """List all registered bulbs with their cached state."""
    return [_bulb_response(mb) for mb in device_manager.all_bulbs()]


@app.get("/api/bulbs/{dev_id}/state", response_model=BulbStateResponse)
async def get_state(dev_id: str):
    """Get cached state for a single bulb."""
    mb = _get_bulb(dev_id)
    return mb.state.to_dict()


@app.post("/api/bulbs/{dev_id}/refresh", response_model=BulbResponse)
async def refresh_state(dev_id: str):
    """Force re-fetch state from device."""
    mb = _get_bulb(dev_id)
    await asyncio.to_thread(mb.fetch_state)
    return _bulb_response(mb)


@app.post("/api/bulbs/{dev_id}/toggle", response_model=BulbResponse)
async def toggle(dev_id: str):
    """Toggle bulb on/off."""
    mb = _get_bulb(dev_id)
    await asyncio.to_thread(mb.turn_off if mb.state.on else mb.turn_on)
    return _bulb_response(mb)


@app.post("/api/bulbs/{dev_id}/on", response_model=BulbResponse)
async def turn_on(dev_id: str):
    mb = _get_bulb(dev_id)
    await asyncio.to_thread(mb.turn_on)
    return _bulb_response(mb)


@app.post("/api/bulbs/{dev_id}/off", response_model=BulbResponse)
async def turn_off(dev_id: str):
    mb = _get_bulb(dev_id)
    await asyncio.to_thread(mb.turn_off)
    return _bulb_response(mb)


@app.post("/api/bulbs/{dev_id}/hsv", response_model=BulbResponse)
async def set_hsv(dev_id: str, req: HsvRequest):
    """Set HSV colour mode."""
    mb = _get_bulb(dev_id)
    await asyncio.to_thread(mb.set_hsv, req.h, req.s, req.v)
    return _bulb_response(mb)


@app.post("/api/bulbs/{dev_id}/white", response_model=BulbResponse)
async def set_white(dev_id: str, req: WhiteRequest):
    """Set white mode with brightness and color temperature."""
    mb = _get_bulb(dev_id)
    await asyncio.to_thread(mb.set_white, req.brightness, req.color_temp)
    return _bulb_response(mb)


@app.post("/api/bulbs/{dev_id}/brightness", response_model=BulbResponse)
async def set_brightness(dev_id: str, req: BrightnessRequest):
    """Set brightness (keeps current mode)."""
    mb = _get_bulb(dev_id)
    await asyncio.to_thread(mb.set_brightness, req.value)
    return _bulb_response(mb)


# ── WebSocket ────────────────────────────────────────────────────────────────

_ws_clients: set[WebSocket] = set()


async def _broadcast_state(dev_id: str, state: dict) -> None:
    """Send state update to all connected WebSocket clients in parallel."""
    if not _ws_clients:
        return
    msg = json.dumps({"dev_id": dev_id, "state": state})
    clients = list(_ws_clients)
    results = await asyncio.gather(
        *[ws.send_text(msg) for ws in clients],
        return_exceptions=True,
    )
    for ws, result in zip(clients, results):
        if isinstance(result, Exception):
            _ws_clients.discard(ws)


async def _refresh_and_push(ws: WebSocket, bulbs: list) -> None:
    """Background: re-fetch each bulb, push fresh state to this WS client."""
    async def one(mb):
        try:
            await asyncio.to_thread(mb.fetch_state)
            # fetch_state now triggers the WS broadcast via callback,
            # but that goes to ALL clients. We also send it directly to THIS
            # client to ensure they get the immediate result of their connection refresh.
            await ws.send_text(json.dumps({
                "dev_id": mb.dev_id,
                "state": mb.state.to_dict(),
            }))
        except Exception:
            pass
    await asyncio.gather(*[one(mb) for mb in bulbs], return_exceptions=True)


@app.websocket("/api/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.add(ws)
    logger.info("WS client connected (%d total)", len(_ws_clients))

    # Send cached snapshot immediately so the client renders without delay.
    # A background task then re-fetches each bulb and pushes updated state —
    # this reconciles drift from module effects (LoL flash, ready-check pulse).
    bulbs = device_manager.all_bulbs()
    try:
        for mb in bulbs:
            await ws.send_text(json.dumps({
                "dev_id": mb.dev_id,
                "state": mb.state.to_dict(),
            }))
    except Exception:
        _ws_clients.discard(ws)
        return

    refresh_task = asyncio.create_task(_refresh_and_push(ws, bulbs))

    try:
        while True:
            data = await ws.receive_text()
            logger.debug("WS recv: %s", data[:200])
    except WebSocketDisconnect:
        pass
    finally:
        refresh_task.cancel()
        _ws_clients.discard(ws)
        logger.info("WS client disconnected (%d remaining)", len(_ws_clients))


# ── Health ───────────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {"status": "ok", "devices": len(device_manager.all_bulbs())}


# ── Module API ───────────────────────────────────────────────────────────────

@app.get("/api/modules")
async def list_modules():
    """List all registered modules with their status."""
    return module_manager.get_status()


@app.post("/api/modules/{name}/enable")
async def enable_module(name: str):
    ok = await module_manager.enable_module(name)
    if not ok:
        raise HTTPException(404, f"Module {name} not found")
    mod = module_manager.get(name)
    return mod.get_status() if mod else {}


@app.post("/api/modules/{name}/disable")
async def disable_module(name: str):
    ok = await module_manager.disable_module(name)
    if not ok:
        raise HTTPException(404, f"Module {name} not found")
    mod = module_manager.get(name)
    return mod.get_status() if mod else {}


@app.get("/api/modules/{name}/config")
async def get_module_config(name: str):
    mod = module_manager.get(name)
    if mod is None:
        raise HTTPException(404, f"Module {name} not found")
    return mod.get_config()


@app.post("/api/modules/{name}/config")
async def set_module_config(name: str, config: dict):
    mod = module_manager.get(name)
    if mod is None:
        raise HTTPException(404, f"Module {name} not found")
    mod.set_config(config)
    return mod.get_config()


# ── Serve Flutter web build ─────────────────────────────────────────────────

# Flutter web emits content-hashed filenames under assets/ and canvaskit/ (safe
# to cache aggressively) and non-hashed entrypoints (index.html, main.dart.js,
# flutter.js) that must revalidate so clients pick up new builds.
_IMMUTABLE_PREFIXES = ("assets/", "canvaskit/", "icons/")
_NO_CACHE_FILES = {"index.html", "main.dart.js", "flutter.js",
                   "flutter_bootstrap.js", "flutter_service_worker.js",
                   "manifest.json", "version.json"}


def _cache_control_for(rel_path: str) -> str:
    name = rel_path.rsplit("/", 1)[-1]
    if name in _NO_CACHE_FILES:
        return "no-cache"
    if rel_path.startswith(_IMMUTABLE_PREFIXES):
        return "public, max-age=31536000, immutable"
    return "public, max-age=3600"


if WEB_BUILD_DIR.is_dir():

    class _CachedStatic(StaticFiles):
        """StaticFiles with Cache-Control tuned for Flutter web output."""

        async def get_response(self, path: str, scope: Scope):
            resp = await super().get_response(path, scope)
            if resp.status_code == 200:
                resp.headers["Cache-Control"] = _cache_control_for(path)
            return resp

    app.mount(
        "/static",
        _CachedStatic(directory=str(WEB_BUILD_DIR)),
        name="flutter_static",
    )

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve Flutter web app; any unknown path returns index.html (SPA)."""
        file = WEB_BUILD_DIR / full_path
        if full_path and file.is_file():
            return FileResponse(
                str(file),
                headers={"Cache-Control": _cache_control_for(full_path)},
            )
        return FileResponse(
            str(WEB_BUILD_DIR / "index.html"),
            headers={"Cache-Control": "no-cache"},
        )

