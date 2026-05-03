"""League of Legends Live Client Data module.

Polls the local LoL client API during a match and triggers lighting effects
on Smart Connect bulbs in response to in-game events.

Per-player bulb routing: each tracked nick has a set of bulb dev_ids.
Personal events (kills, deaths, streaks, respawn, low-HP, level-ups)
flash only that player's bulbs; team-wide events (dragon, baron, game end,
first brick, ace, first blood) flash the union of all tracked bulbs on the
affected team.

Endpoint docs: https://developer.riotgames.com/docs/lol#game-client-api
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import ssl
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import httpx
from pydantic import BaseModel, Field

from modules.base import BaseModule


class _PlayerEntry(BaseModel):
    """Wire-format for a single player binding in PUT /players."""
    nick: str
    bulbs: list[str] = Field(default_factory=list)


class _LCUClient:
    """Thin wrapper around the League Client Update (LCU) local API.

    LCU lives at https://127.0.0.1:<port> with a dynamic port + password
    stored in a lockfile alongside the client binary (self-signed TLS).
    """

    def __init__(self, explicit_path: str | None = None) -> None:
        self._explicit_path = explicit_path
        self._cache: tuple[int, str, float] | None = None  # (port, password, mtime)

    def set_lockfile_path(self, path: str | None) -> None:
        if path != self._explicit_path:
            self._explicit_path = path
            self._cache = None

    def _candidates(self) -> list[Path]:
        out: list[Path] = []
        if self._explicit_path:
            out.append(Path(self._explicit_path))
        for c in _LCU_LOCKFILE_CANDIDATES:
            out.append(Path(c))
        lad = os.environ.get("LOCALAPPDATA")
        if lad:
            out.append(Path(lad) / "Riot Games" / "League of Legends" / "lockfile")
        return out

    def _read(self) -> tuple[int, str] | None:
        for path in self._candidates():
            try:
                if not path.is_file():
                    continue
                mtime = path.stat().st_mtime
                if self._cache and self._cache[2] == mtime:
                    return self._cache[0], self._cache[1]
                content = path.read_text(encoding="utf-8").strip()
                parts = content.split(":")
                # Format: name:pid:port:password:protocol
                if len(parts) >= 5:
                    port = int(parts[2])
                    password = parts[3]
                    self._cache = (port, password, mtime)
                    return port, password
            except Exception:
                continue
        self._cache = None
        return None

    @property
    def available(self) -> bool:
        return self._read() is not None

    async def get_json(self, client: httpx.AsyncClient, path: str) -> Any | None:
        creds = self._read()
        if not creds:
            return None
        port, password = creds
        url = f"https://127.0.0.1:{port}{path}"
        try:
            r = await client.get(url, auth=("riot", password), timeout=3.0)
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout):
            return None
        except Exception as e:
            logger.debug("LCU %s error: %s", path, e)
            return None
        if r.status_code != 200:
            return None
        try:
            return r.json()
        except ValueError:
            return None

logger = logging.getLogger("smart_connect.mod.lol_live")

_BASE = "https://127.0.0.1:2999/liveclientdata"
_EVENTS_URL = f"{_BASE}/eventdata"
_PLAYER_URL = f"{_BASE}/activeplayername"
_ACTIVE_PLAYER_URL = f"{_BASE}/activeplayer"
_PLAYERLIST_URL = f"{_BASE}/playerlist"
_GAMESTATS_URL = f"{_BASE}/gamestats"

# LCU (League Client Update) — used pre-game for ready-check detection.
_LCU_READY_CHECK_PATH = "/lol-matchmaking/v1/ready-check"
_LCU_CURRENT_SUMMONER_PATH = "/lol-summoner/v1/current-summoner"
_LCU_LOCKFILE_CANDIDATES = (
    r"C:\Riot Games\League of Legends\lockfile",
    "/Applications/League of Legends.app/Contents/LoL/lockfile",
)

# Persistent config file
_CONFIG_PATH = (Path(__file__).resolve().parent.parent / "data" / "lol_live.json")

# Default effect colours (Tuya HSV hex "hhhhssssvvvv")
_COLOURS: dict[str, str] = {
    # personal combat
    "kill":         "005503e803e8",  # green
    "death":        "000003e803e8",  # red
    "assist":       "00b403e803e8",  # blue
    "multikill":    "010e03e803e8",  # gold
    "low_hp":       "00000bb803e8",  # strong red
    "level_up":     "003c012c03e8",  # warm white
    "respawn":      "003c006403e8",  # soft white

    # kill-streak tiers (kills since last death)
    "spree_3":      "001e03e803e8",  # orange (Killing Spree)
    "spree_4":      "001203e803e8",  # red-orange (Rampage)
    "spree_5":      "010e03e803e8",  # gold (Unstoppable)
    "spree_6":      "011803e803e8",  # purple (Dominating)
    "spree_7":      "013203e803e8",  # magenta (Godlike)
    "spree_8":      "003c012c03e8",  # blinding white (Legendary)

    # team-wide objectives
    "first_blood":  "000a03e803e8",
    "first_brick":  "010e03e803e8",  # gold
    "ace":          "010e03e803e8",
    "dragon":       "002303e803e8",
    "baron":        "011803e803e8",
    "herald":       "00dc03e803e8",
    "turret_kill":        "00aa03e803e8",    # generic fallback cyan
    "turret_lost":        "000003e80320",    # generic fallback dim red
    "turret_inhib_kill":  "010e03e803e8",    # inhib-tier killed (gold)
    "turret_inhib_lost":  "000003e80480",    # inhib-tier lost (rich red)
    "turret_nexus_kill":  "003c03e803e8",    # nexus-tier killed (bright gold)
    "turret_nexus_lost":  "000003e803e8",    # nexus-tier lost (full red)
    "inhib_kill":         "00f003e803e8",
    "inhib_lost":         "000003e80258",

    # Pre-game (LCU ready-check pulse)
    "ready_check":        "00f003e803e8",    # solid blue

    # Global "all bulbs" events (match start, victory, defeat)
    "game_start":         "00b403e803e8",    # bright blue
    "victory":            "005503e803e8",
    "defeat":             "000003e803e8",
}

_EVENT_QUEUE_MAX = 16

# Low-HP hysteresis thresholds (fraction of max)
_LOW_HP_TRIGGER = 0.20
_LOW_HP_RESET = 0.30


def _make_insecure_ssl_ctx() -> ssl.SSLContext:
    """LCU and Live Client both serve self-signed certs on localhost —
    disabling verification here is standard and expected."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# Single shared context; cheap to reuse across httpx clients.
_INSECURE_SSL_CTX = _make_insecure_ssl_ctx()


def _normalize_name(name: str | None) -> str:
    if not name:
        return ""
    n = name.strip().strip('"').strip()
    if "#" in n:
        n = n.split("#", 1)[0]
    return n.casefold()


@dataclass
class PlayerBinding:
    """A tracked in-game nick mapped to a set of bulb dev_ids."""
    nick: str
    bulbs: list[str] = field(default_factory=list)

    # Runtime state — cleared per game
    aliases: set[str] = field(default_factory=set)
    team: str | None = None              # "ORDER" / "CHAOS"
    kills: int = 0
    deaths: int = 0
    streak: int = 0                      # kills since last death
    last_level: int | None = None
    is_dead: bool = False
    low_hp_active: bool = False          # hysteresis flag for LowHP
    is_active: bool = False              # true if this is the client's /activeplayer

    def to_config_dict(self) -> dict:
        return {"nick": self.nick, "bulbs": list(self.bulbs)}

    def reset_runtime(self) -> None:
        self.aliases = set()
        self.team = None
        self.kills = 0
        self.deaths = 0
        self.streak = 0
        self.last_level = None
        self.is_dead = False
        self.low_hp_active = False
        self.is_active = False


class LoLLiveModule(BaseModule):
    name = "lol_live"
    description = "Реакция ламп на события League of Legends (Live Client Data)"

    def __init__(self) -> None:
        super().__init__()
        self._poll_task: asyncio.Task | None = None
        self._effect_task: asyncio.Task | None = None
        self._lcu_task: asyncio.Task | None = None
        self._ready_pulse_task: asyncio.Task | None = None

        # Runtime game state
        self._seen_ids: set[int] = set()
        self._active_player: str | None = None   # raw name reported by client
        self._game_mode: str | None = None       # e.g. "ARAM", "CLASSIC"

        # Config
        self._poll_interval: float = 1.5
        self._game_check_interval: float = 10.0
        self._ready_check_interval: float = 2.0
        self._flash_count: int = 3
        self._flash_duration: float = 0.25
        self._ready_on_ms: int = 700
        self._ready_off_ms: int = 400
        self._lcu_lockfile_path: str | None = None
        self._colours = dict(_COLOURS)
        self._players: list[PlayerBinding] = []
        self._lcu = _LCUClient()

        self._effect_queue: asyncio.Queue[tuple[str, tuple[str, ...]]] = (
            asyncio.Queue(maxsize=_EVENT_QUEUE_MAX)
        )
        self._max_log: int = 50
        self._event_log: deque[dict[str, Any]] = deque(maxlen=self._max_log)

        # Pre-game snapshot per dev_id (only for bulbs assigned to any player)
        self._pre_game_snapshots: dict[str, dict] | None = None

        self._load_config()

    # ── Config persistence ───────────────────────────────────────────────

    def _load_config(self) -> None:
        try:
            if _CONFIG_PATH.is_file():
                raw = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
                self.set_config(raw, persist=False)
                logger.info("Loaded lol_live config from %s", _CONFIG_PATH)
        except Exception as e:
            logger.warning("Failed to load %s: %s", _CONFIG_PATH, e)

    def _save_config(self) -> None:
        try:
            _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            _CONFIG_PATH.write_text(
                json.dumps(self.get_config(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning("Failed to save %s: %s", _CONFIG_PATH, e)

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def on_start(self, device_manager) -> None:
        await super().on_start(device_manager)
        self._reset_game_state()
        self._effect_queue = asyncio.Queue(maxsize=_EVENT_QUEUE_MAX)
        self._poll_task = asyncio.create_task(self._run_loop())
        self._effect_task = asyncio.create_task(self._effect_worker())
        self._lcu_task = asyncio.create_task(self._lcu_loop())
        logger.info("LoL Live module started (players=%d)", len(self._players))

    async def on_stop(self) -> None:
        for t in (self._poll_task, self._effect_task, self._lcu_task,
                  self._ready_pulse_task):
            if t:
                t.cancel()
        self._poll_task = None
        self._effect_task = None
        self._lcu_task = None
        self._ready_pulse_task = None
        try:
            await self._exit_game_mode()
        except Exception as e:
            logger.debug("exit_game_mode on stop: %s", e)
        await super().on_stop()
        logger.info("LoL Live module stopped")

    def _reset_game_state(self) -> None:
        self._seen_ids.clear()
        self._active_player = None
        self._game_mode = None
        for p in self._players:
            p.reset_runtime()

    # ── Config / Status ──────────────────────────────────────────────────

    def get_config(self) -> dict[str, Any]:
        return {
            "poll_interval": self._poll_interval,
            "flash_count": self._flash_count,
            "flash_duration": self._flash_duration,
            "ready_on_ms": self._ready_on_ms,
            "ready_off_ms": self._ready_off_ms,
            "lcu_lockfile_path": self._lcu_lockfile_path,
            "colours": self._colours,
            "players": [p.to_config_dict() for p in self._players],
        }

    def set_config(self, config: dict[str, Any], persist: bool = True) -> None:
        if "poll_interval" in config:
            self._poll_interval = float(config["poll_interval"])
        if "flash_count" in config:
            self._flash_count = int(config["flash_count"])
        if "flash_duration" in config:
            self._flash_duration = float(config["flash_duration"])
        if "ready_on_ms" in config:
            self._ready_on_ms = int(config["ready_on_ms"])
        if "ready_off_ms" in config:
            self._ready_off_ms = int(config["ready_off_ms"])
        if "lcu_lockfile_path" in config:
            v = config["lcu_lockfile_path"]
            self._lcu_lockfile_path = str(v) if v else None
            self._lcu.set_lockfile_path(self._lcu_lockfile_path)
        if "colours" in config and isinstance(config["colours"], dict):
            self._colours.update(config["colours"])
        if "players" in config and isinstance(config["players"], list):
            new_players: list[PlayerBinding] = []
            seen_nicks: set[str] = set()
            for entry in config["players"]:
                if not isinstance(entry, dict):
                    continue
                nick = str(entry.get("nick", "")).strip()
                bulbs = entry.get("bulbs") or []
                if not nick or nick.casefold() in seen_nicks:
                    continue
                seen_nicks.add(nick.casefold())
                new_players.append(PlayerBinding(
                    nick=nick,
                    bulbs=[str(b) for b in bulbs if b],
                ))
            self._players = new_players
        if persist:
            self._save_config()

    def get_status(self) -> dict[str, Any]:
        base = super().get_status()
        base["active_player"] = self._active_player
        base["game_mode"] = self._game_mode
        base["events_seen"] = len(self._seen_ids)
        base["in_game"] = self._active_player is not None
        base["event_log"] = list(self._event_log)
        base["queued_effects"] = self._effect_queue.qsize()
        base["lcu_available"] = self._lcu.available
        base["ready_check_pulse"] = (
            self._ready_pulse_task is not None
            and not self._ready_pulse_task.done()
        )
        base["players"] = [
            {
                "nick": p.nick,
                "bulbs": list(p.bulbs),
                "team": p.team,
                "kills": p.kills,
                "deaths": p.deaths,
                "streak": p.streak,
                "is_dead": p.is_dead,
                "is_active": p.is_active,
            }
            for p in self._players
        ]
        return base

    def _log_event(self, icon: str, title: str, detail: str = "") -> None:
        entry = {"ts": time.time(), "icon": icon, "title": title, "detail": detail}
        self._event_log.append(entry)

    # ── Bulb-set helpers ────────────────────────────────────────────────

    def _all_tracked_bulbs(self) -> set[str]:
        out: set[str] = set()
        for p in self._players:
            out.update(p.bulbs)
        return out

    def _all_system_bulbs(self) -> set[str]:
        """Every bulb known to the device manager — for global events."""
        dm = self._device_manager
        if dm is None:
            return set()
        return {mb.dev_id for mb in dm.all_bulbs()}

    def _team_bulbs(self, team: str | None) -> set[str]:
        if not team:
            return self._all_tracked_bulbs()
        out: set[str] = set()
        for p in self._players:
            if p.team == team:
                out.update(p.bulbs)
        return out

    def _player_by_name(self, name: str | None) -> PlayerBinding | None:
        n = _normalize_name(name)
        if not n:
            return None
        for p in self._players:
            if n in p.aliases:
                return p
        return None

    # ── Main loop ────────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        async with httpx.AsyncClient(verify=_INSECURE_SSL_CTX, timeout=3.0) as client:
            while True:
                try:
                    await self._wait_for_game(client)
                    await self._enter_game_mode()
                    await self._poll_game(client)
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    logger.debug("Loop iteration error: %s", e)
                    await asyncio.sleep(self._game_check_interval)
                finally:
                    try:
                        await self._exit_game_mode()
                    except Exception as e:
                        logger.warning("exit_game_mode error: %s", e)
                    self._reset_game_state()

    async def _wait_for_game(self, client: httpx.AsyncClient) -> None:
        while True:
            try:
                resp = await client.get(_PLAYER_URL)
                if resp.status_code == 200:
                    raw = resp.text.strip().strip('"')
                    self._active_player = raw
                    self._seen_ids.clear()
                    await self._identify_players(client, raw)
                    await self._fetch_game_mode(client)
                    tracked = [p.nick for p in self._players if p.aliases]
                    logger.info(
                        "Game detected — mode=%s active=%s tracked=%s",
                        self._game_mode, raw, tracked,
                    )
                    self._log_event(
                        "game",
                        f"Игра определена ({self._game_mode or '?'})",
                        ", ".join(tracked) or raw,
                    )
                    return
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout):
                pass
            await asyncio.sleep(self._game_check_interval)

    async def _fetch_game_mode(self, client: httpx.AsyncClient) -> None:
        try:
            r = await client.get(_GAMESTATS_URL)
        except Exception:
            return
        if r.status_code != 200:
            return
        try:
            data = r.json() or {}
        except ValueError:
            return
        mode = data.get("gameMode")
        if isinstance(mode, str):
            self._game_mode = mode.upper()

    async def _identify_players(self, client: httpx.AsyncClient,
                                 active_raw: str) -> None:
        """Populate each PlayerBinding with aliases + team from the live client."""
        for p in self._players:
            p.reset_runtime()
            p.aliases = {_normalize_name(p.nick)}

        # Resolve active player's full identity
        active_aliases: set[str] = {_normalize_name(active_raw)}
        try:
            r = await client.get(_ACTIVE_PLAYER_URL)
            if r.status_code == 200:
                data = r.json()
                for key in ("summonerName", "riotId", "riotIdGameName"):
                    v = data.get(key)
                    if isinstance(v, str):
                        active_aliases.add(_normalize_name(v))
        except Exception as e:
            logger.debug("activeplayer fetch failed: %s", e)
        active_aliases.discard("")

        # Merge into the tracked player that matches; mark is_active
        active_binding: PlayerBinding | None = None
        for p in self._players:
            if p.aliases & active_aliases:
                p.aliases |= active_aliases
                p.is_active = True
                active_binding = p
                break

        # Pull /playerlist to resolve teams + add extra aliases per-player
        try:
            r = await client.get(_PLAYERLIST_URL)
            if r.status_code == 200:
                players = r.json() or []
                for entry in players:
                    candidates = {
                        _normalize_name(entry.get("summonerName")),
                        _normalize_name(entry.get("riotId")),
                        _normalize_name(entry.get("riotIdGameName")),
                    }
                    candidates.discard("")
                    for p in self._players:
                        if p.aliases & candidates:
                            p.aliases |= candidates
                            team = entry.get("team")
                            if isinstance(team, str):
                                p.team = team.upper()
                            scores = entry.get("scores") or {}
                            p.kills = int(scores.get("kills") or 0)
                            p.deaths = int(scores.get("deaths") or 0)
                            p.last_level = entry.get("level")
                            p.is_dead = bool(entry.get("isDead"))
                            break
        except Exception as e:
            logger.debug("playerlist fetch failed: %s", e)

        if active_binding is None:
            logger.info("Active player %r not in tracked list — personal events "
                        "(HP/level-up) will be skipped for them", active_raw)

    async def _poll_game(self, client: httpx.AsyncClient) -> None:
        """Drive event stream + per-player stat polling until the game ends.

        All three Live Client endpoints are hit concurrently each tick —
        serialising them added ~3x latency per cycle for no benefit.
        """
        consecutive_errors = 0
        net_errors = (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout)
        while True:
            results = await asyncio.gather(
                self._poll_event_stream(client),
                self._poll_active_player(client),
                self._poll_playerlist(client),
                return_exceptions=True,
            )
            had_net_error = False
            for r in results:
                if isinstance(r, _GameEnded):
                    return
                if isinstance(r, net_errors):
                    had_net_error = True
                elif isinstance(r, Exception):
                    logger.debug("poll subtask error: %s", r)
            if had_net_error:
                consecutive_errors += 1
                if consecutive_errors > 5:
                    logger.info("Lost connection to LoL client — game ended")
                    return
            else:
                consecutive_errors = 0
            await asyncio.sleep(self._poll_interval)

    async def _poll_event_stream(self, client: httpx.AsyncClient) -> None:
        resp = await client.get(_EVENTS_URL)
        if resp.status_code != 200:
            if resp.status_code in (404,) and self._seen_ids:
                raise _GameEnded()
            return
        data = resp.json()
        for ev in data.get("Events", []):
            eid = ev.get("EventID")
            if eid is None or eid in self._seen_ids:
                continue
            self._seen_ids.add(eid)
            try:
                await self._handle_event(ev)
            except Exception as e:
                logger.warning("Event handler error for %s: %s", ev, e)

    async def _poll_active_player(self, client: httpx.AsyncClient) -> None:
        """LowHP (active player only) — polled separately so we don't miss dips."""
        active = next((p for p in self._players if p.is_active), None)
        if active is None or not active.bulbs:
            return
        try:
            r = await client.get(_ACTIVE_PLAYER_URL)
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout):
            return
        if r.status_code != 200:
            return
        try:
            stats = (r.json() or {}).get("championStats") or {}
        except ValueError:
            return
        max_hp = float(stats.get("maxHealth") or 0)
        cur_hp = float(stats.get("currentHealth") or 0)
        if max_hp <= 0:
            return
        pct = cur_hp / max_hp
        if not active.is_dead and pct <= _LOW_HP_TRIGGER and not active.low_hp_active:
            active.low_hp_active = True
            self._log_event("low_hp", f"Мало HP: {int(pct*100)}%", "")
            self._queue_effect("low_hp", active.bulbs)
        elif pct >= _LOW_HP_RESET and active.low_hp_active:
            active.low_hp_active = False

    async def _poll_playerlist(self, client: httpx.AsyncClient) -> None:
        """Detect level-ups and respawns for every tracked player."""
        tracked = [p for p in self._players if p.bulbs and p.aliases]
        if not tracked:
            return
        try:
            r = await client.get(_PLAYERLIST_URL)
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout):
            return
        if r.status_code != 200:
            return
        try:
            entries = r.json() or []
        except ValueError:
            return

        for entry in entries:
            candidates = {
                _normalize_name(entry.get("summonerName")),
                _normalize_name(entry.get("riotId")),
                _normalize_name(entry.get("riotIdGameName")),
            }
            candidates.discard("")
            match = next((p for p in tracked if p.aliases & candidates), None)
            if match is None:
                continue

            # Level up
            new_level = entry.get("level")
            if (isinstance(new_level, int)
                    and match.last_level is not None
                    and new_level > match.last_level):
                self._log_event("level_up", f"{match.nick}: ур. {new_level}", "")
                self._queue_effect("level_up", match.bulbs)
            if isinstance(new_level, int):
                match.last_level = new_level

            # Respawn: isDead true -> false
            is_dead_now = bool(entry.get("isDead"))
            if match.is_dead and not is_dead_now:
                self._log_event("respawn", f"{match.nick}: респавн", "")
                self._queue_effect("respawn", match.bulbs)
                match.low_hp_active = False  # clear HP latch on respawn
            match.is_dead = is_dead_now

    # ── Event handling ───────────────────────────────────────────────────

    def _turret_side(self, turret_id: str | None) -> str | None:
        """Return 'ORDER'/'CHAOS' owner team of a 'Turret_T1_*' / 'Turret_T2_*' id."""
        if not turret_id:
            return None
        if "_T1_" in turret_id:
            return "ORDER"
        if "_T2_" in turret_id:
            return "CHAOS"
        return None

    @staticmethod
    def _turret_tier(turret_id: str | None) -> str | None:
        """Classify a turret as outer/inner/inhib/nexus by its numeric code.

        Turret IDs look like 'Turret_T1_C_01_A' / 'Turret_T2_L_02_A' etc.
        The two-digit code tends to sit between underscores: higher numbers
        are deeper in the base (inhib/nexus towers).
        """
        if not turret_id:
            return None
        m = re.search(r"_(\d{2})_", turret_id)
        if not m:
            return None
        try:
            n = int(m.group(1))
        except ValueError:
            return None
        if n <= 3:
            return "outer"
        if n == 4:
            return "inner"
        if n == 5:
            return "inhib"
        return "nexus"

    def _other_team(self, team: str | None) -> str | None:
        if team == "ORDER":
            return "CHAOS"
        if team == "CHAOS":
            return "ORDER"
        return None

    async def _handle_event(self, ev: dict) -> None:
        name = ev.get("EventName", "")
        game_time = ev.get("EventTime", 0)
        t_str = f"{int(game_time)//60}:{int(game_time)%60:02d}"

        if name == "ChampionKill":
            self._handle_champion_kill(ev, t_str)

        elif name == "Multikill":
            killer = self._player_by_name(ev.get("KillerName"))
            if killer:
                streak = ev.get("KillStreak", 0)
                self._log_event("multikill", f"{killer.nick}: мультикилл x{streak}", t_str)
                self._queue_effect("multikill", killer.bulbs)

        elif name == "Ace":
            acer_team = (ev.get("AcingTeam") or "").upper()
            bulbs = self._team_bulbs(acer_team) or self._all_tracked_bulbs()
            self._log_event("ace", f"Ace — {acer_team or '?'}", t_str)
            self._queue_effect("ace", bulbs)

        elif name == "FirstBlood":
            recipient = self._player_by_name(ev.get("Recipient"))
            bulbs = recipient.bulbs if recipient else self._all_tracked_bulbs()
            self._log_event("first_blood",
                            f"First Blood{': ' + recipient.nick if recipient else ''}",
                            t_str)
            self._queue_effect("first_blood", bulbs)

        elif name == "FirstBrick":
            killer = self._player_by_name(ev.get("KillerName"))
            bulbs = killer.bulbs if killer else self._all_tracked_bulbs()
            self._log_event("first_brick",
                            f"Первая башня{': ' + killer.nick if killer else ''}",
                            t_str)
            self._queue_effect("first_brick", bulbs)

        elif name == "DragonKill":
            self._handle_team_objective(ev, "dragon", ev.get("DragonType") or "Дракон", t_str)

        elif name == "BaronKill":
            self._handle_team_objective(ev, "baron", "Барон", t_str)

        elif name in ("HeraldKill", "RiftHeraldKill"):
            self._handle_team_objective(ev, "herald", "Вестник", t_str)

        elif name == "TurretKilled":
            struct_id = ev.get("TurretKilled")
            tier = self._turret_tier(struct_id)
            # Upgrade to tiered key only when we have a specific colour for it
            killed_key = "turret_kill"
            lost_key = "turret_lost"
            tier_label = "Башня"
            if tier in ("inhib", "nexus"):
                if f"turret_{tier}_kill" in self._colours:
                    killed_key = f"turret_{tier}_kill"
                if f"turret_{tier}_lost" in self._colours:
                    lost_key = f"turret_{tier}_lost"
                tier_label = "Башня базы" if tier == "nexus" else "Башня инхиба"
            self._handle_structure(ev, struct_id,
                                   killed_key=killed_key,
                                   lost_key=lost_key,
                                   label=tier_label, t_str=t_str)

        elif name == "InhibKilled":
            self._handle_structure(ev, ev.get("InhibKilled"),
                                   killed_key="inhib_kill",
                                   lost_key="inhib_lost",
                                   label="Ингибитор", t_str=t_str)

        elif name == "GameEnd":
            result = ev.get("Result", "")
            effect = "victory" if result == "Win" else "defeat"
            self._log_event(effect, "Победа!" if result == "Win" else "Поражение", t_str)
            self._queue_effect(effect, self._all_system_bulbs())

        elif name == "GameStart":
            self._log_event("game_start", "Игра началась", t_str)
            self._queue_effect("game_start", self._all_system_bulbs())

    def _handle_champion_kill(self, ev: dict, t_str: str) -> None:
        killer_name = ev.get("KillerName")
        victim_name = ev.get("VictimName")
        assisters = ev.get("Assisters") or []

        killer = self._player_by_name(killer_name)
        victim = self._player_by_name(victim_name)

        if killer:
            killer.kills += 1
            killer.streak += 1
            self._log_event("kill", f"{killer.nick} → {victim_name}", t_str)
            self._queue_effect("kill", killer.bulbs)
            # Kill-streak tier
            tier = min(max(killer.streak, 3), 8)
            if killer.streak >= 3:
                self._log_event(f"spree_{tier}",
                                f"{killer.nick}: серия x{killer.streak}", t_str)
                self._queue_effect(f"spree_{tier}", killer.bulbs)

        if victim:
            victim.deaths += 1
            victim.streak = 0
            victim.low_hp_active = False
            self._log_event("death", f"{victim.nick} ← {killer_name}", t_str)
            self._queue_effect("death", victim.bulbs)

        for a_name in assisters:
            assister = self._player_by_name(a_name)
            if assister and assister is not killer:
                self._log_event("assist", f"{assister.nick}: ассист {victim_name}", t_str)
                self._queue_effect("assist", assister.bulbs)

    def _handle_team_objective(self, ev: dict, key: str,
                                label: str, t_str: str) -> None:
        killer = self._player_by_name(ev.get("KillerName"))
        stolen = str(ev.get("Stolen", "")).lower() == "true"
        if killer:
            bulbs = killer.bulbs
            detail = f"{killer.nick}{' (украл)' if stolen else ''} {t_str}".strip()
        else:
            bulbs = self._all_tracked_bulbs()
            detail = f"{'(украл) ' if stolen else ''}{t_str}".strip()
        self._log_event(key, label, detail)
        self._queue_effect(key, bulbs)

    def _handle_structure(self, ev: dict, struct_id: str | None,
                           killed_key: str, lost_key: str,
                           label: str, t_str: str) -> None:
        """Turret/Inhib routing: tracked killer → their bulbs; else route
        by owner side vs every tracked player's team."""
        owner = self._turret_side(struct_id)   # who owns the structure
        killer = self._player_by_name(ev.get("KillerName"))

        if killer is not None:
            # Tracked player did it
            bulbs = killer.bulbs
            is_ally_target = (killer.team is not None and killer.team == owner)
            key = lost_key if is_ally_target else killed_key
            suffix = "потерян" if is_ally_target else "снесён"
            self._log_event(key, f"{label} {suffix} — {killer.nick}", t_str)
            self._queue_effect(key, bulbs)
            return

        # Untracked killer — light tracked players on the affected side
        if owner is None:
            self._log_event(killed_key, f"{label}", t_str)
            self._queue_effect(killed_key, self._all_tracked_bulbs())
            return
        # Allies on the owning team "lost" the structure; opponents "killed" it.
        lost_bulbs = self._team_bulbs(owner)
        won_bulbs = self._team_bulbs(self._other_team(owner))
        if lost_bulbs:
            self._log_event(lost_key, f"{label} потерян", t_str)
            self._queue_effect(lost_key, lost_bulbs)
        if won_bulbs:
            self._log_event(killed_key, f"{label} снесён", t_str)
            self._queue_effect(killed_key, won_bulbs)

    # ── Effect queue / worker ───────────────────────────────────────────

    def _queue_effect(self, key: str, bulbs: Iterable[str]) -> None:
        if not self.enabled:
            return
        bulb_tuple = tuple(sorted(set(bulbs)))
        if not bulb_tuple:
            return
        try:
            self._effect_queue.put_nowait((key, bulb_tuple))
        except asyncio.QueueFull:
            try:
                self._effect_queue.get_nowait()
                self._effect_queue.task_done()
                self._effect_queue.put_nowait((key, bulb_tuple))
            except Exception:
                logger.debug("Effect queue saturated — dropping '%s'", key)

    async def _effect_worker(self) -> None:
        while True:
            try:
                key, bulbs = await self._effect_queue.get()
                try:
                    await self._flash(key, bulbs)
                except Exception as e:
                    logger.error("Flash '%s' failed: %s", key, e)
                finally:
                    self._effect_queue.task_done()
            except asyncio.CancelledError:
                return

    # ── LCU ready-check (pre-game blue pulse) ────────────────────────────

    async def _lcu_loop(self) -> None:
        """Poll LCU ready-check endpoint; start/stop the blue pulse task."""
        async with httpx.AsyncClient(verify=_INSECURE_SSL_CTX, timeout=3.0) as client:
            while True:
                try:
                    await self._lcu_tick(client)
                except asyncio.CancelledError:
                    await self._stop_ready_pulse()
                    return
                except Exception as e:
                    logger.debug("LCU tick error: %s", e)
                await asyncio.sleep(self._ready_check_interval)

    async def _lcu_tick(self, client: httpx.AsyncClient) -> None:
        # Skip pulse during an actual match — in-game light show takes over.
        if self._active_player is not None:
            await self._stop_ready_pulse()
            return
        if not self.enabled or not self._lcu.available:
            await self._stop_ready_pulse()
            return

        data = await self._lcu.get_json(client, _LCU_READY_CHECK_PATH)
        if not isinstance(data, dict):
            await self._stop_ready_pulse()
            return

        state = data.get("state")
        player_response = data.get("playerResponse")
        prompt_open = (state == "InProgress" and player_response == "None")

        if prompt_open:
            if self._ready_pulse_task is None or self._ready_pulse_task.done():
                bulbs = await self._resolve_ready_check_bulbs(client)
                if bulbs:
                    self._log_event("ready_check", "Найдена игра — принять?", "")
                    self._ready_pulse_task = asyncio.create_task(
                        self._ready_check_pulse_loop(bulbs)
                    )
        else:
            await self._stop_ready_pulse()

    async def _stop_ready_pulse(self) -> None:
        t = self._ready_pulse_task
        if t is None:
            return
        self._ready_pulse_task = None
        if not t.done():
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    async def _resolve_ready_check_bulbs(
        self, client: httpx.AsyncClient
    ) -> tuple[str, ...]:
        """Find the client's tracked player via LCU; fall back to all tracked."""
        me_aliases: set[str] = set()
        data = await self._lcu.get_json(client, _LCU_CURRENT_SUMMONER_PATH)
        if isinstance(data, dict):
            for key in ("displayName", "gameName", "internalName"):
                v = data.get(key)
                if isinstance(v, str):
                    me_aliases.add(_normalize_name(v))
            riot_id = data.get("riotId")
            if isinstance(riot_id, dict):
                for key in ("gameName", "tagLine"):
                    v = riot_id.get(key)
                    if isinstance(v, str):
                        me_aliases.add(_normalize_name(v))
        me_aliases.discard("")

        for p in self._players:
            nick_norm = _normalize_name(p.nick)
            if nick_norm and nick_norm in me_aliases:
                if p.bulbs:
                    return tuple(sorted(set(p.bulbs)))

        return tuple(sorted(self._all_tracked_bulbs()))

    async def _ready_check_pulse_loop(self, bulb_ids: tuple[str, ...]) -> None:
        """Blue on/off pulse until cancelled; restore snapshot on exit."""
        colour_hex = self._colours.get("ready_check")
        dm = self.device_manager
        if not colour_hex or dm is None:
            return
        bulbs = [mb for mb in dm.all_bulbs() if mb.dev_id in bulb_ids]
        if not bulbs:
            return

        loop = asyncio.get_running_loop()
        await asyncio.gather(
            *[loop.run_in_executor(None, mb.fetch_state) for mb in bulbs],
            return_exceptions=True,
        )
        snapshots = {mb.dev_id: mb.state.to_dict() for mb in bulbs}

        on_delay = max(0.05, self._ready_on_ms / 1000.0)
        off_delay = max(0.05, self._ready_off_ms / 1000.0)

        try:
            while True:
                # If user manually interacted with any pulse bulb, stop the pulse
                if any(mb.last_manual_interaction > time.time() - 5.0 for mb in bulbs):
                    logger.info("Ready-check pulse interrupted by manual interaction")
                    break

                await asyncio.gather(
                    *[loop.run_in_executor(
                        None, lambda m=mb: m.set_colour_hex(colour_hex, nowait=True, is_manual=False)
                    ) for mb in bulbs],
                    return_exceptions=True,
                )
                await asyncio.sleep(on_delay)
                await asyncio.gather(
                    *[loop.run_in_executor(
                        None, lambda m=mb: m.turn_off(is_manual=False)
                    ) for mb in bulbs],
                    return_exceptions=True,
                )
                await asyncio.sleep(off_delay)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning("Ready-check pulse error: %s", e)
        finally:
            try:
                # Only restore if user hasn't taken over
                await asyncio.gather(
                    *[self._restore_with_retry(mb, snapshots.get(mb.dev_id, {}), loop)
                      for mb in bulbs if mb.last_manual_interaction < time.time() - 2.0],
                    return_exceptions=True,
                )
            except Exception as e:
                logger.debug("Ready-check restore error: %s", e)

    # ── Game-mode lifecycle (bulbs held OFF between events) ─────────────

    async def _enter_game_mode(self) -> None:
        dm = self.device_manager
        # Always initialise so global events can lazily add their own snapshots.
        self._pre_game_snapshots = {}
        self._game_start_time = time.time()
        wanted = self._all_tracked_bulbs()
        bulbs = [mb for mb in dm.all_bulbs() if mb.dev_id in wanted]
        if not bulbs:
            logger.info("No tracked bulbs — skipping enter_game_mode")
            return
        loop = asyncio.get_running_loop()

        await asyncio.gather(
            *[loop.run_in_executor(None, mb.fetch_state) for mb in bulbs],
            return_exceptions=True,
        )
        for mb in bulbs:
            self._pre_game_snapshots[mb.dev_id] = mb.state.to_dict()
        logger.info("Entering game mode — snapshot captured for %d bulbs",
                    len(bulbs))
        await asyncio.gather(
            *[self._force_off(mb, loop) for mb in bulbs],
            return_exceptions=True,
        )

    async def _exit_game_mode(self) -> None:
        snaps = self._pre_game_snapshots
        self._pre_game_snapshots = None
        if not snaps:
            return
        dm = self._device_manager
        if dm is None:
            return

        # Drop any pending flash effects so they can't fire after restore and
        # leave bulbs in the last event colour.
        while not self._effect_queue.empty():
            try:
                self._effect_queue.get_nowait()
                self._effect_queue.task_done()
            except asyncio.QueueEmpty:
                break

        loop = asyncio.get_running_loop()
        # Filter out bulbs that were manually interacted with DURING the game
        bulbs = [mb for mb in dm.all_bulbs() if mb.dev_id in snaps]
        to_restore = []
        for mb in bulbs:
            if mb.last_manual_interaction > self._game_start_time:
                logger.info("[%s] skipping restore — manual interaction detected during game", mb.name)
            else:
                to_restore.append(mb)

        logger.info("Exiting game mode — restoring %d/%d bulbs", len(to_restore), len(bulbs))
        await asyncio.gather(
            *[self._restore_with_retry(mb, snaps.get(mb.dev_id, {}), loop)
              for mb in to_restore],
            return_exceptions=True,
        )

    # ── Lighting effects ─────────────────────────────────────────────────

    async def _flash(self, effect_key: str, bulb_ids: tuple[str, ...]) -> None:
        colour_hex = self._colours.get(effect_key)
        if not colour_hex or not self.enabled:
            return
        dm = self.device_manager
        loop = asyncio.get_running_loop()
        bulbs = [mb for mb in dm.all_bulbs() if mb.dev_id in bulb_ids]
        if not bulbs:
            return

        # Lazily snapshot any bulb that wasn't captured at enter_game_mode
        # (e.g. non-tracked bulbs pulled in by a global event) so we can
        # still restore it at game exit.
        snaps = self._pre_game_snapshots
        if snaps is not None:
            missing = [mb for mb in bulbs if mb.dev_id not in snaps]
            if missing:
                await asyncio.gather(
                    *[loop.run_in_executor(None, mb.fetch_state) for mb in missing],
                    return_exceptions=True,
                )
                for mb in missing:
                    snaps[mb.dev_id] = mb.state.to_dict()

        await asyncio.gather(
            *[self._flash_one(mb, colour_hex, loop) for mb in bulbs],
            return_exceptions=True,
        )

    async def _flash_one(self, mb, colour_hex: str, loop) -> None:
        try:
            for i in range(self._flash_count):
                # Interrupt flash if user manually interacted recently
                if mb.last_manual_interaction > time.time() - 3.0:
                    logger.debug("[%s] flash interrupted by manual interaction", mb.name)
                    return

                try:
                    await loop.run_in_executor(
                        None, lambda: mb.set_colour_hex(colour_hex, nowait=True, is_manual=False)
                    )
                except Exception as e:
                    logger.debug("[%s] flash on error: %s", mb.name, e)
                await asyncio.sleep(self._flash_duration)

                if i < self._flash_count - 1:
                    try:
                        await loop.run_in_executor(
                            None, lambda: mb.turn_off(is_manual=False)
                        )
                    except Exception as e:
                        logger.debug("[%s] flash off error: %s", mb.name, e)
                    await asyncio.sleep(self._flash_duration * 0.6)
        except Exception as e:
            logger.warning("[%s] flash phase error: %s", mb.name, e)

        await asyncio.sleep(0.2)
        # Final force_off only if still no manual interaction
        if mb.last_manual_interaction < time.time() - 2.0:
            await self._force_off(mb, loop)

    async def _force_off(self, mb, loop, max_attempts: int = 3) -> None:
        last_err: Exception | None = None
        for attempt in range(max_attempts):
            try:
                await loop.run_in_executor(None, lambda: mb.turn_off(is_manual=False))
                await asyncio.sleep(0.15)
                try:
                    await loop.run_in_executor(None, mb.fetch_state)
                except Exception:
                    pass
                if mb.state.on is False:
                    return
                last_err = RuntimeError("state still 'on' after turn_off")
            except Exception as e:
                last_err = e
                logger.warning("[%s] force_off attempt %d failed: %s",
                               mb.name, attempt + 1, e)
                try:
                    await loop.run_in_executor(None, mb.reconnect)
                except Exception:
                    pass
            await asyncio.sleep(0.3 * (attempt + 1))
        logger.error("[%s] force_off exhausted retries (%s)", mb.name, last_err)

    async def _restore_with_retry(self, mb, snapshot: dict, loop,
                                   max_attempts: int = 3) -> None:
        if not snapshot:
            return
        last_err: Exception | None = None
        for attempt in range(max_attempts):
            try:
                await loop.run_in_executor(
                    None, lambda: mb.apply_snapshot(snapshot, nowait=False, is_manual=False)
                )
                await asyncio.sleep(0.15)
                try:
                    await loop.run_in_executor(None, mb.fetch_state)
                except Exception:
                    pass
                if self._state_matches(mb.state.to_dict(), snapshot):
                    return
                last_err = RuntimeError("state mismatch after restore")
            except Exception as e:
                last_err = e
                logger.warning("[%s] restore attempt %d failed: %s",
                               mb.name, attempt + 1, e)
                try:
                    await loop.run_in_executor(None, mb.reconnect)
                except Exception:
                    pass
            await asyncio.sleep(0.3 * (attempt + 1))
        logger.error("[%s] restore exhausted retries (%s)", mb.name, last_err)

    @staticmethod
    def _state_matches(current: dict, snapshot: dict) -> bool:
        if current.get("on") != snapshot.get("on"):
            return False
        if not snapshot.get("on"):
            return True
        if current.get("mode") != snapshot.get("mode"):
            return False
        if snapshot.get("mode") == "white":
            return (current.get("brightness") == snapshot.get("brightness")
                    and current.get("color_temp") == snapshot.get("color_temp"))
        return current.get("hsv_hex") == snapshot.get("hsv_hex")

    # ── Routes ──────────────────────────────────────────────────────────

    def register_routes(self, app) -> None:
        """Register extra REST endpoints for player-bulb binding ergonomics."""
        from fastapi import Body

        @app.get("/api/modules/lol_live/players")
        async def _get_players():
            return [p.to_config_dict() for p in self._players]

        @app.put("/api/modules/lol_live/players")
        async def _set_players(
            players: list[_PlayerEntry] = Body(..., embed=False),
        ):
            self.set_config({"players": [p.model_dump() for p in players]})
            return [p.to_config_dict() for p in self._players]


class _GameEnded(Exception):
    """Internal signal — raised when poll cycle decides the game is over."""
