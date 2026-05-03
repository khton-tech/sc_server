"""Clap listener module.

Continuously listens on an input audio device, detects short loud transients
as "claps", and toggles a configurable set of bulbs when two claps arrive
within a user-tunable window.

The audio capture runs on the sounddevice callback thread; bulb actions are
dispatched back onto the main asyncio loop via ``call_soon_threadsafe`` —
mirroring how ``DeviceManager`` surfaces state changes to the WS layer.

`sounddevice` / PortAudio may be absent on some hosts; the import is lazy so
the server still starts (the module just reports ``start_error`` in status).
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

from .base import BaseModule

logger = logging.getLogger("smart_connect.mod.clap_listen")

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "data" / "clap_listen.json"
_MAX_LOG = 30

# Feature extraction window: ~64 ms of audio (1024 samples at 16 kHz).
# Large enough to capture a clap's full envelope, small enough to keep FFT fast.
_FEATURE_WIN_SAMPLES = 1024

# Frequency bands (Hz) used to build a coarse spectrum signature.
_BANDS_HZ = ((0, 500), (500, 1500), (1500, 3000),
             (3000, 5000), (5000, 7000), (7000, 8000))


class ClapListenModule(BaseModule):
    name = "clap_listen"
    description = "Двойной хлопок включает/выключает лампы"

    def __init__(self) -> None:
        super().__init__()

        # ── Tunable config ──────────────────────────────────────────────
        self._threshold: float = 0.25          # peak amplitude (0..1) to count as a clap
        self._min_gap_ms: int = 120            # debounce between clap hits
        self._double_clap_window_ms: int = 700 # max spacing for a double-clap
        self._cooldown_ms: int = 1500          # lock-out after firing
        self._sample_rate: int = 16000
        self._block_ms: int = 30               # analysis window size
        self._device: int | None = None        # input device index (None = default)
        self._bulb_ids: list[str] = []         # empty = every known bulb

        # ── Runtime state ───────────────────────────────────────────────
        self._stream: Any = None                # sounddevice.InputStream
        self._loop: asyncio.AbstractEventLoop | None = None
        self._last_clap_ts: float = 0.0
        self._last_trigger_ts: float = 0.0
        self._last_reject_log_ts: float = 0.0
        self._pending_first_clap: float | None = None
        self._current_level: float = 0.0
        self._peak_recent: float = 0.0
        self._event_log: deque[dict[str, Any]] = deque(maxlen=_MAX_LOG)
        self._lock = threading.Lock()
        self._start_error: str | None = None
        self._available_devices: list[dict[str, Any]] = []

        # Rolling audio buffer — populated by the sd callback so we can pull
        # a fixed window of samples around each detected transient.
        self._ring_buffer: deque[Any] = deque(maxlen=8)

        # Acoustic profile built by calibration — feature vector + cosine
        # similarity threshold. If None, no classifier runs (any loud
        # transient counts as a clap).
        self._profile: dict[str, Any] | None = None
        self._last_similarity: float = 0.0
        self._reject_count: int = 0

        # Calibration state
        self._calibration_mode: bool = False
        self._calibration_label: str | None = None   # "positive" | "negative"
        self._calibration_target: int = 0
        self._calibration_positive: list[list[float]] = []
        self._calibration_negative: list[list[float]] = []

        self._load_config()

    # ── Config persistence ───────────────────────────────────────────────

    def _load_config(self) -> None:
        try:
            if _CONFIG_PATH.is_file():
                raw = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
                self.set_config(raw, persist=False)
                logger.info("Loaded clap_listen config from %s", _CONFIG_PATH)
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

    def get_config(self) -> dict[str, Any]:
        return {
            "threshold": self._threshold,
            "min_gap_ms": self._min_gap_ms,
            "double_clap_window_ms": self._double_clap_window_ms,
            "cooldown_ms": self._cooldown_ms,
            "sample_rate": self._sample_rate,
            "block_ms": self._block_ms,
            "device": self._device,
            "bulb_ids": list(self._bulb_ids),
            "profile": self._profile,
        }

    def set_config(self, config: dict[str, Any], persist: bool = True) -> None:
        stream_params_changed = False

        if "threshold" in config:
            try:
                self._threshold = max(0.01, min(1.0, float(config["threshold"])))
            except (TypeError, ValueError):
                pass
        if "min_gap_ms" in config:
            try:
                self._min_gap_ms = max(30, int(config["min_gap_ms"]))
            except (TypeError, ValueError):
                pass
        if "double_clap_window_ms" in config:
            try:
                self._double_clap_window_ms = max(150, int(config["double_clap_window_ms"]))
            except (TypeError, ValueError):
                pass
        if "cooldown_ms" in config:
            try:
                self._cooldown_ms = max(200, int(config["cooldown_ms"]))
            except (TypeError, ValueError):
                pass
        if "sample_rate" in config:
            try:
                new = int(config["sample_rate"])
                if new != self._sample_rate:
                    self._sample_rate = new
                    stream_params_changed = True
            except (TypeError, ValueError):
                pass
        if "block_ms" in config:
            try:
                new = max(10, min(100, int(config["block_ms"])))
                if new != self._block_ms:
                    self._block_ms = new
                    stream_params_changed = True
            except (TypeError, ValueError):
                pass
        if "device" in config:
            v = config["device"]
            new_dev: int | None
            if v is None or v == "":
                new_dev = None
            else:
                try:
                    new_dev = int(v)
                except (TypeError, ValueError):
                    new_dev = None
            if new_dev != self._device:
                self._device = new_dev
                stream_params_changed = True
        if "bulb_ids" in config and isinstance(config["bulb_ids"], list):
            self._bulb_ids = [str(b) for b in config["bulb_ids"] if b]
        if "profile" in config:
            prof = config["profile"]
            if isinstance(prof, dict) and isinstance(prof.get("mean"), list):
                self._profile = {
                    "mean": [float(x) for x in prof["mean"]],
                    "threshold": float(prof.get("threshold", 0.85)),
                    "pos_count": int(prof.get("pos_count", 0)),
                    "neg_count": int(prof.get("neg_count", 0)),
                    "min_pos_sim": prof.get("min_pos_sim"),
                    "max_neg_sim": prof.get("max_neg_sim"),
                }
            elif prof is None:
                self._profile = None

        if persist:
            self._save_config()

        if stream_params_changed and self.enabled:
            try:
                self._stop_stream()
                self._start_stream()
            except Exception as e:
                logger.error("Failed to restart audio stream: %s", e)
                self._start_error = str(e)

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def on_start(self, device_manager) -> None:
        await super().on_start(device_manager)
        self._loop = asyncio.get_running_loop()
        self._start_error = None
        self._pending_first_clap = None
        self._refresh_devices()
        try:
            self._start_stream()
            logger.info("Clap listener started (device=%s)", self._device)
            self._log_event("start", "Слушаю микрофон")
        except Exception as e:
            self._start_error = str(e)
            logger.error("Failed to start clap listener: %s", e)
            self._log_event("error", f"Не удалось запустить: {e}")

    async def on_stop(self) -> None:
        self._stop_stream()
        self._log_event("stop", "Остановлено")
        await super().on_stop()
        logger.info("Clap listener stopped")

    # ── Audio stream ─────────────────────────────────────────────────────

    def _refresh_devices(self) -> None:
        try:
            import sounddevice as sd
            devices = sd.query_devices()
            self._available_devices = [
                {
                    "index": i,
                    "name": d.get("name", f"#{i}"),
                    "max_input_channels": int(d.get("max_input_channels", 0) or 0),
                    "default_sample_rate": int(d.get("default_samplerate", 0) or 0),
                }
                for i, d in enumerate(devices)
                if int(d.get("max_input_channels", 0) or 0) > 0
            ]
        except Exception as e:
            logger.warning("Failed to list audio devices: %s", e)
            self._available_devices = []

    def _start_stream(self) -> None:
        import sounddevice as sd  # lazy import — PortAudio may be missing
        if self._stream is not None:
            return
        block_size = max(64, int(self._sample_rate * self._block_ms / 1000))
        # Size the ring buffer to cover at least 200 ms — enough to extract
        # the 1024-sample feature window plus some slack at small block sizes.
        maxlen = max(8, int(200 / max(1, self._block_ms)) + 2)
        self._ring_buffer = deque(maxlen=maxlen)
        stream = sd.InputStream(
            samplerate=self._sample_rate,
            channels=1,
            blocksize=block_size,
            dtype="float32",
            device=self._device,
            callback=self._on_audio,
        )
        stream.start()
        self._stream = stream
        self._start_error = None

    def _stop_stream(self) -> None:
        s = self._stream
        self._stream = None
        if s is None:
            return
        try:
            s.stop()
            s.close()
        except Exception as e:
            logger.debug("Error closing audio stream: %s", e)

    # ── Audio callback (sounddevice thread) ─────────────────────────────

    def _on_audio(self, indata, frames, time_info, status):  # noqa: ARG002
        if status:
            logger.debug("Audio status: %s", status)
        import numpy as np

        data = indata[:, 0] if indata.ndim > 1 else indata
        if len(data) == 0:
            return
        # Keep a copy — sounddevice reuses the buffer between callbacks.
        self._ring_buffer.append(np.asarray(data, dtype=np.float32).copy())
        peak = float(np.max(np.abs(data)))

        now = time.monotonic()

        # Meter: exponential decay per block (~1 s to decay to zero)
        with self._lock:
            self._current_level = peak
            self._peak_recent = max(peak, self._peak_recent * 0.92)

        if peak < self._threshold:
            return

        # ── Feature extraction (spectral + temporal fingerprint) ─────────
        window = self._collect_feature_window(np)
        features = self._extract_features(window, np) if window is not None else None

        # ── Calibration capture: record samples, don't trigger toggle ────
        if self._calibration_mode and features is not None:
            if now - self._last_clap_ts < self._min_gap_ms / 1000.0:
                return
            self._last_clap_ts = now
            with self._lock:
                target = (self._calibration_positive
                          if self._calibration_label == "positive"
                          else self._calibration_negative)
                if len(target) < self._calibration_target:
                    target.append(features.tolist())
                    collected = len(target)
                    if collected >= self._calibration_target:
                        self._calibration_mode = False
                    self._log_event(
                        "calib",
                        f"{'Хлопок' if self._calibration_label == 'positive' else 'Шум'}"
                        f" {collected}/{self._calibration_target} записан"
                        f" (пик {peak:.2f})",
                    )
            return

        # ── Classifier: reject sounds that don't match the trained profile
        if self._profile is not None and features is not None:
            sim = self._cosine_similarity(features, self._profile["mean"], np)
            self._last_similarity = sim
            if sim < self._profile["threshold"]:
                self._reject_count += 1
                # Rate-limit rejection logging so typing doesn't flood the log
                if now - self._last_reject_log_ts > 0.5:
                    self._last_reject_log_ts = now
                    self._log_event(
                        "reject",
                        f"Шум отклонён (сх. {sim:.2f} < {self._profile['threshold']:.2f})",
                    )
                return

        # Debounce: the tail of a clap can exceed threshold for several blocks
        if now - self._last_clap_ts < self._min_gap_ms / 1000.0:
            return
        self._last_clap_ts = now

        # Post-trigger cooldown — avoid one double-clap re-firing via echo
        if now - self._last_trigger_ts < self._cooldown_ms / 1000.0:
            return

        first = self._pending_first_clap
        window_s = self._double_clap_window_ms / 1000.0
        sim_hint = (f", сх. {self._last_similarity:.2f}"
                    if self._profile is not None else "")
        if first is not None and (now - first) <= window_s:
            self._pending_first_clap = None
            self._last_trigger_ts = now
            gap_ms = int((now - first) * 1000)
            self._log_event(
                "double_clap",
                f"Двойной хлопок (Δ {gap_ms} мс, пик {peak:.2f}{sim_hint})",
            )
            self._dispatch_toggle()
        else:
            self._pending_first_clap = now
            self._log_event("clap", f"Хлопок (пик {peak:.2f}{sim_hint})")

    # ── Feature extraction / classifier ──────────────────────────────────

    def _collect_feature_window(self, np):
        """Pull the last ``_FEATURE_WIN_SAMPLES`` samples from the ring buffer."""
        if not self._ring_buffer:
            return None
        try:
            all_samples = np.concatenate(list(self._ring_buffer))
        except ValueError:
            return None
        if len(all_samples) < _FEATURE_WIN_SAMPLES:
            return None
        return all_samples[-_FEATURE_WIN_SAMPLES:]

    def _extract_features(self, samples, np):
        """Compact spectral + temporal descriptor for a short audio window.

        Output is a fixed-length feature vector:
            [crest_factor, zero_crossing_rate, spectral_centroid_norm,
             log-energy per frequency band...]
        """
        try:
            n = len(samples)
            if n == 0:
                return None
            peak = float(np.max(np.abs(samples)))
            rms = float(np.sqrt(np.mean(samples * samples))) + 1e-9
            crest = peak / rms

            # Zero-crossing rate — high for broadband noise, low for tonal sounds
            zcr = float(np.mean(np.abs(np.diff(np.sign(samples))))) / 2.0

            # Spectral magnitude
            windowed = samples * np.hanning(n)
            spectrum = np.abs(np.fft.rfft(windowed))
            freqs = np.fft.rfftfreq(n, 1.0 / self._sample_rate)

            # Spectral centroid — "brightness" of the sound
            total = float(np.sum(spectrum)) + 1e-9
            centroid = float(np.sum(freqs * spectrum) / total)
            centroid_norm = centroid / (self._sample_rate / 2)

            # Log-energy per frequency band
            band_energies: list[float] = []
            for lo, hi in _BANDS_HZ:
                mask = (freqs >= lo) & (freqs < hi)
                e = float(np.sum(spectrum[mask] * spectrum[mask])) + 1e-9
                band_energies.append(float(np.log(e)))

            vec = np.array([crest, zcr, centroid_norm] + band_energies,
                           dtype=np.float32)
            if not np.all(np.isfinite(vec)):
                return None
            return vec
        except Exception as e:
            logger.debug("Feature extraction failed: %s", e)
            return None

    @staticmethod
    def _cosine_similarity(a, b, np) -> float:
        """Cosine similarity in [-1, 1] — 1 means identical direction."""
        a = np.asarray(a, dtype=np.float32)
        b = np.asarray(b, dtype=np.float32)
        na = float(np.linalg.norm(a))
        nb = float(np.linalg.norm(b))
        if na == 0.0 or nb == 0.0:
            return 0.0
        return float(np.dot(a, b) / (na * nb))

    # ── Calibration API ─────────────────────────────────────────────────

    def start_calibration(self, label: str, count: int) -> dict[str, Any]:
        """Arm the audio callback to label the next N captured transients."""
        label = "positive" if str(label).lower().startswith("p") else "negative"
        count = max(1, min(50, int(count)))
        with self._lock:
            if label == "positive":
                self._calibration_positive = []
            else:
                self._calibration_negative = []
            self._calibration_label = label
            self._calibration_target = count
            self._calibration_mode = True
        self._log_event(
            "calib",
            f"Начата запись: {'хлопков' if label == 'positive' else 'шумов'}"
            f" ×{count}",
        )
        return self.calibration_status()

    def calibration_status(self) -> dict[str, Any]:
        with self._lock:
            label = self._calibration_label
            target = self._calibration_target
            pos = len(self._calibration_positive)
            neg = len(self._calibration_negative)
            collected = pos if label == "positive" else neg
        return {
            "active": self._calibration_mode,
            "label": label,
            "target": target,
            "collected": collected,
            "positives_total": pos,
            "negatives_total": neg,
            "has_profile": self._profile is not None,
            "profile": self._profile,
        }

    def finish_calibration(self) -> dict[str, Any]:
        """Build the acoustic profile from captured samples."""
        import numpy as np

        with self._lock:
            self._calibration_mode = False
            pos = list(self._calibration_positive)
            neg = list(self._calibration_negative)

        if len(pos) < 2:
            return {
                "ok": False,
                "error": "Нужно как минимум 2 хлопка для калибровки",
            }

        pos_arr = np.array(pos, dtype=np.float32)
        mean = np.mean(pos_arr, axis=0)

        pos_sims = [self._cosine_similarity(v, mean, np) for v in pos]
        neg_sims = ([self._cosine_similarity(v, mean, np) for v in neg]
                    if neg else [])
        min_pos = min(pos_sims)
        max_neg = max(neg_sims) if neg_sims else None

        # Place the threshold between the two populations. If they overlap,
        # fall back to a conservative margin below the worst clap.
        if max_neg is not None and max_neg < min_pos:
            threshold = (min_pos + max_neg) / 2.0
        elif max_neg is not None:
            threshold = max(0.5, min_pos - 0.03)
        else:
            threshold = max(0.5, min_pos - 0.05)
        threshold = float(min(0.99, max(0.3, threshold)))

        self._profile = {
            "mean": [float(x) for x in mean.tolist()],
            "threshold": threshold,
            "pos_count": len(pos),
            "neg_count": len(neg),
            "min_pos_sim": float(min_pos),
            "max_neg_sim": float(max_neg) if max_neg is not None else None,
        }
        self._reject_count = 0
        self._save_config()
        self._log_event(
            "calib",
            f"Профиль обучен: хлопков {len(pos)}, шумов {len(neg)},"
            f" порог {threshold:.2f}",
        )
        return {
            "ok": True,
            "profile": self._profile,
        }

    def clear_calibration(self) -> dict[str, Any]:
        with self._lock:
            self._calibration_mode = False
            self._calibration_label = None
            self._calibration_target = 0
            self._calibration_positive = []
            self._calibration_negative = []
            self._profile = None
        self._reject_count = 0
        self._save_config()
        self._log_event("calib", "Профиль сброшен")
        return {"ok": True}

    def _dispatch_toggle(self) -> None:
        loop = self._loop
        if loop is None or not loop.is_running():
            return
        loop.call_soon_threadsafe(
            lambda: asyncio.create_task(self._toggle_bulbs())
        )

    async def _toggle_bulbs(self) -> None:
        dm = self._device_manager
        if dm is None:
            return
        if self._bulb_ids:
            bulbs = [mb for mb in dm.all_bulbs() if mb.dev_id in self._bulb_ids]
        else:
            bulbs = list(dm.all_bulbs())
        if not bulbs:
            self._log_event("toggle", "Нет ламп для управления")
            return

        any_on = any(mb.state.on for mb in bulbs)
        loop = asyncio.get_running_loop()

        self._log_event(
            "toggle",
            f"{'Выключаю' if any_on else 'Включаю'} {len(bulbs)} ламп",
        )

        async def one(mb):
            try:
                await loop.run_in_executor(
                    None, mb.turn_off if any_on else mb.turn_on
                )
            except Exception as e:
                logger.warning("[%s] toggle failed: %s", mb.name, e)

        await asyncio.gather(*[one(mb) for mb in bulbs], return_exceptions=True)

    # ── Logging / Status ────────────────────────────────────────────────

    def _log_event(self, kind: str, text: str) -> None:
        self._event_log.append({"ts": time.time(), "kind": kind, "text": text})

    def get_status(self) -> dict[str, Any]:
        base = super().get_status()
        with self._lock:
            cur = self._current_level
            peak_recent = self._peak_recent
        base["listening"] = self._stream is not None
        base["start_error"] = self._start_error
        base["current_level"] = round(cur, 4)
        base["peak_recent"] = round(peak_recent, 4)
        base["threshold"] = self._threshold
        base["min_gap_ms"] = self._min_gap_ms
        base["double_clap_window_ms"] = self._double_clap_window_ms
        base["cooldown_ms"] = self._cooldown_ms
        base["device"] = self._device
        base["available_devices"] = list(self._available_devices)
        base["bulb_ids"] = list(self._bulb_ids)
        base["event_log"] = list(self._event_log)
        base["profile"] = self._profile
        base["last_similarity"] = round(self._last_similarity, 4)
        base["reject_count"] = self._reject_count
        base["calibration"] = self.calibration_status()
        return base

    # ── Routes ──────────────────────────────────────────────────────────

    def register_routes(self, app) -> None:
        from fastapi import Body

        @app.get("/api/modules/clap_listen/devices")
        async def _get_devices():
            self._refresh_devices()
            return self._available_devices

        @app.post("/api/modules/clap_listen/test")
        async def _test_toggle():
            """Simulate a double-clap — useful for calibrating bulb selection."""
            self._log_event("test", "Тест: симуляция двойного хлопка")
            await self._toggle_bulbs()
            return {"ok": True}

        @app.post("/api/modules/clap_listen/restart")
        async def _restart_stream():
            self._stop_stream()
            self._start_error = None
            try:
                self._start_stream()
                self._log_event("start", "Перезапуск микрофона")
                return {"ok": True}
            except Exception as e:
                self._start_error = str(e)
                self._log_event("error", f"Не удалось запустить: {e}")
                return {"ok": False, "error": str(e)}

        @app.post("/api/modules/clap_listen/calibrate/start")
        async def _calib_start(body: dict = Body(...)):
            label = str(body.get("label", "positive"))
            count = int(body.get("count", 5))
            return self.start_calibration(label, count)

        @app.get("/api/modules/clap_listen/calibrate/status")
        async def _calib_status():
            return self.calibration_status()

        @app.post("/api/modules/clap_listen/calibrate/finish")
        async def _calib_finish():
            return self.finish_calibration()

        @app.post("/api/modules/clap_listen/calibrate/clear")
        async def _calib_clear():
            return self.clear_calibration()
