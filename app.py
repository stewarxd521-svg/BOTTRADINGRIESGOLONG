from __future__ import annotations
import asyncio
import concurrent.futures
import csv
import hashlib
import io
import hmac
import json
import os
import sys
import tempfile
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from math import floor
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
import urllib.error
import urllib.request

from flask import Flask, jsonify, make_response, render_template_string, request

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from WS import SymbolWebSocketPriceCache, _ALL_MARKET_ENABLED  # noqa: E402
from KlineWebSocketCache_v4 import KlineWebSocketCache      # noqa: E402

# ── ExecutorBridge (señales al Executor externo) ─────────────────────────────
import urllib.error
import urllib.request
from dataclasses import dataclass as _dataclass_eb


@_dataclass_eb
class _ExecutorSignalConfig:
    executor_url:   str = ""
    signal_secret:   str = "clave-secreta-aleatoria"
    poll_secs:       int = 5
    timeout_signal:  int = 8
    timeout_state:   int = 8


class ExecutorBridge:
    """Envía señales de apertura/cierre al Executor y consulta su estado."""

    def __init__(
        self,
        executor_url: str = "",
        signal_secret: str = "clave-secreta-aleatoria",
        poll_secs: int = 5,
        logger=None,
    ) -> None:
        self.config = _ExecutorSignalConfig(
            executor_url=executor_url.strip().rstrip("/"),
            signal_secret=signal_secret,
            poll_secs=int(poll_secs),
        )
        self.logger = logger or print

    def _log(self, message: str) -> None:
        try:
            self.logger(message)
        except Exception:
            pass

    def _build_signal_request(self, payload: dict) -> urllib.request.Request:
        body = json.dumps(payload).encode("utf-8")
        return urllib.request.Request(
            f"{self.config.executor_url}/signal",
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Signal-Secret": self.config.signal_secret,
            },
            method="POST",
        )

    def send_signal_sync(self, payload: dict) -> None:
        """Envía una señal al Executor. No lanza excepción: solo registra el error."""
        if not self.config.executor_url:
            return
        try:
            req = self._build_signal_request(payload)
            with urllib.request.urlopen(req, timeout=self.config.timeout_signal) as resp:
                resp.read()
                self._log(
                    f"[executor] ✓ señal enviada: {payload.get('action')} {payload.get('symbol')}"
                )
        except Exception as exc:
            self._log(
                f"[executor] error enviando {payload.get('action')} "
                f"{payload.get('symbol')}: {exc}"
            )

    async def send_signal_async(self, payload: dict) -> None:
        """Versión no bloqueante para usar desde el event loop."""
        await asyncio.to_thread(self.send_signal_sync, payload)

    def fetch_state_sync(self) -> Optional[dict]:
        """Lee /api/state del Executor."""
        if not self.config.executor_url:
            return None
        try:
            req = urllib.request.Request(
                f"{self.config.executor_url}/api/state", method="GET"
            )
            with urllib.request.urlopen(req, timeout=self.config.timeout_state) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None

    def notify_open(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        price: float,
        quantity: float,
        notional: float = 0.0,
        level: float = 0.0,
    ) -> None:
        """Notifica apertura de posición al Executor sin bloquear el loop."""
        payload = {
            "action":    "open",
            "trade_id":  trade_id,
            "symbol":    symbol,
            "direction": direction,
            "price":     price,
            "quantity":  quantity,
            "notional":  notional,
            "level":     level,
        }
        self.notify_async(payload)

    def notify_close(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        reason: str,
        close_price: float,
        pnl: float = 0.0,
    ) -> None:
        """Notifica cierre de posición al Executor sin bloquear el loop."""
        payload = {
            "action":      "close",
            "trade_id":    trade_id,
            "symbol":      symbol,
            "direction":   direction,
            "reason":      reason,
            "close_price": close_price,
            "pnl":         pnl,
        }
        self.notify_async(payload)

    def notify_async(self, payload: dict) -> None:
        """Dispara el envío sin bloquear el event loop."""
        if not self.config.executor_url:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            threading.Thread(
                target=self.send_signal_sync,
                args=(payload,),
                daemon=True,
            ).start()
            return
        loop.create_task(self.send_signal_async(payload))



# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL      = os.getenv("BASE_URL",    "https://fapi.binance.com")
QUOTE_ASSET   = os.getenv("QUOTE_ASSET", "USDT")
PAPER_MODE    = os.getenv("PAPER_MODE",   "true").lower() == "true"
LIVE_TRADING  = os.getenv("LIVE_TRADING", "false").lower() == "true"
API_KEY       = os.getenv("BINANCE_API_KEY",    "")
API_SECRET    = os.getenv("BINANCE_API_SECRET", "")
LEVERAGE      = int(os.getenv("LEVERAGE", "1"))
STATE_FILE    = os.getenv("STATE_FILE", os.path.join(tempfile.gettempdir(), "botshort_state.json"))
# ── Gestión de símbolos ───────────────────────────────────────────────────────
INITIAL_SYMBOLS = [ s.strip() for s in os.getenv("INITIAL_SYMBOLS", "").split(",") if s.strip() ]
# Lista completa de símbolos (solo para saber cuáles son perpetuos operables):
# REST inicial + caché en disco + refresh cada 12 h
SYMBOLS_CACHE_FILE   = os.getenv(
    "SYMBOLS_CACHE_FILE",
    os.path.join(tempfile.gettempdir(), "futures_symbols_cache.json")
)
SYMBOL_REFRESH_HOURS = int(os.getenv("SYMBOL_REFRESH_HOURS", "12"))

# ── Vigilancia continua (sin ciclos de filtrado ni sleeps de detección) ───────
# UNA sola conexión WebSocket permanente (WS.py):
#   • !ticker@arr    → cambio 24h + open 24h de TODOS los símbolos (~1 s)
#   • !markPrice@arr → precio de TODOS los símbolos (1 s)
#   • <sym>@bookTicker (ms) → solo para los símbolos del RADAR
# Radar = símbolos con cambio >= MIN_GAIN_FILTER (o con posición abierta).
# Cada tick de un símbolo del radar dispara su evaluación (entradas/TP/SL).
MIN_GAIN_FILTER        = float(os.getenv("MIN_GAIN_FILTER",        "20"))   # % 24h para entrar al radar
WATCH_PRUNE_SECS       = float(os.getenv("WATCH_PRUNE_SECS",       "20"))   # limpieza del radar (housekeeping)
WATCH_PRUNE_HYSTERESIS = float(os.getenv("WATCH_PRUNE_HYSTERESIS", "5"))    # sale al bajar de MIN_GAIN_FILTER-5
# Red de seguridad: re-evalúa posiciones/radar aunque no lleguen ticks (libro
# quieto, reintentos tras error, fin de cooldown). NO es la vía principal.
# 0 = desactivada.
SAFETY_TICK_SECS       = float(os.getenv("SAFETY_TICK_SECS",       "1.0"))
PRICE_MAX_AGE_S        = float(os.getenv("PRICE_MAX_AGE_S",        "5"))    # un precio más viejo no se usa para operar
# true = el % de cambio se recalcula en cada tick con (precio / open24h - 1),
# en vez de esperar al ticker 24h (que llega cada ~1 s).
USE_LIVE_CHANGE        = os.getenv("USE_LIVE_CHANGE", "true").lower() == "true"
# ── Señal de entrada: cruce EMA rápida/lenta sobre las N cripto más activas ──
EMA_INTERVAL           = os.getenv("EMA_INTERVAL", "1m")
EMA_FAST               = int(os.getenv("EMA_FAST", "100"))
EMA_SLOW               = int(os.getenv("EMA_SLOW", "200"))
EMA_TOP_N              = int(os.getenv("EMA_TOP_N", "200"))
# Velas (cierres) guardadas por símbolo: máximo 1500 (límite de Binance por petición).
# Con 1500 velas el periodo máximo admitido para la EMA lenta es 500 (≈ 3× de historia).
EMA_MAX_CANDLES        = max(50, min(int(os.getenv("EMA_MAX_CANDLES", "1500")), 1500))
EMA_MAX_PERIOD         = max(2, EMA_MAX_CANDLES // 3)
if not (2 <= EMA_FAST < EMA_SLOW <= EMA_MAX_PERIOD):
    print(f"EMA_FAST/EMA_SLOW inválidas ({EMA_FAST}/{EMA_SLOW}); uso 100/200", flush=True)
    EMA_FAST, EMA_SLOW = 100, 200
EMA_UNIVERSE_REFRESH_S = float(os.getenv("EMA_UNIVERSE_REFRESH_S", "900"))
ENTRY_ERROR_BACKOFF_S  = float(os.getenv("ENTRY_ERROR_BACKOFF_S",  "5"))    # pausa tras fallo al abrir
CLOSE_ERROR_BACKOFF_S  = float(os.getenv("CLOSE_ERROR_BACKOFF_S",  "3"))    # pausa tras fallo al cerrar
STATE_PERSIST_SECS     = float(os.getenv("STATE_PERSIST_SECS",     "10"))   # guardado periódico del estado

# ── Dashboard ─────────────────────────────────────────────────────────────────
LIVE_POLL_MS   = int(os.getenv("LIVE_POLL_MS",   "250"))    # el navegador pide precios cada 250 ms (payload mínimo)
STATUS_POLL_MS = int(os.getenv("STATUS_POLL_MS", "1500"))   # estructura completa de las tablas

# ── Resto de parámetros operativos ────────────────────────────────────────────
COOLDOWN_SECONDS     = int(os.getenv("COOLDOWN_SECONDS",     "86400"))

# Tiempo de gracia al detener un cache WS (segundos)
WS_STOP_GRACE        = float(os.getenv("WS_STOP_GRACE", "0.4"))

# Precio máximo permitido para abrir nuevas entradas (bloqueo permanente si supera)
# 0 = desactivado (con las 200 más activas hay monedas de miles de USD).
MAX_PRICE_BLOCK = float(os.getenv("MAX_PRICE_BLOCK", "0"))

# DCA: el tramo de nivel 0 lo abre el cruce EMA; los siguientes se abren cuando el
# precio va EN CONTRA de la 1.ª entrada esos % (ajusta DCA_ADVERSE_LEVELS a tu gusto).
# Estos valores de entorno son la escalera "de fábrica"; la escalera VIGENTE es
# ENTRY_LADDER, editable desde la web y guardada en SETTINGS_FILE.
ENTRY_LEVELS    = [float(x) for x in os.getenv("DCA_ADVERSE_LEVELS", "0,2,4,6,8,10,12").split(",")]
ENTRY_NOTIONALS = [float(x) for x in os.getenv("ENTRY_NOTIONALS", "5,5,10,20,40,80,160").split(",")]
LADDER_MAX_ROWS = 25

Ladder = Tuple[Tuple[float, float], ...]          # ((% en contra, notional USDT), ...)


def _validate_ladder(levels: Any, notionals: Any) -> Ladder:
    """Valida una escalera DCA. El tramo 1 es la entrada del cruce (0 %); los
    siguientes tienen % en contra estrictamente crecientes. ValueError si no vale."""
    if not isinstance(levels, (list, tuple)) or not isinstance(notionals, (list, tuple)):
        raise ValueError("La escalera debe enviarse como dos listas: levels y notionals")
    if len(levels) != len(notionals):
        raise ValueError("levels y notionals deben tener la misma cantidad de tramos")
    if not 1 <= len(levels) <= LADDER_MAX_ROWS:
        raise ValueError(f"La escalera debe tener entre 1 y {LADDER_MAX_ROWS} tramos")
    rows: List[Tuple[float, float]] = []
    for i, (lv, nt) in enumerate(zip(levels, notionals)):
        try:
            lv = float(str(lv).replace(",", "."))
            nt = float(str(nt).replace(",", "."))
        except (TypeError, ValueError):
            raise ValueError(f"Tramo {i + 1}: el % y el notional deben ser números")
        if lv != lv or nt != nt:
            raise ValueError(f"Tramo {i + 1}: valor no numérico")
        if i == 0:
            if lv != 0:
                raise ValueError("El tramo 1 es la entrada del cruce EMA: su % en contra debe ser 0")
        else:
            if not 0 < lv <= 500:
                raise ValueError(f"Tramo {i + 1}: el % en contra debe estar entre 0 y 500")
            if lv <= rows[-1][0]:
                raise ValueError(f"Tramo {i + 1}: el % en contra ({lv:g}) debe ser mayor que el "
                                 f"del tramo {i} ({rows[-1][0]:g})")
        if not 0 < nt <= 1_000_000:
            raise ValueError(f"Tramo {i + 1}: el notional debe ser mayor que 0")
        rows.append((lv, nt))
    return tuple(rows)


def _factory_ladder() -> Ladder:
    n_rows = min(len(ENTRY_LEVELS), len(ENTRY_NOTIONALS))
    try:
        return _validate_ladder(ENTRY_LEVELS[:n_rows], ENTRY_NOTIONALS[:n_rows])
    except ValueError as exc:
        print(f"DCA_ADVERSE_LEVELS/ENTRY_NOTIONALS no válidos ({exc}); se usan tal cual", flush=True)
        return tuple(zip(ENTRY_LEVELS, ENTRY_NOTIONALS))


FACTORY_LADDER: Ladder = _factory_ladder()
ENTRY_LADDER:   Ladder = FACTORY_LADDER       # se reasigna entera (nunca se muta) → lectura atómica
# Nombre de variable de entorno NUEVO a propósito: así un TAKE_PROFIT_FRACTION=0.14284
# viejo en tu hosting no pisa el 0.07.
TAKE_PROFIT_FRACTION = float(os.getenv("EMA_TAKE_PROFIT_FRACTION", "0.07"))
# Rango permitido al cambiarlo desde la web (fracción del notional: 0.07 = 7 %)
TP_MIN, TP_MAX = 0.001, 5.0

# Stop loss por defecto en USD (pérdida absoluta, valor negativo). Es el SL
# "estándar": se usa en cuanto la posición tiene 2 o más tramos.
# Puede sobreescribirse por posición desde el dashboard (POST /api/set-sl/<symbol>).
DEFAULT_STOP_LOSS_USD = float(os.getenv("DEFAULT_STOP_LOSS_USD", "-8.0"))

# Stop loss del PRIMER tramo: pérdida máxima = notional del primer fill × 0.251.
# Ej.: primer tramo de 5 USDT → SL = -1.255 USD. Al abrirse el 2.º tramo la
# posición vuelve al SL estándar (DEFAULT_STOP_LOSS_USD). Un SL fijado a mano
# desde el dashboard SIEMPRE se respeta.
FIRST_TRANCHE_SL_FRACTION = float(os.getenv("FIRST_TRANCHE_SL_FRACTION", "0.251"))

# ── Executor externo ──────────────────────────────────────────────────────────
EXECUTOR_URL    = os.getenv("EXECUTOR_URL",    "https://executor-5lu0.onrender.com")
EXECUTOR_SECRET = os.getenv("EXECUTOR_SECRET", "clave-secreta-aleatoria")

# ── Persistencia de estadísticas (MFE/MAE) y ajustes editables desde la web ───
# IMPORTANTE: en hosting con disco efímero (Render free, etc.) apunta estas rutas
# a un disco persistente (STATS_FILE=/data/trade_stats.jsonl) o se perderán al redeploy.
STATS_FILE    = os.getenv("STATS_FILE",    os.path.join(_HERE, "trade_stats.jsonl"))
SETTINGS_FILE = os.getenv("SETTINGS_FILE", os.path.join(_HERE, "bot_settings.json"))


# ─────────────────────────────────────────────────────────────────────────────
# GESTIÓN DE RIESGO (editable desde la web, persistente en SETTINGS_FILE)
# ─────────────────────────────────────────────────────────────────────────────
#  • Stop global: si el PnL NO realizado total ≤ global_stop_usd → se cierran
#    TODAS las posiciones (una a una) y, si global_stop_pause, se pausan las
#    entradas nuevas hasta que las reanudes desde la web.
#  • Límite de exposición: no se abre una posición nueva si
#    (notional abierto + notional del nuevo tramo) > max_exposure_usd.
#    Con exposure_include_dca=True también frena los tramos DCA.
#  • Filtros BTC 24h (dos, independientes):
#      - bajista: BTCUSDT 24h < btc_filter_threshold (%) → frena btc_filter_mode
#      - alcista: BTCUSDT 24h > btc_up_threshold     (%) → frena btc_up_mode
#    modo = lado que se frena: "all" (LONG y SHORT) | "long" | "short".
#  • Condición EMA del DCA: cuando la posición ya tiene dca_ema_after tramos,
#    los siguientes DCA solo entran si LONG: precio > EMA(dca_ema_period) y
#    SHORT: precio < EMA(dca_ema_period), sobre las velas de EMA_INTERVAL.
#  • Pausa: entries_paused bloquea posiciones NUEVAS (el DCA, TP y SL de las
#    abiertas siguen funcionando).
# Ninguna de estas guardas toca la detección de cruces ni el seguimiento de
# símbolos: solo deciden si una señal ya detectada puede abrir.

def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "si", "sí")


BTC_FILTER_SYMBOL = os.getenv("BTC_FILTER_SYMBOL", "BTCUSDT")
BTC_MAX_AGE_S     = float(os.getenv("BTC_MAX_AGE_S", "120"))   # dato 24h más viejo → "sin dato"
GLOBAL_STOP_REARM_S = float(os.getenv("GLOBAL_STOP_REARM_S", "10"))  # pausa entre disparos


@dataclass
class RiskSettings:
    global_stop_enabled:  bool  = _env_bool("GLOBAL_STOP_ENABLED", True)
    global_stop_usd:      float = float(os.getenv("GLOBAL_STOP_USD", "-5"))
    global_stop_pause:    bool  = _env_bool("GLOBAL_STOP_PAUSE", True)
    exposure_enabled:     bool  = _env_bool("MAX_EXPOSURE_ENABLED", True)
    max_exposure_usd:     float = float(os.getenv("MAX_EXPOSURE_USD", "800"))
    exposure_include_dca: bool  = _env_bool("MAX_EXPOSURE_INCLUDE_DCA", False)
    # Filtro BTC bajista (BTC 24h < umbral)
    btc_filter_enabled:   bool  = _env_bool("BTC_FILTER_ENABLED", False)
    btc_filter_threshold: float = float(os.getenv("BTC_FILTER_THRESHOLD", "0"))
    btc_filter_mode:      str   = os.getenv("BTC_FILTER_MODE", "all")         # all | long | short
    # Filtro BTC alcista (BTC 24h > umbral)
    btc_up_enabled:       bool  = _env_bool("BTC_UP_FILTER_ENABLED", False)
    btc_up_threshold:     float = float(os.getenv("BTC_UP_FILTER_THRESHOLD", "0"))
    btc_up_mode:          str   = os.getenv("BTC_UP_FILTER_MODE", "short")    # all | long | short
    # Condición EMA para el DCA a partir de N tramos
    dca_ema_enabled:      bool  = _env_bool("DCA_EMA_ENABLED", False)
    dca_ema_period:       int   = int(os.getenv("DCA_EMA_PERIOD", "500"))
    dca_ema_after:        int   = int(os.getenv("DCA_EMA_AFTER", "3"))
    # Estado de pausa (también se guarda: sobrevive a un reinicio)
    entries_paused:       bool  = False
    pause_reason:         str   = ""
    paused_at:            float = 0.0


RISK = RiskSettings()

# Validadores: clave → (tipo, función de validación que lanza ValueError)
def _v_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on", "si", "sí"):
        return True
    if s in ("0", "false", "no", "off", ""):
        return False
    raise ValueError(f"valor booleano inválido: {v!r}")


def _v_float(lo: float, hi: float, msg: str):
    def _f(v: Any) -> float:
        try:
            x = float(str(v).replace(",", "."))
        except (TypeError, ValueError):
            raise ValueError(msg)
        if x != x or not (lo <= x <= hi):          # NaN o fuera de rango
            raise ValueError(msg)
        return x
    return _f


def _v_int(lo: int, hi: int, msg: str):
    def _f(v: Any) -> int:
        try:
            x = float(str(v).replace(",", "."))
        except (TypeError, ValueError):
            raise ValueError(msg)
        if x != x or x != int(x) or not (lo <= x <= hi):
            raise ValueError(msg)
        return int(x)
    return _f


def _v_mode(v: Any) -> str:
    s = str(v).strip().lower()
    if s not in ("all", "long", "short"):
        raise ValueError("El lado a frenar debe ser 'all', 'long' o 'short'")
    return s


RISK_FIELDS = {
    "global_stop_enabled":  _v_bool,
    "global_stop_usd":      _v_float(-1_000_000, -0.01, "El stop global debe ser un número negativo (ej. -5)"),
    "global_stop_pause":    _v_bool,
    "exposure_enabled":     _v_bool,
    "max_exposure_usd":     _v_float(1, 100_000_000, "La exposición máxima debe ser un número positivo (ej. 800)"),
    "exposure_include_dca": _v_bool,
    "btc_filter_enabled":   _v_bool,
    "btc_filter_threshold": _v_float(-100, 100, "El umbral BTC debe estar entre -100 y 100 %"),
    "btc_filter_mode":      _v_mode,
    "btc_up_enabled":       _v_bool,
    "btc_up_threshold":     _v_float(-100, 100, "El umbral BTC debe estar entre -100 y 100 %"),
    "btc_up_mode":          _v_mode,
    "dca_ema_enabled":      _v_bool,
    "dca_ema_period":       _v_int(2, EMA_MAX_PERIOD,
                                   f"La EMA del DCA debe ser un entero entre 2 y {EMA_MAX_PERIOD}"),
    "dca_ema_after":        _v_int(1, LADDER_MAX_ROWS,
                                   f"Los tramos previos deben ser un entero entre 1 y {LADDER_MAX_ROWS}"),
}


def _apply_risk_dict(data: dict, strict: bool) -> Dict[str, Any]:
    """Valida y aplica las claves conocidas de `data` sobre RISK.
    strict=True → ValueError ante el primer valor inválido (sin aplicar nada)."""
    parsed: Dict[str, Any] = {}
    for key, fn in RISK_FIELDS.items():
        if key not in data:
            continue
        try:
            parsed[key] = fn(data[key])
        except ValueError:
            if strict:
                raise
    for key, val in parsed.items():
        setattr(RISK, key, val)
    return parsed


def _load_settings() -> None:
    """Restaura los ajustes guardados desde la web (SL global, multiplicador del TP,
    EMA rápida/lenta y gestión de riesgo). Sobrescriben los valores de entorno."""
    global DEFAULT_STOP_LOSS_USD, TAKE_PROFIT_FRACTION, EMA_FAST, EMA_SLOW, ENTRY_LADDER
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return
    lad = data.get("ladder")
    if isinstance(lad, dict):
        try:
            ENTRY_LADDER = _validate_ladder(lad.get("levels"), lad.get("notionals"))
        except ValueError as exc:
            print(f"Escalera guardada no válida ({exc}); uso la de fábrica", flush=True)
    risk = data.get("risk")
    if isinstance(risk, dict):
        _apply_risk_dict(risk, strict=False)
        RISK.entries_paused = bool(risk.get("entries_paused", False))
        RISK.pause_reason   = str(risk.get("pause_reason", "") or "")
        try:
            RISK.paused_at = float(risk.get("paused_at", 0) or 0)
        except Exception:
            RISK.paused_at = 0.0
    try:
        val = float(data.get("default_stop_loss_usd"))
        if val < 0:
            DEFAULT_STOP_LOSS_USD = val
    except Exception:
        pass
    try:
        tp = float(data.get("take_profit_fraction"))
        if TP_MIN <= tp <= TP_MAX:
            TAKE_PROFIT_FRACTION = tp
    except Exception:
        pass
    try:
        f, sl = int(data.get("ema_fast")), int(data.get("ema_slow"))
        if 2 <= f < sl <= EMA_MAX_PERIOD:
            EMA_FAST, EMA_SLOW = f, sl
    except Exception:
        pass


_SETTINGS_LOCK = threading.Lock()


def _save_settings() -> Optional[str]:
    """Guarda TODOS los ajustes editables en disco (atómico). Devuelve error o None."""
    tmp = f"{SETTINGS_FILE}.tmp"
    try:
        with _SETTINGS_LOCK:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({
                    "default_stop_loss_usd": DEFAULT_STOP_LOSS_USD,
                    "take_profit_fraction":  TAKE_PROFIT_FRACTION,
                    "ema_fast":              EMA_FAST,
                    "ema_slow":              EMA_SLOW,
                    "ladder": {
                        "levels":    [lv for lv, _ in ENTRY_LADDER],
                        "notionals": [nt for _, nt in ENTRY_LADDER],
                    },
                    "risk":                  asdict(RISK),
                }, fh)
            os.replace(tmp, SETTINGS_FILE)
        return None
    except Exception as exc:
        return str(exc)


_load_settings()


# ─────────────────────────────────────────────────────────────────────────────
# MODELOS DE DATOS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Fill:
    level:       float
    notional:    float
    entry_price: float
    qty:         float
    opened_at:   float = field(default_factory=time.time)


@dataclass
class BotPosition:
    symbol:       str
    fills:        List[Fill] = field(default_factory=list)
    realized_pnl: float = 0.0
    status:       str   = "OPEN"
    trade_id:     int   = 0
    direction:    str   = "SHORT"     # "SHORT" | "LONG"
    # Stop loss vigente en USD (pérdida absoluta, valor negativo).
    sl_usd:       float = DEFAULT_STOP_LOSS_USD
    # True si el SL lo fijó el usuario desde el dashboard: el bot NO lo toca más.
    sl_manual:    bool  = False
    # ── Excursiones de la operación (PnL no realizado, USD y % del notional) ──
    opened_ts:    float = field(default_factory=time.time)
    mfe_usd:      float = 0.0    # máximo a favor  (>= 0)
    mae_usd:      float = 0.0    # máximo en contra (<= 0)
    mfe_pct:      float = 0.0
    mae_pct:      float = 0.0
    mfe_ts:       float = 0.0
    mae_ts:       float = 0.0
    low_price:    float = 0.0    # precio mínimo visto (favorable en un short)
    high_price:   float = 0.0    # precio máximo visto (adverso en un short)

    def update_excursions(self, price: float) -> None:
        """Actualiza MFE/MAE con el precio actual. Llamar con self.lock tomado."""
        if price <= 0 or not self.fills:
            return
        pnl      = self.unrealized_pnl(price)
        notional = self.notional
        pct      = (pnl / notional * 100.0) if notional > 0 else 0.0
        now      = time.time()
        if pnl > self.mfe_usd:
            self.mfe_usd, self.mfe_ts = pnl, now
        if pnl < self.mae_usd:
            self.mae_usd, self.mae_ts = pnl, now
        if pct > self.mfe_pct:
            self.mfe_pct = pct
        if pct < self.mae_pct:
            self.mae_pct = pct
        if self.low_price <= 0 or price < self.low_price:
            self.low_price = price
        if price > self.high_price:
            self.high_price = price

    def reset_excursions(self) -> None:
        self.opened_ts = time.time()
        self.mfe_usd = self.mae_usd = self.mfe_pct = self.mae_pct = 0.0
        self.mfe_ts = self.mae_ts = 0.0
        self.low_price = self.high_price = 0.0

    @property
    def qty(self) -> float:
        return sum(f.qty for f in self.fills)

    @property
    def notional(self) -> float:
        return sum(f.notional for f in self.fills)

    @property
    def avg_entry(self) -> float:
        if self.qty <= 0:
            return 0.0
        return sum(f.entry_price * f.qty for f in self.fills) / self.qty

    @property
    def pnl_sign(self) -> float:
        """+1 en short (gana si el precio baja), -1 en long."""
        return 1.0 if self.direction == "SHORT" else -1.0

    def unrealized_pnl(self, mark_price: float) -> float:
        if mark_price <= 0:
            return 0.0
        return self.pnl_sign * sum((f.entry_price - mark_price) * f.qty for f in self.fills)

    def sl_price(self) -> float:
        """Precio al que se alcanza sl_usd (short: por encima; long: por debajo)."""
        q = self.qty
        if q <= 0:
            return 0.0
        return self.avg_entry - self.pnl_sign * self.sl_usd / q

    def adverse_pct(self, price: float) -> float:
        """% que el precio se movió EN CONTRA respecto a la 1.ª entrada."""
        if not self.fills or price <= 0:
            return 0.0
        p0 = self.fills[0].entry_price
        if p0 <= 0:
            return 0.0
        return (price / p0 - 1.0) * 100.0 * self.pnl_sign

    def opened_levels(self) -> set:
        return {f.level for f in self.fills}

    # ── Stop loss automático por tramos ──────────────────────────────────────
    def auto_sl_usd(self) -> float:
        """1 solo tramo → -(notional del primer fill × 0.251).
        2 o más tramos → SL estándar (DEFAULT_STOP_LOSS_USD)."""
        if len(self.fills) == 1:
            return -abs(self.fills[0].notional) * FIRST_TRANCHE_SL_FRACTION
        return DEFAULT_STOP_LOSS_USD

    def refresh_auto_sl(self) -> None:
        """Recalcula el SL según los tramos abiertos, salvo SL manual."""
        if not self.sl_manual:
            self.sl_usd = self.auto_sl_usd()

    @property
    def sl_mode(self) -> str:
        if self.sl_manual:
            return "manual"
        return "1er tramo" if len(self.fills) == 1 else "estándar"


# ─────────────────────────────────────────────────────────────────────────────
# CLIENTE BINANCE FUTURES
# ─────────────────────────────────────────────────────────────────────────────

class BinanceFuturesClient:
    def __init__(self) -> None:
        self.exchange_filters: Dict[str, Dict[str, float]] = {}

    async def start(self) -> None:
        await self.load_exchange_info()

    async def request(self, method: str, path: str,
                      params: Optional[dict] = None, signed: bool = False,
                      timeout: int = 15) -> Any:
        return await asyncio.to_thread(
            self._sync_request, BASE_URL, method, path, params, signed, timeout
        )

    def _sync_request(self, base_url: str, method: str, path: str,
                      params: Optional[dict] = None, signed: bool = False,
                      timeout: int = 15) -> Any:
        params  = dict(params or {})
        headers = {"User-Agent": "BOTSHORT/2.0"}
        if signed:
            if not API_KEY or not API_SECRET:
                raise RuntimeError("Faltan BINANCE_API_KEY / BINANCE_API_SECRET")
            params["timestamp"]  = int(time.time() * 1000)
            params["recvWindow"] = 5000
            query     = urlencode(params, doseq=True)
            signature = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
            params["signature"] = signature
            headers["X-MBX-APIKEY"] = API_KEY
        elif API_KEY:
            headers["X-MBX-APIKEY"] = API_KEY

        query = urlencode(params, doseq=True)
        url   = f"{base_url}{path}" + (f"?{query}" if query else "")
        req   = urllib.request.Request(url, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Binance HTTP {exc.code}: {body[:300]}") from exc

    async def load_exchange_info(self) -> None:
        data    = await self.request("GET", "/fapi/v1/exchangeInfo")
        filters: Dict[str, Dict[str, float]] = {}
        for sym in data.get("symbols", []):
            if sym.get("quoteAsset")    != QUOTE_ASSET:  continue
            if sym.get("contractType")  != "PERPETUAL":  continue
            if sym.get("status")        != "TRADING":    continue
            row = {"stepSize": 0.001, "minQty": 0.0, "minNotional": 5.0}
            for f in sym.get("filters", []):
                if f.get("filterType") == "LOT_SIZE":
                    row["stepSize"] = float(f.get("stepSize", row["stepSize"]))
                    row["minQty"]   = float(f.get("minQty",   row["minQty"]))
                if f.get("filterType") == "MIN_NOTIONAL":
                    row["minNotional"] = float(f.get("notional", row["minNotional"]))
            filters[sym["symbol"]] = row
        self.exchange_filters = filters

    def normalize_qty(self, symbol: str, qty: float) -> float:
        info = self.exchange_filters.get(symbol, {"stepSize": 0.001, "minQty": 0.0})
        step = info["stepSize"]
        norm = floor(qty / step) * step
        decs = max(0, len(f"{step:.12f}".rstrip("0").split(".")[-1]))
        norm = round(norm, decs)
        return norm if norm >= info.get("minQty", 0.0) else 0.0

    async def set_leverage(self, symbol: str) -> None:
        if LEVERAGE > 0 and LIVE_TRADING and not PAPER_MODE:
            await self.request(
                "POST", "/fapi/v1/leverage",
                {"symbol": symbol, "leverage": LEVERAGE}, signed=True
            )

    async def market_open(self, symbol: str, notional: float, price: float,
                          direction: str = "SHORT") -> float:
        min_notional = self.exchange_filters.get(symbol, {}).get("minNotional", 5.0)
        effective    = max(notional, min_notional)
        qty          = self.normalize_qty(symbol, effective / price)
        if qty <= 0:
            raise RuntimeError(f"Qty inválida {symbol}: notional={effective} price={price}")
        if PAPER_MODE or not LIVE_TRADING:
            return qty
        await self.set_leverage(symbol)
        await self.request("POST", "/fapi/v1/order",
            {"symbol": symbol, "side": "SELL" if direction == "SHORT" else "BUY",
             "type": "MARKET", "quantity": qty},
            signed=True)
        return qty

    async def close_order(self, symbol: str, qty: float, direction: str = "SHORT") -> None:
        qty = self.normalize_qty(symbol, qty)
        if qty <= 0 or PAPER_MODE or not LIVE_TRADING:
            return
        # Timeout 10 s: debe completarse antes del timeout de Flask (30 s)
        await self.request("POST", "/fapi/v1/order",
            {"symbol": symbol, "side": "BUY" if direction == "SHORT" else "SELL", "type": "MARKET",
             "quantity": qty, "reduceOnly": "true"},
            signed=True, timeout=10)




# ─────────────────────────────────────────────────────────────────────────────
# BOT PRINCIPAL (motor por eventos)
# ─────────────────────────────────────────────────────────────────────────────

class TradingBot:
    def __init__(self) -> None:
        self.client   = BinanceFuturesClient()
        self.positions: Dict[str, BotPosition] = {}
        self.closed_trades: List[dict] = []
        self.events:        List[str]  = []
        self.lock = threading.Lock()
        self._trade_id_lock = threading.Lock()

        # Cooldown: symbol → timestamp hasta el que está bloqueado
        self.symbol_cooldown: Dict[str, float] = {}

        # Bloqueo permanente por precio
        self.price_blocked: set = set()

        # ── Lista de símbolos operables (REST + caché en disco, refresh c/12h) ──
        self.all_symbols:              List[str] = []
        self._tradable:                frozenset = frozenset()
        self.last_symbols_refresh_at:  float     = 0.0

        # ── WS caches ─────────────────────────────────────────────────────
        self.price_cache:   Optional[SymbolWebSocketPriceCache] = None
        self.kline_cache:   Optional[KlineWebSocketCache]       = None

        # ── Radar (símbolos con bookTicker suscrito). Copy-on-write: SOLO el
        #    loop del bot lo reasigna; el resto (hilo WS, Flask) solo lee. ──
        self.watch: frozenset = frozenset()

        # ── Motor por eventos ─────────────────────────────────────────────
        self._tick_event:   Optional[asyncio.Event] = None   # se crea en el loop del bot
        self._kline_dirty:  Optional[asyncio.Event] = None
        self._pending:      Dict[str, float] = {}            # símbolo → instante del primer tick pendiente
        self._pending_lock  = threading.Lock()
        self._wake_scheduled = False
        self._bg_tasks:     set = set()
        self._entry_inflight: set = set()                    # símbolos con una tanda de entradas en curso
        # (símbolo, nivel) → notional reservado antes de enviar la orden (cuenta para la exposición)
        self._entry_reserved: Dict[Tuple[str, float], float] = {}
        self._entry_backoff:  Dict[str, float] = {}
        self._close_backoff:  Dict[str, float] = {}
        self._log_throttle:   Dict[str, float] = {}

        # ── Métricas ──────────────────────────────────────────────────────
        self.running              = False
        self.scan_count           = 0           # evaluaciones realizadas (1 por símbolo y tick)
        self.last_scan_at         = 0.0
        self.eval_rate            = 0.0         # evaluaciones/s (última ventana de 1 s)
        self.latency_avg_ms       = 0.0         # tick recibido → evaluado (media, ventana 1 s)
        self.latency_max_ms       = 0.0         # idem, máximo
        self._lat_sum = 0.0
        self._lat_n   = 0
        self._lat_max = 0.0
        self.last_error           = ""
        self.last_startup_err     = ""
        self.exchange_symbols     = 0
        self.started_at           = time.time()

        self.loop:   Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None

        # ── Persistencia asíncrona ────────────────────────────────────────
        self._persist_event: Optional[asyncio.Event] = None
        self._persist_debounce_secs: float = 0.35

        # ── Guardia anti doble cierre ─────────────────────────────────────
        self._closing_symbols: set[str] = set()

        # ── Cierre masivo "una a una" (estado visible en la web) ──────────
        self._close_all_active = False        # mientras es True no se abren entradas/DCA nuevos
        self.close_all_state: Dict[str, Any] = self._empty_close_all()

        # ── Executor bridge ───────────────────────────────────────────────
        self._trade_id_seq: int = 0
        self.executor = ExecutorBridge(
            executor_url=EXECUTOR_URL,
            signal_secret=EXECUTOR_SECRET,
        )
        # total PnL realizado acumulado (suma de todos los cierres)
        self.total_realized_pnl: float = 0.0

        # ── Gestión de riesgo (stop global / exposición / filtro BTC) ─────
        self._gstop_last_check: float = 0.0
        self._gstop_rearm_at:   float = 0.0
        self.global_stop_triggers: int = 0
        self.global_stop_last: Dict[str, Any] = {}
        self.blocked_counts: Dict[str, int] = {}             # tipo de bloqueo → nº de señales frenadas
        self.blocked_recent: deque = deque(maxlen=40)        # últimas señales frenadas (para la web)
        self._block_lock = threading.Lock()
        self._ema_cache: Dict[str, Tuple[tuple, Optional[float]]] = {}   # EMA de tendencia del DCA
        self._block_seen: Dict[str, float] = {}              # anti-spam por (símbolo, tipo)

        # ── Estadísticas MFE/MAE por operación cerrada (se cargan del disco) ──
        self.trade_stats: List[dict] = []
        self._stats_lock = threading.Lock()
        self._load_trade_stats()
        self._restore_history()

    # ── Logging ───────────────────────────────────────────────────────────────

    def log(self, msg: str) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        line  = f"{stamp} | {msg}"
        print(line, flush=True)
        with self.lock:
            self.events = [line, *self.events[:99]]

    # ── Start / Stop ──────────────────────────────────────────────────────────

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.thread  = threading.Thread(
            target=self._run_loop, daemon=True, name="BotLoop"
        )
        self.thread.start()

    def stop(self) -> None:
        self.log("Deteniendo bot...")
        self.running = False
        self._stop_price_cache()
        self._stop_kline_cache()

    def _run_loop(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._persist_event = asyncio.Event()
        self._tick_event    = asyncio.Event()
        self._kline_dirty   = asyncio.Event()
        try:
            self.loop.run_until_complete(self._main())
        except Exception as exc:
            self.running    = False
            self.last_error = str(exc)
            self.log(f"Bot detenido por error no controlado: {exc}")

    # ── Supervisor ────────────────────────────────────────────────────────────

    async def _supervised(self, coro_factory, name: str, restart_delay: float = 2.0):
        """Envuelve una corrutina con reinicio automático."""
        while self.running:
            try:
                await coro_factory()
            except asyncio.CancelledError:
                if not self.running:
                    break
                self.log(f"[supervisor] {name}: CancelledError — relanzando en {restart_delay}s...")
            except Exception as exc:
                if not self.running:
                    break
                self.last_error = str(exc)
                self.log(f"[supervisor] {name}: excepción '{exc}' — relanzando en {restart_delay}s...")
            else:
                if not self.running:
                    break
                self.log(f"[supervisor] {name}: retornó inesperadamente — relanzando en {restart_delay}s...")

            try:
                await asyncio.sleep(restart_delay)
            except asyncio.CancelledError:
                if not self.running:
                    break

    # ── Main ──────────────────────────────────────────────────────────────────

    def _next_trade_id(self) -> int:
        """Genera un trade_id único e incremental sin reentrar en self.lock."""
        with self._trade_id_lock:
            self._trade_id_seq += 1
            return self._trade_id_seq

    async def _main(self) -> None:
        # Conectar el logger del executor al sistema de log del bot
        self.executor.logger = self.log
        if EXECUTOR_URL:
            self.log(f"[executor] Bridge configurado → {EXECUTOR_URL}")
        else:
            self.log("[executor] EXECUTOR_URL no configurado — señales desactivadas")

        self.log("Bot iniciado — modo " + (
            "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL"
        ))
        try:
            await self.client.start()
            self.exchange_symbols = len(self.client.exchange_filters)
            self.log(f"ExchangeInfo: {self.exchange_symbols} contratos USDT-M perpetuos")
        except Exception as exc:
            self.last_startup_err = str(exc)
            self.log(f"ExchangeInfo falló ({exc}). Continúo con filtros mínimos.")

        # Lista de perpetuos operables (caché en disco o REST)
        await self._init_all_symbols()
        self._update_tradable()

        if not _ALL_MARKET_ENABLED:
            self.log("⚠️ WS_ALL_MARKET=false: sin !ticker@arr/!markPrice@arr el bot NO puede "
                     "descubrir símbolos. Pon WS_ALL_MARKET=true.")

        # UNA sola conexión WS permanente; el callback despierta al motor en cada tick
        self._start_price_cache()
        self._start_kline_cache()      # detector de cruces EMA (top N más activas)

        tasks = [
            asyncio.create_task(self._supervised(self._engine_loop,             "_engine_loop")),
            asyncio.create_task(self._supervised(self._maintenance_loop,        "_maintenance_loop")),
            asyncio.create_task(self._supervised(self._all_symbols_refresh_loop, "_all_symbols_refresh_loop")),
            asyncio.create_task(self._supervised(self._persist_state_loop,      "_persist_state_loop")),
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    # ── Gestión de WS caches ──────────────────────────────────────────────────

    def _stop_price_cache(self) -> None:
        if self.price_cache:
            try:
                self.price_cache.stop()
            except Exception:
                pass
            time.sleep(WS_STOP_GRACE)
            self.price_cache = None

    def _stop_kline_cache(self) -> None:
        if self.kline_cache:
            try:
                self.kline_cache.stop()
            except Exception:
                pass
            time.sleep(WS_STOP_GRACE)
            self.kline_cache = None

    def _open_position_symbols(self) -> List[str]:
        with self.lock:
            return [
                sym for sym, pos in self.positions.items()
                if pos.status == "OPEN" and pos.fills
            ]

    def _start_price_cache(self) -> None:
        """Arranca el ÚNICO caché WS (permanente). Nace con las posiciones
        abiertas (si las hay); el radar crece solo con los ticks 24h."""
        self._stop_price_cache()
        initial = self._open_position_symbols()
        self.price_cache = SymbolWebSocketPriceCache(initial)
        self.price_cache.set_update_callback(self._on_ws_event)
        self.watch = frozenset(initial)
        self.price_cache.start()
        self.log(
            f"PriceCache permanente iniciado: !ticker@arr + !markPrice@arr (todos los símbolos) "
            f"+ bookTicker ms para el radar (≥{MIN_GAIN_FILTER:.0f}% o posición abierta)"
        )

    def _top_active_symbols(self) -> List[str]:
        """Perpetuos operables ordenados por volumen 24h (del !ticker@arr, sin REST)."""
        pc = self.price_cache
        if pc is None:
            return []
        rows = [(s, t.quote_vol) for s, t in list(pc.ticker_cache.items())
                if t.quote_vol > 0 and self._is_tradable(s)]
        rows.sort(key=lambda r: r[1], reverse=True)
        return [s for s, _ in rows]

    def _start_kline_cache(self) -> None:
        self._stop_kline_cache()
        self.kline_cache = KlineWebSocketCache(
            interval                 = EMA_INTERVAL,
            top_n                    = EMA_TOP_N,
            fast_period              = EMA_FAST,
            slow_period              = EMA_SLOW,
            max_candles              = EMA_MAX_CANDLES,
            universe_provider        = self._top_active_symbols,
            pinned_provider          = self._open_position_symbols,
            universe_refresh_seconds = EMA_UNIVERSE_REFRESH_S,
        )
        self.kline_cache.set_signal_callback(self._on_cross)
        self.kline_cache.start()
        self.log(f"KlineCache EMA{EMA_FAST}/EMA{EMA_SLOW} {EMA_INTERVAL} iniciado "
                 f"(top {EMA_TOP_N} por volumen · hasta {EMA_MAX_CANDLES} velas por símbolo)")

    def _on_cross(self, symbol: str, direction: str, price: float, close_ms: int) -> None:
        """Callback del kline cache (hilo del WS de velas): solo traspasa al loop del bot."""
        loop = self.loop
        if loop is None or not self.running:
            return
        try:
            loop.call_soon_threadsafe(self._handle_cross, symbol, direction, price)
        except Exception:
            pass

    def _handle_cross(self, symbol: str, direction: str, cross_price: float) -> None:
        """Cruce EMA recién cerrado: UP → LONG, DOWN → SHORT. Abre el 1.er tramo.
        Si ya hay posición abierta el cruce se ignora (el DCA/TP/SL la gestionan)."""
        if not self._is_tradable(symbol):
            return
        if self._close_all_active:            # cierre masivo en curso: no abrir nada nuevo
            return
        side = "LONG" if direction == "UP" else "SHORT"
        now = time.time()
        if symbol in self._entry_inflight or symbol in self._closing_symbols:
            return
        if now < self._entry_backoff.get(symbol, 0.0):
            return
        with self.lock:
            if symbol in self.price_blocked or self.symbol_cooldown.get(symbol, 0.0) > now:
                return
            if self.positions.get(symbol) is not None:
                return
        price = self._price_for(symbol)
        if price is None or price <= 0:
            self._log_throttled(f"x_{symbol}", f"Cruce {direction} {symbol} sin precio fresco: omitido")
            return
        if MAX_PRICE_BLOCK > 0 and price > MAX_PRICE_BLOCK:
            self._block_by_price(symbol, price)
            return
        # Guardas de riesgo (pausa · exposición · filtro BTC): la señal se detectó
        # igual que siempre; aquí solo se decide si puede abrir.
        first_level, first_notional = ENTRY_LADDER[0]
        blocked = self._gate_check(side, first_notional, is_dca=False)
        if blocked is not None:
            self._record_block(symbol, side, blocked[0], blocked[1], is_dca=False,
                               extra=f"cruce {direction}")
            return
        self.log(f"CRUCE EMA{EMA_FAST}/{EMA_SLOW} {direction} {symbol} → {side} "
                 f"(cierre vela={cross_price:.6f} | px={price:.6f})")
        self._entry_inflight.add(symbol)
        self._spawn(self._enter_levels(symbol, [(0, first_level, first_notional)], side))

    # ── Guardas de riesgo para abrir ──────────────────────────────────────────

    def _exposure_locked(self) -> float:
        """Notional abierto + notional reservado por órdenes en vuelo. Requiere self.lock."""
        exp = sum(p.notional for p in self.positions.values() if p.status == "OPEN" and p.fills)
        return exp + sum(self._entry_reserved.values())

    def _exposure(self) -> float:
        with self.lock:
            return self._exposure_locked()

    def btc_change(self) -> Tuple[Optional[float], bool]:
        """(cambio 24h de BTCUSDT en %, dato fresco?). None si no hay dato."""
        pc = self.price_cache
        if pc is None:
            return None, False
        t = pc.ticker_cache.get(BTC_FILTER_SYMBOL)
        if t is None:
            return None, False
        fresh = (time.time() - float(getattr(t, "ts", 0) or 0)) <= BTC_MAX_AGE_S
        price = self._display_price(BTC_FILTER_SYMBOL)
        return self._change_for(BTC_FILTER_SYMBOL, price), fresh

    @staticmethod
    def _mode_blocks(mode: str, side: str) -> bool:
        return mode == "all" or (mode == "long" and side == "LONG") or (mode == "short" and side == "SHORT")

    def _btc_side_block(self, side: str, btc: Optional[Tuple[Optional[float], bool]] = None
                        ) -> Optional[Tuple[str, str]]:
        """(tipo, motivo) si algún filtro BTC frena una entrada `side`, o None.
        Filtro bajista: BTC 24h < umbral. Filtro alcista: BTC 24h > umbral.
        Sin dato fresco de BTC, cada filtro activo frena los lados que tiene asignados."""
        filters = (
            ("btc_down", RISK.btc_filter_enabled, RISK.btc_filter_mode, RISK.btc_filter_threshold),
            ("btc_up",   RISK.btc_up_enabled,     RISK.btc_up_mode,     RISK.btc_up_threshold),
        )
        if not any(f[1] for f in filters):
            return None
        chg, fresh = btc if btc is not None else self.btc_change()
        for kind, enabled, mode, thr in filters:
            if not enabled or not self._mode_blocks(mode, side):
                continue
            name = "bajista" if kind == "btc_down" else "alcista"
            if chg is None or not fresh:
                return kind, f"sin dato fresco de {BTC_FILTER_SYMBOL} 24h (filtro {name} activo)"
            if kind == "btc_down" and chg < thr:
                return kind, f"{BTC_FILTER_SYMBOL} 24h {chg:+.2f}% < {thr:+.2f}% (filtro bajista)"
            if kind == "btc_up" and chg > thr:
                return kind, f"{BTC_FILTER_SYMBOL} 24h {chg:+.2f}% > {thr:+.2f}% (filtro alcista)"
        return None

    def _btc_blocks(self) -> Tuple[bool, bool, List[str]]:
        """(frena LONG, frena SHORT, motivos) combinando los dos filtros BTC."""
        btc = self.btc_change()
        blk_l = self._btc_side_block("LONG", btc)
        blk_s = self._btc_side_block("SHORT", btc)
        reasons: List[str] = []
        for b in (blk_l, blk_s):
            if b and b[1] not in reasons:
                reasons.append(b[1])
        return blk_l is not None, blk_s is not None, reasons

    # ── Condición EMA para el DCA a partir de N tramos ───────────────────────

    def _trend_ema(self, symbol: str, period: int) -> Optional[float]:
        """EMA(period) de los cierres guardados por el kline cache (velas de
        EMA_INTERVAL, siembra SMA). Se recalcula solo cuando cierra una vela nueva.
        None si el símbolo no tiene al menos `period` velas."""
        kc = self.kline_cache
        states = getattr(kc, "_states", None) if kc is not None else None
        st = states.get(symbol) if states else None
        if st is None or not getattr(st, "ready", False):
            return None
        key = (getattr(st, "last_ot", 0), len(st.closes), period)
        hit = self._ema_cache.get(symbol)
        if hit is not None and hit[0] == key:
            return hit[1]
        closes = st.closes.tolist()            # copia atómica (el hilo de velas puede añadir)
        value: Optional[float] = None
        if len(closes) >= period:
            ema = sum(closes[:period]) / period
            k = 2.0 / (period + 1)
            for c in closes[period:]:
                ema += k * (c - ema)
            value = ema
        self._ema_cache[symbol] = (key, value)
        return value

    def _dca_trend_block(self, symbol: str, direction: str, idx: int,
                         price: float) -> Optional[Tuple[str, str]]:
        """Si la condición EMA está activa y la posición ya tiene ≥ dca_ema_after
        tramos, el tramo idx (0 = 1.er tramo) solo entra si LONG: precio > EMA y
        SHORT: precio < EMA. Devuelve ("ema_dca", motivo) si lo frena."""
        if not RISK.dca_ema_enabled or idx < RISK.dca_ema_after:
            return None
        p = RISK.dca_ema_period
        ema = self._trend_ema(symbol, p)
        if ema is None:
            return "ema_dca", f"EMA{p} de {symbol} sin velas suficientes (condición activa)"
        if direction == "LONG" and not price > ema:
            return "ema_dca", f"tramo {idx + 1}: precio {price:.6g} ≤ EMA{p} {ema:.6g} (LONG exige precio por encima)"
        if direction == "SHORT" and not price < ema:
            return "ema_dca", f"tramo {idx + 1}: precio {price:.6g} ≥ EMA{p} {ema:.6g} (SHORT exige precio por debajo)"
        return None

    def _gate_check(self, side: str, notional: float, is_dca: bool,
                    exposure: Optional[float] = None) -> Optional[Tuple[str, str]]:
        """Devuelve (tipo, detalle) si la entrada NO puede abrirse, o None.
        NO toma self.lock salvo para calcular la exposición cuando no se pasa."""
        if not is_dca and RISK.entries_paused:
            return "pausa", RISK.pause_reason or "entradas pausadas"
        if RISK.exposure_enabled and (not is_dca or RISK.exposure_include_dca):
            exp = self._exposure() if exposure is None else exposure
            if exp + notional > RISK.max_exposure_usd + 1e-9:
                return ("exposicion",
                        f"exposición {exp:.2f} + {notional:.2f} > límite {RISK.max_exposure_usd:.2f} USDT")
        if not is_dca:
            blk = self._btc_side_block(side)
            if blk is not None:
                return blk
        return None

    def _record_block(self, symbol: str, side: str, kind: str, detail: str,
                      is_dca: bool, extra: str = "") -> None:
        """Registra una señal frenada (máx. 1 vez por minuto por símbolo y tipo)."""
        now = time.time()
        key = f"{symbol}|{kind}|{int(is_dca)}"
        if now - self._block_seen.get(key, 0.0) < 60.0:
            return
        self._block_seen[key] = now
        if len(self._block_seen) > 2000:
            self._block_seen = {k: t for k, t in self._block_seen.items() if now - t < 60.0}
        what = "DCA" if is_dca else "entrada"
        with self._block_lock:
            self.blocked_counts[kind] = self.blocked_counts.get(kind, 0) + 1
            self.blocked_recent.appendleft({
                "ts": now, "symbol": symbol, "side": side, "kind": kind,
                "detail": detail, "what": what, "extra": extra,
            })
        self._log_throttled(f"blk_{kind}",
                            f"⏸ {what} {side} {symbol} frenada ({kind}): {detail}", 30.0)

    # ── Símbolos operables (caché en disco + REST) ────────────────────────────

    def _update_tradable(self) -> None:
        self._tradable = frozenset(self.all_symbols)

    def _is_tradable(self, symbol: str) -> bool:
        tradable = self._tradable
        if tradable:
            return symbol in tradable
        # Sin lista (REST caído): se aceptan perpetuos USDT y se descartan trimestrales (BTCUSDT_250627)
        return symbol.endswith(QUOTE_ASSET) and "_" not in symbol

    def _load_symbols_from_cache(self) -> List[str]:
        """Lee la lista de símbolos desde el archivo de caché en disco."""
        try:
            if not os.path.exists(SYMBOLS_CACHE_FILE):
                return []
            with open(SYMBOLS_CACHE_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            symbols   = data.get("symbols", [])
            saved_at  = data.get("saved_at", 0)
            age_hours = (time.time() - saved_at) / 3600
            if symbols:
                self.log(
                    f"Caché de símbolos cargado: {len(symbols)} símbolos "
                    f"(guardado hace {age_hours:.1f} h)"
                )
            return symbols if isinstance(symbols, list) else []
        except Exception as exc:
            self.log(f"No pude leer caché de símbolos: {exc}")
            return []

    def _save_symbols_to_cache(self, symbols: List[str]) -> None:
        """Guarda la lista de símbolos en disco para recuperación ante bloqueos."""
        tmp = f"{SYMBOLS_CACHE_FILE}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"symbols": symbols, "saved_at": time.time()}, fh)
            os.replace(tmp, SYMBOLS_CACHE_FILE)
            self.log(f"Caché de símbolos guardado: {len(symbols)} símbolos → {SYMBOLS_CACHE_FILE}")
        except Exception as exc:
            self.log(f"No pude guardar caché de símbolos: {exc}")

    async def _refresh_all_symbols(self) -> bool:
        """
        Obtiene todos los símbolos USDT-M perpetuos vía REST (exchangeInfo).
        Guarda el resultado en SYMBOLS_CACHE_FILE como respaldo.
        Devuelve True si tuvo éxito.
        """
        try:
            self.log("REST: obteniendo lista completa de símbolos de futuros USDT-M...")
            data    = await self.client.request("GET", "/fapi/v1/exchangeInfo")
            filters = self.client.exchange_filters

            symbols: List[str] = []
            for sym_info in data.get("symbols", []):
                if sym_info.get("quoteAsset")   != QUOTE_ASSET:  continue
                if sym_info.get("contractType") != "PERPETUAL":  continue
                if sym_info.get("status")       != "TRADING":    continue
                s = sym_info["symbol"]
                # Si exchange_filters ya está cargado, usarlo como filtro extra
                if filters and s not in filters:
                    continue
                symbols.append(s)

            if not symbols:
                self.log("REST: respuesta vacía al obtener símbolos")
                return False

            self.all_symbols             = symbols
            self._update_tradable()
            self.last_symbols_refresh_at = time.time()
            self._save_symbols_to_cache(symbols)
            self.log(f"REST: {len(symbols)} símbolos cargados y guardados en caché")

            if symbols:
               with self.lock:
                   self.price_blocked.clear()   # ← agregar esto al refrescar
               self.log("price_blocked reseteado con el refresh de símbolos")
             
            return True

        except RuntimeError as exc:
            msg = str(exc)
            if "418" in msg:
                self.log(
                    f"REST 418 (IP rate-limit Binance) al obtener símbolos — "
                    f"usando caché si está disponible"
                )
                self.last_error = "HTTP 418 – rate-limit Binance REST al cargar símbolos"
            else:
                self.last_error = msg
                self.log(f"REST _refresh_all_symbols falló: {msg}")
            return False
        except Exception as exc:
            self.last_error = str(exc)
            self.log(f"REST _refresh_all_symbols error: {exc}")
            return False

    async def _init_all_symbols(self) -> None:
        """
        Carga inicial de símbolos:
          1. Intenta leer desde caché en disco (rápido, sin REST).
          2. Si el caché está vacío o falla, hace REST a exchangeInfo.
          3. Si el REST también falla, usa exchange_filters como último recurso.
        """
        # Intento 1: caché en disco
        cached = self._load_symbols_from_cache()
        if cached:
            self.all_symbols             = cached
            self.last_symbols_refresh_at = time.time()  # no forzar refresh inmediato
            # Lanzar refresh en background para actualizar si el caché es viejo
            asyncio.ensure_future(self._maybe_refresh_symbols_cache())
            return

        # Intento 2: REST
        success = await self._refresh_all_symbols()
        if success:
            return

        # Intento 3: exchange_filters (cargados al inicio desde exchangeInfo)
        if self.client.exchange_filters:
            self.all_symbols = list(self.client.exchange_filters.keys())
            self.log(
                f"Usando exchange_filters como fallback: {len(self.all_symbols)} símbolos"
            )
            
            return
        
        # Intento 4: lista inicial fija
        if INITIAL_SYMBOLS:
            self.all_symbols = INITIAL_SYMBOLS.copy()
            self.last_symbols_refresh_at = time.time()
            self.log(
                f"Usando lista inicial fija: {len(self.all_symbols)} símbolos"
            )
            return
        
        else:
            self.log(
                "ADVERTENCIA: No hay símbolos disponibles. "
                "El bot esperará hasta que se obtenga la lista."
            )

            self.all_symbols = INITIAL_SYMBOLS.copy()
            self.last_symbols_refresh_at = time.time()
            self.log(
                f"Usando lista inicial fija: {len(self.all_symbols)} símbolos"
            )

    async def _maybe_refresh_symbols_cache(self) -> None:
        """Refresca el caché si tiene más de SYMBOL_REFRESH_HOURS horas."""
        age = time.time() - self.last_symbols_refresh_at
        if age > SYMBOL_REFRESH_HOURS * 3600:
            await self._refresh_all_symbols()

    async def _all_symbols_refresh_loop(self) -> None:
        """Refresca la lista completa de símbolos cada SYMBOL_REFRESH_HOURS."""
        while self.running:
            await asyncio.sleep(SYMBOL_REFRESH_HOURS * 3600)
            if not self.running:
                break
            self.log(
                f"Refresh de símbolos programado (cada {SYMBOL_REFRESH_HOURS} h)..."
            )
            await self._refresh_all_symbols()

    # ── Motor por eventos ─────────────────────────────────────────────────────
    #
    # Camino de un tick:
    #   WS (hilo ws-price-cache) → _on_ws_event → _enqueue → loop.call_soon_threadsafe(set)
    #   → _engine_loop despierta → _evaluate_symbol (entradas / TP / SL)
    # Sin sleeps ni ciclos: el único retraso es el viaje del propio mensaje.

    def _on_ws_event(self, symbol: str, kind: str) -> None:
        """Callback del WS (hilo del WS, fuera de locks): solo el radar (= posiciones
        abiertas) se evalúa por tick. Las entradas llegan por cruce EMA (_on_cross)."""
        if symbol in self.watch:
            self._enqueue(symbol)

    def _enqueue(self, symbol: str) -> None:
        now = time.perf_counter()
        with self._pending_lock:
            if symbol not in self._pending:
                self._pending[symbol] = now
            if self._wake_scheduled:
                return
            self._wake_scheduled = True
        loop, ev = self.loop, self._tick_event
        try:
            if loop is None or ev is None:
                raise RuntimeError("loop no listo")
            loop.call_soon_threadsafe(ev.set)
        except Exception:
            with self._pending_lock:
                self._wake_scheduled = False

    async def _engine_loop(self) -> None:
        self.log(
            f"Motor por eventos activo (sin ciclos de filtrado ni sleeps de detección | "
            f"radar ≥{MIN_GAIN_FILTER:.0f}% | red de seguridad {SAFETY_TICK_SECS:g}s)"
        )
        ev = self._tick_event
        while self.running:
            await ev.wait()
            ev.clear()
            with self._pending_lock:
                batch = self._pending
                self._pending = {}
                self._wake_scheduled = False
            for sym, t_in in batch.items():
                lat = (time.perf_counter() - t_in) * 1000.0
                self._lat_sum += lat
                self._lat_n   += 1
                if lat > self._lat_max:
                    self._lat_max = lat
                try:
                    self._evaluate_symbol(sym)
                except Exception as exc:
                    self.last_error = str(exc)
                    self._log_throttled("eval_err", f"Error evaluando {sym}: {exc!r}")
            # Stop global por PnL no realizado (con el precio recién llegado)
            try:
                self._check_global_stop()
            except Exception as exc:
                self.last_error = str(exc)
                self._log_throttled("gstop_err", f"Error en stop global: {exc!r}")
            # Cede el loop: sin esto, un flujo continuo de ticks dejaría sin turno
            # a las tareas de órdenes (entradas/cierres).
            await asyncio.sleep(0)

    def _log_throttled(self, key: str, msg: str, secs: float = 5.0) -> None:
        now = time.time()
        if now - self._log_throttle.get(key, 0.0) >= secs:
            self._log_throttle[key] = now
            self.log(msg)

    def _spawn(self, coro) -> None:
        """Lanza una tarea en segundo plano (órdenes) sin bloquear el motor."""
        task = asyncio.get_running_loop().create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_done)

    def _bg_done(self, task) -> None:
        self._bg_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self.last_error = str(exc)
            self._log_throttled("bg_err", f"Tarea en segundo plano falló: {exc!r}")

    # ── Precio / cambio en vivo ───────────────────────────────────────────────

    def _price_for(self, symbol: str) -> Optional[float]:
        """Precio para OPERAR: mid del bookTicker (o mark) con antigüedad máxima
        PRICE_MAX_AGE_S; si no hay, último precio del ticker 24h con la misma
        regla. None = no operar con un dato viejo."""
        pc = self.price_cache
        if pc is None:
            return None
        now = time.time()
        e = pc.price_cache.get(symbol)
        if e and now - e[1] <= PRICE_MAX_AGE_S:
            return e[0]
        t = pc.ticker_cache.get(symbol)
        if t and t.last_price > 0 and now - t.ts <= PRICE_MAX_AGE_S:
            return t.last_price
        return None

    def _display_price(self, symbol: str) -> float:
        """Precio para MOSTRAR: el fresco si lo hay, si no el último conocido."""
        pc = self.price_cache
        if pc is None:
            return 0.0
        p = self._price_for(symbol)
        if p:
            return p
        e = pc.price_cache.get(symbol)
        if e:
            return e[0]
        m = pc.mark_cache.get(symbol)
        if m:
            return m[0]
        t = pc.ticker_cache.get(symbol)
        return t.last_price if t else 0.0

    def _change_for(self, symbol: str, price: float) -> float:
        """% de cambio 24h en vivo: (precio / open24h - 1) con el precio del tick.
        Si no hay open24h cae al % del ticker (que se refresca cada ~1 s)."""
        pc = self.price_cache
        t  = pc.ticker_cache.get(symbol) if pc is not None else None
        if t is None:
            return 0.0
        if USE_LIVE_CHANGE and t.open_24h > 0 and price > 0:
            return (price / t.open_24h - 1.0) * 100.0
        return t.change_pct

    # ── Radar ─────────────────────────────────────────────────────────────────

    def _watch_add(self, symbol: str) -> None:
        """Mete un símbolo en el radar y suscribe su bookTicker (ms). Solo desde el loop."""
        if symbol in self.watch:
            return
        self.watch = self.watch | {symbol}
        pc = self.price_cache
        if pc is not None:
            try:
                pc.ensure_symbols([symbol])
            except Exception as exc:
                self.last_error = str(exc)
                self._log_throttled("watch_add", f"No pude suscribir {symbol}: {exc}")
        if self._kline_dirty is not None:
            self._kline_dirty.set()
        self.log(f"RADAR + {symbol} ({len(self.watch)} vigilados)")

    def _prune_watch(self) -> None:
        """Saca del radar lo que ya no cumple (sin posición y por debajo de
        MIN_GAIN_FILTER - histéresis) y da de baja su bookTicker."""
        pc = self.price_cache
        if pc is None:
            return
        pos_syms = set(self._open_position_symbols())
        floor_pct = MIN_GAIN_FILTER - WATCH_PRUNE_HYSTERESIS
        keep, dropped = set(), []
        for sym in self.watch:
            if sym in pos_syms:
                keep.add(sym)
                continue
            dropped.append(sym)
        if not dropped:
            return
        self.watch = frozenset(keep)
        try:
            pc.update_symbols(sorted(keep | pos_syms))
        except Exception as exc:
            self.last_error = str(exc)
        if self._kline_dirty is not None:
            self._kline_dirty.set()
        self.log(f"RADAR - {len(dropped)} fuera ({', '.join(sorted(dropped)[:6])}"
                 f"{'…' if len(dropped) > 6 else ''}) → {len(self.watch)} vigilados")

    # ── Evaluación de un símbolo (se ejecuta en CADA tick) ────────────────────

    def _evaluate_symbol(self, symbol: str) -> None:
        pc = self.price_cache
        if pc is None:
            return

        with self.lock:
            pos = self.positions.get(symbol)
            has_pos = bool(pos and pos.status == "OPEN" and pos.fills)

        if symbol not in self.watch:
            if has_pos:
                self._watch_add(symbol)
            else:
                return

        price = self._price_for(symbol)
        if price is None or price <= 0:
            return

        self.scan_count  += 1
        self.last_scan_at = time.time()

        if has_pos:
            with self.lock:
                p_ex = self.positions.get(symbol)
                if p_ex is not None and p_ex.status == "OPEN":
                    p_ex.update_excursions(price)
            self._check_exits(symbol, price)

        if has_pos:
            self._check_dca(symbol, price)

    def _check_exits(self, symbol: str, price: float) -> None:
        """TP / SL. Pre-chequeo barato en el loop; el cierre real va a una tarea."""
        if symbol in self._closing_symbols:
            return
        if time.time() < self._close_backoff.get(symbol, 0.0):
            return
        with self.lock:
            pos = self.positions.get(symbol)
            if not pos or pos.status != "OPEN" or not pos.fills:
                return
            pnl    = pos.unrealized_pnl(price)
            target = pos.notional * TAKE_PROFIT_FRACTION
            sl_usd = pos.sl_usd
        if pnl <= sl_usd:
            reason = "SL"
        elif pnl >= target:
            reason = "TP"
        else:
            return
        if not self._begin_close_guard(symbol):       # se toma AQUÍ, antes de crear la tarea
            return
        self._spawn(self._close_position(symbol, price, reason, acquired=True))

    def _check_dca(self, symbol: str, price: float) -> None:
        """Tramos siguientes del DCA: se abren cuando el precio va EN CONTRA de la
        1.ª entrada los % de la escalera (el tramo 1 ya lo abrió el cruce EMA).
        El siguiente tramo es siempre el de índice = nº de tramos ya abiertos, así
        que editar la escalera con posiciones abiertas no duplica ni salta tramos."""
        if self._close_all_active:            # cierre masivo en curso: sin DCA nuevo
            return
        if symbol in self._entry_inflight or symbol in self._closing_symbols:
            return
        if time.time() < self._entry_backoff.get(symbol, 0.0):
            return
        with self.lock:
            pos = self.positions.get(symbol)
            if pos is None or pos.status != "OPEN" or not pos.fills:
                return
            adverse   = pos.adverse_pct(price)
            direction = pos.direction
            n_fills   = len(pos.fills)
        ladder = ENTRY_LADDER
        due = []
        for idx in range(n_fills, len(ladder)):
            lvl, nt = ladder[idx]
            if lvl <= 0 or adverse < lvl or (symbol, idx) in self._entry_reserved:
                break
            due.append((idx, lvl, nt))
        if not due:
            return
        first_idx, first_lvl, first_nt = due[0]
        # Límite de exposición aplicado al DCA (solo si así se configuró en la web)
        blocked = self._gate_check(direction, first_nt, is_dca=True)
        # Condición EMA del DCA (a partir de N tramos)
        if blocked is None:
            blocked = self._dca_trend_block(symbol, direction, first_idx, price)
        if blocked is not None:
            self._record_block(symbol, direction, blocked[0], blocked[1], is_dca=True,
                               extra=f"tramo {first_idx + 1} al {first_lvl:g}%")
            return
        self._entry_inflight.add(symbol)
        self._spawn(self._enter_levels(symbol, due, direction))

    def _block_by_price(self, symbol: str, price: float) -> None:
        with self.lock:
            newly = symbol not in self.price_blocked
            self.price_blocked.add(symbol)
        if newly:
            self.log(f"BLOQUEADO permanente {symbol}: precio {price:.4f} > {MAX_PRICE_BLOCK} USD")

    async def _enter_levels(self, symbol: str, due: list, direction: str) -> None:
        """Abre, en orden, los tramos vencidos [(índice, % en contra, notional), ...].
        Corre como tarea: el motor no espera a la orden."""
        opened_any = False
        try:
            for idx, level, notional in due:
                price = self._price_for(symbol)
                if price is None:
                    break
                if idx > 0:                  # DCA: reconfirma que el precio sigue en contra
                    with self.lock:
                        pos = self.positions.get(symbol)
                        adverse = pos.adverse_pct(price) if pos else 0.0
                    if adverse < level:
                        break
                    trend = self._dca_trend_block(symbol, direction, idx, price)
                    if trend is not None:    # p. ej. el tramo 3 entra y el 4 ya exige la EMA
                        self._record_block(symbol, direction, trend[0], trend[1], is_dca=True,
                                           extra=f"tramo {idx + 1} al {level:g}%")
                        break
                if MAX_PRICE_BLOCK > 0 and price > MAX_PRICE_BLOCK:
                    self._block_by_price(symbol, price)
                    break
                if not await self._ensure_position(symbol, idx, level, notional, price, direction):
                    break
                opened_any = True
        finally:
            self._entry_inflight.discard(symbol)
            if opened_any:                   # reevalúa ya (TP/SL del nuevo tramo, niveles que falten)
                self._enqueue(symbol)

    # ── Cooldown helpers ──────────────────────────────────────────────────────

    def _cooldown_remaining(self, symbol: str) -> float:
        unblock_at = self.symbol_cooldown.get(symbol, 0.0)
        return max(0.0, unblock_at - time.time())

    @staticmethod
    def _fmt_cooldown(seconds: float) -> str:
        s = int(seconds)
        h = s // 3600
        m = (s % 3600) // 60
        r = s % 60
        if h > 0:  return f"{h}h {m:02d}m"
        if m > 0:  return f"{m}m {r:02d}s"
        return f"{r}s"

    def _begin_close_guard(self, symbol: str) -> bool:
        """Evita cierres duplicados del mismo símbolo."""
        with self.lock:
            if symbol in self._closing_symbols:
                return False
            pos = self.positions.get(symbol)
            if not pos or pos.status != "OPEN" or not pos.fills:
                return False
            self._closing_symbols.add(symbol)
            return True

    def _end_close_guard(self, symbol: str) -> None:
        with self.lock:
            self._closing_symbols.discard(symbol)

    # ── Estrategia ────────────────────────────────────────────────────────────

    async def _ensure_position(self, symbol: str, idx: int, level: float, notional: float,
                               price: float, direction: str) -> bool:
        """Abre el tramo de índice `idx` (0 = entrada del cruce). True si se abrió.
        El tramo se RESERVA antes de enviar la orden para que ningún tick lo
        duplique mientras está en vuelo."""
        key = (symbol, idx)
        is_dca = idx > 0
        blocked: Optional[Tuple[str, str]] = None
        with self.lock:
            if self.symbol_cooldown.get(symbol, 0.0) > time.time():
                return False
            if symbol in self._closing_symbols or key in self._entry_reserved:
                return False
            pos = self.positions.get(symbol)
            if idx == 0:
                if pos is not None:                       # ya hay posición: el cruce no abre otra
                    return False
            elif (pos is None or pos.status != "OPEN" or pos.direction != direction
                  or len(pos.fills) != idx):              # el tramo idx solo sigue al idx-1
                return False
            # Comprobación definitiva de las guardas (con la exposición real, incluidas
            # las órdenes en vuelo de otros símbolos) justo antes de reservar.
            blocked = self._gate_check(direction, notional, is_dca,
                                       exposure=self._exposure_locked())
            if blocked is None:
                trade_id = pos.trade_id if (pos is not None and pos.trade_id) else 0
                self._entry_reserved[key] = notional
        if blocked is not None:
            self._record_block(symbol, direction, blocked[0], blocked[1], is_dca=is_dca,
                               extra=f"tramo {idx + 1} al {level:g}%")
            return False

        try:
            if trade_id == 0:
                trade_id = self._next_trade_id()
            try:
                qty = await self.client.market_open(symbol, notional, price, direction)
            except Exception as exc:
                self.last_error = str(exc)
                self._entry_backoff[symbol] = time.time() + ENTRY_ERROR_BACKOFF_S
                self.log(f"Error abriendo {direction} {symbol} tramo {idx + 1} ({level:g}%): {exc}")
                return False

            fill = Fill(level=level, notional=notional, entry_price=price, qty=qty)
            with self.lock:
                pos = self.positions.get(symbol)
                if pos is None:
                    pos = BotPosition(symbol=symbol, trade_id=trade_id, direction=direction)
                    self.positions[symbol] = pos
                elif pos.trade_id == 0:
                    pos.trade_id = trade_id
                else:
                    trade_id = pos.trade_id
                pos.fills.append(fill)
                pos.refresh_auto_sl()           # 1 tramo → notional×0.251 | 2+ → SL estándar | manual → intacto
                sl_now, sl_mode, n_fills = pos.sl_usd, pos.sl_mode, len(pos.fills)

            self._watch_add(symbol)             # la posición abierta siempre queda vigilada
            self.log(
                f"{direction} {symbol}: tramo {idx + 1} ({level:g}% en contra) | {notional:.2f} USDT | "
                f"qty={qty} | px={price:.6f} | trade_id={trade_id} | "
                f"tramos={n_fills} | SL={sl_now:.4f} USD ({sl_mode})"
            )
            self.executor.notify_open(
                trade_id=trade_id,
                symbol=symbol,
                direction=direction,
                price=price,
                quantity=qty,
                notional=notional,
                level=level,
            )
            self.persist_state()
            return True
        finally:
            with self.lock:
                self._entry_reserved.pop(key, None)

    async def _close_position(self, symbol: str, price: float, reason: str,
                              acquired: bool = False) -> bool:
        """Cierre único para TP / SL / MANUAL (antes había 3 copias del mismo código)."""
        if not acquired and not self._begin_close_guard(symbol):
            return False
        try:
            with self.lock:
                pos = self.positions.get(symbol)
                if not pos or pos.status != "OPEN" or not pos.fills:
                    return False
                fills_n   = len(pos.fills)
                snapshot  = list(pos.fills)
                qty       = sum(f.qty for f in snapshot)
                notional  = sum(f.notional for f in snapshot)
                avg_ent   = (sum(f.entry_price * f.qty for f in snapshot) / qty) if qty > 0 else 0.0
                trade_id  = pos.trade_id
                direction = pos.direction
                sl_usd    = pos.sl_usd
                target    = notional * TAKE_PROFIT_FRACTION
                pnl       = pos.pnl_sign * sum((f.entry_price - price) * f.qty for f in snapshot) if price > 0 else 0.0
                # Re-verifica la condición con el estado actual (pudo cambiar el SL desde el dashboard)
                if reason == "SL" and pnl > sl_usd:
                    return False
                if reason == "TP" and pnl < target:
                    return False
                # Último punto de la operación: el precio de cierre también cuenta
                pos.update_excursions(price)
                now_ts   = time.time()
                duration = now_ts - pos.opened_ts
                exc = {
                    "mfe_usd":    pos.mfe_usd,
                    "mae_usd":    pos.mae_usd,
                    "mfe_pct":    pos.mfe_pct,
                    "mae_pct":    pos.mae_pct,
                    "duration_s": duration,
                }
                stat_rec = {
                    "trade_id":      trade_id,
                    "symbol":        symbol,
                    "direction":     direction,
                    "reason":        reason,
                    "opened_at_ts":  pos.opened_ts,
                    "closed_at_ts":  now_ts,
                    "duration_s":    duration,
                    "avg_entry":     avg_ent,
                    "close_price":   price,
                    "qty":           qty,
                    "notional":      notional,
                    "fills_n":       fills_n,
                    "levels":        [f.level for f in snapshot],
                    "sl_usd":        sl_usd,
                    "target":        target,
                    "pnl":           pnl,
                    "pnl_pct":       (pnl / notional * 100.0) if notional > 0 else 0.0,
                    "mfe_usd":       pos.mfe_usd,
                    "mae_usd":       pos.mae_usd,
                    "mfe_pct":       pos.mfe_pct,
                    "mae_pct":       pos.mae_pct,
                    "time_to_mfe_s": (pos.mfe_ts - pos.opened_ts) if pos.mfe_ts else 0.0,
                    "time_to_mae_s": (pos.mae_ts - pos.opened_ts) if pos.mae_ts else 0.0,
                    "low_price":     pos.low_price,
                    "high_price":    pos.high_price,
                }

            try:
                await self.client.close_order(symbol, qty, direction)
            except Exception as exc:
                self.last_error = str(exc)
                self._close_backoff[symbol] = time.time() + CLOSE_ERROR_BACKOFF_S
                label = {"SL": "STOP LOSS", "TP": "take profit", "MANUAL": "cierre manual",
                         "GLOBAL": "stop global"}.get(reason, reason)
                self.log(f"Error cerrando {label} {symbol}: {exc}")
                return False

            unblock_str = ""
            leftover = 0
            with self.lock:
                pos = self.positions.get(symbol)
                if pos:
                    extra = pos.fills[fills_n:]
                    self.total_realized_pnl += pnl
                    if extra:
                        # Se abrió otro tramo MIENTRAS se cerraba: no estaba en la orden de
                        # cierre, así que la posición sigue abierta solo con esos fills.
                        pos.fills = list(extra)
                        leftover  = len(extra)
                        pos.reset_excursions()      # la posición restante empieza sus excursiones de cero
                    else:
                        self.positions.pop(symbol, None)
                        pos.status       = "CLOSED"
                        pos.realized_pnl = pnl
                        unblock_ts  = time.time() + COOLDOWN_SECONDS
                        self.symbol_cooldown[symbol] = unblock_ts
                        unblock_str = datetime.fromtimestamp(
                            unblock_ts, timezone.utc
                        ).strftime("%Y-%m-%d %H:%M UTC")
                    self.closed_trades.insert(0, {
                        "symbol":      symbol,
                        "direction":   direction,
                        "fills_n":     fills_n,
                        "pnl":         pnl,
                        "target":      target,
                        "qty":         qty,
                        "avg_entry":   avg_ent,
                        "close_price": price,
                        "notional":    notional,
                        "closed_at":   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "unblock_at":  unblock_str,
                        "reason":      reason,
                        **exc,
                    })
                    self.closed_trades = self.closed_trades[:500]

            self._record_trade_stat(stat_rec)
            self.executor.notify_close(
                trade_id=trade_id,
                symbol=symbol,
                direction=direction,
                # Al Executor el stop global le llega como "MANUAL" (motivo que ya conoce)
                reason="MANUAL" if reason == "GLOBAL" else reason,
                close_price=price,
                pnl=pnl,
            )
            if reason == "TP":
                self.log(
                    f"CIERRE TP {symbol}: PnL={pnl:.4f} | objetivo={target:.4f} | "
                    f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
                )
            elif reason == "SL":
                self.log(
                    f"⛔ STOP LOSS {symbol}: PnL={pnl:.4f} | SL configurado={sl_usd:.4f} | "
                    f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
                )
            elif reason == "GLOBAL":
                self.log(
                    f"🛑 CIERRE POR STOP GLOBAL {symbol}: PnL={pnl:.4f} | "
                    f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
                )
            else:
                self.log(
                    f"✋ CIERRE MANUAL {symbol}: PnL={pnl:.4f} | "
                    f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
                )
            if leftover:
                self.log(f"⚠️ {symbol}: {leftover} tramo(s) se abrieron durante el cierre; "
                         f"la posición sigue abierta con ellos (sin cooldown)")
            self.persist_state()
            return True
        finally:
            self._end_close_guard(symbol)

    # ── Estadísticas MFE / MAE ────────────────────────────────────────────────

    def _load_trade_stats(self) -> None:
        """Carga el histórico de operaciones (una línea JSON por cierre)."""
        try:
            if not os.path.exists(STATS_FILE):
                return
            rows: List[dict] = []
            with open(STATS_FILE, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        continue
            self.trade_stats = rows
            max_id = max((int(r.get("trade_id", 0) or 0) for r in rows), default=0)
            if max_id > self._trade_id_seq:
                self._trade_id_seq = max_id        # evita repetir trade_id tras reinicios
        except Exception as exc:
            print(f"No pude leer {STATS_FILE}: {exc}", flush=True)

    def _restore_history(self) -> None:
        """Tras un reinicio recupera SOLO el historial (cierres y PnL realizado) del
        último estado guardado. Las posiciones no se restauran: vivían en memoria,
        así que mostrarlas como abiertas sería engañoso."""
        try:
            if not os.path.exists(STATE_FILE):
                return
            with open(STATE_FILE, "r", encoding="utf-8") as fh:
                persisted = json.load(fh)
            closed = persisted.get("closed_trades")
            if isinstance(closed, list) and closed:
                self.closed_trades = [c for c in closed if isinstance(c, dict)][:500]
            self.total_realized_pnl = float(persisted.get("total_realized_pnl", 0.0) or 0.0)
        except Exception as exc:
            print(f"No pude restaurar historial de {STATE_FILE}: {exc}", flush=True)

    def _record_trade_stat(self, rec: dict) -> None:
        with self._stats_lock:
            self.trade_stats.append(rec)
        try:
            with open(STATS_FILE, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception as exc:
            self._log_throttled("stats_write", f"No pude guardar estadística de {rec.get('symbol')}: {exc}")

    @staticmethod
    def _pctl(sorted_vals: List[float], p: float) -> float:
        if not sorted_vals:
            return 0.0
        k = (len(sorted_vals) - 1) * p
        lo, hi = int(floor(k)), min(int(floor(k)) + 1, len(sorted_vals) - 1)
        return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)

    def _summ(self, vals: List[float]) -> dict:
        v = sorted(vals)
        if not v:
            return {"mean": 0.0, "median": 0.0, "p75": 0.0, "p90": 0.0, "min": 0.0, "max": 0.0}
        return {
            "mean":   sum(v) / len(v),
            "median": self._pctl(v, 0.5),
            "p75":    self._pctl(v, 0.75),
            "p90":    self._pctl(v, 0.90),
            "min":    v[0],
            "max":    v[-1],
        }

    def compute_stats(self) -> dict:
        """Resumen estadístico de MFE/MAE (MAE se reporta en valor absoluto)."""
        with self._stats_lock:
            recs = [r for r in self.trade_stats if "mfe_usd" in r]
        groups: Dict[str, dict] = {}
        for name, subset in (
            ("ALL",    recs),
            ("TP",     [r for r in recs if r.get("reason") == "TP"]),
            ("SL",     [r for r in recs if r.get("reason") == "SL"]),
            ("MANUAL", [r for r in recs if r.get("reason") == "MANUAL"]),
            ("GLOBAL", [r for r in recs if r.get("reason") == "GLOBAL"]),
        ):
            groups[name] = {
                "n":          len(subset),
                "mfe_usd":    self._summ([abs(float(r.get("mfe_usd", 0))) for r in subset]),
                "mae_usd":    self._summ([abs(float(r.get("mae_usd", 0))) for r in subset]),
                "mfe_pct":    self._summ([abs(float(r.get("mfe_pct", 0))) for r in subset]),
                "mae_pct":    self._summ([abs(float(r.get("mae_pct", 0))) for r in subset]),
                "duration_s": self._summ([float(r.get("duration_s", 0)) for r in subset]),
                "pnl":        self._summ([float(r.get("pnl", 0)) for r in subset]),
            }
        tps = groups["TP"]["n"]
        total_pnl = sum(float(r.get("pnl", 0)) for r in recs)

        # ¿Qué SL habría cortado ganadoras (TP)? (MAE <= SL → la operación se habría parado)
        tp_mae = [float(r.get("mae_usd", 0)) for r in recs if r.get("reason") == "TP"]
        sl_sim = []
        for sl in (-1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -8.0, -10.0, -12.0, -15.0, -20.0):
            stopped = sum(1 for m in tp_mae if m <= sl)
            sl_sim.append({
                "sl": sl, "tp_stopped": stopped, "tp_total": tps,
                "pct": (stopped / tps * 100.0) if tps else 0.0,
            })
        return {
            "count":     len(recs),
            "win_rate":  (tps / len(recs) * 100.0) if recs else 0.0,
            "total_pnl": total_pnl,
            "avg_pnl":   (total_pnl / len(recs)) if recs else 0.0,
            "groups":    groups,
            "sl_sim":    sl_sim,
            "updated":   time.time(),
        }

    # ── SL global editable en caliente ────────────────────────────────────────

    def set_default_stop_loss(self, sl_usd: float, override_manual: bool = False) -> dict:
        """Cambia el SL estándar (2+ tramos), lo guarda en disco y lo aplica ya a las
        posiciones abiertas. Los SL manuales se respetan salvo override_manual=True."""
        global DEFAULT_STOP_LOSS_USD
        DEFAULT_STOP_LOSS_USD = float(sl_usd)
        updated: List[str] = []
        with self.lock:
            for sym, pos in self.positions.items():
                if pos.status != "OPEN" or not pos.fills:
                    continue
                if override_manual:
                    pos.sl_manual = False
                before = pos.sl_usd
                pos.refresh_auto_sl()
                if pos.sl_usd != before:
                    updated.append(sym)
        err = _save_settings()
        if err:
            self.log(f"No pude guardar ajustes: {err}")
        self.log(f"SL GLOBAL = {DEFAULT_STOP_LOSS_USD:.4f} USD "
                 f"(posiciones actualizadas: {', '.join(updated) or 'ninguna'}"
                 f"{' | manuales sobrescritos' if override_manual else ''})")
        self.persist_state()
        for sym in self._open_position_symbols():
            self._enqueue(sym)          # reevalúa ya con el nuevo SL
        return {"default_stop_loss_usd": DEFAULT_STOP_LOSS_USD, "updated": updated}

    def set_stop_loss(self, symbol: str, sl_usd: float) -> bool:
        """Fija un stop loss MANUAL (USD, negativo) para una posición abierta.
        Queda marcado como manual: el SL automático por tramos ya no lo pisa."""
        symbol = symbol.upper().strip()
        with self.lock:
            pos = self.positions.get(symbol)
            if not pos or pos.status != "OPEN":
                return False
            pos.sl_usd    = sl_usd
            pos.sl_manual = True
        self.log(f"Stop loss MANUAL para {symbol}: {sl_usd:.4f} USD")
        self.persist_state()
        # Reevalúa ya con el nuevo SL (por si el precio actual ya lo cruza)
        self._enqueue(symbol)
        return True

    async def close_position_manual(self, symbol: str) -> bool:
        """Cierre manual de una posición abierta desde la UI."""
        symbol = symbol.upper().strip()
        price = self._display_price(symbol)
        return await self._close_position(symbol, price, "MANUAL")

    # ── Multiplicador del TP editable en caliente ─────────────────────────────

    def set_take_profit(self, fraction: float) -> dict:
        """Cambia el multiplicador del TP (objetivo = notional × fraction). Se aplica
        ya a TODAS las posiciones abiertas y se guarda en disco. ValueError si es inválido."""
        global TAKE_PROFIT_FRACTION
        fraction = float(fraction)
        if not (TP_MIN <= fraction <= TP_MAX):
            raise ValueError(f"El multiplicador del TP debe estar entre {TP_MIN:g} y {TP_MAX:g}")
        TAKE_PROFIT_FRACTION = fraction
        err = _save_settings()
        if err:
            self.log(f"No pude guardar ajustes: {err}")
        self.log(f"TP MULTIPLICADOR = {fraction:g} ({fraction * 100:g}% del notional) — "
                 f"aplicado a las posiciones abiertas")
        self.persist_state()
        for sym in self._open_position_symbols():
            self._enqueue(sym)              # reevalúa ya con el nuevo objetivo
        return {"take_profit_fraction": fraction, "take_profit_pct": fraction * 100.0}

    # ── EMA rápida / lenta editables en caliente ──────────────────────────────

    def set_ema_periods(self, fast: int, slow: int) -> dict:
        """Cambia EMA rápida/lenta. El cache recalcula con los cierres guardados
        (hasta 1500 por símbolo) sin descargar nada y sin emitir cruces falsos.
        ValueError si los periodos no son válidos."""
        global EMA_FAST, EMA_SLOW
        kc = self.kline_cache
        if kc is None:
            raise RuntimeError("El detector de EMA no está activo todavía")
        recomputed = kc.set_periods(fast, slow)          # valida y recalcula (ValueError si falla)
        EMA_FAST, EMA_SLOW = kc.fast_period, kc.slow_period
        err = _save_settings()
        if err:
            self.log(f"No pude guardar ajustes: {err}")
        self.log(f"EMA cambiada a {EMA_FAST}/{EMA_SLOW} ({EMA_INTERVAL}) — "
                 f"{recomputed} símbolos recalculados con su historial")
        self.persist_state()
        return {"ema_fast": EMA_FAST, "ema_slow": EMA_SLOW, "recomputed": recomputed}

    # ── Escalera DCA editable en caliente ─────────────────────────────────────

    @staticmethod
    def ladder_view() -> dict:
        lad, fac = ENTRY_LADDER, FACTORY_LADDER
        return {
            "levels":            [lv for lv, _ in lad],
            "notionals":         [nt for _, nt in lad],
            "factory_levels":    [lv for lv, _ in fac],
            "factory_notionals": [nt for _, nt in fac],
            "is_factory":        lad == fac,
            "max_rows":          LADDER_MAX_ROWS,
        }

    def set_ladder(self, levels: Any, notionals: Any, origin: str = "web") -> dict:
        """Sustituye la escalera DCA (valida antes; ValueError si no vale). Se aplica
        ya a las posiciones abiertas: su próximo tramo es el de índice = nº de
        tramos que ya tienen."""
        global ENTRY_LADDER
        new = _validate_ladder(levels, notionals)
        old = ENTRY_LADDER
        ENTRY_LADDER = new
        err = _save_settings()
        if err:
            self.log(f"No pude guardar ajustes: {err}")
        fmt = lambda lad: ", ".join(f"{lv:g}%→{nt:g}" for lv, nt in lad)   # noqa: E731
        self.log(f"ESCALERA DCA {'restaurada de fábrica' if origin == 'factory' else 'actualizada'}: "
                 f"[{fmt(new)}] (antes [{fmt(old)}])")
        self.persist_state()
        for sym in self._open_position_symbols():
            self._enqueue(sym)                  # un tramo nuevo puede estar ya vencido
        return self.ladder_view()

    def reset_ladder(self) -> dict:
        return self.set_ladder([lv for lv, _ in FACTORY_LADDER],
                               [nt for _, nt in FACTORY_LADDER], origin="factory")

    # ── Gestión de riesgo editable en caliente ────────────────────────────────

    def set_risk(self, data: dict) -> dict:
        """Aplica (validando) los ajustes de riesgo recibidos de la web y los guarda.
        ValueError si algún valor es inválido (en ese caso no se aplica nada)."""
        if not isinstance(data, dict):
            raise ValueError("Cuerpo JSON inválido")
        before = asdict(RISK)
        changed = _apply_risk_dict(data, strict=True)
        if not changed:
            raise ValueError("No se recibió ningún ajuste de riesgo reconocido")
        if any(k.startswith("global_stop") for k in changed):
            self._gstop_rearm_at = 0.0          # un umbral nuevo se evalúa ya
        err = _save_settings()
        if err:
            self.log(f"No pude guardar ajustes: {err}")
        diffs = [f"{k}: {before[k]} → {v}" for k, v in changed.items() if before.get(k) != v]
        self.log("RIESGO actualizado — " + (", ".join(diffs) if diffs else "sin cambios"))
        self.persist_state()
        for sym in self._open_position_symbols():
            self._enqueue(sym)                  # reevalúa ya (p. ej. un DCA que la EMA frenaba)
        return {"risk": self.gate_view(full=False)}

    def set_pause(self, paused: bool, reason: str = "") -> dict:
        """Pausa / reanuda la apertura de posiciones NUEVAS (DCA/TP/SL siguen activos)."""
        paused = bool(paused)
        RISK.entries_paused = paused
        RISK.pause_reason   = (reason or "Pausa manual") if paused else ""
        RISK.paused_at      = time.time() if paused else 0.0
        err = _save_settings()
        if err:
            self.log(f"No pude guardar ajustes: {err}")
        if paused:
            self.log(f"⏸ ENTRADAS PAUSADAS — {RISK.pause_reason}")
        else:
            self.log("▶ ENTRADAS REANUDADAS")
        self.persist_state()
        return {"paused": RISK.entries_paused, "pause_reason": RISK.pause_reason}

    def _unrealized_total_locked(self) -> Tuple[float, int]:
        """(PnL no realizado total, nº de posiciones abiertas). Requiere self.lock."""
        total, n_open = 0.0, 0
        for sym, pos in self.positions.items():
            if pos.status != "OPEN" or not pos.fills:
                continue
            n_open += 1
            total += pos.unrealized_pnl(self._display_price(sym))
        return total, n_open

    def _check_global_stop(self) -> None:
        """Si el PnL no realizado total ≤ RISK.global_stop_usd, cierra TODO (una a una)
        y, si así está configurado, pausa las entradas nuevas. Corre en el loop del bot."""
        if not RISK.global_stop_enabled or self._close_all_active:
            return
        now = time.time()
        if now - self._gstop_last_check < 0.2 or now < self._gstop_rearm_at:
            return
        self._gstop_last_check = now
        with self.lock:
            total, n_open = self._unrealized_total_locked()
        if n_open == 0 or total > RISK.global_stop_usd:
            return
        threshold = RISK.global_stop_usd
        self._gstop_rearm_at = now + GLOBAL_STOP_REARM_S
        self.global_stop_triggers += 1
        self.global_stop_last = {
            "ts": now, "pnl": total, "threshold": threshold, "positions": n_open,
            "at": datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
        self.log(f"🛑 STOP GLOBAL: PnL no realizado {total:.4f} ≤ {threshold:.2f} USD "
                 f"→ cierre de {n_open} posición(es)")
        if RISK.global_stop_pause and not RISK.entries_paused:
            self.set_pause(True, f"Stop global disparado ({total:.2f} USD ≤ {threshold:.2f})")
        res = self.start_close_all(origin="GLOBAL")
        if not res.get("ok"):
            self.log(f"Stop global: no pude lanzar el cierre masivo: {res.get('error')}")

    def gate_view(self, full: bool = True) -> dict:
        """Estado de las guardas de entrada para la web."""
        with self.lock:
            exposure = sum(p.notional for p in self.positions.values()
                           if p.status == "OPEN" and p.fills)
            unreal, n_open = self._unrealized_total_locked()
        btc_chg, btc_fresh = self.btc_change()
        blk_long, blk_short, btc_reasons = self._btc_blocks()
        first_nt = ENTRY_LADDER[0][1] if ENTRY_LADDER else 0.0
        exp_blocks = bool(RISK.exposure_enabled
                          and exposure + first_nt > RISK.max_exposure_usd + 1e-9)
        common = RISK.entries_paused or exp_blocks or self._close_all_active
        view: Dict[str, Any] = {
            "paused":            RISK.entries_paused,
            "pause_reason":      RISK.pause_reason,
            "paused_at":         RISK.paused_at,
            "close_all_active":  self._close_all_active,
            "exposure":          exposure,
            "exposure_enabled":  RISK.exposure_enabled,
            "max_exposure":      RISK.max_exposure_usd,
            "exposure_include_dca": RISK.exposure_include_dca,
            "exposure_blocks":   exp_blocks,
            "btc_symbol":        BTC_FILTER_SYMBOL,
            "btc_enabled":       RISK.btc_filter_enabled,
            "btc_change":        btc_chg,
            "btc_fresh":         btc_fresh,
            "btc_threshold":     RISK.btc_filter_threshold,
            "btc_mode":          RISK.btc_filter_mode,
            "btc_up_enabled":    RISK.btc_up_enabled,
            "btc_up_threshold":  RISK.btc_up_threshold,
            "btc_up_mode":       RISK.btc_up_mode,
            "btc_blocks_long":   blk_long,
            "btc_blocks_short":  blk_short,
            "btc_reasons":       btc_reasons,
            "btc_reason":        "; ".join(btc_reasons),
            "dca_ema_enabled":   RISK.dca_ema_enabled,
            "dca_ema_period":    RISK.dca_ema_period,
            "dca_ema_after":     RISK.dca_ema_after,
            "gstop_enabled":     RISK.global_stop_enabled,
            "gstop_usd":         RISK.global_stop_usd,
            "gstop_pause":       RISK.global_stop_pause,
            "gstop_triggers":    self.global_stop_triggers,
            "gstop_last":        dict(self.global_stop_last),
            "unrealized":        unreal,
            "open_positions":    n_open,
            "can_open_long":     not (common or blk_long),
            "can_open_short":    not (common or blk_short),
        }
        with self._block_lock:
            view["blocked_counts"] = dict(self.blocked_counts)
            if full:
                view["blocked_recent"] = list(self.blocked_recent)
        return view

    # ── Cierre masivo, una operación tras otra ────────────────────────────────

    @staticmethod
    def _empty_close_all() -> Dict[str, Any]:
        return {"running": False, "cancel": False, "total": 0, "done": 0, "ok": 0,
                "failed": 0, "current": "", "failed_symbols": [],
                "started": 0.0, "finished": 0.0, "origin": ""}

    def close_all_view(self) -> dict:
        with self.lock:
            v = dict(self.close_all_state)
            v["failed_symbols"] = list(v.get("failed_symbols", []))
        return v

    def start_close_all(self, origin: str = "MANUAL") -> dict:
        """Lanza el cierre secuencial de TODAS las posiciones abiertas (desde Flask o
        desde el propio loop: stop global). Devuelve al instante; el progreso se lee
        en close_all_view(). origin: "MANUAL" | "GLOBAL"."""
        if not self.loop or not self.loop.is_running():
            return {"ok": False, "error": "Bot loop no está activo", "code": 503}
        with self.lock:
            if self.close_all_state.get("running"):
                return {"ok": False, "error": "Ya hay un cierre masivo en curso", "code": 409}
            n_open = sum(1 for p in self.positions.values() if p.status == "OPEN" and p.fills)
            if n_open == 0:
                return {"ok": False, "error": "No hay posiciones abiertas", "code": 404}
            self.close_all_state = self._empty_close_all()
            self.close_all_state.update(running=True, total=n_open, started=time.time(),
                                        origin=origin)
            self._close_all_active = True
        coro = self._close_all_sequential()
        if threading.current_thread() is self.thread:
            self._spawn(coro)                      # ya estamos en el loop del bot
        else:
            asyncio.run_coroutine_threadsafe(coro, self.loop)
        return {"ok": True, "total": n_open}

    def cancel_close_all(self) -> bool:
        with self.lock:
            if not self.close_all_state.get("running"):
                return False
            self.close_all_state["cancel"] = True
        self.log("Cierre masivo: cancelación solicitada (termina la operación en curso)")
        return True

    def _is_open(self, symbol: str) -> bool:
        with self.lock:
            pos = self.positions.get(symbol)
            return bool(pos and pos.status == "OPEN" and pos.fills)

    async def _close_one_for_bulk(self, symbol: str, reason: str = "MANUAL") -> bool:
        """Cierra UNA posición a mercado. Reintenta hasta 3 veces (p. ej. si un TP/SL
        la está cerrando justo ahora). True si la posición ya no está abierta."""
        for _ in range(3):
            if not self._is_open(symbol):
                return True
            price = self._display_price(symbol)
            if price > 0:
                try:
                    if await self._close_position(symbol, price, reason):
                        return True
                except Exception as exc:
                    self.last_error = str(exc)
                    self.log(f"Cierre masivo: error cerrando {symbol}: {exc}")
            await asyncio.sleep(0.6)
        return not self._is_open(symbol)

    async def _close_all_sequential(self) -> None:
        st = self.close_all_state
        ok_set: set = set()
        bad_set: set = set()
        attempts: Dict[str, int] = {}
        reason = "GLOBAL" if st.get("origin") == "GLOBAL" else "MANUAL"
        self.log(f"CIERRE MASIVO iniciado ({'stop global' if reason == 'GLOBAL' else 'manual'}): "
                 f"{st.get('total', 0)} posición(es), una a una")
        try:
            while not st["cancel"]:
                todo = [s for s in self._open_position_symbols() if attempts.get(s, 0) < 2]
                if not todo:
                    break
                with self.lock:
                    st["total"] = max(st["total"], len(ok_set | bad_set | set(todo)))
                for sym in todo:
                    if st["cancel"]:
                        break
                    attempts[sym] = attempts.get(sym, 0) + 1
                    with self.lock:
                        st["current"] = sym
                    closed = await self._close_one_for_bulk(sym, reason)
                    with self.lock:
                        if closed:
                            ok_set.add(sym); bad_set.discard(sym)
                        else:
                            bad_set.add(sym); ok_set.discard(sym)
                        st["ok"], st["failed"] = len(ok_set), len(bad_set)
                        st["done"] = len(ok_set) + len(bad_set)
                        st["failed_symbols"] = sorted(bad_set)
                    await asyncio.sleep(0.15)          # respiro entre órdenes
        except Exception as exc:
            self.last_error = str(exc)
            self.log(f"Cierre masivo: error inesperado: {exc!r}")
        finally:
            with self.lock:
                st["running"]  = False
                st["current"]  = ""
                st["finished"] = time.time()
                cancelled = st["cancel"]
                summary = (f"{st['ok']} cerrada(s), {st['failed']} con error"
                           + (f" ({', '.join(st['failed_symbols'])})" if st["failed_symbols"] else ""))
            self._close_all_active = False
            self.log(f"CIERRE MASIVO {'CANCELADO' if cancelled else 'terminado'}: {summary}")
            self.persist_state()

    # ── Mantenimiento (housekeeping; NO es la vía de detección) ───────────────

    async def _maintenance_loop(self) -> None:
        """Red de seguridad + limpieza del radar + métricas + guardado periódico.
        Las entradas y los TP/SL los dispara cada tick del WS, no este bucle."""
        step = SAFETY_TICK_SECS if SAFETY_TICK_SECS > 0 else 1.0
        step = max(0.2, min(step, 1.0))
        last_rate = last_prune = last_persist = time.time()
        last_count = self.scan_count
        while self.running:
            await asyncio.sleep(step)
            now = time.time()
            try:
                if SAFETY_TICK_SECS > 0:
                    for sym in set(self.watch) | set(self._open_position_symbols()):
                        self._enqueue(sym)

                # Stop global también aquí: cubre el caso de que no lleguen ticks
                self._check_global_stop()

                if now - last_rate >= 1.0:
                    dt = now - last_rate
                    self.eval_rate = (self.scan_count - last_count) / dt
                    if self._lat_n:
                        self.latency_avg_ms = self._lat_sum / self._lat_n
                        self.latency_max_ms = self._lat_max
                    self._lat_sum, self._lat_n, self._lat_max = 0.0, 0, 0.0
                    last_count, last_rate = self.scan_count, now

                if now - last_prune >= WATCH_PRUNE_SECS:
                    last_prune = now
                    self._prune_watch()
                    with self.lock:
                        self.symbol_cooldown = {s: ts for s, ts in self.symbol_cooldown.items() if ts > now}
                    self._entry_backoff = {s: ts for s, ts in self._entry_backoff.items() if ts > now}
                    self._close_backoff = {s: ts for s, ts in self._close_backoff.items() if ts > now}

                if now - last_persist >= STATE_PERSIST_SECS:
                    last_persist = now
                    self.persist_state()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                self._log_throttled("maint_err", f"Error en mantenimiento: {exc!r}")

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def _winners_now(self, pos_syms: set) -> List[dict]:
        """Radar ordenado por cambio en vivo: ≥ MIN_GAIN_FILTER o con posición."""
        rows = []
        for sym in self.watch:
            price  = self._display_price(sym)
            change = self._change_for(sym, price)
            if change >= MIN_GAIN_FILTER or sym in pos_syms:
                rows.append({
                    "symbol":    sym,
                    "change":    change,
                    "price":     price,
                    "market":    "futures",
                    "can_short": True,
                })
        rows.sort(key=lambda r: r["change"], reverse=True)
        return rows

    def live_payload(self) -> dict:
        """Payload MÍNIMO para el refresco del navegador (precio, cambio y PnL):
        sin eventos, sin cierres, sin rehacer tablas."""
        now = time.time()
        pos_out: Dict[str, dict] = {}
        total_unreal = 0.0
        total_notional = 0.0
        with self.lock:
            for sym, pos in self.positions.items():
                if pos.status != "OPEN" or not pos.fills:
                    continue
                price = self._display_price(sym)
                pnl   = pos.unrealized_pnl(price)
                total_unreal += pnl
                notional = pos.notional
                total_notional += notional
                pos_out[sym] = {
                    "p":   price,
                    "c":   self._change_for(sym, price),
                    "pnl": pnl,
                    "sl":  pos.sl_price(),
                    "slu": pos.sl_usd,
                    "tp":  notional * TAKE_PROFIT_FRACTION,
                    "mfe": max(pos.mfe_usd, pnl),
                    "mae": min(pos.mae_usd, pnl),
                }
        winners = [{"s": w["symbol"], "p": w["price"], "c": w["change"]}
                   for w in self._winners_now(set(pos_out))]
        return {
            "ts":               now,
            "positions":        pos_out,
            "winners":          winners,
            "total_unrealized": total_unreal,
            "total_notional":   total_notional,
            "gate":             self.gate_view(full=False),
            "scan_count":       self.scan_count,
            "eval_rate":        self.eval_rate,
            "latency_avg_ms":   self.latency_avg_ms,
            "latency_max_ms":   self.latency_max_ms,
            "watch_count":      len(self.watch),
            "close_all":        self.close_all_view(),
        }

    def _build_snapshot(self) -> dict:
        ws_stats: dict = {}
        kl_stats: dict = {}
        try:
            if self.price_cache:
                ws_stats = self.price_cache.get_stats()
        except Exception:
            pass
        try:
            if self.kline_cache:
                kl_stats = self.kline_cache.get_stats()
        except Exception:
            pass

        with self.lock:
            positions_raw      = dict(self.positions)
            closed             = list(self.closed_trades[:130])
            events             = list(self.events[:50])
            cooldown_snap      = dict(self.symbol_cooldown)
            price_blocked_snap = set(self.price_blocked)
            all_symbols_count  = len(self.all_symbols)
            total_realized_pnl = self.total_realized_pnl

        now = time.time()
        pos_syms = {s for s, p in positions_raw.items() if p.status == "OPEN" and p.fills}
        winners_raw = self._winners_now(pos_syms)
        change_by_sym = {w["symbol"]: w["change"] for w in winners_raw}

        winners_out = []
        for w in winners_raw:
            sym       = w["symbol"]
            remaining = max(0.0, cooldown_snap.get(sym, 0.0) - now)
            winners_out.append({
                **w,
                "cooldown_remaining": remaining,
                "cooldown_str":       self._fmt_cooldown(remaining) if remaining > 0 else "",
                "price_blocked":      sym in price_blocked_snap,
            })

        open_positions = []
        total_unreal   = 0.0
        total_notional = 0.0
        for symbol, pos in positions_raw.items():
            if pos.status != "OPEN" or not pos.fills:
                continue
            price = self._display_price(symbol)
            pnl   = pos.unrealized_pnl(price)
            total_unreal   += pnl
            total_notional += pos.notional
            # Short SL: (avg_entry - sl_price) * qty = sl_usd → sl_price = avg_entry - sl_usd/qty
            sl_price = pos.sl_price()
            # Próximo tramo DCA y estado de la condición EMA (informativo para la web)
            ladder = ENTRY_LADDER
            nxt = len(pos.fills)
            ema_val = (self._trend_ema(symbol, RISK.dca_ema_period)
                       if RISK.dca_ema_enabled else None)
            ema_applies = bool(RISK.dca_ema_enabled and nxt < len(ladder)
                               and nxt >= RISK.dca_ema_after)
            ema_ok = None
            if ema_val is not None and price > 0:
                ema_ok = price > ema_val if pos.direction == "LONG" else price < ema_val
            dca_next = {
                "idx":         nxt,
                "level":       ladder[nxt][0] if nxt < len(ladder) else None,
                "notional":    ladder[nxt][1] if nxt < len(ladder) else None,
                "adverse":     pos.adverse_pct(price),
                "ema_period":  RISK.dca_ema_period,
                "ema":         ema_val,
                "ema_applies": ema_applies,
                "ema_ok":      ema_ok,
            }
            open_positions.append({
                "symbol":          symbol,
                "direction":       pos.direction,
                "mark_price":      price,
                "avg_entry":       pos.avg_entry,
                "qty":             pos.qty,
                "notional":        pos.notional,
                "target":          pos.notional * TAKE_PROFIT_FRACTION,
                "unrealized_pnl":  pnl,
                "stop_loss_price": sl_price,
                "stop_loss_usd":   pos.sl_usd,
                "sl_mode":         pos.sl_mode,
                "trade_id":        pos.trade_id,
                "mfe_usd":         max(pos.mfe_usd, pnl),
                "mae_usd":         min(pos.mae_usd, pnl),
                "mfe_pct":         pos.mfe_pct,
                "mae_pct":         pos.mae_pct,
                "opened_ts":       pos.opened_ts,
                "fills":           [f.__dict__ for f in pos.fills],
                "change":          change_by_sym.get(symbol, self._change_for(symbol, price)),
                "dca_next":        dca_next,
            })

        active_cooldowns = {
            sym: {
                "remaining_s":   round(ts - now, 0),
                "remaining_str": self._fmt_cooldown(ts - now),
                "unblock_utc":   datetime.fromtimestamp(ts, timezone.utc).strftime(
                    "%Y-%m-%d %H:%M UTC"
                ),
            }
            for sym, ts in cooldown_snap.items() if ts > now
        }

        last_scan_text = (
            datetime.fromtimestamp(self.last_scan_at, timezone.utc)
            .strftime("%Y-%m-%d %H:%M:%S UTC")
            if self.last_scan_at else "pendiente"
        )
        watch = sorted(self.watch)

        return {
            "mode":              "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL",
            "running":           self.running,
            "thread_alive":      bool(self.thread and self.thread.is_alive()),
            "started_at":        self.started_at,
            "uptime_seconds":    round(now - self.started_at, 1),
            "scan_count":        self.scan_count,
            "last_scan_text":    last_scan_text,
            # Motor por eventos
            "eval_rate":         self.eval_rate,
            "latency_avg_ms":    self.latency_avg_ms,
            "latency_max_ms":    self.latency_max_ms,
            "live_poll_ms":      LIVE_POLL_MS,
            "status_poll_ms":    STATUS_POLL_MS,
            "min_gain_filter":   MIN_GAIN_FILTER,
            "safety_tick_secs":  SAFETY_TICK_SECS,
            # Símbolos / radar
            "all_symbols_count": all_symbols_count,
            "universe_count":    ws_stats.get("active_tickers", 0),
            "subscribed_count":  len(watch),
            "subscribed_symbols": watch,
            "ws_connected":      bool(ws_stats.get("connected", False)),
            "book_ticks":        ws_stats.get("book_ticks", 0),
            # Resto
            "last_error":        self.last_error,
            "last_startup_err":  self.last_startup_err,
            "exchange_symbols":  self.exchange_symbols,
            "entry_levels":      [lv for lv, _ in ENTRY_LADDER],
            "entry_notionals":   [nt for _, nt in ENTRY_LADDER],
            "ladder":            self.ladder_view(),
            "take_profit_fraction": TAKE_PROFIT_FRACTION,
            "tp_min":            TP_MIN,
            "tp_max":            TP_MAX,
            "ema_fast":          EMA_FAST,
            "ema_slow":          EMA_SLOW,
            "ema_interval":      EMA_INTERVAL,
            "ema_max_period":    EMA_MAX_PERIOD,
            "ema_max_candles":   EMA_MAX_CANDLES,
            "close_all":         self.close_all_view(),
            "gate":              self.gate_view(full=True),
            "take_profit_pct":   TAKE_PROFIT_FRACTION * 100,
            "default_stop_loss_usd":      DEFAULT_STOP_LOSS_USD,
            "first_tranche_sl_fraction":  FIRST_TRANCHE_SL_FRACTION,
            "total_unrealized":   total_unreal,
            "total_realized_pnl": total_realized_pnl,
            "total_notional":    total_notional,
            "executor_url":      EXECUTOR_URL or "",
            "positions":         open_positions,
            "winners":           winners_out,
            "closed_trades":     closed,
            "events":            events,
            "cooldown_count":    len(active_cooldowns),
            "cooldowns":         active_cooldowns,
            "cooldown_hours":    COOLDOWN_SECONDS / 3600,
            "price_blocked":     sorted(price_blocked_snap),
            "price_blocked_count": len(price_blocked_snap),
            "max_price_block":   MAX_PRICE_BLOCK,
            "price_ws": {
                "active_prices":  ws_stats.get("active_prices",  0),
                "active_tickers": ws_stats.get("active_tickers", 0),
                "total":          ws_stats.get("total_symbols",  0),
                "stale":          ws_stats.get("stale_symbols",  0),
            },
            "kline_ws": {
                "pairs_with_data": kl_stats.get("ready_symbols", 0),
                "tracked":         kl_stats.get("tracked_symbols", 0),
                "stored_candles":  kl_stats.get("stored_candles", 0),
                "total_messages":  kl_stats.get("closed_candles", 0),
                "active_conns":    int(bool(kl_stats.get("connected", False))),
                "signals":         kl_stats.get("signals", 0),
                "stale_signals":   kl_stats.get("stale_signals", 0),
                "warming":         kl_stats.get("warming", 0),
            },
            "ts": now,
        }


    def _write_state_file(self, snap: dict) -> None:
        '''Escribe el estado en disco de forma atómica.'''
        if not snap["positions"] and not snap["closed_trades"] and snap["scan_count"] <= 0:
            return

        tmp = f"{STATE_FILE}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(snap, fh, ensure_ascii=False, default=str)
            os.replace(tmp, STATE_FILE)
        except Exception as exc:
            self.log(f"No pude persistir estado: {exc}")

    async def _persist_state_loop(self) -> None:
        '''Flusher en segundo plano para persistir estado sin bloquear el loop.'''
        if self._persist_event is None:
            self._persist_event = asyncio.Event()

        while self.running:
            try:
                await self._persist_event.wait()
                self._persist_event.clear()

                # Debounce: agrupa ráfagas de cambios consecutivos.
                try:
                    await asyncio.sleep(self._persist_debounce_secs)
                except asyncio.CancelledError:
                    if not self.running:
                        break
                    raise

                while self._persist_event.is_set():
                    self._persist_event.clear()
                    try:
                        await asyncio.sleep(self._persist_debounce_secs)
                    except asyncio.CancelledError:
                        if not self.running:
                            break
                        raise

                if not self.running:
                    break

                snap = self._build_snapshot()
                if not snap["positions"] and not snap["closed_trades"] and snap["scan_count"] <= 0:
                    continue

                await asyncio.to_thread(self._write_state_file, snap)

            except asyncio.CancelledError:
                if not self.running:
                    break
            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"No pude persistir estado en background: {exc}")
                try:
                    await asyncio.sleep(1.0)
                except asyncio.CancelledError:
                    if not self.running:
                        break
                    raise

    # ── Persistencia ──────────────────────────────────────────────────────────

    def persist_state(self) -> None:
        """Solicita persistencia asíncrona del estado sin bloquear el loop."""
        if self._persist_event is None:
            snap = self._build_snapshot()
            self._write_state_file(snap)
            return

        try:
            if self.loop and self.loop.is_running():
                self.loop.call_soon_threadsafe(self._persist_event.set)
            else:
                self._persist_event.set()
        except Exception:
            snap = self._build_snapshot()
            self._write_state_file(snap)

    def snapshot(self) -> dict:
        # Siempre el estado VIVO: el historial ya se restauró al arrancar
        # (_restore_history). Antes, tras un reinicio sin posiciones se servía el
        # archivo guardado entero, con posiciones fantasma y ajustes viejos.
        live = self._build_snapshot()
        live["state_source"] = "memory"
        return live


# ─────────────────────────────────────────────────────────────────────────────
# FLASK APP
# ─────────────────────────────────────────────────────────────────────────────

bot = TradingBot()
bot.start()

app = Flask(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HTML + JS
# ─────────────────────────────────────────────────────────────────────────────

HTML = r"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <meta name="theme-color" content="#131a24">
  <title>Bot Short</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Barlow:wght@400;500;600;700&family=Barlow+Semi+Condensed:wght@500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --ink: #131a24; --panel: #1a2230; --panel2: #202a3a; --rule: #2a3444; --rule2: #35415a;
      --txt: #e4e9f1; --muted: #8994a6; --dim: #5f6b80;
      --blue: #6a9cff; --blue-d: #3f6fd6;
      --ok: #3dd68c; --warn: #f2b33d; --bad: #f0605d; --off: #4a5468;
      --long: #3dd68c; --short: #f07c5d;
      --r-lg: 12px; --r-md: 8px; --r-sm: 6px;
      --font: "Barlow", system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
      --font-c: "Barlow Semi Condensed", "Barlow", system-ui, sans-serif;
    }
    * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
    html { scroll-behavior: smooth; }
    @media (prefers-reduced-motion: reduce) { html { scroll-behavior: auto; } * { transition: none !important; animation: none !important; } }
    body {
      margin: 0; background: var(--ink); color: var(--txt);
      font: 15px/1.45 var(--font); font-variant-numeric: tabular-nums;
      padding-bottom: env(safe-area-inset-bottom);
    }
    a { color: var(--blue); }
    button, input, select { font: inherit; color: inherit; }
    :focus-visible { outline: 2px solid var(--blue); outline-offset: 2px; }
    .pos { color: var(--ok); } .neg { color: var(--bad); } .warn-t { color: var(--warn); } .muted { color: var(--muted); }
    .num { font-family: var(--font-c); font-weight: 600; }

    /* ── Barra superior ───────────────────────────────────────────── */
    .top {
      position: sticky; top: 0; z-index: 40; background: rgba(19,26,36,.94);
      backdrop-filter: blur(10px); -webkit-backdrop-filter: blur(10px);
      border-bottom: 1px solid var(--rule); padding: calc(8px + env(safe-area-inset-top)) 20px 0;
    }
    .top-row { display: flex; align-items: center; gap: 14px; max-width: 1320px; margin: 0 auto; min-height: 44px; flex-wrap: wrap; }
    .brand { font-family: var(--font-c); font-weight: 700; font-size: 21px; letter-spacing: .2px; }
    .brand span { color: var(--muted); font-weight: 500; font-size: 15px; margin-left: 8px; }
    .mode { font-family: var(--font-c); font-weight: 700; font-size: 13px; padding: 2px 10px; border-radius: 999px; border: 1px solid; }
    .mode.paper { color: var(--warn); border-color: rgba(242,179,61,.5); }
    .mode.real { color: var(--bad); border-color: rgba(240,96,93,.6); background: rgba(240,96,93,.08); }
    .grow { flex: 1; }
    .conn { display: flex; gap: 14px; font-size: 13px; color: var(--muted); align-items: center; }
    .conn span { display: inline-flex; align-items: center; gap: 6px; }
    .tabs { display: flex; gap: 2px; max-width: 1320px; margin: 4px auto 0; overflow-x: auto; scrollbar-width: none; }
    .tabs::-webkit-scrollbar { display: none; }
    .tabs a { flex: 0 0 auto; color: var(--muted); text-decoration: none; font-size: 14px; font-weight: 500; padding: 8px 12px; border-bottom: 2px solid transparent; }
    .tabs a:hover { color: var(--txt); border-bottom-color: var(--rule2); }

    /* ── Lámparas ─────────────────────────────────────────────────── */
    .lamp { width: 10px; height: 10px; border-radius: 50%; background: var(--off); flex: 0 0 auto; display: inline-block; }
    .lamp.ok   { background: var(--ok);   box-shadow: 0 0 0 3px rgba(61,214,140,.16), 0 0 10px rgba(61,214,140,.55); }
    .lamp.warn { background: var(--warn); box-shadow: 0 0 0 3px rgba(242,179,61,.16), 0 0 10px rgba(242,179,61,.5); }
    .lamp.bad  { background: var(--bad);  box-shadow: 0 0 0 3px rgba(240,96,93,.18), 0 0 12px rgba(240,96,93,.6); }
    .lamp.off  { background: var(--off); }
    .lamp.blink { animation: blink 1.2s steps(2, start) infinite; }
    @keyframes blink { to { opacity: .35; } }

    /* ── Estructura ───────────────────────────────────────────────── */
    main { max-width: 1320px; margin: 0 auto; padding: 20px; display: grid; gap: 20px; }
    .anchor { scroll-margin-top: 104px; }

    /* Lectura principal + interlocks */
    .hero { display: grid; grid-template-columns: minmax(250px, 1fr) 3fr; gap: 0; border: 1px solid var(--rule); border-radius: var(--r-lg); background: var(--panel); overflow: hidden; }
    .readout { padding: 18px 20px; border-right: 1px solid var(--rule); display: flex; flex-direction: column; gap: 14px; }
    .readout .big { font-family: var(--font-c); font-weight: 700; font-size: 44px; line-height: 1; letter-spacing: -.5px; }
    .readout .cap { color: var(--muted); font-size: 14px; margin-bottom: 4px; }
    .readout dl { margin: 0; display: grid; grid-template-columns: 1fr 1fr; gap: 10px 16px; }
    .readout dt { color: var(--muted); font-size: 13px; }
    .readout dd { margin: 0; font-family: var(--font-c); font-weight: 600; font-size: 19px; }

    .interlocks { display: grid; grid-template-columns: repeat(4, 1fr); }
    .il { position: relative; padding: 16px 18px 18px; border-left: 1px solid var(--rule); display: flex; flex-direction: column; gap: 6px; text-align: left; background: none; border-top: 0; border-right: 0; border-bottom: 0; cursor: pointer; min-width: 0; }
    .il:first-child { border-left: 0; }
    .il:hover { background: rgba(255,255,255,.025); }
    .il-head { display: flex; align-items: center; gap: 9px; font-size: 14px; color: var(--muted); font-weight: 500; }
    .il-val { font-family: var(--font-c); font-weight: 700; font-size: 24px; line-height: 1.15; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .il-val small { font-size: 14px; color: var(--muted); font-weight: 500; margin-left: 4px; }
    .il-sub { font-size: 13px; color: var(--muted); min-height: 19px; }
    .il-bar { height: 4px; border-radius: 2px; background: var(--rule); overflow: hidden; margin-top: 2px; }
    .il-bar i { display: block; height: 100%; width: 0; background: var(--ok); transition: width .3s, background .3s; }
    .il[data-state="warn"] .il-bar i { background: var(--warn); }
    .il[data-state="bad"]  .il-bar i { background: var(--bad); }
    .il[data-state="bad"]::after { content: ""; position: absolute; left: 0; right: 0; bottom: 0; height: 2px; background: var(--bad); }
    .il[data-state="warn"]::after { content: ""; position: absolute; left: 0; right: 0; bottom: 0; height: 2px; background: var(--warn); }
    .sides { display: flex; gap: 6px; margin-top: 2px; }
    .side-ok, .side-no { font-family: var(--font-c); font-weight: 700; font-size: 12px; padding: 1px 7px; border-radius: var(--r-sm); border: 1px solid; }
    .side-ok { color: var(--ok); border-color: rgba(61,214,140,.4); }
    .side-no { color: var(--dim); border-color: var(--rule2); text-decoration: line-through; }

    .alert { border: 1px solid rgba(240,96,93,.5); background: rgba(240,96,93,.07); border-radius: var(--r-md); padding: 10px 14px; font-size: 14px; display: none; gap: 10px; align-items: flex-start; }
    .alert pre { margin: 0; white-space: pre-wrap; font: 13px/1.5 var(--font); color: #f6b4b2; }

    .cols { display: grid; grid-template-columns: minmax(0, 1fr) 400px; gap: 20px; align-items: start; }
    .panel { border: 1px solid var(--rule); border-radius: var(--r-lg); background: var(--panel); }
    .panel-head { display: flex; align-items: center; gap: 10px; padding: 14px 18px; border-bottom: 1px solid var(--rule); flex-wrap: wrap; }
    .panel-head h2 { margin: 0; font-family: var(--font-c); font-size: 19px; font-weight: 700; }
    .count { font-family: var(--font-c); font-weight: 600; color: var(--muted); font-size: 15px; }
    .link-btn { background: none; border: 0; color: var(--blue); cursor: pointer; font-size: 14px; padding: 4px 6px; border-radius: var(--r-sm); }
    .link-btn:hover { background: rgba(106,156,255,.1); }

    /* ── Posiciones (filas desplegables) ──────────────────────────── */
    .poslist details { border-bottom: 1px solid var(--rule); }
    .poslist details:last-child { border-bottom: 0; }
    .poslist summary {
      list-style: none; cursor: pointer; display: grid; align-items: center; gap: 14px;
      grid-template-columns: 56px minmax(0, 1fr) 72px 150px 92px 14px; padding: 13px 18px;
    }
    .poslist summary::-webkit-details-marker, .acc > summary::-webkit-details-marker { display: none; }
    .poslist summary:hover { background: rgba(255,255,255,.02); }
    .side { font-family: var(--font-c); font-weight: 700; font-size: 13px; text-align: center; padding: 2px 0; border-radius: var(--r-sm); }
    .side.long  { color: var(--long);  background: rgba(61,214,140,.1); }
    .side.short { color: var(--short); background: rgba(240,124,93,.12); }
    .pos-id { min-width: 0; display: flex; flex-direction: column; }
    .pos-id b { font-family: var(--font-c); font-size: 17px; font-weight: 700; }
    .pos-id small { color: var(--muted); font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .chg { font-family: var(--font-c); font-weight: 600; font-size: 15px; text-align: right; }
    .pnl { font-family: var(--font-c); font-weight: 700; font-size: 20px; text-align: right; }
    .range { position: relative; height: 6px; border-radius: 3px; background: linear-gradient(90deg, rgba(240,96,93,.55), rgba(240,96,93,.12) 45%, rgba(61,214,140,.12) 55%, rgba(61,214,140,.55)); }
    .range .zero { position: absolute; top: -3px; width: 1px; height: 12px; background: var(--rule2); left: 50%; }
    .range .mark { position: absolute; top: -4px; width: 4px; height: 14px; margin-left: -2px; border-radius: 2px; background: var(--txt); left: 50%; transition: left .25s; }
    .chev { width: 14px; height: 14px; position: relative; }
    .chev::before { content: ""; position: absolute; inset: 3px; border-right: 2px solid var(--muted); border-bottom: 2px solid var(--muted); transform: rotate(-45deg); transition: transform .2s; }
    details[open] > summary .chev::before { transform: rotate(45deg); }
    .pos-body { padding: 4px 18px 18px 88px; display: grid; gap: 14px; }
    .kv { margin: 0; display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 0; border: 1px solid var(--rule); border-radius: var(--r-md); overflow: hidden; }
    .kv > div { padding: 9px 12px; border-right: 1px solid var(--rule); border-bottom: 1px solid var(--rule); margin: 0 -1px -1px 0; }
    .kv dt { color: var(--muted); font-size: 12.5px; }
    .kv dd { margin: 2px 0 0; font-family: var(--font-c); font-weight: 600; font-size: 16px; }
    .kv dd small { font-family: var(--font); font-weight: 400; color: var(--muted); font-size: 12.5px; }
    .actions { display: flex; gap: 8px; flex-wrap: wrap; }
    .empty { padding: 28px 18px; color: var(--muted); text-align: center; }
    .empty b { display: block; color: var(--txt); font-weight: 600; margin-bottom: 2px; }

    /* ── Tablas ───────────────────────────────────────────────────── */
    .tablewrap { overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { padding: 9px 14px; text-align: right; white-space: nowrap; border-bottom: 1px solid var(--rule); }
    th:first-child, td:first-child { text-align: left; }
    th { color: var(--muted); font-weight: 500; font-size: 13px; background: var(--panel2); position: sticky; top: 0; }
    tbody tr:last-child td { border-bottom: 0; }
    td.sym { font-family: var(--font-c); font-weight: 700; font-size: 15px; }
    table.fills { border: 1px solid var(--rule); border-radius: var(--r-md); overflow: hidden; border-collapse: separate; border-spacing: 0; }
    .tag { font-family: var(--font-c); font-weight: 700; font-size: 12.5px; padding: 1px 8px; border-radius: var(--r-sm); }
    .tag.tp { color: var(--ok); background: rgba(61,214,140,.1); }
    .tag.sl { color: var(--bad); background: rgba(240,96,93,.1); }
    .tag.manual { color: var(--warn); background: rgba(242,179,61,.1); }
    .tag.global { color: #fff; background: rgba(240,96,93,.55); }

    /* ── Botones y formularios ────────────────────────────────────── */
    .btn { appearance: none; border: 1px solid var(--rule2); background: var(--panel2); color: var(--txt); border-radius: var(--r-md); padding: 9px 14px; min-height: 40px; font-weight: 600; font-size: 14px; cursor: pointer; text-decoration: none; display: inline-flex; align-items: center; justify-content: center; gap: 8px; }
    .btn:hover { border-color: var(--muted); }
    .btn:disabled { opacity: .45; cursor: not-allowed; }
    .btn.primary { background: var(--blue-d); border-color: var(--blue-d); color: #fff; }
    .btn.primary:hover { background: #4a7be3; }
    .btn.danger { color: #ffb3b1; border-color: rgba(240,96,93,.55); background: rgba(240,96,93,.08); }
    .btn.danger:hover { background: rgba(240,96,93,.16); }
    .btn.amber { color: #ffd88f; border-color: rgba(242,179,61,.55); background: rgba(242,179,61,.08); }
    .btn.green { color: #9ef0c6; border-color: rgba(61,214,140,.55); background: rgba(61,214,140,.08); }
    .btn.block { width: 100%; }

    .ctl { padding: 16px 18px; display: grid; gap: 12px; }
    .ctl + .ctl { border-top: 1px solid var(--rule); }
    .ctl-title { display: flex; align-items: center; gap: 9px; font-weight: 600; }
    .ctl p { margin: 0; color: var(--muted); font-size: 13.5px; }
    .progress { height: 6px; border-radius: 3px; background: var(--rule); overflow: hidden; display: none; }
    .progress i { display: block; height: 100%; width: 0; background: var(--bad); transition: width .25s; }
    .msg { font-size: 13.5px; min-height: 0; }

    .acc { border-top: 1px solid var(--rule); }
    .acc > summary { list-style: none; cursor: pointer; display: flex; align-items: center; gap: 12px; padding: 13px 18px; }
    .acc > summary:hover { background: rgba(255,255,255,.02); }
    .acc-name { font-weight: 600; flex: 1; }
    .acc-val { font-family: var(--font-c); font-weight: 600; color: var(--muted); font-size: 15px; white-space: nowrap; }
    .acc-val.on { color: var(--txt); }
    .acc-body { padding: 2px 18px 18px; display: grid; gap: 14px; }
    .acc-body p.hint { margin: 0; color: var(--muted); font-size: 13.5px; }
    .field { display: grid; gap: 5px; }
    .field label { font-size: 13.5px; color: var(--muted); }
    .field-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    input[type=number], select {
      width: 100%; background: var(--ink); border: 1px solid var(--rule2); border-radius: var(--r-md);
      padding: 10px 12px; font-size: 16px; font-family: var(--font-c); font-weight: 600; outline: none; min-height: 42px;
    }
    select { font-family: var(--font); font-weight: 500; font-size: 15px; }
    input[type=number]:focus, select:focus { border-color: var(--blue); }
    input[data-dirty] { border-color: var(--warn); }
    .switch { display: flex; align-items: center; gap: 12px; cursor: pointer; font-size: 14.5px; position: relative; }
    .switch input { position: absolute; opacity: 0; width: 1px; height: 1px; }
    .switch .track { width: 40px; height: 22px; border-radius: 11px; background: var(--rule2); position: relative; flex: 0 0 auto; transition: background .2s; }
    .switch .track::after { content: ""; position: absolute; top: 3px; left: 3px; width: 16px; height: 16px; border-radius: 50%; background: #c6cfdd; transition: transform .2s, background .2s; }
    .switch input:checked + .track { background: var(--blue-d); }
    .switch input:checked + .track::after { transform: translateX(18px); background: #fff; }
    .switch input:focus-visible + .track { outline: 2px solid var(--blue); outline-offset: 2px; }
    .switch input[data-dirty] + .track { box-shadow: 0 0 0 2px var(--warn); }
    .chips { display: flex; flex-wrap: wrap; gap: 6px; }
    .chip { font-size: 13px; padding: 3px 9px; border-radius: var(--r-sm); background: var(--ink); border: 1px solid var(--rule); color: var(--muted); }
    .chip b { color: var(--txt); font-family: var(--font-c); font-weight: 600; }
    .sr { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; }
    table.ladder th, table.ladder td { padding: 4px 8px; }
    table.ladder th:last-child, table.ladder td:last-child { padding: 2px 4px; width: 40px; }
    table.ladder td:nth-child(2), table.ladder td:nth-child(3) { padding: 3px 2px; }
    table.ladder input.cell {
      width: 100%; min-width: 58px; max-width: 92px; min-height: 36px; margin-left: auto; display: block;
      background: transparent; border: 1px solid transparent; border-radius: var(--r-sm);
      padding: 6px 8px; text-align: right; font-size: 16px; -moz-appearance: textfield; appearance: textfield;
    }
    table.ladder input.cell::-webkit-inner-spin-button, table.ladder input.cell::-webkit-outer-spin-button { -webkit-appearance: none; margin: 0; }
    table.ladder input.cell:hover { border-color: var(--rule2); }
    table.ladder input.cell:focus { border-color: var(--blue); background: var(--ink); }
    table.ladder input.cell[data-dirty] { border-color: var(--warn); }
    table.ladder input.cell.invalid { border-color: var(--bad); background: rgba(240,96,93,.08); }
    .icon-btn { background: none; border: 1px solid transparent; color: var(--muted); width: 32px; height: 32px; border-radius: var(--r-sm); cursor: pointer; font-size: 19px; line-height: 1; }
    .icon-btn:hover { color: var(--bad); border-color: rgba(240,96,93,.5); }
    .ema-tag { margin-left: 6px; font-family: var(--font-c); font-weight: 700; font-size: 11px; color: var(--blue); border: 1px solid rgba(106,156,255,.45); border-radius: 4px; padding: 0 4px; }
    .ema-tag:empty { display: none; }
    .factory-row { display: flex; gap: 12px; align-items: center; justify-content: space-between; flex-wrap: wrap; padding-top: 12px; border-top: 1px solid var(--rule); font-size: 13px; }
    .factory-row span { flex: 1 1 180px; }
    .dca-next { font-size: 14px; color: var(--muted); line-height: 1.5; }
    .dca-next b { font-family: var(--font-c); font-weight: 600; color: var(--txt); }
    .dca-next b.pos { color: var(--ok); } .dca-next b.neg { color: var(--bad); }
    .filters { display: flex; gap: 4px; flex-wrap: wrap; }
    .filters button { background: none; border: 1px solid var(--rule2); color: var(--muted); border-radius: 999px; padding: 4px 12px; font-size: 13.5px; cursor: pointer; }
    .filters button[aria-pressed="true"] { color: var(--txt); border-color: var(--blue); background: rgba(106,156,255,.12); }

    /* ── Secciones desplegables de ancho completo ─────────────────── */
    details.section { border: 1px solid var(--rule); border-radius: var(--r-lg); background: var(--panel); }
    details.section > summary { list-style: none; cursor: pointer; display: flex; align-items: center; gap: 12px; padding: 15px 18px; }
    details.section > summary::-webkit-details-marker { display: none; }
    details.section > summary h2 { margin: 0; font-family: var(--font-c); font-size: 19px; font-weight: 700; }
    details.section > summary .sum { color: var(--muted); font-size: 14px; flex: 1; }
    details.section[open] > summary { border-bottom: 1px solid var(--rule); }
    .section-body { padding: 16px 18px; display: grid; gap: 16px; }
    .section-body.flush { padding: 0; }
    .statgrid { display: grid; grid-template-columns: repeat(4, 1fr); border: 1px solid var(--rule); border-radius: var(--r-md); overflow: hidden; }
    .statgrid > div { padding: 12px 14px; border-left: 1px solid var(--rule); }
    .statgrid > div:first-child { border-left: 0; }
    .statgrid span { display: block; color: var(--muted); font-size: 13px; }
    .statgrid b { font-family: var(--font-c); font-size: 24px; font-weight: 700; }
    .twocol { display: grid; grid-template-columns: 1.7fr 1fr; gap: 16px; }
    .sub-h { font-size: 14px; color: var(--muted); margin: 0 0 8px; font-weight: 500; }
    .boxed { border: 1px solid var(--rule); border-radius: var(--r-md); overflow: hidden; }
    .diag { display: grid; grid-template-columns: repeat(auto-fill, minmax(170px, 1fr)); border: 1px solid var(--rule); border-radius: var(--r-md); overflow: hidden; }
    .diag > div { padding: 10px 14px; border-right: 1px solid var(--rule); border-bottom: 1px solid var(--rule); margin: 0 -1px -1px 0; }
    .diag span { display: block; color: var(--muted); font-size: 12.5px; }
    .diag b { font-family: var(--font-c); font-weight: 600; font-size: 17px; }
    .log { list-style: none; margin: 0; padding: 0; max-height: 360px; overflow: auto; font-size: 13.5px; border: 1px solid var(--rule); border-radius: var(--r-md); }
    .log li { padding: 6px 12px; border-bottom: 1px solid var(--rule); color: #c3cbd8; display: grid; grid-template-columns: 70px 1fr; gap: 10px; }
    .log li:last-child { border-bottom: 0; }
    .log time { color: var(--dim); font-family: var(--font-c); }
    .log li.bad { color: #ffb3b1; } .log li.good { color: #9ef0c6; } .log li.warn { color: #ffd88f; }
    .blk-list { list-style: none; margin: 0; padding: 0; }
    .blk-list li { display: grid; grid-template-columns: 56px 56px minmax(90px, auto) 1fr; gap: 12px; align-items: baseline; padding: 9px 14px; border-bottom: 1px solid var(--rule); font-size: 14px; }
    .blk-list li:last-child { border-bottom: 0; }
    .blk-list time { color: var(--dim); font-family: var(--font-c); }
    .blk-list .why { color: var(--muted); }

    /* ── Modal y avisos ───────────────────────────────────────────── */
    .overlay { display: none; position: fixed; inset: 0; background: rgba(5,8,12,.7); z-index: 100; align-items: center; justify-content: center; padding: 16px; }
    .modal { width: 380px; max-width: 100%; background: var(--panel); border: 1px solid var(--rule2); border-radius: var(--r-lg); padding: 20px; display: grid; gap: 14px; }
    .modal h3 { margin: 0; font-family: var(--font-c); font-size: 20px; }
    .toasts { position: fixed; right: 16px; bottom: calc(16px + env(safe-area-inset-bottom)); z-index: 120; display: grid; gap: 8px; max-width: min(420px, calc(100vw - 32px)); }
    .toast { background: var(--panel2); border: 1px solid var(--rule2); border-left: 3px solid var(--blue); border-radius: var(--r-md); padding: 10px 14px; font-size: 14px; box-shadow: 0 8px 24px rgba(0,0,0,.35); transition: opacity .3s, transform .3s; }
    .toast.ok { border-left-color: var(--ok); } .toast.bad { border-left-color: var(--bad); }
    .toast.out { opacity: 0; transform: translateY(6px); }

    /* ── Responsive ───────────────────────────────────────────────── */
    @media (max-width: 1100px) {
      .cols { grid-template-columns: 1fr; }
      .hero { grid-template-columns: 1fr; }
      .readout { border-right: 0; border-bottom: 1px solid var(--rule); }
    }
    @media (max-width: 860px) {
      .interlocks { grid-template-columns: 1fr 1fr; }
      .il:nth-child(odd) { border-left: 0; }
      .il:nth-child(n+3) { border-top: 1px solid var(--rule); }
      .statgrid { grid-template-columns: 1fr 1fr; }
      .statgrid > div:nth-child(3) { border-left: 0; }
      .statgrid > div:nth-child(n+3) { border-top: 1px solid var(--rule); }
      .twocol { grid-template-columns: 1fr; }
    }
    @media (max-width: 640px) {
      .top { padding-left: 14px; padding-right: 14px; }
      .brand span, .conn .lbl { display: none; }
      main { padding: 14px; gap: 14px; }
      .readout .big { font-size: 38px; }
      .il { padding: 13px 14px 15px; }
      .il-val { font-size: 20px; }
      .poslist summary { grid-template-columns: 50px minmax(0, 1fr) auto 14px; gap: 10px; padding: 12px 14px; }
      .poslist summary .range, .poslist summary .chg { display: none; }
      .pnl { font-size: 18px; }
      .pos-body { padding: 4px 14px 16px; }
      .field-row { grid-template-columns: 1fr; }
      table.ladder th, table.ladder td { padding: 4px 5px; }
      table.ladder input.cell { min-width: 50px; padding: 6px 5px; }
      .blk-list li { grid-template-columns: 48px 1fr; }
      .blk-list li .why { grid-column: 1 / -1; }
      table.rt thead { display: none; }
      table.rt, .rt tbody, .rt tr, .rt td { display: block; width: 100%; }
      .rt tr { border-bottom: 1px solid var(--rule); padding: 8px 14px; }
      .rt td { display: flex; justify-content: space-between; gap: 12px; padding: 3px 0; border: 0; white-space: normal; text-align: right; }
      .rt td::before { content: attr(data-label); color: var(--muted); font-size: 13px; text-align: left; }
      .rt td:first-child { text-align: right; }
    }
  </style>
</head>
<body>
<header class="top">
  <div class="top-row">
    <div class="brand">Bot Short<span>Binance USDT-M, cruce EMA</span></div>
    <span id="modeBadge" class="mode paper">—</span>
    <div class="grow"></div>
    <div class="conn" aria-label="Estado de conexión">
      <span title="Entradas nuevas"><i id="lampEntries" class="lamp off"></i><span class="lbl" id="lblEntries">Entradas</span></span>
      <span title="WebSocket de precios"><i id="lampWs" class="lamp off"></i><span class="lbl">Precios</span></span>
      <span title="Refresco del panel"><i id="lampPoll" class="lamp off"></i><span class="lbl">Panel</span></span>
    </div>
  </div>
  <nav class="tabs" aria-label="Secciones">
    <a href="#sec-pos">Posiciones</a>
    <a href="#sec-ctl">Control</a>
    <a href="#sec-cfg">Ajustes</a>
    <a href="#sec-hist">Historial</a>
    <a href="#sec-stats">Estadísticas</a>
    <a href="#sec-blk">Señales frenadas</a>
    <a href="#sec-radar">Radar</a>
    <a href="#sec-diag">Diagnóstico</a>
  </nav>
</header>

<main>
  <!-- Lectura principal + interlocks -->
  <section class="hero" aria-label="Resumen y guardas de riesgo">
    <div class="readout">
      <div>
        <div class="cap">PnL no realizado</div>
        <div id="pnl" class="big">—</div>
      </div>
      <dl>
        <div><dt>PnL realizado</dt><dd id="realizedPnl">—</dd></div>
        <div><dt>Posiciones abiertas</dt><dd id="openCount">—</dd></div>
        <div><dt>En cooldown</dt><dd id="cdCount">—</dd></div>
        <div><dt>Señales frenadas</dt><dd id="blkCount">—</dd></div>
      </dl>
    </div>
    <div class="interlocks">
      <button class="il" id="ilEntries" data-go="sec-ctl" data-state="off">
        <span class="il-head"><i class="lamp off"></i>Entradas nuevas</span>
        <span class="il-val" data-v>—</span>
        <span class="sides" data-sides></span>
        <span class="il-sub" data-s></span>
      </button>
      <button class="il" id="ilExposure" data-go="cfg-exposure" data-state="off">
        <span class="il-head"><i class="lamp off"></i>Exposición</span>
        <span class="il-val" data-v>—</span>
        <span class="il-bar"><i data-b></i></span>
        <span class="il-sub" data-s></span>
      </button>
      <button class="il" id="ilBtc" data-go="cfg-btc,cfg-btcup" data-state="off">
        <span class="il-head"><i class="lamp off"></i>BTCUSDT 24h</span>
        <span class="il-val" data-v>—</span>
        <span class="il-sub" data-s></span>
      </button>
      <button class="il" id="ilGstop" data-go="cfg-gstop" data-state="off">
        <span class="il-head"><i class="lamp off"></i>Stop global</span>
        <span class="il-val" data-v>—</span>
        <span class="il-bar"><i data-b></i></span>
        <span class="il-sub" data-s></span>
      </button>
    </div>
  </section>

  <div id="errorBox" class="alert" role="alert"><i class="lamp bad"></i><pre id="lastError"></pre></div>

  <div class="cols">
    <!-- Posiciones -->
    <section id="sec-pos" class="panel anchor">
      <div class="panel-head">
        <h2>Posiciones abiertas</h2><span class="count" id="posCount">0</span>
        <div class="grow"></div>
        <button class="link-btn" id="expandAll">Desplegar todas</button>
        <button class="link-btn" id="collapseAll">Plegar todas</button>
      </div>
      <div id="posList" class="poslist">
        <div class="empty"><b>Sin posiciones abiertas</b>El bot abrirá cuando un cruce EMA pase las guardas de riesgo.</div>
      </div>
    </section>

    <!-- Control + ajustes -->
    <aside class="panel">
      <div id="sec-ctl" class="anchor">
        <div class="ctl">
          <div class="ctl-title"><i id="lampPause" class="lamp off"></i>Entradas nuevas</div>
          <p id="pauseText">—</p>
          <button id="pauseBtn" class="btn amber block">Pausar entradas</button>
        </div>
        <div class="ctl">
          <div class="ctl-title">Cerrar todas las posiciones</div>
          <p>Cierra cada posición a mercado, una detrás de otra. Mientras dura no se abren entradas ni DCA, y cada símbolo cerrado entra en cooldown.</p>
          <div class="actions">
            <button id="caStart" class="btn danger" style="flex:1">Cerrar todas</button>
            <button id="caCancel" class="btn" style="display:none">Detener</button>
          </div>
          <div class="progress" id="caBarWrap"><i id="caBar"></i></div>
          <div id="caMsg" class="msg"></div>
        </div>
      </div>

      <div id="sec-cfg" class="anchor">
        <div class="panel-head" style="border-top:1px solid var(--rule)"><h2>Ajustes</h2><span class="muted" style="font-size:13.5px">se guardan y sobreviven a reinicios</span></div>

        <details class="acc" id="cfg-gstop">
          <summary><span class="acc-name">Stop global por PnL</span><span class="acc-val" id="cvGstop">—</span><span class="chev"></span></summary>
          <div class="acc-body" data-panel>
            <p class="hint">Si la suma del PnL no realizado de todas las posiciones llega a este valor, el bot las cierra todas a mercado, una a una.</p>
            <label class="switch"><input type="checkbox" id="gsEnabled"><span class="track"></span>Stop global activado</label>
            <div class="field">
              <label for="gsUsd">Cerrar todo cuando el PnL no realizado sea igual o menor que (USD)</label>
              <input id="gsUsd" type="number" step="0.5" placeholder="-5">
            </div>
            <label class="switch"><input type="checkbox" id="gsPause"><span class="track"></span>Pausar entradas nuevas tras dispararse</label>
            <p class="hint" id="gsLast"></p>
            <button class="btn primary" id="gsSave">Guardar stop global</button>
          </div>
        </details>

        <details class="acc" id="cfg-exposure">
          <summary><span class="acc-name">Límite de exposición</span><span class="acc-val" id="cvExposure">—</span><span class="chev"></span></summary>
          <div class="acc-body" data-panel>
            <p class="hint">No se abre una posición nueva si el notional abierto más el del nuevo tramo supera el límite. Las posiciones abiertas siguen su TP, SL y DCA.</p>
            <label class="switch"><input type="checkbox" id="exEnabled"><span class="track"></span>Límite activado</label>
            <div class="field">
              <label for="exMax">Exposición máxima (USDT de notional)</label>
              <input id="exMax" type="number" step="10" min="1" inputmode="decimal" placeholder="800">
            </div>
            <label class="switch"><input type="checkbox" id="exDca"><span class="track"></span>Aplicar también a los tramos DCA</label>
            <button class="btn primary" id="exSave">Guardar límite</button>
          </div>
        </details>

        <details class="acc" id="cfg-btc">
          <summary><span class="acc-name">Filtro BTC bajista</span><span class="acc-val" id="cvBtc">—</span><span class="chev"></span></summary>
          <div class="acc-body" data-panel>
            <p class="hint">Frena posiciones nuevas cuando el cambio de 24 h de BTCUSDT está por debajo del umbral. Los cruces se siguen detectando igual; solo se frena la apertura.</p>
            <label class="switch"><input type="checkbox" id="btcEnabled"><span class="track"></span>Filtro bajista activado</label>
            <div class="field-row">
              <div class="field">
                <label for="btcThr">Frena si BTC 24h es menor que (%)</label>
                <input id="btcThr" type="number" step="0.5" placeholder="0">
              </div>
              <div class="field">
                <label for="btcMode">Qué frena</label>
                <select id="btcMode">
                  <option value="all">LONG y SHORT</option>
                  <option value="long">Solo LONG</option>
                  <option value="short">Solo SHORT</option>
                </select>
              </div>
            </div>
            <button class="btn primary" id="btcSave">Guardar filtro bajista</button>
          </div>
        </details>

        <details class="acc" id="cfg-btcup">
          <summary><span class="acc-name">Filtro BTC alcista</span><span class="acc-val" id="cvBtcUp">—</span><span class="chev"></span></summary>
          <div class="acc-body" data-panel>
            <p class="hint">Frena posiciones nuevas cuando el cambio de 24 h de BTCUSDT está por encima del umbral. Funciona aparte del filtro bajista: puedes activar uno, el otro o los dos.</p>
            <label class="switch"><input type="checkbox" id="btcUpEnabled"><span class="track"></span>Filtro alcista activado</label>
            <div class="field-row">
              <div class="field">
                <label for="btcUpThr">Frena si BTC 24h es mayor que (%)</label>
                <input id="btcUpThr" type="number" step="0.5" placeholder="0">
              </div>
              <div class="field">
                <label for="btcUpMode">Qué frena</label>
                <select id="btcUpMode">
                  <option value="short">Solo SHORT</option>
                  <option value="long">Solo LONG</option>
                  <option value="all">LONG y SHORT</option>
                </select>
              </div>
            </div>
            <button class="btn primary" id="btcUpSave">Guardar filtro alcista</button>
          </div>
        </details>

        <details class="acc" id="cfg-sl">
          <summary><span class="acc-name">Stop loss por posición</span><span class="acc-val on" id="cvSl">—</span><span class="chev"></span></summary>
          <div class="acc-body" data-panel>
            <p class="hint" id="slHint">—</p>
            <div class="field">
              <label for="gslInput">Stop loss estándar para 2 o más tramos (USD)</label>
              <input id="gslInput" type="number" step="0.5" placeholder="-8">
            </div>
            <label class="switch"><input type="checkbox" id="gslOverride"><span class="track"></span>Sobrescribir también los SL fijados a mano</label>
            <button class="btn primary" id="gslSave">Guardar stop loss</button>
          </div>
        </details>

        <details class="acc" id="cfg-tp">
          <summary><span class="acc-name">Take profit</span><span class="acc-val on" id="cvTp">—</span><span class="chev"></span></summary>
          <div class="acc-body" data-panel>
            <p class="hint">Objetivo de cada posición = notional × multiplicador. Se aplica al instante a las posiciones abiertas.</p>
            <div class="field">
              <label for="tpInput">Multiplicador (0.07 = 7 % del notional)</label>
              <input id="tpInput" type="number" step="0.005" min="0.001" max="5" inputmode="decimal" placeholder="0.07">
            </div>
            <div class="chips" id="tpPreview"></div>
            <button class="btn primary" id="tpSave">Guardar take profit</button>
          </div>
        </details>

        <details class="acc" id="cfg-ema">
          <summary><span class="acc-name">Cruce de EMAs</span><span class="acc-val on" id="cvEma">—</span><span class="chev"></span></summary>
          <div class="acc-body" data-panel>
            <p class="hint" id="emaNote">—</p>
            <div class="field-row">
              <div class="field"><label for="emaFastInput">EMA rápida</label><input id="emaFastInput" type="number" step="1" min="2" inputmode="numeric"></div>
              <div class="field"><label for="emaSlowInput">EMA lenta</label><input id="emaSlowInput" type="number" step="1" min="3" inputmode="numeric"></div>
            </div>
            <button class="btn primary" id="emaSave">Guardar EMAs</button>
          </div>
        </details>

        <details class="acc" id="cfg-dca">
          <summary><span class="acc-name">Escalera DCA</span><span class="acc-val on" id="cvDca">—</span><span class="chev"></span></summary>
          <div class="acc-body" id="ladderPanel">
            <p class="hint">Toca un número para cambiarlo. Cada tramo entra cuando el precio va en contra de la primera entrada el % indicado. Al guardar se aplica también a las posiciones abiertas: su próximo tramo pasa a ser el siguiente de esta lista.</p>
            <div class="boxed tablewrap">
              <table class="ladder">
                <thead><tr><th>Tramo</th><th>% en contra</th><th>Notional</th><th>Total</th><th><span class="sr">Borrar</span></th></tr></thead>
                <tbody id="tbDca"></tbody>
              </table>
            </div>
            <div id="ladMsg" class="msg"></div>
            <div class="actions">
              <button class="btn" id="ladAdd">Añadir tramo</button>
              <button class="btn" id="ladDiscard" style="display:none">Descartar cambios</button>
              <button class="btn primary" id="ladSave" disabled style="flex:1">Guardar escalera</button>
            </div>
            <div class="factory-row">
              <span class="muted" id="ladFactoryTxt">—</span>
              <button class="btn amber" id="ladReset">Restaurar de fábrica</button>
            </div>
          </div>
        </details>

        <details class="acc" id="cfg-emadca">
          <summary><span class="acc-name">Condición EMA para DCA</span><span class="acc-val" id="cvEmaDca">—</span><span class="chev"></span></summary>
          <div class="acc-body" data-panel>
            <p class="hint">Cuando una posición ya tiene cierto número de tramos, los siguientes DCA solo entran a favor de la tendencia: en LONG con el precio por encima de la EMA y en SHORT por debajo.</p>
            <label class="switch"><input type="checkbox" id="edEnabled"><span class="track"></span>Condición activada</label>
            <div class="field-row">
              <div class="field"><label for="edPeriod">EMA (periodo)</label><input id="edPeriod" type="number" step="1" min="2" inputmode="numeric" placeholder="500"></div>
              <div class="field"><label for="edAfter">Tramos abiertos antes de exigirla</label><input id="edAfter" type="number" step="1" min="1" inputmode="numeric" placeholder="3"></div>
            </div>
            <p class="hint" id="edExplain">—</p>
            <button class="btn primary" id="edSave">Guardar condición</button>
          </div>
        </details>
      </div>
    </aside>
  </div>

  <!-- Historial -->
  <details id="sec-hist" class="section anchor" open>
    <summary><h2>Operaciones cerradas</h2><span class="sum" id="histSum"></span><span class="chev"></span></summary>
    <div class="section-body flush">
      <div style="padding:12px 18px 0" class="filters" id="histFilters" role="group" aria-label="Filtrar por motivo">
        <button data-f="ALL" aria-pressed="true">Todas</button>
        <button data-f="TP" aria-pressed="false">Take profit</button>
        <button data-f="SL" aria-pressed="false">Stop loss</button>
        <button data-f="MANUAL" aria-pressed="false">Manual</button>
        <button data-f="GLOBAL" aria-pressed="false">Stop global</button>
      </div>
      <div class="tablewrap" style="margin-top:12px; max-height:520px; overflow:auto">
        <table class="rt">
          <thead><tr><th>Símbolo</th><th>Lado</th><th>Motivo</th><th>PnL</th><th>MFE</th><th>MAE</th><th>Duración</th><th>Entrada</th><th>Cierre</th><th>Cooldown hasta</th><th>Cerrada</th></tr></thead>
          <tbody id="tbClosed"><tr><td colspan="11" class="muted">Todavía no hay cierres.</td></tr></tbody>
        </table>
      </div>
    </div>
  </details>

  <!-- Estadísticas -->
  <details id="sec-stats" class="section anchor">
    <summary><h2>Estadísticas MFE / MAE</h2><span class="sum" id="statsSum">Mejor y peor PnL alcanzado por cada operación</span><span class="chev"></span></summary>
    <div class="section-body">
      <div class="statgrid">
        <div><span>Operaciones</span><b id="stCount">0</b></div>
        <div><span>Acierto (TP)</span><b id="stWin">—</b></div>
        <div><span>MFE mediana</span><b class="pos" id="stMfe">—</b></div>
        <div><span>MAE mediana</span><b class="neg" id="stMae">—</b></div>
      </div>
      <div class="twocol">
        <div>
          <h3 class="sub-h">Por tipo de cierre (USD, MAE en valor absoluto)</h3>
          <div class="boxed tablewrap">
            <table>
              <thead><tr><th>Grupo</th><th>N</th><th>MFE med</th><th>MFE p90</th><th>MAE med</th><th>MAE p90</th><th>MFE %</th><th>MAE %</th><th>Duración</th></tr></thead>
              <tbody id="tbStatGroups"><tr><td colspan="9" class="muted">Sin datos aún</td></tr></tbody>
            </table>
          </div>
        </div>
        <div>
          <h3 class="sub-h">Take profits que cada SL habría cortado</h3>
          <div class="boxed tablewrap">
            <table>
              <thead><tr><th>SL USD</th><th>TP cortados</th><th>%</th></tr></thead>
              <tbody id="tbSlSim"><tr><td colspan="3" class="muted">Sin datos aún</td></tr></tbody>
            </table>
          </div>
        </div>
      </div>
      <div><a class="btn" href="/api/trades.csv" download>Descargar CSV</a></div>
    </div>
  </details>

  <!-- Señales frenadas -->
  <details id="sec-blk" class="section anchor">
    <summary><h2>Señales frenadas</h2><span class="sum" id="blkSum">Cruces y DCA que no abrieron por una guarda</span><span class="chev"></span></summary>
    <div class="section-body flush">
      <ul class="blk-list" id="blkList"><li class="muted" style="display:block">Ninguna señal frenada desde el arranque.</li></ul>
    </div>
  </details>

  <!-- Radar + cooldowns -->
  <details id="sec-radar" class="section anchor">
    <summary><h2>Radar y cooldowns</h2><span class="sum" id="radarSum"></span><span class="chev"></span></summary>
    <div class="section-body">
      <div>
        <h3 class="sub-h">Símbolos vigilados por tick</h3>
        <div class="boxed tablewrap">
          <table>
            <thead><tr><th>Símbolo</th><th>Cambio 24h</th><th>Precio</th></tr></thead>
            <tbody id="tbWinners"><tr><td colspan="3" class="muted">Nada en el radar.</td></tr></tbody>
          </table>
        </div>
      </div>
      <div>
        <h3 class="sub-h">En cooldown tras cerrar</h3>
        <div class="boxed tablewrap">
          <table class="rt">
            <thead><tr><th>Símbolo</th><th>Tiempo restante</th><th>Se libera (UTC)</th></tr></thead>
            <tbody id="tbCooldown"><tr><td colspan="3" class="muted">Ningún símbolo en cooldown.</td></tr></tbody>
          </table>
        </div>
      </div>
    </div>
  </details>

  <!-- Diagnóstico -->
  <details id="sec-diag" class="section anchor">
    <summary><h2>Diagnóstico y eventos</h2><span class="sum" id="diagSum"></span><span class="chev"></span></summary>
    <div class="section-body">
      <div class="diag">
        <div><span>Última evaluación</span><b id="scan">—</b></div>
        <div><span>Evaluaciones</span><b id="scanCount">—</b></div>
        <div><span>Evaluaciones por segundo</span><b id="evalRate">—</b></div>
        <div><span>Latencia tick → evaluación</span><b id="latency">—</b></div>
        <div><span>Perpetuos operables</span><b id="allSymbols">—</b></div>
        <div><span>Universo WS</span><b id="universe">—</b></div>
        <div><span>Radar</span><b id="subCount">—</b></div>
        <div><span>WebSocket precios</span><b id="rbWs">—</b></div>
        <div><span>Velas EMA</span><b id="klCandles">—</b></div>
        <div><span>Símbolos EMA listos</span><b id="klPairs">—</b></div>
        <div><span>Cruces detectados</span><b id="klSignals">—</b></div>
        <div><span>Red de seguridad</span><b id="rbSafety">—</b></div>
        <div><span>Executor</span><b id="executorStatus">—</b></div>
        <div><span>Tiempo activo</span><b id="uptime">—</b></div>
      </div>
      <div>
        <h3 class="sub-h">Eventos del bot</h3>
        <ol class="log" id="events"></ol>
      </div>
    </div>
  </details>
</main>

<!-- Modal: SL de una posición -->
<div id="slModalOverlay" class="overlay" role="dialog" aria-modal="true" aria-labelledby="slModalTitle">
  <div class="modal">
    <h3 id="slModalTitle">Stop loss de <span id="slModalSymbol">—</span></h3>
    <div class="field">
      <label for="slModalInput">Pérdida máxima en USD. Un SL fijado a mano no lo cambia el bot.</label>
      <input id="slModalInput" type="number" step="0.5">
    </div>
    <p id="slModalError" class="neg" style="display:none;margin:0;font-size:13.5px"></p>
    <div class="actions" style="justify-content:flex-end">
      <button id="slModalCancel" class="btn">Cancelar</button>
      <button id="slModalSave" class="btn primary">Guardar stop loss</button>
    </div>
  </div>
</div>

<div class="toasts" id="toasts" aria-live="polite"></div>

<script>
// ── Utilidades ──────────────────────────────────────────────────────────────
const q   = id => document.getElementById(id);
const n   = v => { const p = Number(v); return isFinite(p) ? p : 0; };
const fx  = (v, d = 2) => n(v).toFixed(d);
const px  = v => { const x = n(v), a = Math.abs(x); if (!x) return '0'; return x.toFixed(a >= 100 ? 2 : a >= 1 ? 4 : a >= 0.01 ? 6 : 8); };
const sgn = (v, d = 3) => { const x = n(v); const s = Math.abs(x).toFixed(d); return (x > 0 && +s ? '+' : x < 0 && +s ? '−' : '') + s; };
const pctS = (v, d = 2) => sgn(v, d) + ' %';
const cls = v => n(v) > 0 ? 'pos' : n(v) < 0 ? 'neg' : '';
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const clamp01 = x => Math.max(0, Math.min(1, x));
function setTxt(id, t) { const el = typeof id === 'string' ? q(id) : id; if (el && el.textContent !== t) el.textContent = t; }
function setHTML(el, html) { if (el && el._h !== html) { el._h = html; el.innerHTML = html; } }
function hhmm(ts) { if (!n(ts)) return '—'; const d = new Date(n(ts) * 1000); return d.toLocaleTimeString('es-CO', { hour: '2-digit', minute: '2-digit', hour12: false }); }
function fmtDur(secs) {
  const s = Math.max(0, Math.floor(n(secs)));
  if (s >= 86400) return `${Math.floor(s / 86400)} d ${Math.floor((s % 86400) / 3600)} h`;
  if (s >= 3600) return `${Math.floor(s / 3600)} h ${String(Math.floor((s % 3600) / 60)).padStart(2, '0')} min`;
  if (s >= 60) return `${Math.floor(s / 60)} min ${String(s % 60).padStart(2, '0')} s`;
  return `${s} s`;
}
function setLamp(el, state, blink) { if (!el) return; const c = 'lamp ' + state + (blink ? ' blink' : ''); if (el.className !== c) el.className = c; }
function toast(msg, kind = '') {
  const t = document.createElement('div');
  t.className = 'toast ' + kind; t.textContent = msg; q('toasts').appendChild(t);
  setTimeout(() => t.classList.add('out'), 4200);
  setTimeout(() => t.remove(), 4600);
}
async function postJSON(url, body) {
  const opts = { method: 'POST', cache: 'no-store', headers: {} };
  if (body !== undefined) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
  const resp = await fetch(url, opts);
  let data = {};
  try { data = await resp.json(); } catch (e) { /* sin cuerpo JSON */ }
  if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
  return data;
}
// Valores negativos: acepta "5" o "-5" y devuelve -5 (cómodo en el teclado del móvil)
function negVal(id) {
  const v = parseFloat(String(q(id).value).replace(',', '.'));
  return isFinite(v) && v !== 0 ? -Math.abs(v) : NaN;
}
function numVal(id) { const v = parseFloat(String(q(id).value).replace(',', '.')); return isFinite(v) ? v : NaN; }

// ── Campos editables: no se pisan mientras el usuario los está cambiando ────
function isEditing(el) { return el && (document.activeElement === el || el.dataset.dirty); }
function fillVal(id, v) { const el = q(id); if (el && !isEditing(el) && String(el.value) !== String(v)) el.value = v; }
function fillChk(id, v) { const el = q(id); if (el && !isEditing(el)) el.checked = !!v; }
function clearDirty(scope) { scope.querySelectorAll('[data-dirty]').forEach(el => delete el.dataset.dirty); }
function busy(btn, on, label) { if (!btn) return; if (on) { btn.dataset.label = btn.textContent; btn.textContent = 'Guardando…'; btn.disabled = true; } else { btn.textContent = label || btn.dataset.label || btn.textContent; btn.disabled = false; } }

// ── Estado del cliente ──────────────────────────────────────────────────────
let _d = null, _gate = null, _closeAll = {};
let _openPos = new Set(), _posSig = '', _posMeta = {};
let _histFilter = 'ALL';
let livePollMs = 250, statusPollMs = 1500, pollOk = false, liveOk = false;

// ── Interlocks ──────────────────────────────────────────────────────────────
function tile(id, state, value, sub, bar) {
  const el = q(id); if (!el) return;
  if (el.dataset.state !== state) el.dataset.state = state;
  setLamp(el.querySelector('.lamp'), state, state === 'bad');
  setHTML(el.querySelector('[data-v]'), value);
  setTxt(el.querySelector('[data-s]'), sub || '');
  const b = el.querySelector('[data-b]');
  if (b && bar !== undefined) b.style.width = (clamp01(bar) * 100).toFixed(1) + '%';
}

function renderGate(g) {
  if (!g) return;
  _gate = g;
  // 1) Entradas nuevas
  let st = 'ok', val = 'Permitidas', sub = 'Un cruce EMA puede abrir posición';
  const globalRun = !!(_closeAll.running && _closeAll.origin === 'GLOBAL');
  if (g.close_all_active) { st = 'bad'; val = 'Cerrando todo'; sub = globalRun ? 'Stop global disparado' : 'Cierre masivo en curso'; }
  else if (g.paused) { st = 'warn'; val = 'En pausa'; sub = (g.pause_reason || 'Pausa manual') + (n(g.paused_at) ? `, desde las ${hhmm(g.paused_at)}` : ''); }
  else if (!g.can_open_long && !g.can_open_short) {
    st = 'bad'; val = 'Bloqueadas';
    sub = g.exposure_blocks ? 'Exposición al límite'
        : (g.btc_change === null || !g.btc_fresh) ? 'Filtro BTC sin dato fresco' : 'El filtro BTC frena todo';
  } else if (!g.can_open_long || !g.can_open_short) {
    st = 'warn'; val = 'Solo ' + (g.can_open_long ? 'LONG' : 'SHORT');
    sub = `El filtro BTC frena ${g.can_open_long ? 'SHORT' : 'LONG'}`;
  }
  tile('ilEntries', st, val, sub);
  setHTML(q('ilEntries').querySelector('[data-sides]'),
    `<span class="${g.can_open_long ? 'side-ok' : 'side-no'}">LONG</span><span class="${g.can_open_short ? 'side-ok' : 'side-no'}">SHORT</span>`);
  setLamp(q('lampEntries'), st);
  setTxt('lblEntries', 'Entradas: ' + val.toLowerCase());

  // Panel de pausa
  setLamp(q('lampPause'), g.paused ? 'warn' : 'ok');
  setTxt('pauseText', g.paused
    ? `En pausa: ${g.pause_reason || 'pausa manual'}. Las posiciones abiertas siguen con su TP, SL y DCA.`
    : 'Sin pausa. Pausar frena solo las posiciones nuevas; las abiertas siguen con su TP, SL y DCA.');
  const pb = q('pauseBtn');
  if (!pb.disabled) { setTxt(pb, g.paused ? 'Reanudar entradas' : 'Pausar entradas'); pb.className = 'btn block ' + (g.paused ? 'green' : 'amber'); }

  // 2) Exposición
  const exp = n(g.exposure), mx = n(g.max_exposure), ratio = mx > 0 ? exp / mx : 0;
  st = !g.exposure_enabled ? 'off' : g.exposure_blocks ? 'bad' : ratio >= 0.8 ? 'warn' : 'ok';
  val = g.exposure_enabled ? `${fx(exp, 0)}<small>/ ${fx(mx, 0)} USDT</small>` : `${fx(exp, 0)}<small>USDT</small>`;
  sub = !g.exposure_enabled ? 'Sin límite' : g.exposure_blocks ? 'Posiciones nuevas en pausa'
      : `Quedan ${fx(Math.max(0, mx - exp), 0)} USDT` + (g.exposure_include_dca ? ', incluye DCA' : '');
  tile('ilExposure', st, val, sub, g.exposure_enabled ? ratio : 0);
  setTxt('cvExposure', g.exposure_enabled ? `${fx(mx, 0)} USDT` + (g.exposure_include_dca ? ' con DCA' : '') : 'Apagado');
  q('cvExposure').classList.toggle('on', !!g.exposure_enabled);

  // 3) BTC 24h (filtro bajista + filtro alcista)
  const chg = g.btc_change;
  const thr = n(g.btc_threshold), thrUp = n(g.btc_up_threshold);
  const anyBtc = !!(g.btc_enabled || g.btc_up_enabled);
  const bL = !!g.btc_blocks_long, bS = !!g.btc_blocks_short;
  st = !anyBtc ? 'off' : (bL && bS) ? 'bad' : (bL || bS) ? 'warn' : 'ok';
  val = chg === null || chg === undefined ? '<span class="muted">sin dato</span>'
      : `<span class="${cls(chg)}">${pctS(chg)}</span>`;
  if (!anyBtc) sub = 'Filtros apagados';
  else if (bL || bS) {
    const rs = (g.btc_reasons || []).join(' ');
    const who = /sin dato/.test(rs) ? 'Sin dato fresco, se frena'
      : (/alcista/.test(rs) && /bajista/.test(rs)) ? 'Los filtros frenan'
      : /alcista/.test(rs) ? 'El filtro alcista frena' : 'El filtro bajista frena';
    sub = `${who} ${bL && bS ? 'LONG y SHORT' : bL ? 'LONG' : 'SHORT'}`;
  } else {
    sub = g.btc_enabled && g.btc_up_enabled ? `Entre ${sgn(thr, 1)} % y ${sgn(thrUp, 1)} %: pasa los dos filtros`
        : g.btc_enabled ? `Sobre ${sgn(thr, 1)} %: pasa el filtro bajista` : `Bajo ${sgn(thrUp, 1)} %: pasa el filtro alcista`;
  }
  tile('ilBtc', st, val, sub);
  const MODE = { all: 'LONG y SHORT', long: 'solo LONG', short: 'solo SHORT' };
  setTxt('cvBtc', g.btc_enabled ? `Bajo ${sgn(thr, 1)} %: ${MODE[g.btc_mode] || g.btc_mode}` : 'Apagado');
  q('cvBtc').classList.toggle('on', !!g.btc_enabled);
  setTxt('cvBtcUp', g.btc_up_enabled ? `Sobre ${sgn(thrUp, 1)} %: ${MODE[g.btc_up_mode] || g.btc_up_mode}` : 'Apagado');
  q('cvBtcUp').classList.toggle('on', !!g.btc_up_enabled);

  // Condición EMA del DCA
  setTxt('cvEmaDca', g.dca_ema_enabled ? `EMA ${n(g.dca_ema_period)} desde el tramo ${n(g.dca_ema_after) + 1}` : 'Apagado');
  q('cvEmaDca').classList.toggle('on', !!g.dca_ema_enabled);
  fillChk('edEnabled', g.dca_ema_enabled); fillVal('edPeriod', n(g.dca_ema_period)); fillVal('edAfter', n(g.dca_ema_after));
  const edSig = [g.dca_ema_enabled, g.dca_ema_period, g.dca_ema_after].join('|');
  if (edSig !== _edSig) { _edSig = edSig; edExplain(); updateLadderMeta(); }

  // 4) Stop global
  const un = n(g.unrealized), stop = n(g.gstop_usd);
  const used = (un < 0 && stop < 0) ? un / stop : 0;
  st = !g.gstop_enabled ? 'off' : globalRun ? 'bad' : used >= 0.6 ? 'warn' : 'ok';
  val = `<span class="${cls(un)}">${sgn(un, 2)}</span><small>/ ${fx(stop, 2)} USD</small>`;
  const last = g.gstop_last || {};
  sub = !g.gstop_enabled ? 'Desactivado'
      : n(g.gstop_triggers) ? `${g.gstop_triggers} disparo${g.gstop_triggers === 1 ? '' : 's'}, el último a las ${hhmm(last.ts)}`
      : `Cierra todo al llegar a ${fx(stop, 2)} USD`;
  tile('ilGstop', st, val, sub, g.gstop_enabled ? used : 0);
  setTxt('cvGstop', g.gstop_enabled ? `${fx(stop, 2)} USD` : 'Apagado');
  q('cvGstop').classList.toggle('on', !!g.gstop_enabled);
  setTxt('gsLast', n(g.gstop_triggers)
    ? `Último disparo: ${last.at || hhmm(last.ts)} con PnL ${sgn(last.pnl, 2)} USD sobre ${n(last.positions)} posición(es).`
    : 'Aún no se ha disparado desde el arranque.');

  // Formularios de riesgo
  fillChk('gsEnabled', g.gstop_enabled); fillVal('gsUsd', fx(stop, 2)); fillChk('gsPause', g.gstop_pause);
  fillChk('exEnabled', g.exposure_enabled); fillVal('exMax', +fx(mx, 2)); fillChk('exDca', g.exposure_include_dca);
  fillChk('btcEnabled', g.btc_enabled); fillVal('btcThr', +fx(thr, 2)); fillVal('btcMode', g.btc_mode || 'all');
  fillChk('btcUpEnabled', g.btc_up_enabled); fillVal('btcUpThr', +fx(thrUp, 2)); fillVal('btcUpMode', g.btc_up_mode || 'short');

  // Contador de señales frenadas
  const total = Object.values(g.blocked_counts || {}).reduce((a, b) => a + n(b), 0);
  setTxt('blkCount', String(total));
}

// ── Posiciones ──────────────────────────────────────────────────────────────
function rangePos(pnl, sl, tp) {
  const span = n(tp) - n(sl);
  if (span <= 0) return [0.5, 0.5];
  return [clamp01((n(pnl) - n(sl)) / span), clamp01((0 - n(sl)) / span)];
}
function posItem(p) {
  const sym = p.symbol, side = p.direction === 'LONG' ? 'LONG' : 'SHORT';
  const fills = Array.isArray(p.fills) ? p.fills : [];
  const pnl = n(p.unrealized_pnl);
  const [m, z] = rangePos(pnl, p.stop_loss_usd, p.target);
  const rows = fills.map((f, i) => `<tr><td>${i + 1}</td><td>${fmtN(f.level)} %</td><td>${fx(f.notional, 2)}</td><td>${px(f.entry_price)}</td><td>${f.qty}</td><td>${hhmm(f.opened_at)}</td></tr>`).join('');
  return `<details id="prow_${sym}" data-sym="${sym}"${_openPos.has(sym) ? ' open' : ''}>
    <summary>
      <span class="side ${side.toLowerCase()}">${side}</span>
      <span class="pos-id"><b>${sym}</b><small>${fills.length} tramo${fills.length === 1 ? '' : 's'}, ${fx(p.notional, 2)} USDT</small></span>
      <span id="pc_${sym}" class="chg ${cls(p.change)}">${pctS(p.change)}</span>
      <span class="range" title="PnL entre el stop loss (izquierda) y el take profit (derecha)"><i class="zero" style="left:${(z * 100).toFixed(1)}%"></i><i class="mark" id="pm_${sym}" style="left:${(m * 100).toFixed(1)}%"></i></span>
      <span id="ppnl_${sym}" class="pnl ${cls(pnl)}">${sgn(pnl)}</span>
      <span class="chev" aria-hidden="true"></span>
    </summary>
    <div class="pos-body">
      <dl class="kv">
        <div><dt>Entrada media</dt><dd>${px(p.avg_entry)}</dd></div>
        <div><dt>Precio actual</dt><dd id="pp_${sym}">${px(p.mark_price)}</dd></div>
        <div><dt>Cantidad</dt><dd>${p.qty}</dd></div>
        <div><dt>Take profit</dt><dd class="pos">+${fx(p.target, 3)} USD</dd></div>
        <div><dt>Stop loss</dt><dd><span id="psl_${sym}">${px(p.stop_loss_price)}</span> <small>${sgn(p.stop_loss_usd, 3)} USD, ${esc(p.sl_mode || '')}</small></dd></div>
        <div><dt>MFE (mejor)</dt><dd id="pmfe_${sym}" class="pos">${sgn(p.mfe_usd)}</dd></div>
        <div><dt>MAE (peor)</dt><dd id="pmae_${sym}" class="neg">${sgn(p.mae_usd)}</dd></div>
        <div><dt>Abierta hace</dt><dd id="pdur_${sym}">${fmtDur(Date.now() / 1000 - n(p.opened_ts))}</dd></div>
        <div><dt>Trade</dt><dd>#${n(p.trade_id)}</dd></div>
      </dl>
      <p class="dca-next" id="pdca_${sym}" style="margin:0">${dcaHtml(p)}</p>
      <div class="tablewrap"><table class="fills">
        <thead><tr><th>#</th><th>En contra</th><th>Notional</th><th>Precio</th><th>Cantidad</th><th>Hora</th></tr></thead>
        <tbody>${rows}</tbody>
      </table></div>
      <div class="actions">
        <button class="btn" data-act="sl" data-sym="${sym}" data-sl="${n(p.stop_loss_usd)}">Editar stop loss</button>
        <a class="btn" href="https://www.binance.com/en/futures/${sym}" target="_blank" rel="noopener">Abrir en Binance</a>
        <button class="btn danger" data-act="close" data-sym="${sym}">Cerrar posición</button>
      </div>
    </div>
  </details>`;
}
// Estado del próximo DCA de una posición (y de la condición EMA, si aplica)
function dcaHtml(p) {
  const d = p.dca_next || {};
  if (d.level === null || d.level === undefined) return 'Escalera completa: no quedan tramos DCA para esta posición.';
  let s = `Próximo DCA: tramo ${n(d.idx) + 1}, <b>${fx(d.notional, 2)} USDT</b> al <b>${fmtN(d.level)} %</b> en contra (ahora ${sgn(d.adverse, 2)} %).`;
  if (d.ema_applies) {
    const where = p.direction === 'LONG' ? 'por encima' : 'por debajo';
    s += (d.ema === null || d.ema === undefined)
      ? ` Exige el precio ${where} de la EMA ${n(d.ema_period)}, que aún no tiene velas suficientes: <b class="neg">frenado</b>.`
      : ` Exige el precio ${where} de la EMA ${n(d.ema_period)} (${px(d.ema)}): <b class="${d.ema_ok ? 'pos' : 'neg'}">${d.ema_ok ? 'se cumple' : 'no se cumple'}</b>.`;
  }
  return s;
}
function renderPositions(positions) {
  setTxt('posCount', String(positions.length));
  setTxt('openCount', String(positions.length));
  _posMeta = {};
  positions.forEach(p => { _posMeta[p.symbol] = { opened: n(p.opened_ts) }; });
  const sig = JSON.stringify(positions.map(p => [p.symbol, p.direction, p.avg_entry, p.qty, p.notional, p.target,
    p.stop_loss_usd, p.sl_mode, (p.fills || []).length]));
  if (sig !== _posSig) {
    _posSig = sig;
    const live = new Set(positions.map(p => p.symbol));
    _openPos.forEach(s => { if (!live.has(s)) _openPos.delete(s); });
    q('posList').innerHTML = positions.length ? positions.map(posItem).join('')
      : '<div class="empty"><b>Sin posiciones abiertas</b>El bot abrirá cuando un cruce EMA pase las guardas de riesgo.</div>';
  }
  positions.forEach(p => setHTML(q('pdca_' + p.symbol), dcaHtml(p)));
}
function patchPosition(sym, p) {
  setTxt('pp_' + sym, px(p.p));
  const c = q('pc_' + sym); if (c) { setTxt(c, pctS(p.c)); c.className = 'chg ' + cls(p.c); }
  const pn = q('ppnl_' + sym); if (pn) { setTxt(pn, sgn(p.pnl)); pn.className = 'pnl ' + cls(p.pnl); }
  setTxt('psl_' + sym, px(p.sl));
  setTxt('pmfe_' + sym, sgn(p.mfe));
  setTxt('pmae_' + sym, sgn(p.mae));
  const mk = q('pm_' + sym);
  if (mk) mk.style.left = (rangePos(p.pnl, p.slu, p.tp)[0] * 100).toFixed(1) + '%';
}

// ── Historial ───────────────────────────────────────────────────────────────
const REASONS = { TP: ['tp', 'Take profit'], SL: ['sl', 'Stop loss'], MANUAL: ['manual', 'Manual'], GLOBAL: ['global', 'Stop global'] };
function renderHistory() {
  const closed = (_d && Array.isArray(_d.closed_trades)) ? _d.closed_trades : [];
  const rows = closed.filter(t => _histFilter === 'ALL' || t.reason === _histFilter);
  const html = rows.length ? rows.map(t => {
    const r = REASONS[t.reason] || ['', t.reason || '—'];
    return `<tr>
      <td data-label="Símbolo" class="sym">${esc(t.symbol)}</td>
      <td data-label="Lado">${esc(t.direction || '—')}</td>
      <td data-label="Motivo"><span class="tag ${r[0]}">${esc(r[1])}</span></td>
      <td data-label="PnL" class="num ${cls(t.pnl)}">${sgn(t.pnl)}</td>
      <td data-label="MFE" class="pos">${t.mfe_usd === undefined ? '—' : sgn(t.mfe_usd)}</td>
      <td data-label="MAE" class="neg">${t.mae_usd === undefined ? '—' : sgn(t.mae_usd)}</td>
      <td data-label="Duración">${t.duration_s === undefined ? '—' : fmtDur(t.duration_s)}</td>
      <td data-label="Entrada">${px(t.avg_entry)}</td>
      <td data-label="Cierre">${px(t.close_price)}</td>
      <td data-label="Cooldown hasta" class="muted">${esc(t.unblock_at || '—')}</td>
      <td data-label="Cerrada" class="muted">${esc(t.closed_at || '')}</td>
    </tr>`;
  }).join('') : `<tr><td colspan="11" class="muted">${closed.length ? 'Ningún cierre con ese motivo.' : 'Todavía no hay cierres.'}</td></tr>`;
  setHTML(q('tbClosed'), html);
  const rp = n(_d && _d.total_realized_pnl);
  setHTML(q('histSum'), closed.length ? `${closed.length} cierres, realizado <b class="${cls(rp)}">${sgn(rp)} USDT</b>` : 'Sin cierres');
}

// ── Cierre masivo ───────────────────────────────────────────────────────────
function renderCloseAll(ca, openN) {
  ca = ca || {}; _closeAll = ca;
  const running = !!ca.running, total = n(ca.total), done = n(ca.done);
  const btn = q('caStart'), cancel = q('caCancel'), wrap = q('caBarWrap'), msg = q('caMsg');
  setTxt(btn, running ? `Cerrando ${done} de ${total}…` : `Cerrar todas (${openN})`);
  btn.disabled = running || openN === 0;
  cancel.style.display = running ? '' : 'none';
  cancel.disabled = !!ca.cancel;
  wrap.style.display = (running || n(ca.started) > 0) ? 'block' : 'none';
  q('caBar').style.width = (running ? (total ? done / total * 100 : 0) : 100) + '%';
  const origin = ca.origin === 'GLOBAL' ? 'Stop global: ' : '';
  if (running) {
    msg.className = 'msg warn-t';
    setTxt(msg, origin + (ca.cancel ? 'deteniendo tras la operación en curso. ' : '') + (ca.current ? `Cerrando ${ca.current}. ` : 'Preparando. ')
      + `${n(ca.ok)} cerradas` + (n(ca.failed) ? `, ${n(ca.failed)} con error` : '') + '.');
  } else if (n(ca.started) > 0) {
    const bad = (ca.failed_symbols || []).join(', ');
    msg.className = 'msg ' + (n(ca.failed) ? 'neg' : 'pos');
    setTxt(msg, origin + (ca.cancel ? 'detenido' : 'terminado') + ` a las ${hhmm(ca.finished)}: ${n(ca.ok)} cerradas`
      + (n(ca.failed) ? `, ${n(ca.failed)} con error (${bad})` : '') + '.');
  } else { setTxt(msg, ''); }
}

// ── Render completo (cada ~1.5 s) ───────────────────────────────────────────
function render(d) {
  if (!d) return;
  _d = d;
  if (n(d.live_poll_ms))   livePollMs   = Math.max(100, n(d.live_poll_ms));
  if (n(d.status_poll_ms)) statusPollMs = Math.max(500, n(d.status_poll_ms));

  const mode = d.mode || '—';
  setTxt('modeBadge', mode === 'REAL' ? 'Real' : mode === 'PAPER' ? 'Paper' : mode);
  q('modeBadge').className = 'mode ' + (mode === 'REAL' ? 'real' : 'paper');
  setLamp(q('lampWs'), d.ws_connected ? 'ok' : 'bad');

  const pu = n(d.total_unrealized), rp = n(d.total_realized_pnl);
  setTxt('pnl', sgn(pu)); q('pnl').className = 'big ' + cls(pu);
  setTxt('realizedPnl', sgn(rp)); q('realizedPnl').className = cls(rp);
  setTxt('cdCount', String(n(d.cooldown_count)));

  const positions = Array.isArray(d.positions) ? d.positions : [];
  renderCloseAll(d.close_all, positions.length);
  renderGate(d.gate);
  renderPositions(positions);
  renderHistory();

  // Ajustes: SL, TP, EMA, DCA
  const frac = n(d.first_tranche_sl_fraction || 0.251), defSl = n(d.default_stop_loss_usd ?? -8);
  setTxt('cvSl', `${fx(defSl, 2)} USD`);
  setTxt('slHint', `Con 1 tramo el SL es el notional del tramo × ${frac} (por ejemplo, 5 USDT → ${fx(-5 * frac, 3)} USD). Desde el 2.º tramo se usa este SL estándar. Se aplica al instante a las posiciones abiertas.`);
  fillVal('gslInput', fx(defSl, 2));

  const tpf = n(d.take_profit_fraction || 0.07);
  setTxt('cvTp', `${+(tpf * 100).toFixed(2)} % del notional`);
  fillVal('tpInput', +tpf.toFixed(4));
  setHTML(q('tpPreview'), [5, 10, 20, 50, 100].map(v => `<span class="chip">${v} USDT gana <b>+${+(v * tpf).toFixed(3)}</b></span>`).join(''));

  const kw = d.kline_ws || {};
  if (d.ema_fast !== undefined) {
    setTxt('cvEma', `${n(d.ema_fast)} / ${n(d.ema_slow)}, velas de ${d.ema_interval || ''}`);
    fillVal('emaFastInput', n(d.ema_fast)); fillVal('emaSlowInput', n(d.ema_slow));
    q('emaFastInput').max = q('emaSlowInput').max = n(d.ema_max_period || 500);
    setTxt('emaNote', `La EMA lenta admite hasta ${n(d.ema_max_period)} (se guardan ${n(d.ema_max_candles)} velas por símbolo; ${n(kw.pairs_with_data)} símbolos listos). Al cambiarlas se recalculan con esas velas, sin descargar nada y sin cruces falsos. Las posiciones abiertas no cambian.`);
  }
  q('edPeriod').max = n(d.ema_max_period || 500);
  ladderSync(d.ladder);

  // Señales frenadas
  const g = d.gate || {};
  const KIND = { pausa: 'Pausa', exposicion: 'Exposición', btc: 'Filtro BTC', btc_down: 'Filtro BTC bajista',
                 btc_up: 'Filtro BTC alcista', ema_dca: 'Condición EMA' };
  const recent = Array.isArray(g.blocked_recent) ? g.blocked_recent : [];
  setHTML(q('blkList'), recent.length ? recent.map(b => `<li>
      <time>${hhmm(b.ts)}</time><span class="side ${b.side === 'LONG' ? 'long' : 'short'}">${esc(b.side)}</span>
      <b>${esc(b.symbol)}</b>
      <span class="why">${esc(KIND[b.kind] || b.kind)} frenó ${b.what === 'DCA' ? 'un DCA' : 'la entrada'}${b.extra ? ' (' + esc(b.extra) + ')' : ''}: ${esc(b.detail)}</span>
    </li>`).join('') : '<li class="muted" style="display:block">Ninguna señal frenada desde el arranque.</li>');
  const counts = g.blocked_counts || {};
  const parts = Object.keys(counts).map(k => `${KIND[k] || k}: ${counts[k]}`);
  setTxt('blkSum', parts.length ? parts.join(', ') : 'Cruces y DCA que no abrieron por una guarda');

  // Radar y cooldowns
  const winners = Array.isArray(d.winners) ? d.winners : [];
  setHTML(q('tbWinners'), winners.length ? winners.map(w => `<tr id="wrow_${w.symbol}">
      <td class="sym">${w.symbol}</td><td id="wc_${w.symbol}" class="num ${cls(w.change)}">${pctS(w.change)}</td><td id="wp_${w.symbol}">${px(w.price)}</td></tr>`).join('')
    : '<tr><td colspan="3" class="muted">Nada en el radar.</td></tr>');
  const cds = Object.entries(d.cooldowns || {}).sort((a, b) => n(a[1].remaining_s) - n(b[1].remaining_s));
  setHTML(q('tbCooldown'), cds.length ? cds.map(([s, i]) => `<tr><td data-label="Símbolo" class="sym">${s}</td>
      <td data-label="Restante" class="warn-t" data-cd="${n(i.remaining_s)}">${fmtDur(i.remaining_s)}</td><td data-label="Se libera" class="muted">${esc(i.unblock_utc)}</td></tr>`).join('')
    : '<tr><td colspan="3" class="muted">Ningún símbolo en cooldown.</td></tr>');
  setTxt('radarSum', `${winners.length} en el radar, ${cds.length} en cooldown`);

  // Diagnóstico
  setTxt('scan', d.last_scan_text || 'pendiente');
  setTxt('scanCount', String(n(d.scan_count)));
  setTxt('evalRate', fx(d.eval_rate, 0));
  setTxt('latency', `${fx(d.latency_avg_ms, 1)} ms media, ${fx(d.latency_max_ms, 1)} máx`);
  setTxt('allSymbols', String(n(d.all_symbols_count)));
  setTxt('universe', String(n(d.universe_count)));
  setTxt('subCount', String(n(d.subscribed_count)));
  setTxt('rbWs', d.ws_connected ? 'Conectado' : 'Desconectado');
  q('rbWs').className = d.ws_connected ? 'pos' : 'neg';
  setTxt('klCandles', String(n(kw.stored_candles)));
  setTxt('klPairs', `${n(kw.pairs_with_data)} de ${n(kw.tracked)}`);
  setTxt('klSignals', `${n(kw.signals)}` + (n(kw.stale_signals) ? ` (${n(kw.stale_signals)} viejos)` : ''));
  setTxt('rbSafety', n(d.safety_tick_secs) > 0 ? `cada ${n(d.safety_tick_secs)} s` : 'apagada');
  setTxt('executorStatus', d.executor_url ? d.executor_url.replace(/^https?:\/\//, '').split('/')[0] : 'No configurado');
  setTxt('uptime', fmtDur(d.uptime_seconds));
  setTxt('diagSum', `${d.ws_connected ? 'WebSocket conectado' : 'WebSocket desconectado'}, ${fx(d.eval_rate, 0)} evaluaciones/s`);

  const evs = Array.isArray(d.events) ? d.events : [];
  setHTML(q('events'), evs.map(line => {
    const i = line.indexOf(' | ');
    const dt = new Date(line.slice(0, 19).replace(' ', 'T') + 'Z');
    const stamp = (i > 0 && !isNaN(dt)) ? dt.toLocaleTimeString('es-CO', { hour12: false }) : '';
    const text = i > 0 ? line.slice(i + 3) : line;
    const k = /STOP GLOBAL|STOP LOSS|Error|error|falló|⛔|🛑/.test(text) ? 'bad'
      : /CIERRE TP|REANUDADAS/.test(text) ? 'good' : /PAUSA|frenada|⚠️|CIERRE MASIVO/.test(text) ? 'warn' : '';
    return `<li class="${k}"><time>${esc(stamp)}</time><span>${esc(text)}</span></li>`;
  }).join(''));

  const err = d.last_error || d.last_startup_err || '';
  q('errorBox').style.display = err ? 'flex' : 'none';
  setTxt('lastError', err);
}

// ── Refresco en vivo (cada ~250 ms): solo parchea valores ───────────────────
function applyLive(l) {
  if (!l) return;
  const pos = l.positions || {};
  const keys = Object.keys(pos);
  let mismatch = keys.length !== Object.keys(_posMeta).length;
  for (const sym of keys) {
    if (!q('prow_' + sym)) { mismatch = true; continue; }
    patchPosition(sym, pos[sym]);
  }
  for (const w of (Array.isArray(l.winners) ? l.winners : [])) {
    setTxt('wp_' + w.s, px(w.p));
    const c = q('wc_' + w.s); if (c) { setTxt(c, pctS(w.c)); c.className = 'num ' + cls(w.c); }
  }
  const pu = n(l.total_unrealized);
  setTxt('pnl', sgn(pu)); q('pnl').className = 'big ' + cls(pu);
  renderCloseAll(l.close_all, keys.length);
  renderGate(l.gate);
  setTxt('scanCount', String(n(l.scan_count)));
  setTxt('evalRate', fx(l.eval_rate, 0));
  setTxt('latency', `${fx(l.latency_avg_ms, 1)} ms media, ${fx(l.latency_max_ms, 1)} máx`);
  setTxt('subCount', String(n(l.watch_count)));
  if (mismatch) requestFull();
}

// ── Estadísticas ────────────────────────────────────────────────────────────
function renderStats(s) {
  if (!s) return;
  setTxt('stCount', String(n(s.count)));
  setTxt('stWin', s.count ? fx(s.win_rate, 1) + ' %' : '—');
  const all = (s.groups || {}).ALL;
  setTxt('stMfe', all && all.n ? '+' + fx(all.mfe_usd.median, 3) : '—');
  setTxt('stMae', all && all.n ? '−' + fx(all.mae_usd.median, 3) : '—');
  setTxt('statsSum', s.count ? `${n(s.count)} operaciones, acierto ${fx(s.win_rate, 1)} %, PnL total ${sgn(s.total_pnl)}` : 'Mejor y peor PnL alcanzado por cada operación');
  const names = { ALL: 'Todas', TP: 'Take profit', SL: 'Stop loss', MANUAL: 'Manual', GLOBAL: 'Stop global' };
  const rows = ['ALL', 'TP', 'SL', 'MANUAL', 'GLOBAL'].filter(k => (s.groups || {})[k] && s.groups[k].n).map(k => {
    const g = s.groups[k];
    return `<tr><td>${names[k]}</td><td>${g.n}</td>
      <td class="pos">${fx(g.mfe_usd.median, 3)}</td><td class="pos">${fx(g.mfe_usd.p90, 3)}</td>
      <td class="neg">${fx(g.mae_usd.median, 3)}</td><td class="neg">${fx(g.mae_usd.p90, 3)}</td>
      <td>${fx(g.mfe_pct.median, 1)} %</td><td>${fx(g.mae_pct.median, 1)} %</td><td>${fmtDur(g.duration_s.median)}</td></tr>`;
  });
  setHTML(q('tbStatGroups'), rows.length ? rows.join('') : '<tr><td colspan="9" class="muted">Sin datos aún</td></tr>');
  const sim = Array.isArray(s.sl_sim) ? s.sl_sim : [];
  setHTML(q('tbSlSim'), sim.length && sim[0].tp_total
    ? sim.map(r => `<tr><td>${fx(r.sl, 1)}</td><td>${r.tp_stopped} de ${r.tp_total}</td>
        <td class="${n(r.pct) > 20 ? 'neg' : n(r.pct) > 5 ? 'warn-t' : 'pos'}">${fx(r.pct, 0)} %</td></tr>`).join('')
    : '<tr><td colspan="3" class="muted">Aún no hay take profits registrados</td></tr>');
}
let statsTimer = null;
async function loadStats() {
  try { const r = await fetch('/api/stats', { cache: 'no-store' }); if (r.ok) renderStats(await r.json()); }
  catch (e) { /* reintenta en el siguiente ciclo */ }
  finally { clearTimeout(statsTimer); statsTimer = setTimeout(loadStats, 15000); }
}

// ── Acciones ────────────────────────────────────────────────────────────────
async function togglePause() {
  const btn = q('pauseBtn'), resume = !!(_gate && _gate.paused);
  if (!resume && !confirm('¿Pausar la apertura de posiciones nuevas?\n\nLas posiciones abiertas siguen con su TP, SL y DCA.')) return;
  busy(btn, true);
  try {
    await postJSON('/api/pause', { paused: !resume, reason: 'Pausa manual' });
    toast(resume ? 'Entradas reanudadas' : 'Entradas en pausa', 'ok');
    requestFull(true);
  } catch (e) { toast('No se pudo cambiar la pausa: ' + e.message, 'bad'); }
  finally { busy(btn, false, resume ? 'Pausar entradas' : 'Reanudar entradas'); }
}

async function saveRisk(btnId, body, okMsg) {
  const btn = q(btnId);
  busy(btn, true);
  try {
    const data = await postJSON('/api/set-risk', body);
    clearDirty(btn.closest('[data-panel]'));
    if (data.risk) renderGate(data.risk);
    toast(okMsg, 'ok');
    requestFull(true);
  } catch (e) { toast('No se guardó: ' + e.message, 'bad'); }
  finally { busy(btn, false); }
}
function saveGlobalStop() {
  const usd = negVal('gsUsd'), enabled = q('gsEnabled').checked;
  if (isNaN(usd)) { toast('Escribe la pérdida a la que se cierra todo, por ejemplo 5 o -5.', 'bad'); return; }
  const un = _gate ? n(_gate.unrealized) : 0, open = _gate ? n(_gate.open_positions) : 0;
  if (enabled && open > 0 && un <= usd &&
      !confirm(`El PnL no realizado ya es ${sgn(un, 2)} USD, así que el stop global cerrará todas las posiciones de inmediato.\n\n¿Guardar igualmente?`)) return;
  saveRisk('gsSave', { global_stop_enabled: enabled, global_stop_usd: usd, global_stop_pause: q('gsPause').checked },
    enabled ? `Stop global guardado en ${fx(usd, 2)} USD` : 'Stop global desactivado');
}
function saveExposure() {
  const mx = numVal('exMax');
  if (isNaN(mx) || mx <= 0) { toast('La exposición máxima debe ser un número positivo, por ejemplo 800.', 'bad'); return; }
  saveRisk('exSave', { exposure_enabled: q('exEnabled').checked, max_exposure_usd: mx, exposure_include_dca: q('exDca').checked },
    q('exEnabled').checked ? `Límite de exposición guardado en ${fx(mx, 0)} USDT` : 'Límite de exposición desactivado');
}
function saveBtc() {
  const thr = numVal('btcThr');
  if (isNaN(thr)) { toast('Escribe el umbral en %, por ejemplo 0.', 'bad'); return; }
  saveRisk('btcSave', { btc_filter_enabled: q('btcEnabled').checked, btc_filter_threshold: thr, btc_filter_mode: q('btcMode').value },
    q('btcEnabled').checked ? 'Filtro BTC bajista activado' : 'Filtro BTC bajista desactivado');
}
function saveBtcUp() {
  const thr = numVal('btcUpThr');
  if (isNaN(thr)) { toast('Escribe el umbral en %, por ejemplo 0.', 'bad'); return; }
  saveRisk('btcUpSave', { btc_up_enabled: q('btcUpEnabled').checked, btc_up_threshold: thr, btc_up_mode: q('btcUpMode').value },
    q('btcUpEnabled').checked ? 'Filtro BTC alcista activado' : 'Filtro BTC alcista desactivado');
}

// ── Condición EMA del DCA ───────────────────────────────────────────────────
let _edSig = '';
function edExplain() {
  const on = q('edEnabled').checked, p = parseInt(q('edPeriod').value, 10), a = parseInt(q('edAfter').value, 10);
  const iv = (_d && _d.ema_interval) || '1m', mx = n(_d && _d.ema_max_period) || 500;
  if (!(p >= 2 && p <= mx) || !(a >= 1)) { setTxt('edExplain', `El periodo va de 2 a ${mx} y los tramos previos desde 1.`); return; }
  setTxt('edExplain', (on ? 'Con' : 'Apagada. Al activarla, con')
    + ` ${a} tramo${a === 1 ? '' : 's'} ya abierto${a === 1 ? '' : 's'}, el tramo ${a + 1} y los siguientes solo entran si en LONG el precio está por encima de la EMA ${p} y en SHORT por debajo. Usa las velas de ${iv} que ya guarda el detector de cruces.`);
}
function saveEmaDca() {
  const p = parseInt(q('edPeriod').value, 10), a = parseInt(q('edAfter').value, 10);
  const mx = n(_d && _d.ema_max_period) || 500;
  if (!(p >= 2 && p <= mx)) { toast(`La EMA debe ser un entero entre 2 y ${mx}.`, 'bad'); return; }
  if (!(a >= 1 && a <= 25)) { toast('Los tramos previos deben ser un entero entre 1 y 25.', 'bad'); return; }
  const on = q('edEnabled').checked;
  saveRisk('edSave', { dca_ema_enabled: on, dca_ema_period: p, dca_ema_after: a },
    on ? `Condición EMA ${p} activa desde el tramo ${a + 1}` : 'Condición EMA del DCA desactivada');
}

// ── Escalera DCA editable ───────────────────────────────────────────────────
const fmtN = (v, d = 4) => String(+n(v).toFixed(d));
const _lad = { srv: [], fac: [], draft: [], dirty: false, sig: '', max: 25, isFactory: true };
function ladderSync(L) {
  if (!L) return;
  const rowsOf = (lv, nt) => (lv || []).map((l, i) => ({ l: n(l), n: n((nt || [])[i]) }));
  _lad.srv = rowsOf(L.levels, L.notionals);
  _lad.fac = rowsOf(L.factory_levels, L.factory_notionals);
  _lad.max = n(L.max_rows) || 25;
  _lad.isFactory = !!L.is_factory;
  const sig = JSON.stringify(_lad.srv);
  if (!_lad.dirty && sig !== _lad.sig) {
    _lad.sig = sig;
    _lad.draft = _lad.srv.map(r => ({ ...r }));
    renderLadder();
  } else {
    updateLadderMeta();
  }
}
function ladderCheck(rows) {
  const errs = [], warns = [], bad = new Set();
  if (!rows.length) errs.push('La escalera necesita al menos el tramo 1.');
  if (rows.length > _lad.max) errs.push(`Máximo ${_lad.max} tramos.`);
  rows.forEach((r, i) => {
    if (!isFinite(r.n) || r.n <= 0) { errs.push(`Tramo ${i + 1}: el notional debe ser mayor que 0.`); bad.add('n' + i); }
    else if (r.n < 5) warns.push(`Tramo ${i + 1}: Binance pide unos 5 USDT de mínimo; el bot enviará el mínimo del contrato.`);
    if (i > 0) {
      if (!isFinite(r.l) || r.l <= 0 || r.l > 500) { errs.push(`Tramo ${i + 1}: el % en contra va de 0 a 500.`); bad.add('l' + i); }
      else if (isFinite(rows[i - 1].l) && r.l <= rows[i - 1].l) {
        errs.push(`Tramo ${i + 1}: el % en contra debe ser mayor que el del tramo ${i} (${fmtN(rows[i - 1].l)} %).`); bad.add('l' + i);
      }
    }
  });
  return { errs, warns, bad };
}
function renderLadder() {
  const rows = _lad.draft || [];
  q('tbDca').innerHTML = rows.map((r, i) => `<tr>
      <td>${i + 1}<span class="ema-tag" id="ltag_${i}"></span></td>
      <td>${i === 0 ? '<span class="muted">cruce EMA</span>'
        : `<input class="cell" type="number" step="0.5" min="0" data-i="${i}" data-k="l" value="${isFinite(r.l) ? fmtN(r.l) : ''}" aria-label="% en contra del tramo ${i + 1}">`}</td>
      <td><input class="cell" type="number" step="1" min="0" inputmode="decimal" data-i="${i}" data-k="n" value="${isFinite(r.n) ? fmtN(r.n) : ''}" aria-label="Notional del tramo ${i + 1}"></td>
      <td class="muted" id="lacc_${i}"></td>
      <td>${i === 0 ? '' : `<button class="icon-btn" data-del="${i}" title="Borrar tramo ${i + 1}" aria-label="Borrar tramo ${i + 1}">×</button>`}</td>
    </tr>`).join('');
  updateLadderMeta();
}
function updateLadderMeta() {
  const rows = _lad.draft || [];
  const g = _gate || {};
  let acc = 0;
  rows.forEach((r, i) => {
    acc += isFinite(r.n) ? r.n : 0;
    setTxt('lacc_' + i, fx(acc, 2));
    const t = q('ltag_' + i);
    if (t) {
      const on = !!(g.dca_ema_enabled && i >= n(g.dca_ema_after));
      setTxt(t, on ? 'EMA' : '');
      t.title = on ? `Este tramo exige la condición de la EMA ${n(g.dca_ema_period)}` : '';
    }
  });
  const { errs, warns, bad } = ladderCheck(rows);
  document.querySelectorAll('#tbDca input.cell').forEach(el => {
    const i = +el.dataset.i, k = el.dataset.k, s = _lad.srv[i], r = rows[i];
    el.classList.toggle('invalid', bad.has(k + i));
    el.toggleAttribute('data-dirty', !s || !r || s[k] !== r[k]);   // resalta lo que difiere de lo guardado
  });
  const msg = q('ladMsg');
  if (errs.length) { msg.className = 'msg neg'; setTxt(msg, errs[0]); }
  else if (_lad.dirty) { msg.className = 'msg warn-t'; setTxt(msg, 'Cambios sin guardar.' + (warns.length ? ' ' + warns[0] : '')); }
  else { msg.className = 'msg muted'; setTxt(msg, warns[0] || ''); }
  q('ladSave').disabled = !_lad.dirty || errs.length > 0;
  q('ladDiscard').style.display = _lad.dirty ? '' : 'none';
  q('ladAdd').disabled = rows.length >= _lad.max;
  setTxt('cvDca', _lad.dirty ? 'Sin guardar' : `${rows.length} tramo${rows.length === 1 ? '' : 's'}, ${fx(acc, 0)} USDT`);
  q('cvDca').classList.toggle('warn-t', _lad.dirty);
  const NB = ' ';
  setTxt('ladFactoryTxt', 'De fábrica (% en contra → USDT): '
    + _lad.fac.map(r => `${fmtN(r.l)}${NB}%${NB}→${NB}${fmtN(r.n)}`).join(', '));
  q('ladReset').disabled = _lad.isFactory && !_lad.dirty;
}
function ladderEdited() { _lad.dirty = true; updateLadderMeta(); }
function ladderAdd() {
  const rows = _lad.draft;
  if (rows.length >= _lad.max) return;
  const last = rows[rows.length - 1] || { l: 0, n: 5 }, prev = rows[rows.length - 2];
  const step = prev && isFinite(prev.l) && isFinite(last.l) && last.l > prev.l ? last.l - prev.l : 2;
  rows.push({ l: (isFinite(last.l) ? last.l : 0) + step, n: rows.length === 1 ? (isFinite(last.n) ? last.n : 5) : (isFinite(last.n) ? last.n * 2 : 5) });
  _lad.dirty = true;
  renderLadder();
  const el = document.querySelector(`#tbDca input[data-i="${rows.length - 1}"][data-k="l"]`);
  if (el) { el.focus(); el.select(); }
}
function ladderDiscard() {
  _lad.dirty = false; _lad.sig = JSON.stringify(_lad.srv);
  _lad.draft = _lad.srv.map(r => ({ ...r }));
  renderLadder();
}
function ladderApplied(data, msg) {
  _lad.dirty = false;
  _lad.sig = '';                       // fuerza a redibujar con lo que confirmó el servidor
  ladderSync(data);
  toast(msg, 'ok');
  requestFull(true);
}
async function ladderSave() {
  const rows = _lad.draft, { errs } = ladderCheck(rows);
  if (errs.length) { toast(errs[0], 'bad'); return; }
  const open = Object.keys(_posMeta).length;
  if (open && !confirm(`Hay ${open} posición(es) abierta(s). Al guardar, su próximo tramo pasa a ser el siguiente de la nueva escalera (y entra ya si el precio está lo bastante en contra).\n\n¿Guardar la escalera?`)) return;
  const btn = q('ladSave');
  busy(btn, true);
  try {
    const data = await postJSON('/api/set-ladder', { levels: rows.map((r, i) => i === 0 ? 0 : r.l), notionals: rows.map(r => r.n) });
    clearDirty(q('ladderPanel'));
    ladderApplied(data, `Escalera guardada: ${rows.length} tramos`);
  } catch (e) { toast('No se guardó la escalera: ' + e.message, 'bad'); }
  finally { busy(btn, false); updateLadderMeta(); }
}
async function ladderReset() {
  const fac = _lad.fac.map((r, i) => `Tramo ${i + 1}: ${i === 0 ? 'cruce EMA' : fmtN(r.l) + ' % en contra'}, ${fmtN(r.n)} USDT`).join('\n');
  if (!confirm(`¿Restaurar la escalera de fábrica?\n\n${fac}\n\nSe aplica ya, también a las posiciones abiertas.`)) return;
  const btn = q('ladReset');
  busy(btn, true);
  try {
    const data = await postJSON('/api/reset-ladder');
    clearDirty(q('ladderPanel'));
    ladderApplied(data, 'Escalera restaurada de fábrica');
  } catch (e) { toast('No se pudo restaurar: ' + e.message, 'bad'); }
  finally { busy(btn, false, 'Restaurar de fábrica'); updateLadderMeta(); }
}

async function saveGlobalSl() {
  const v = negVal('gslInput'), btn = q('gslSave');
  if (isNaN(v)) { toast('Escribe el stop loss en USD, por ejemplo 8 o -8.', 'bad'); return; }
  busy(btn, true);
  try {
    const data = await postJSON('/api/set-default-sl', { sl_usd: v, override_manual: q('gslOverride').checked });
    clearDirty(btn.closest('[data-panel]'));
    toast(`Stop loss estándar en ${fx(v, 2)} USD, ${(data.updated || []).length} posición(es) actualizada(s)`, 'ok');
    requestFull(true);
  } catch (e) { toast('No se guardó: ' + e.message, 'bad'); }
  finally { busy(btn, false); }
}
async function saveTp() {
  const v = numVal('tpInput'), btn = q('tpSave');
  if (isNaN(v) || v < 0.001 || v > 5) { toast('El multiplicador va de 0.001 a 5, por ejemplo 0.07.', 'bad'); return; }
  busy(btn, true);
  try {
    await postJSON('/api/set-take-profit', { fraction: v });
    clearDirty(btn.closest('[data-panel]'));
    toast(`Take profit en ${+(v * 100).toFixed(2)} % del notional`, 'ok');
    requestFull(true);
  } catch (e) { toast('No se guardó: ' + e.message, 'bad'); }
  finally { busy(btn, false); }
}
async function saveEma() {
  const f = parseInt(q('emaFastInput').value, 10), s = parseInt(q('emaSlowInput').value, 10), btn = q('emaSave');
  if (isNaN(f) || isNaN(s)) { toast('Escribe un número entero en cada EMA.', 'bad'); return; }
  if (f >= s) { toast('La EMA rápida debe ser menor que la lenta.', 'bad'); return; }
  busy(btn, true);
  try {
    const data = await postJSON('/api/set-ema', { fast: f, slow: s });
    clearDirty(btn.closest('[data-panel]'));
    toast(`EMAs en ${data.ema_fast} / ${data.ema_slow}, ${data.recomputed} símbolos recalculados`, 'ok');
    requestFull(true);
  } catch (e) { toast('No se guardó: ' + e.message, 'bad'); }
  finally { busy(btn, false); }
}
async function closeAll() {
  const k = Object.keys(_posMeta).length;
  if (!k) return;
  if (!confirm(`¿Cerrar las ${k} posiciones abiertas, una a una, a precio de mercado?\n\nNo se puede deshacer y cada símbolo cerrado entra en cooldown.`)) return;
  q('caStart').disabled = true;
  try { await postJSON('/api/close-all'); toast('Cierre de todas las posiciones en marcha', 'ok'); }
  catch (e) { toast('No se pudo iniciar el cierre: ' + e.message, 'bad'); }
  finally { requestFull(true); }
}
async function cancelCloseAll() {
  try { await postJSON('/api/close-all/cancel'); toast('El cierre se detendrá tras la operación en curso'); }
  catch (e) { toast(e.message, 'bad'); }
  finally { requestFull(true); }
}
async function closePosition(sym, btn) {
  if (!confirm(`¿Cerrar ${sym} a precio de mercado?\n\nNo se puede deshacer.`)) return;
  btn.disabled = true; btn.textContent = 'Cerrando…';
  try { await postJSON(`/api/close/${sym}`); toast(`${sym} cerrada`, 'ok'); setTimeout(loadStats, 1500); }
  catch (e) { btn.disabled = false; btn.textContent = 'Cerrar posición'; toast(`No se cerró ${sym}: ${e.message}`, 'bad'); }
  finally { requestFull(true); }
}

// Modal SL por posición
let _slSym = null;
function editStopLoss(sym, cur) {
  _slSym = sym; setTxt('slModalSymbol', sym);
  q('slModalInput').value = cur; q('slModalError').style.display = 'none';
  q('slModalOverlay').style.display = 'flex';
  setTimeout(() => q('slModalInput').focus(), 50);
}
function closeSlModal() { q('slModalOverlay').style.display = 'none'; _slSym = null; }
async function saveSlModal() {
  if (!_slSym) return;
  const v = negVal('slModalInput'), btn = q('slModalSave');
  if (isNaN(v)) { setTxt('slModalError', 'Escribe la pérdida máxima, por ejemplo 5 o -5.'); q('slModalError').style.display = 'block'; return; }
  busy(btn, true);
  try { await postJSON(`/api/set-sl/${_slSym}`, { sl_usd: v }); toast(`Stop loss de ${_slSym} en ${fx(v, 2)} USD`, 'ok'); closeSlModal(); requestFull(true); }
  catch (e) { setTxt('slModalError', e.message); q('slModalError').style.display = 'block'; }
  finally { busy(btn, false); }
}

// ── Eventos de la interfaz ──────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  // Marca como "editado" cualquier campo de ajustes que el usuario toque
  const cfg = q('sec-cfg');
  ['input', 'change'].forEach(ev => cfg.addEventListener(ev, e => {
    if (e.target.matches('input, select') && !e.target.matches('.cell')) e.target.dataset.dirty = '1';
  }));
  cfg.addEventListener('keydown', e => {
    if (e.key !== 'Enter' || !e.target.matches('input')) return;
    const b = e.target.closest('[data-panel]'); if (b) { const s = b.querySelector('.btn.primary'); if (s) s.click(); }
  });

  q('gsSave').addEventListener('click', saveGlobalStop);
  q('exSave').addEventListener('click', saveExposure);
  q('btcSave').addEventListener('click', saveBtc);
  q('btcUpSave').addEventListener('click', saveBtcUp);
  q('edSave').addEventListener('click', saveEmaDca);
  ['edEnabled', 'edPeriod', 'edAfter'].forEach(id => ['input', 'change'].forEach(ev => q(id).addEventListener(ev, edExplain)));

  // Escalera DCA: edición sobre la misma celda, borrar, añadir, guardar, restaurar
  const tb = q('tbDca');
  tb.addEventListener('input', e => {
    const el = e.target; if (!el.matches('input.cell')) return;
    const row = _lad.draft[+el.dataset.i]; if (!row) return;
    row[el.dataset.k] = parseFloat(String(el.value).replace(',', '.'));
    ladderEdited();
  });
  tb.addEventListener('keydown', e => { if (e.key === 'Enter' && e.target.matches('input.cell')) { e.preventDefault(); e.stopPropagation(); e.target.blur(); } });
  tb.addEventListener('click', e => {
    const b = e.target.closest('button[data-del]'); if (!b) return;
    _lad.draft.splice(+b.dataset.del, 1);
    _lad.dirty = true; renderLadder();
  });
  q('ladAdd').addEventListener('click', ladderAdd);
  q('ladSave').addEventListener('click', ladderSave);
  q('ladDiscard').addEventListener('click', ladderDiscard);
  q('ladReset').addEventListener('click', ladderReset);
  q('gslSave').addEventListener('click', saveGlobalSl);
  q('tpSave').addEventListener('click', saveTp);
  q('emaSave').addEventListener('click', saveEma);
  q('pauseBtn').addEventListener('click', togglePause);
  q('caStart').addEventListener('click', closeAll);
  q('caCancel').addEventListener('click', cancelCloseAll);

  // Interlocks: llevan a su ajuste
  document.querySelectorAll('.il').forEach(el => el.addEventListener('click', () => {
    const ids = el.dataset.go.split(',');
    ids.slice(1).forEach(id => { const x = q(id); if (x && x.tagName === 'DETAILS') x.open = true; });
    const t = q(ids[0]); if (!t) return;
    if (t.tagName === 'DETAILS') t.open = true;
    t.scrollIntoView({ block: 'start' });
  }));

  // Posiciones: recuerda cuáles están desplegadas y delega los botones
  const pl = q('posList');
  pl.addEventListener('toggle', e => {
    const s = e.target.dataset && e.target.dataset.sym; if (!s) return;
    if (e.target.open) _openPos.add(s); else _openPos.delete(s);
  }, true);
  pl.addEventListener('click', e => {
    const b = e.target.closest('button[data-act]'); if (!b) return;
    if (b.dataset.act === 'sl') editStopLoss(b.dataset.sym, n(b.dataset.sl));
    if (b.dataset.act === 'close') closePosition(b.dataset.sym, b);
  });
  q('expandAll').addEventListener('click', () => pl.querySelectorAll('details').forEach(d => d.open = true));
  q('collapseAll').addEventListener('click', () => pl.querySelectorAll('details').forEach(d => d.open = false));

  q('histFilters').addEventListener('click', e => {
    const b = e.target.closest('button[data-f]'); if (!b) return;
    _histFilter = b.dataset.f;
    q('histFilters').querySelectorAll('button').forEach(x => x.setAttribute('aria-pressed', String(x === b)));
    renderHistory();
  });

  q('slModalCancel').addEventListener('click', closeSlModal);
  q('slModalSave').addEventListener('click', saveSlModal);
  q('slModalOverlay').addEventListener('click', e => { if (e.target.id === 'slModalOverlay') closeSlModal(); });
  q('slModalInput').addEventListener('keydown', e => { if (e.key === 'Enter') saveSlModal(); if (e.key === 'Escape') closeSlModal(); });
});

// Relojes locales: duración de posiciones y cooldowns restantes
let _lastFull = 0;
setInterval(() => {
  const now = Date.now() / 1000;
  Object.entries(_posMeta).forEach(([s, m]) => setTxt('pdur_' + s, fmtDur(now - m.opened)));
  const el = (Date.now() - _lastFull) / 1000;
  document.querySelectorAll('[data-cd]').forEach(td => { const r = n(td.dataset.cd) - el; setTxt(td, r > 0 ? fmtDur(r) : 'liberado'); });
}, 1000);

// ── Bucle 1: precios en vivo ────────────────────────────────────────────────
let liveTimer = null, liveFails = 0;
async function livePoll() {
  const t0 = performance.now();
  try {
    const r = await fetch('/api/live', { cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    applyLive(await r.json());
    liveFails = 0; liveOk = true;
  } catch (e) { liveFails++; liveOk = false; }
  finally {
    setLamp(q('lampPoll'), (pollOk && liveOk) ? 'ok' : (pollOk || liveOk) ? 'warn' : 'bad');
    const spent = performance.now() - t0, backoff = liveFails ? Math.min(3000, liveFails * 300) : 0;
    liveTimer = setTimeout(livePoll, Math.max(0, livePollMs - spent) + backoff);
  }
}

// ── Bucle 2: estructura completa ────────────────────────────────────────────
let statusTimer = null, statusDelay = 1500, statusInFlight = false;
async function pollStatus() {
  if (statusInFlight) return;
  statusInFlight = true; _lastFull = Date.now();
  try {
    const r = await fetch('/api/status', { cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    render(await r.json());
    pollOk = true; statusDelay = statusPollMs;
  } catch (e) {
    pollOk = false; statusDelay = Math.min(statusDelay * 1.5, 15000);
    console.warn('Error leyendo /api/status:', e.message);
  } finally {
    statusInFlight = false;
    statusTimer = setTimeout(pollStatus, statusDelay);
  }
}
function requestFull(force) {
  if (statusInFlight) return;
  if (!force && Date.now() - _lastFull < 1000) return;
  clearTimeout(statusTimer);
  statusTimer = setTimeout(pollStatus, force ? 250 : 0);
}

livePoll();
pollStatus();
loadStats();
</script>
</body>
</html>
"""

# ─────────────────────────────────────────────────────────────────────────────
# RUTAS FLASK
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    resp = make_response(HTML)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/api/status")
def api_status():
    resp = jsonify(bot.snapshot())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/api/live")
def api_live():
    """Payload mínimo (precio, cambio y PnL) para el refresco en vivo del navegador."""
    resp = jsonify(bot.live_payload())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.post("/api/close/<symbol>")
def api_close(symbol: str):
    """Cierre manual de una posición abierta."""
    symbol = symbol.upper().strip()
    if not bot.loop or not bot.loop.is_running():
        return jsonify({"ok": False, "error": "Bot loop no está activo"}), 503
    future = asyncio.run_coroutine_threadsafe(
        bot.close_position_manual(symbol), bot.loop
    )
    try:
        ok = future.result(timeout=15)
    except concurrent.futures.TimeoutError:
        # La tarea sigue corriendo en el loop del bot; no la cancelamos para
        # no dejar el cierre a medias, pero avisamos con un mensaje claro.
        return jsonify({
            "ok": False,
            "error": "Tiempo de espera agotado cerrando posición (el cierre puede completarse en segundo plano, revisa en unos segundos)",
        }), 504
    except Exception as exc:
        msg = str(exc).strip() or f"{type(exc).__name__} (timeout esperando respuesta del exchange)"
        return jsonify({"ok": False, "error": msg}), 500
    if ok:
        return jsonify({"ok": True, "symbol": symbol, "msg": "Posición cerrada manualmente"})
    return jsonify({"ok": False, "symbol": symbol, "error": "Posición no encontrada o ya cerrada"}), 404


@app.post("/api/set-sl/<symbol>")
def api_set_sl(symbol: str):
    """Establece el stop loss (en USD, valor negativo) de una posición abierta."""
    symbol = symbol.upper().strip()
    data = request.get_json(silent=True) or {}
    try:
        sl_usd = float(data.get("sl_usd"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "sl_usd inválido"}), 400
    if sl_usd >= 0:
        return jsonify({"ok": False, "error": "sl_usd debe ser un valor negativo (pérdida)"}), 400
    ok = bot.set_stop_loss(symbol, sl_usd)
    if ok:
        return jsonify({"ok": True, "symbol": symbol, "sl_usd": sl_usd})
    return jsonify({"ok": False, "symbol": symbol, "error": "Posición no encontrada o cerrada"}), 404


@app.post("/api/force-close/<symbol>")
def api_force_close(symbol: str):
    """Cierre de EMERGENCIA: ignora el guard anti-doble-cierre y libera el
    símbolo aunque haya quedado 'pegado' en _closing_symbols por un timeout
    o deadlock previo. Úsalo solo si /api/close se queda atascado."""
    symbol = symbol.upper().strip()
    if not bot.loop or not bot.loop.is_running():
        return jsonify({"ok": False, "error": "Bot loop no está activo"}), 503

    # 1) liberar el guard sin esperar a que la tarea vieja termine
    with bot.lock:
        bot._closing_symbols.discard(symbol)

    future = asyncio.run_coroutine_threadsafe(
        bot.close_position_manual(symbol), bot.loop
    )
    try:
        ok = future.result(timeout=20)
    except concurrent.futures.TimeoutError:
        return jsonify({
            "ok": False,
            "error": "Tiempo de espera agotado en cierre forzado (revisa /api/status en unos segundos)",
        }), 504
    except Exception as exc:
        msg = str(exc).strip() or f"{type(exc).__name__} (timeout esperando respuesta del exchange)"
        return jsonify({"ok": False, "error": msg}), 500
    if ok:
        return jsonify({"ok": True, "symbol": symbol, "msg": "Posición cerrada (forzado)"})
    return jsonify({"ok": False, "symbol": symbol, "error": "Posición no encontrada o ya cerrada"}), 404


@app.get("/api/stats")
def api_stats():
    """Estadísticas agregadas de MFE / MAE de todas las operaciones cerradas."""
    resp = jsonify(bot.compute_stats())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/api/trades")
def api_trades():
    """Operaciones cerradas con su MFE/MAE (últimas N, por defecto 200)."""
    try:
        limit = max(1, min(5000, int(request.args.get("limit", "200"))))
    except ValueError:
        limit = 200
    with bot._stats_lock:
        rows = list(bot.trade_stats[-limit:])
    rows.reverse()
    resp = jsonify(rows)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/api/trades.csv")
def api_trades_csv():
    """Descarga CSV de todo el histórico (para analizarlo en Excel / pandas)."""
    cols = [
        "trade_id", "symbol", "direction", "reason", "opened_at_ts", "closed_at_ts",
        "duration_s", "avg_entry", "close_price", "qty", "notional", "fills_n", "levels",
        "sl_usd", "target", "pnl", "pnl_pct", "mfe_usd", "mae_usd", "mfe_pct", "mae_pct",
        "time_to_mfe_s", "time_to_mae_s", "low_price", "high_price",
    ]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(cols)
    with bot._stats_lock:
        rows = list(bot.trade_stats)
    for r in rows:
        out = []
        for c in cols:
            v = r.get(c, "")
            out.append("|".join(str(x) for x in v) if isinstance(v, list) else v)
        writer.writerow(out)
    resp = make_response(buf.getvalue())
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=trade_stats.csv"
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.post("/api/set-default-sl")
def api_set_default_sl():
    """Cambia el SL global (USD, negativo) sin reiniciar el bot."""
    data = request.get_json(silent=True) or {}
    try:
        sl_usd = float(data.get("sl_usd"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "sl_usd inválido"}), 400
    if sl_usd >= 0:
        return jsonify({"ok": False, "error": "sl_usd debe ser un valor negativo (pérdida)"}), 400
    if sl_usd < -100000:
        return jsonify({"ok": False, "error": "sl_usd fuera de rango"}), 400
    override = bool(data.get("override_manual", False))
    result = bot.set_default_stop_loss(sl_usd, override_manual=override)
    return jsonify({"ok": True, **result})


@app.post("/api/set-take-profit")
def api_set_take_profit():
    """Cambia el multiplicador del TP (fracción del notional) sin reiniciar el bot."""
    data = request.get_json(silent=True) or {}
    try:
        fraction = float(data.get("fraction"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "fraction inválido"}), 400
    try:
        result = bot.set_take_profit(fraction)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, **result})


@app.post("/api/set-ema")
def api_set_ema():
    """Cambia la EMA rápida y la lenta en caliente (recalcula con el historial guardado)."""
    data = request.get_json(silent=True) or {}
    try:
        fast, slow = int(data.get("fast")), int(data.get("slow"))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "fast y slow deben ser enteros"}), 400
    try:
        result = bot.set_ema_periods(fast, slow)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except RuntimeError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 503
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc) or type(exc).__name__}), 500
    return jsonify({"ok": True, **result})


@app.post("/api/close-all")
def api_close_all():
    """Cierra TODAS las posiciones abiertas, una tras otra. Responde al instante;
    el progreso llega en close_all de /api/status y /api/live."""
    res = bot.start_close_all()
    if res.get("ok"):
        return jsonify({"ok": True, "total": res["total"]})
    return jsonify({"ok": False, "error": res.get("error", "error")}), res.get("code", 500)


@app.get("/api/risk")
def api_risk():
    """Estado de las guardas de entrada (stop global, exposición, filtro BTC, pausa)."""
    resp = jsonify(bot.gate_view(full=True))
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.post("/api/set-risk")
def api_set_risk():
    """Cambia en caliente cualquier ajuste de riesgo. Cuerpo JSON parcial, p. ej.:
    {"global_stop_enabled": true, "global_stop_usd": -5}
    {"exposure_enabled": true, "max_exposure_usd": 800, "exposure_include_dca": false}
    {"btc_filter_enabled": true, "btc_filter_threshold": 0, "btc_filter_mode": "all"}
    {"btc_up_enabled": true, "btc_up_threshold": 0, "btc_up_mode": "short"}
    {"dca_ema_enabled": true, "dca_ema_period": 500, "dca_ema_after": 3}"""
    data = request.get_json(silent=True)
    try:
        result = bot.set_risk(data or {})
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, **result})


@app.post("/api/pause")
def api_pause():
    """Pausa ({"paused": true}) o reanuda ({"paused": false}) las entradas nuevas."""
    data = request.get_json(silent=True) or {}
    try:
        paused = _v_bool(data.get("paused", True))
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    reason = str(data.get("reason", "") or "").strip()[:120]
    return jsonify({"ok": True, **bot.set_pause(paused, reason or "Pausa manual")})


@app.get("/api/ladder")
def api_ladder():
    resp = jsonify(bot.ladder_view())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.post("/api/set-ladder")
def api_set_ladder():
    """Sustituye la escalera DCA: {"levels": [0, 2, 4, ...], "notionals": [5, 5, 10, ...]}.
    El tramo 1 debe tener 0 % (es la entrada del cruce EMA)."""
    data = request.get_json(silent=True) or {}
    try:
        result = bot.set_ladder(data.get("levels"), data.get("notionals"))
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, **result})


@app.post("/api/reset-ladder")
def api_reset_ladder():
    """Restaura la escalera de fábrica (DCA_ADVERSE_LEVELS / ENTRY_NOTIONALS)."""
    try:
        result = bot.reset_ladder()
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, **result})


@app.post("/api/close-all/cancel")
def api_close_all_cancel():
    """Detiene el cierre masivo tras la operación en curso."""
    if bot.cancel_close_all():
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "No hay un cierre masivo en curso"}), 409


@app.get("/api/close-all")
def api_close_all_status():
    resp = jsonify(bot.close_all_view())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/health")
def health():
    snap = bot.snapshot()
    return jsonify({
        "ok":                True,
        "running":           bot.running,
        "mode":              snap["mode"],
        "ws_connected":      snap.get("ws_connected", False),
        "scan_count":        snap["scan_count"],
        "eval_rate":         snap.get("eval_rate", 0),
        "latency_avg_ms":    snap.get("latency_avg_ms", 0),
        "all_symbols_count": snap["all_symbols_count"],
        "universe_count":    snap.get("universe_count", 0),
        "subscribed_count":  snap["subscribed_count"],
        "last_error":        snap["last_error"],
        "cooldown_count":    snap["cooldown_count"],
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
