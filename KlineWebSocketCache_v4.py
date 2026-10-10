"""
KlineWebSocketCache — v7  (detector de cruces EMA · hasta 1500 velas por símbolo)
================================================================================

CAMBIOS v7 (Render free: 512 MB de RAM y 0.1 CPU)
────────────────────────────────────────────────
  • El bucle del WebSocket ya no envuelve cada mensaje en asyncio.wait_for
    (≈800 mensajes/s con 200 símbolos = 800 temporizadores por segundo). El
    silencio del stream lo vigila una tarea aparte cada pocos segundos.
  • Con websockets ≥ 13 se reciben bytes (recv(decode=False)): solo se
    decodifican las velas cerradas, no las ~800 actualizaciones por segundo.
  • Tras un corte del WebSocket, cada símbolo descarga SOLO las velas que le
    faltan (1 petición de peso 1-2 y unos KB) en vez de re-sembrar 1500 velas
    (peso 10, ~200 KB y ~1.5 MB de objetos Python por símbolo, × 200 a la vez).
  • Si llegaba un hueco mientras el símbolo ya se estaba sembrando, el aviso se
    perdía y el símbolo podía quedarse sin estar listo para siempre: ahora se
    repite la siembra al terminar.
  • El nº de velas guardadas NO depende de los periodos: siempre son hasta
    max_candles cierres por símbolo (≈2.4 MB para 200 símbolos), igual con
    EMA 100/200 que con 200/500.

QUÉ HACE
────────
  • Sigue las N (200) criptos USDT-M más activas (por volumen 24h).
  • Por cada símbolo guarda los últimos `max_candles` CIERRES (máximo 1500,
    límite de Binance por petición) en un array('d') de 8 bytes por vela:
    200 símbolos × 1500 velas ≈ 2.4 MB en total.
  • Las EMA rápida/lenta se mantienen de forma incremental (O(1) por vela
    cerrada) y se pueden CAMBIAR EN CALIENTE con set_periods(fast, slow):
    se recalculan al instante desde los cierres guardados, sin volver a
    descargar nada y sin disparar cruces falsos.
  • Cuando en una vela CERRADA la EMA rápida cruza la lenta, dispara el
    callback  cb(symbol, "UP" | "DOWN", close_price, close_time_ms).
        UP   = EMA rápida cruza hacia ARRIBA de la EMA lenta
        DOWN = EMA rápida cruza hacia ABAJO  de la EMA lenta

PERIODO MÁXIMO
──────────────
  Para que una EMA sea fiable necesita ~3 veces su periodo de historia, así
  que con 1500 velas el periodo máximo admitido es 500 (max_period).

FLUJO
─────
  1. Universo: provider() devuelve símbolos ordenados por actividad (se
     refresca cada universe_refresh_seconds). Histéresis: un símbolo ya
     seguido no sale hasta caer del puesto top_n + rank_margin, y los
     símbolos "pinned" (posiciones abiertas) nunca salen.
  2. Warm-up por REST: UNA petición (limit=max_candles, peso 10) por símbolo
     nuevo para llenar el historial de cierres y sembrar las EMA.
  3. WebSocket único con SUBSCRIBE/UNSUBSCRIBE en caliente. Solo se
     parsean los mensajes con x=true (vela cerrada).
  4. Si falta una vela (corte de red) se detecta por el hueco en
     open_time y SOLO ese símbolo descarga por REST las velas que le faltan
     (o, si el hueco es muy grande, se re-siembra entero).
  5. Todo el estado se modifica únicamente desde el hilo/loop del cache
     (también set_periods), así que no hay condiciones de carrera.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import random
import threading
import time
from array import array
from typing import Callable, Dict, List, Optional, Set, Tuple

import aiohttp
import websockets


# =============================================================================
# TOKEN BUCKET (rate-limiter REST)
# =============================================================================

class _TokenBucket:
    def __init__(self, capacity: int = 1_200, refill_rate: float = 20.0) -> None:
        self.capacity    = capacity
        self.refill_rate = refill_rate
        self._tokens     = float(capacity)
        self._last       = time.monotonic()
        self._lock       = asyncio.Lock()

    @staticmethod
    def weight_for_limit(limit: int) -> int:
        if limit <= 100:  return 1
        if limit <= 500:  return 2
        if limit <= 1000: return 5
        return 10

    async def acquire(self, weight: int = 1) -> None:
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(self.capacity,
                               self._tokens + (now - self._last) * self.refill_rate)
            self._last = now
            if self._tokens >= weight:
                self._tokens -= weight
                return
            wait = (weight - self._tokens) / self.refill_rate
            await asyncio.sleep(wait)
            self._tokens = 0.0


# =============================================================================
# ESTADO POR SÍMBOLO (6 campos, sin velas)
# =============================================================================

class _EmaState:
    """Historial de cierres (máx. max_candles) + EMA actuales de un símbolo."""
    __slots__ = ("closes", "fast", "slow", "sign", "last_ot", "ready")

    def __init__(self) -> None:
        self.closes  = array("d")   # últimos cierres, el más reciente al final
        self.fast    = 0.0
        self.slow    = 0.0
        self.sign    = 0      # último signo NO cero de (fast - slow)
        self.last_ot = 0      # open_time (ms) de la última vela procesada
        self.ready   = False  # False mientras se siembra por REST

    @property
    def n(self) -> int:       # velas disponibles en el historial
        return len(self.closes)


SignalCallback = Callable[[str, str, float, int], None]


# =============================================================================
# CLASE PRINCIPAL
# =============================================================================

class KlineWebSocketCache:
    """Detector de cruces EMA fast/slow sobre las N criptos más activas."""

    BASE_REST_URL = "https://fapi.binance.com"
    MAX_KLINES    = 1500        # máximo de velas por petición REST de Binance
    GAP_FILL_MAX  = 300         # huecos de hasta 300 velas se rellenan sin re-sembrar todo
    # Mismo endpoint que usa WS.py (por defecto). Sobrescribible con KLINE_WS_URL.
    DEFAULT_WS_URL = os.environ.get("KLINE_WS_URL", "wss://fstream.binance.com/market/stream")

    def __init__(
        self,
        *,
        interval: str = "1m",
        top_n: int = 200,
        fast_period: int = 100,
        slow_period: int = 200,
        max_candles: int = 1500,
        min_candles: Optional[int] = None,
        universe_provider: Optional[Callable[[], List[str]]] = None,
        pinned_provider: Optional[Callable[[], List[str]]] = None,
        universe_refresh_seconds: float = 900.0,
        rank_margin: int = 50,
        max_signal_age_seconds: float = 20.0,
        silence_threshold_seconds: float = 60.0,
        rest_concurrency: int = 4,
        rest_timeout: float = 8.0,
        rest_retries: int = 4,
        ws_url: Optional[str] = None,
    ) -> None:
        self.interval       = interval
        self.top_n          = int(top_n)
        # Historial por símbolo: máximo 1500 (límite de Binance por petición)
        self.max_candles    = max(50, min(int(max_candles), self.MAX_KLINES))
        # Con ~3× el periodo de historia la EMA ya es fiable
        self.max_period     = max(2, self.max_candles // 3)
        self._fixed_min     = int(min_candles) if min_candles else None
        self.fast_period    = 0
        self.slow_period    = 0
        self._af = self._as = 0.0
        self._apply_periods_values(int(fast_period), int(slow_period))
        self._iv_ms = self._interval_ms(interval)

        self._provider        = universe_provider
        self._pinned_provider = pinned_provider
        self.universe_refresh_s = float(universe_refresh_seconds)
        self.rank_margin      = int(rank_margin)
        self.max_signal_age_ms = float(max_signal_age_seconds) * 1000.0
        self.silence_s        = float(silence_threshold_seconds)
        self.rest_concurrency = int(rest_concurrency)
        self.rest_timeout     = float(rest_timeout)
        self.rest_retries     = int(rest_retries)
        self.ws_url           = ws_url or self.DEFAULT_WS_URL

        # Estado
        self._states:  Dict[str, _EmaState] = {}
        self._pending: Dict[str, List[Tuple[int, float, int]]] = {}
        self._warming: Set[str] = set()
        self._rewarm:  Set[str] = set()     # pidieron siembra mientras ya había una en curso
        self._signal_cb: Optional[SignalCallback] = None

        # WS
        self._ws = None
        self._subscribed: Set[str] = set()
        self._req_id = 0
        self._last_msg = 0.0
        self._connected = False
        self._close_reason = ""
        self._sub_lock: Optional[asyncio.Lock] = None

        # REST
        self._session: Optional[aiohttp.ClientSession] = None
        self._bucket:  Optional[_TokenBucket] = None
        self._sem:     Optional[asyncio.Semaphore] = None
        self._pause_until = 0.0

        # Métricas
        self.closed_candles = 0
        self.signals        = 0
        self.stale_signals  = 0
        self.gap_resyncs    = 0
        self.gap_fills      = 0         # huecos rellenados descargando solo lo que faltaba
        self.full_warmups   = 0         # siembras completas (1500 velas)
        self.reconnects     = 0
        self.last_error     = ""

        # Infra
        self._loop:   Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._tasks:  Set[asyncio.Task] = set()

    # ─────────────────────────────────────────────────────────────────────
    # Utilidades
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _interval_ms(interval: str) -> int:
        units = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
        n = int("".join(c for c in interval if c.isdigit()))
        u = "".join(c for c in interval if c.isalpha())
        return n * units.get(u, 60_000)

    # ─────────────────────────────────────────────────────────────────────
    # Periodos de EMA (cambiables en caliente)
    # ─────────────────────────────────────────────────────────────────────

    @property
    def min_candles(self) -> int:
        """Velas mínimas para aceptar un cruce (por defecto = periodo lento)."""
        return self._fixed_min or self.slow_period

    def validate_periods(self, fast: int, slow: int) -> Tuple[int, int]:
        try:
            fast, slow = int(fast), int(slow)
        except (TypeError, ValueError):
            raise ValueError("Los periodos deben ser números enteros")
        if fast < 2:
            raise ValueError("La EMA rápida debe ser ≥ 2")
        if fast >= slow:
            raise ValueError("La EMA rápida debe ser menor que la EMA lenta")
        if slow > self.max_period:
            raise ValueError(
                f"La EMA lenta no puede superar {self.max_period} "
                f"(con {self.max_candles} velas guardadas por símbolo)")
        return fast, slow

    def _apply_periods_values(self, fast: int, slow: int) -> None:
        fast, slow = self.validate_periods(fast, slow)
        self.fast_period = fast
        self.slow_period = slow
        self._af = 2.0 / (fast + 1)
        self._as = 2.0 / (slow + 1)

    def _recompute(self, st: _EmaState) -> None:
        """Recalcula fast/slow/sign desde el historial de cierres (sin emitir cruces)."""
        closes = st.closes
        if not closes:
            st.fast = st.slow = 0.0
            st.sign = 0
            return
        af, as_ = self._af, self._as
        fast = slow = closes[0]
        sign = 0
        for i in range(1, len(closes)):
            c = closes[i]
            fast += af * (c - fast)
            slow += as_ * (c - slow)
            d = fast - slow
            if d > 0.0:
                sign = 1
            elif d < 0.0:
                sign = -1
        st.fast, st.slow, st.sign = fast, slow, sign

    def _apply_periods_now(self, fast: int, slow: int) -> int:
        """Aplica los periodos y recalcula todos los símbolos. Debe ejecutarse
        en el loop del cache (o con el cache parado). Devuelve nº recalculados."""
        self._apply_periods_values(fast, slow)
        done = 0
        for st in self._states.values():
            if st.ready and st.closes:
                self._recompute(st)
                done += 1
        return done

    def set_periods(self, fast: int, slow: int) -> int:
        """Cambia EMA rápida/lenta EN CALIENTE. Recalcula con los cierres
        guardados (no descarga nada) y NO emite cruces por el cambio.
        Lanza ValueError si los valores no son válidos."""
        fast, slow = self.validate_periods(fast, slow)          # valida ya, en el hilo llamador
        loop = self._loop
        if self._running and loop is not None and loop.is_running():
            async def _do() -> int:
                return self._apply_periods_now(fast, slow)
            fut = asyncio.run_coroutine_threadsafe(_do(), loop)
            return fut.result(timeout=15)
        return self._apply_periods_now(fast, slow)

    def set_signal_callback(self, cb: Optional[SignalCallback]) -> None:
        """cb(symbol, "UP"|"DOWN", close_price, close_time_ms). Se llama desde
        el hilo del WS: debe ser instantáneo (encolar y salir)."""
        self._signal_cb = cb

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    # ─────────────────────────────────────────────────────────────────────
    # EMA incremental + detección de cruce  (núcleo, O(1))
    # ─────────────────────────────────────────────────────────────────────

    def _push_close(self, st: _EmaState, close: float) -> None:
        """Añade un cierre al historial (máx. max_candles)."""
        st.closes.append(close)
        if len(st.closes) > self.max_candles:
            del st.closes[0]

    def _step(self, st: _EmaState, close: float) -> int:
        """Procesa una vela cerrada: guarda el cierre y actualiza las EMA.
        Devuelve +1 (cruce hacia arriba), -1 (hacia abajo) o 0 (sin cruce)."""
        first = not st.closes
        self._push_close(st, close)
        if first:
            st.fast = st.slow = close
        else:
            st.fast += self._af * (close - st.fast)
            st.slow += self._as * (close - st.slow)
        d   = st.fast - st.slow
        new = 1 if d > 0.0 else (-1 if d < 0.0 else 0)
        cross = 0
        if new != 0:
            if st.sign != 0 and new != st.sign:
                cross = new
            st.sign = new
        return cross

    def _on_closed(self, sym: str, ot: int, close: float, ct: int) -> None:
        st = self._states.get(sym)
        if st is None:
            return
        if not st.ready:
            pend = self._pending.get(sym)
            if pend is not None and len(pend) < 8:
                pend.append((ot, close, ct))
            return
        if ot <= st.last_ot:
            return                                   # duplicada / desordenada
        if st.last_ot and ot > st.last_ot + self._iv_ms:
            # Hueco: se perdió ≥1 vela. Re-sembrar SOLO este símbolo.
            st.ready = False
            self._pending[sym] = [(ot, close, ct)]
            self.gap_resyncs += 1
            self._schedule_warmup(sym)
            return
        st.last_ot = ot
        cross = self._step(st, close)
        if cross and st.n >= self.min_candles:
            self._emit(sym, cross, close, ct)

    def _emit(self, sym: str, cross: int, close: float, ct: int) -> None:
        self.signals += 1
        if (time.time() * 1000.0 - ct) > self.max_signal_age_ms:
            self.stale_signals += 1                  # cruce de una vela vieja: no es "recién"
            return
        cb = self._signal_cb
        if cb is None:
            return
        try:
            cb(sym, "UP" if cross > 0 else "DOWN", close, ct)
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────────────────
    # REST
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch(self, url: str, params: dict, weight: int):
        attempt = 0
        while True:
            wait = self._pause_until - time.time()
            if wait > 0:
                await asyncio.sleep(wait)
            await self._bucket.acquire(weight)
            try:
                timeout = aiohttp.ClientTimeout(total=self.rest_timeout)
                async with self._session.get(url, params=params, timeout=timeout) as resp:
                    if resp.status in (418, 429):
                        default = 300.0 if resp.status == 418 else 60.0
                        ra = float(resp.headers.get("Retry-After", default))
                        self._pause_until = time.time() + ra + random.uniform(2, 10)
                        print(f"🚫 Kline REST pausado {ra:.0f}s (HTTP {resp.status})")
                        attempt += 1
                        if attempt > self.rest_retries:
                            raise RuntimeError(f"HTTP {resp.status} persistente")
                        continue
                    resp.raise_for_status()
                    return await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                attempt += 1
                if attempt > self.rest_retries:
                    raise
                await asyncio.sleep(min(30.0, 0.5 * (2 ** attempt)) + random.uniform(0, 0.5))

    # ─────────────────────────────────────────────────────────────────────
    # Warm-up (siembra de EMA; las velas se descartan)
    # ─────────────────────────────────────────────────────────────────────

    def _schedule_warmup(self, sym: str) -> None:
        if not self._running or sym not in self._states:
            return
        if sym in self._warming:
            # Ya hay una siembra en curso: se repite al terminar si el símbolo sigue
            # sin estar listo (antes este aviso se perdía y el símbolo se atascaba).
            self._rewarm.add(sym)
            return
        self._warming.add(sym)
        self._pending.setdefault(sym, [])
        self._spawn(self._warmup(sym))

    async def _gap_fill(self, sym: str, st: _EmaState) -> bool:
        """Rellena un hueco corto descargando SOLO las velas que faltan (peso 1-2,
        unos KB) en vez de re-sembrar 1500 velas (peso 10, ~200 KB y ~1.5 MB de
        objetos Python por símbolo; tras un corte del WS, para los ~200 a la vez).
        Las EMA siguen de forma incremental, como si no se hubiera perdido nada.
        Devuelve False si hace falta la siembra completa."""
        pend = self._pending.get(sym) or ()
        newest = max((p[0] for p in pend), default=0)
        if newest <= st.last_ot:
            return False
        missing = (newest - st.last_ot) // self._iv_ms     # incluye la vela que destapó el hueco
        if missing > self.GAP_FILL_MAX:
            return False
        limit  = int(missing) + 2
        params = {"symbol": sym, "interval": self.interval,
                  "startTime": st.last_ot + self._iv_ms, "limit": limit}
        async with self._sem:
            data = await self._fetch(
                f"{self.BASE_REST_URL}/fapi/v1/klines", params,
                _TokenBucket.weight_for_limit(limit),
            )
        if self._states.get(sym) is not st:
            return True                                  # salió del universo mientras se descargaba
        now_ms = int(time.time() * 1000)
        applied = 0
        for k in data:
            ot, ct = int(k[0]), int(k[6])
            if ct >= now_ms:
                break                                    # vela aún abierta
            if ot <= st.last_ot:
                continue
            if ot != st.last_ot + self._iv_ms:
                return False                             # el REST también tiene hueco: siembra completa
            close = float(k[4])
            st.last_ot = ot
            applied += 1
            cross = self._step(st, close)
            if cross and st.n >= self.min_candles:
                self._emit(sym, cross, close, ct)        # _emit descarta los cruces de velas viejas
        if not applied:
            return False
        st.ready = True
        self.gap_fills += 1
        for ot, c, ct in self._pending.pop(sym, []):     # velas llegadas mientras tanto
            self._on_closed(sym, ot, c, ct)
        return True

    async def _warmup(self, sym: str) -> None:
        retry = False
        try:
            st = self._states.get(sym)
            if st is not None and st.closes and st.last_ot:
                if await self._gap_fill(sym, st):
                    return
            limit  = self.max_candles
            params = {"symbol": sym, "interval": self.interval, "limit": limit}
            async with self._sem:
                data = await self._fetch(
                    f"{self.BASE_REST_URL}/fapi/v1/klines", params,
                    _TokenBucket.weight_for_limit(limit),
                )
            if sym not in self._states:
                return                               # salió del universo mientras se descargaba

            now_ms = int(time.time() * 1000)
            new = _EmaState()
            for k in data:
                if int(k[6]) >= now_ms:              # vela aún abierta
                    continue
                new.last_ot = int(k[0])
                new.closes.append(float(k[4]))
            del data
            if len(new.closes) > self.max_candles:   # por seguridad
                del new.closes[:len(new.closes) - self.max_candles]
            self._recompute(new)                     # EMA con los periodos ACTUALES
            new.ready = len(new.closes) > 0
            self._states[sym] = new
            self.full_warmups += 1

            for ot, c, ct in self._pending.pop(sym, []):   # velas llegadas durante la siembra
                self._on_closed(sym, ot, c, ct)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.last_error = f"warmup {sym}: {e}"
            print(f"⚠️  Warm-up {sym} falló: {e}")
            retry = True
        finally:
            self._warming.discard(sym)
            again = sym in self._rewarm
            self._rewarm.discard(sym)
            st = self._states.get(sym)
            if self._running and st is not None and not st.ready:
                if retry:
                    self._loop.call_later(15.0, self._schedule_warmup, sym)
                elif again:
                    self._loop.call_later(1.0, self._schedule_warmup, sym)

    # ─────────────────────────────────────────────────────────────────────
    # Universo (top N más activas)
    # ─────────────────────────────────────────────────────────────────────

    async def _load_ranked(self) -> List[str]:
        if self._provider is not None:
            try:
                return [s.upper() for s in await asyncio.to_thread(self._provider)]
            except Exception as e:
                print(f"⚠️  universe_provider falló: {e}")
                return []
        # Sin provider: REST (peso 40, una vez por refresco)
        try:
            data = await self._fetch(f"{self.BASE_REST_URL}/fapi/v1/ticker/24hr", {}, 40)
        except Exception as e:
            print(f"⚠️  ticker/24hr falló: {e}")
            return []
        rows = [(d["symbol"], float(d.get("quoteVolume", 0) or 0)) for d in data
                if d.get("symbol", "").endswith("USDT") and "_" not in d["symbol"]]
        rows.sort(key=lambda r: r[1], reverse=True)
        return [s for s, _ in rows]

    async def _refresh_universe(self) -> bool:
        ranked = await self._load_ranked()
        if not ranked:
            return False

        top       = ranked[: self.top_n]
        keep_zone = set(ranked[: self.top_n + self.rank_margin])
        pinned: Set[str] = set()
        if self._pinned_provider is not None:
            try:
                pinned = {s.upper() for s in self._pinned_provider()}
            except Exception:
                pinned = set()

        wanted = set(top) | pinned | {s for s in self._states if s in keep_zone}
        cur    = set(self._states)
        to_add = sorted(wanted - cur)
        to_del = sorted(cur - wanted)

        for s in to_del:
            self._states.pop(s, None)
            self._pending.pop(s, None)
        for s in to_add:
            self._states[s] = _EmaState()
            self._schedule_warmup(s)

        await self._reconcile()
        if to_add or to_del or not cur:
            print(f"🔄 Universo EMA: {len(self._states)} símbolos "
                  f"(+{len(to_add)} / -{len(to_del)})")
        return True

    async def _universe_loop(self) -> None:
        while self._running:
            ok = False
            try:
                ok = await self._refresh_universe()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = f"universo: {e}"
                print(f"⚠️  Refresco de universo: {e}")
            await asyncio.sleep(self.universe_refresh_s if ok else 5.0)

    # ─────────────────────────────────────────────────────────────────────
    # WebSocket
    # ─────────────────────────────────────────────────────────────────────

    def _stream_name(self, sym: str) -> str:
        return f"{sym.lower()}@kline_{self.interval}"

    async def _reconcile(self) -> None:
        """Alinea las suscripciones del socket con el universo actual
        (SUBSCRIBE/UNSUBSCRIBE en caliente, ≤ ~5 msg/s)."""
        if self._sub_lock is None:
            self._sub_lock = asyncio.Lock()
        async with self._sub_lock:
            ws = self._ws
            if ws is None:
                return
            wanted = {self._stream_name(s) for s in list(self._states)}
            to_add = sorted(wanted - self._subscribed)
            to_del = sorted(self._subscribed - wanted)
            try:
                for method, items in (("SUBSCRIBE", to_add), ("UNSUBSCRIBE", to_del)):
                    for i in range(0, len(items), 50):
                        if self._ws is not ws:
                            return
                        part = items[i:i + 50]
                        self._req_id += 1
                        await ws.send(json.dumps(
                            {"method": method, "params": part, "id": self._req_id}))
                        if method == "SUBSCRIBE":
                            self._subscribed.update(part)
                        else:
                            self._subscribed.difference_update(part)
                        await asyncio.sleep(0.2)
            except Exception as e:
                print(f"⚠️  Reconcile WS: {e}")        # la reconexión re-suscribe todo

    def _handle_raw(self, raw) -> None:
        # Filtro barato: solo velas cerradas. Evita parsear ~99% de mensajes.
        if '"x":true' not in raw:
            return
        self._parse_closed(raw)

    def _handle_raw_bytes(self, raw) -> None:
        """Igual que _handle_raw, con los bytes del socket sin decodificar."""
        try:
            if b'"x":true' not in raw:
                return
        except TypeError:                            # llegó texto: se trata como texto
            self._handle_raw(raw)
            return
        self._parse_closed(raw)

    def _parse_closed(self, raw) -> None:
        try:
            ev = json.loads(raw).get("data")         # json.loads acepta str y bytes
            if not ev or ev.get("e") != "kline":
                return
            k = ev["k"]
            self.closed_candles += 1
            self._on_closed(ev["s"], int(k["t"]), float(k["c"]), int(k["T"]))
        except Exception:
            pass

    @staticmethod
    def _recv_bytes_ok(ws) -> bool:
        """websockets ≥ 13 admite recv(decode=False): entrega bytes sin pasarlos a
        texto, así solo se decodifican las velas cerradas."""
        try:
            return "decode" in inspect.signature(ws.recv).parameters
        except (TypeError, ValueError):
            return False

    async def _watchdog(self, ws) -> None:
        """Detecta un stream silencioso sin crear un temporizador por mensaje
        (antes cada recv iba dentro de asyncio.wait_for)."""
        step = max(1.0, min(10.0, self.silence_s / 6.0))
        while self._running and self._ws is ws:
            await asyncio.sleep(step)
            quiet = time.time() - self._last_msg
            if quiet > self.silence_s:
                self._close_reason = f"stream silencioso ({quiet:.0f}s sin mensajes)"
                try:
                    await ws.close()
                except Exception:
                    pass
                return

    async def _ws_loop(self) -> None:
        delay = 1.0
        while self._running:
            dog: Optional[asyncio.Task] = None
            opened = 0.0
            self._close_reason = ""
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20, ping_timeout=20, close_timeout=5,
                    max_size=2 ** 20, max_queue=512, compression=None,
                ) as ws:
                    self._ws = ws
                    self._subscribed = set()
                    self._last_msg = time.time()
                    await self._reconcile()
                    self._connected = True
                    opened = time.time()
                    print(f"✅ Kline WS conectado — {len(self._subscribed)} streams")
                    dog = asyncio.get_running_loop().create_task(self._watchdog(ws))

                    # Bucle caliente (~800 mensajes/s con 200 símbolos): sin wait_for.
                    if self._recv_bytes_ok(ws):
                        handle = self._handle_raw_bytes
                        while self._running:
                            raw = await ws.recv(decode=False)
                            self._last_msg = time.time()
                            handle(raw)
                    else:
                        handle = self._handle_raw
                        while self._running:
                            raw = await ws.recv()
                            self._last_msg = time.time()
                            handle(raw)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.reconnects += 1
                if opened and time.time() - opened >= 60.0:
                    delay = 1.0      # estuvo sana un rato: se reintenta enseguida
                # (si se cae nada más conectar, la espera sigue creciendo hasta 30 s
                #  en vez de reconectar cada segundo)
                why = self._close_reason or str(e) or type(e).__name__
                self.last_error = f"ws: {why}"
                print(f"🔴 Kline WS: {why} — reconectando en {delay:.1f}s")
            finally:
                if dog is not None:
                    dog.cancel()
                self._ws = None
                self._connected = False
            if not self._running:
                break
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 30.0)

    # ─────────────────────────────────────────────────────────────────────
    # Ciclo de vida
    # ─────────────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        loop = asyncio.new_event_loop()
        self._loop = loop
        thread = threading.Thread(
            target=lambda: (asyncio.set_event_loop(loop), loop.run_forever()),
            daemon=True, name="KlineEMALoop",
        )
        thread.start()
        self._thread = thread

        async def _startup() -> None:
            self._bucket = _TokenBucket()
            self._sem    = asyncio.Semaphore(self.rest_concurrency)
            self._sub_lock = asyncio.Lock()
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=self.rest_concurrency * 2,
                                               keepalive_timeout=30))
            self._spawn(self._ws_loop())
            self._spawn(self._universe_loop())
            print(f"🚀 KlineEMA v7: EMA{self.fast_period}/EMA{self.slow_period} "
                  f"· {self.interval} · top {self.top_n} más activas "
                  f"· {self.max_candles} velas/símbolo")

        asyncio.run_coroutine_threadsafe(_startup(), loop)

    async def _shutdown_async(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None

    def stop(self) -> None:
        self._running = False
        loop = self._loop
        if loop and loop.is_running():
            fut = asyncio.run_coroutine_threadsafe(self._shutdown_async(), loop)
            try:
                fut.result(timeout=10)
            except Exception:
                pass
            loop.call_soon_threadsafe(loop.stop)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        self._loop = None
        self._thread = None
        self._states.clear()
        self._pending.clear()
        self._warming.clear()
        self._rewarm.clear()
        self._subscribed.clear()
        print("✅ KlineEMA detenido")

    # ─────────────────────────────────────────────────────────────────────
    # Consulta
    # ─────────────────────────────────────────────────────────────────────

    def symbols(self) -> List[str]:
        return sorted(self._states)

    def get_ema(self, symbol: str) -> Optional[Tuple[float, float, int]]:
        """(ema_rápida, ema_lenta, signo) o None si no está seguido / no listo."""
        st = self._states.get(symbol.upper())
        if st is None or not st.ready:
            return None
        return st.fast, st.slow, st.sign

    def get_stats(self) -> dict:
        states = list(self._states.values())
        return {
            "fast_period":       self.fast_period,
            "slow_period":       self.slow_period,
            "max_period":        self.max_period,
            "max_candles":       self.max_candles,
            "stored_candles":    sum(s.n for s in states),
            "tracked_symbols":   len(states),
            "ready_symbols":     sum(1 for s in states if s.ready and s.n >= self.min_candles),
            "warming":           len(self._warming),
            "closed_candles":    self.closed_candles,
            "signals":           self.signals,
            "stale_signals":     self.stale_signals,
            "gap_resyncs":       self.gap_resyncs,
            "gap_fills":         self.gap_fills,
            "full_warmups":      self.full_warmups,
            "connected":         self._connected,
            "reconnects":        self.reconnects,
            "last_error":        self.last_error,
        }


# =============================================================================
# PRUEBA RÁPIDA
# =============================================================================

if __name__ == "__main__":
    def _cb(sym: str, direction: str, price: float, ct: int) -> None:
        print(f"⚡ CRUCE {direction:4s} {sym:12s} px={price}")

    cache = KlineWebSocketCache(top_n=200)
    cache.set_signal_callback(_cb)
    cache.start()
    try:
        while True:
            time.sleep(30)
            print(cache.get_stats())
    except KeyboardInterrupt:
        cache.stop()
