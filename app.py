from __future__ import annotations
import asyncio
import concurrent.futures
import csv
import gzip
import hashlib
import http.client
import io
import hmac
import json
import os
import re
import socket
import sys
import tempfile
import threading
import time
import zlib
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from math import floor
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlsplit
import urllib.error
import urllib.request
import urllib.response

from flask import Flask, Response, jsonify, make_response, render_template_string, request

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)


# ─────────────────────────────────────────────────────────────────────────────
# MEMORIA (Render free: 512 MB de RAM y 0.1 CPU)
# ─────────────────────────────────────────────────────────────────────────────
# Si el proceso pasa de 512 MB, Render lo mata y lo reinicia, y con el reinicio
# se pierden las posiciones (viven en memoria) y los ajustes hechos desde la web
# (el disco de Render free se borra en cada reinicio).
# glibc (el malloc de Linux) puede reservar hasta 8 "arenas" por núcleo, y en un
# contenedor con la CPU recortada (0.1 en Render) sigue contando los núcleos de la
# máquina. Con varios hilos (WS de precios, velas, bot, servidor web) creando y
# soltando JSON grandes cada segundo (!ticker@arr ~200 KB, /api/status ~60 KB,
# 200 descargas de 1500 velas…), la memoria liberada puede quedarse repartida en
# arenas sin volver al sistema y el RSS crecer sin que Python tenga ninguna fuga.
# Se previene al arrancar, antes de crear hilos:
#   • MALLOC_ARENAS  (2):   máximo de arenas.
#   • MALLOC_MMAP_KB (128): los bloques ≥ 128 KB van siempre a mmap y vuelven al
#     sistema al liberarse (si no, glibc sube ese umbral solo y se quedan).
#   • MEM_TRIM_SECS  (60):  malloc_trim(0) devuelve al sistema la memoria libre.
# El bot vigila su RSS: lo muestra en el panel y en /health, lo apunta en el log
# cada MEM_LOG_MIN minutos y avisa si pasa de MEM_WARN_MB.
MEM_LIMIT_MB  = float(os.getenv("MEM_LIMIT_MB", "512"))
MEM_WARN_MB   = float(os.getenv("MEM_WARN_MB", "400"))
MEM_TRIM_SECS = float(os.getenv("MEM_TRIM_SECS", "60"))
MEM_LOG_MIN   = float(os.getenv("MEM_LOG_MIN", "10"))


def _load_libc():
    if not sys.platform.startswith("linux"):
        return None
    try:
        import ctypes
        try:
            return ctypes.CDLL("libc.so.6")
        except OSError:
            import ctypes.util
            name = ctypes.util.find_library("c")
            return ctypes.CDLL(name) if name else None
    except Exception:
        return None


_LIBC = _load_libc()


def _tune_malloc() -> str:
    """Limita las arenas de glibc y fija el umbral de mmap (ver arriba)."""
    if _LIBC is None or not hasattr(_LIBC, "mallopt"):
        return "malloc sin ajustar (no es glibc)"
    done: List[str] = []
    try:
        arenas = int(os.getenv("MALLOC_ARENAS", "2"))
        if arenas > 0 and _LIBC.mallopt(-8, arenas) == 1:            # M_ARENA_MAX
            done.append(f"máx. {arenas} arenas")
        mmap_kb = int(os.getenv("MALLOC_MMAP_KB", "128"))
        if mmap_kb > 0 and _LIBC.mallopt(-3, mmap_kb * 1024) == 1:    # M_MMAP_THRESHOLD
            done.append(f"bloques ≥ {mmap_kb} KB por mmap")
    except Exception as exc:
        return f"ajuste de malloc falló: {exc}"
    return "malloc: " + (", ".join(done) if done else "sin cambios")


_MALLOC_NOTE = _tune_malloc()
try:
    _PAGE_SIZE = int(os.sysconf("SC_PAGE_SIZE"))
except Exception:
    _PAGE_SIZE = 4096


def _malloc_trim() -> bool:
    """Devuelve al sistema la memoria libre que glibc tenga guardada."""
    if _LIBC is None:
        return False
    try:
        return bool(_LIBC.malloc_trim(0))
    except Exception:
        return False


def _rss_mb() -> float:
    """Memoria real (RSS) del proceso en MB."""
    try:
        with open("/proc/self/statm", "rb") as fh:
            return int(fh.read().split()[1]) * _PAGE_SIZE / 1048576.0
    except Exception:
        pass
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0   # pico (Linux: KB)
    except Exception:
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# PROXIES DE ARRANQUE PARA EL REST DE BINANCE (primeras PROXY_BOOTSTRAP_HOURS)
# ─────────────────────────────────────────────────────────────────────────────
# Problema: al arrancar, Binance suele tener bloqueada la IP del servidor
# (HTTP 418) y la descarga de velas se queda esperando horas.
# Solución: durante las primeras PROXY_BOOTSTRAP_HOURS (4 h por defecto) las
# peticiones REST PÚBLICAS a Binance (velas, exchangeInfo…) pueden salir por
# los proxies de PROXY_URLS (separados por comas), rotándolos:
#   • PROXY_MODE=auto (por defecto): sale directo mientras la IP del servidor
#     responda; en cuanto Binance la bloquea o la limita (418/429), la rechaza
#     (403/451) o su peso del minuto se acerca al tope, la petición sale por el
#     siguiente proxy. Así no se gasta cuota de los proxies si no hace falta.
#   • PROXY_MODE=always: durante la ventana todo sale por los proxies (la IP
#     del servidor solo se usa si ningún proxy responde).
# Pasada la ventana, todo vuelve a salir directo aunque la IP esté bloqueada.
# Nunca pasan por un proxy las peticiones firmadas (órdenes, cuenta) ni las que
# no son GET. Se instala ANTES de importar WS y KlineWebSocketCache_v4 y cubre
# urllib, requests y aiohttp: también enruta la descarga de velas de esos módulos.

_PROXY_URLS_RAW       = os.getenv("PROXY_URLS", "http://fixie:7xOistPTXaKiKbh@ventoux.usefixie.com:80,http://fixie:TRPp7JUSFpzGPQn@ventoux.usefixie.com:80,http://fixie:CuLSweHyTOG4Lg3@ventoux.usefixie.com:80,http://fixie:wg6P9WLEMevEurg@ventoux.usefixie.com:80")
PROXY_BOOTSTRAP_HOURS = max(0.0, float(os.getenv("PROXY_BOOTSTRAP_HOURS", "4") or 0))
PROXY_MODE            = (os.getenv("PROXY_MODE", "auto") or "auto").strip().lower()
PROXY_WEIGHT_LIMIT    = int(os.getenv("PROXY_WEIGHT_LIMIT", "2000"))   # Binance Futures: 2400 de peso/min por IP
PROXY_TIMEOUT_S       = float(os.getenv("PROXY_TIMEOUT_S", "30"))
# Pasadas las PROXY_BOOTSTRAP_HOURS: true = el REST sale directo, pero si Binance
# bloquea la IP del servidor (418/429) se usan los proxies hasta que se libere.
PROXY_FALLBACK        = (os.getenv("PROXY_FALLBACK", "true") or "true").strip().lower() in ("1", "true", "yes", "on", "si", "sí")
_BINANCE_REST_HOST_RE = re.compile(r"^(fapi|dapi|api)\d*\.binance\.com$", re.I)


def _fmt_secs(seconds: float) -> str:
    s = max(0, int(round(seconds)))
    h, m, r = s // 3600, (s % 3600) // 60, s % 60
    if h:
        return f"{h} h {m:02d} min"
    if m:
        return f"{m} min {r:02d} s"
    return f"{r} s"


def _mask_proxy(url: str) -> str:
    """http://usuario:clave@host:80 → usuario:***@host:80 (nunca muestra la clave)."""
    try:
        u = urlsplit(url)
        port = f":{u.port}" if u.port else ""
        user = f"{u.username}:***@" if u.username else ""
        return f"{user}{u.hostname or '?'}{port}"
    except Exception:
        return "proxy"


def _parse_proxy_urls(raw: str) -> List[str]:
    """Lista de proxies de PROXY_URLS (comas, espacios o ';'; acepta comillas)."""
    out: List[str] = []
    raw = re.sub(r"^\s*(export\s+)?PROXY_URLS\s*=\s*", "", raw or "")   # si se pegó "PROXY_URLS=..."
    for part in re.split(r"[\s,;]+", raw.strip()):
        p = part.strip().strip("'\"")
        if not p:
            continue
        if "://" not in p:
            p = "http://" + p
        try:
            u = urlsplit(p)
            ok = u.scheme in ("http", "https") and bool(u.hostname)
            _ = u.port                      # ValueError si el puerto no es un número
        except ValueError:
            ok = False
        if not ok:
            print(f"PROXY_URLS: entrada no válida ignorada ({_mask_proxy(p)})", flush=True)
            continue
        if p not in out:
            out.append(p)
    return out


def _estimate_weight(url: str, params: Any = None) -> int:
    """Peso aproximado de Binance Futures para una petición pública."""
    try:
        u = urlsplit(url)
        path = u.path.rstrip("/").lower()
        qs = {k.lower(): v[-1] for k, v in parse_qs(u.query).items()}
    except Exception:
        return 5
    if isinstance(params, dict):
        qs.update({str(k).lower(): str(v) for k, v in params.items()})
    elif isinstance(params, (list, tuple)):
        for item in params:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                qs[str(item[0]).lower()] = str(item[1])
    if path.endswith("klines"):                   # klines, continuousKlines, markPriceKlines…
        try:
            limit = int(float(qs.get("limit", "500")))
        except ValueError:
            limit = 500
        return 1 if limit < 100 else 2 if limit < 500 else 5 if limit <= 1000 else 10
    if path.endswith(("/exchangeinfo", "/time", "/ping")):
        return 1
    if "/ticker/" in path:
        return 2 if "symbol" in qs else 40
    if path.endswith("/depth"):
        return 20
    return 5


def _retry_after(headers: Any) -> Optional[float]:
    try:
        v = headers.get("Retry-After") if headers is not None else None
    except Exception:
        v = None
    if not v:
        return None
    try:
        return max(1.0, float(str(v).strip()))
    except ValueError:
        return None


def _proxy_status_from_exc(exc: BaseException) -> Optional[int]:
    """Código HTTP de un fallo del proxy (p. ej. 'Tunnel connection failed: 407 …')."""
    st = getattr(exc, "status", None)
    if isinstance(st, int):
        return st
    m = re.search(r"Tunnel connection failed:\s*(\d{3})", str(exc))
    if m is None:
        m = re.search(r"\b(407)\b", str(exc))
    return int(m.group(1)) if m else None


def _clone_request(req: urllib.request.Request,
                   extra: Optional[Dict[str, str]] = None) -> urllib.request.Request:
    """Copia limpia de una Request (urllib la modifica al pasar por un proxy)."""
    hdrs = dict(req.header_items())
    if extra:
        hdrs.update(extra)
    return urllib.request.Request(req.full_url, data=None, headers=hdrs, method=req.get_method())


def _rewrap_response(resp: Any, body: bytes) -> Any:
    """Devuelve el cuerpo ya descomprimido con la misma interfaz que urlopen."""
    msg = resp.headers
    try:
        del msg["Content-Encoding"]
        if msg.get("Content-Length") is not None:
            msg.replace_header("Content-Length", str(len(body)))
    except Exception:
        pass
    out = urllib.response.addinfourl(io.BytesIO(body), msg, resp.geturl(),
                                     getattr(resp, "status", None) or 200)
    out.reason = getattr(resp, "reason", "OK")
    try:
        resp.close()
    except Exception:
        pass
    return out


class _Route:
    """Una salida a Binance: la IP del servidor (url=None) o un proxy."""

    def __init__(self, idx: int, url: Optional[str]) -> None:
        self.idx = idx
        self.url = url
        self.name = "Directo" if url is None else f"Proxy {idx}"
        self.label = "IP del servidor" if url is None else _mask_proxy(url)
        self.cool_until = 0.0
        self.cool_reason = ""
        self.cool_kind = ""
        self.min_key = 0            # minuto (time // 60) al que corresponde min_weight
        self.min_weight = 0         # peso usado en ese minuto (cabecera X-MBX-USED-WEIGHT-1M)
        self.ok = 0
        self.fail = 0
        self.last_status = 0
        self.last_ts = 0.0
        self.bytes = 0
        self.opener: Optional[urllib.request.OpenerDirector] = None


class BinanceRestRouter:
    """Reparte las peticiones REST públicas a Binance entre la IP del servidor y
    los proxies durante la ventana de arranque (ver comentario de arriba)."""

    def __init__(self, proxy_urls: List[str], hours: float, mode: str,
                 weight_limit: int, base_url: str, timeout_s: float, fallback: bool = True) -> None:
        self.direct = _Route(0, None)
        self.proxies = [_Route(i + 1, u) for i, u in enumerate(proxy_urls)]
        for r in self.proxies:
            op = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": r.url, "https": r.url}))
            op._rest_router_internal = True          # su .open() no se vuelve a enrutar
            r.opener = op
        self.hours = max(0.0, float(hours))
        self.mode = mode if mode in ("auto", "always") else "auto"
        self.weight_limit = max(50, int(weight_limit))
        self.timeout_s = float(timeout_s)
        self.started = time.time()
        self.window_end = self.started + self.hours * 3600.0
        self.fallback = bool(fallback)
        self.configured = bool(self.proxies) and (self.hours > 0 or self.fallback)
        self._starved_log = 0.0                      # último aviso de "sin salida disponible"
        self.lock = threading.Lock()
        self.logger = None                           # se conecta a bot.log al arrancar
        self._rr = 0                                 # siguiente proxy de la rotación
        self._direct_ok = False                      # la IP del servidor respondió bien
        self._direct_probe = False                   # hay una petición directa de prueba en vuelo
        self._ended_logged = False
        self._klines_warned = False
        self.stats: Dict[str, Any] = {"routed": 0, "via_proxy": 0, "direct": 0,
                                      "klines": 0, "libs": {}}
        host = (urlsplit(base_url).hostname or "").lower()
        self.hosts = {h for h in (host, "fapi.binance.com", "dapi.binance.com", "api.binance.com") if h}

    # ── Estado ────────────────────────────────────────────────────────────────

    def in_window(self) -> bool:
        return self.configured and time.time() < self.window_end

    def active(self) -> bool:
        """El enrutador decide la salida: dentro de la ventana, o después si hay
        respaldo (directo primero, proxies solo si Binance bloquea la IP)."""
        return self.configured and (time.time() < self.window_end or self.fallback)

    def _emit(self, msgs: List[str]) -> None:
        for m in msgs:
            try:
                (self.logger or (lambda s: print(s, flush=True)))(m)
            except Exception:
                pass

    def startup_message(self) -> str:
        if not self.configured:
            if self.proxies and self.hours <= 0:
                return "[proxy] PROXY_BOOTSTRAP_HOURS=0: proxies de arranque desactivados"
            return "[proxy] PROXY_URLS vacío: el REST de Binance sale siempre directo"
        how = ("directo y, si Binance limita la IP del servidor, por los proxies"
               if self.mode == "auto" else "siempre por los proxies")
        after = ("después, directo con los proxies de respaldo si Binance bloquea la IP"
                 if self.fallback else "después, siempre directo")
        return (f"[proxy] {len(self.proxies)} proxies para el REST de Binance: primeras "
                f"{self.hours:g} h modo {self.mode} ({how}); {after}: "
                + ", ".join(r.label for r in self.proxies))

    def should_route(self, url: str, method: Optional[str], params: Any = None) -> bool:
        if not self.active():
            return False
        if (method or "GET").upper() not in ("GET", "HEAD"):
            return False
        try:
            u = urlsplit(str(url))
        except Exception:
            return False
        if u.scheme not in ("http", "https"):
            return False
        host = (u.hostname or "").lower()
        if host not in self.hosts and not _BINANCE_REST_HOST_RE.match(host):
            return False
        if "signature=" in (u.query or "") or (params is not None and "signature" in str(params)):
            return False                             # órdenes/cuenta: siempre directo
        return True

    # ── Selección de ruta ────────────────────────────────────────────────────

    def _pick(self, weight: int, tried: set) -> Tuple[Optional[_Route], float]:
        """(ruta, 0) o (None, segundos a esperar; 0 = no hay nada que esperar)."""
        now = time.time()
        mk = int(now // 60)
        with self.lock:
            n = len(self.proxies)
            rot = [self.proxies[(self._rr + i) % n] for i in range(n)]
            always = self.mode == "always" and now < self.window_end
            order = rot + [self.direct] if always else [self.direct] + rot
            soonest: Optional[float] = None
            for r in order:
                if r in tried:
                    continue
                if r.cool_until > now:
                    soonest = r.cool_until if soonest is None else min(soonest, r.cool_until)
                    continue
                if r is self.direct and not self._direct_ok and self._direct_probe:
                    continue                         # ya hay una prueba en vuelo: no insistir en paralelo
                used = r.min_weight if r.min_key == mk else 0
                if used + weight > self.weight_limit:
                    nxt = (mk + 1) * 60 + 1.0
                    soonest = nxt if soonest is None else min(soonest, nxt)
                    continue
                if r.min_key != mk:
                    r.min_key, r.min_weight = mk, 0
                r.min_weight += weight               # reserva (la cabecera de Binance la corrige)
                if r is self.direct:
                    if not self._direct_ok:
                        self._direct_probe = True
                else:
                    self._rr = (self.proxies.index(r) + 1) % n
                return r, 0.0
        if soonest is not None:
            return None, max(0.2, soonest - now)
        return None, 0.0

    def _starved(self) -> None:
        """Aviso (máx. 1 cada 5 min) de que ninguna salida está disponible."""
        now = time.time()
        with self.lock:
            if now - self._starved_log < 300:
                return
            self._starved_log = now
            parts = []
            for r in [self.direct] + self.proxies:
                if r.cool_until > now:
                    parts.append(f"{r.name}: {r.cool_reason or 'en pausa'} ({_fmt_secs(r.cool_until - now)})")
                else:
                    parts.append(f"{r.name}: al tope de peso del minuto")
        self._emit(["[proxy] ⚠️ Ninguna salida a Binance disponible ahora; la descarga de velas espera "
                    "a la primera que se libere (sin insistir sobre la IP bloqueada). " + " · ".join(parts)])

    def _direct_banned(self) -> bool:
        d = self.direct
        return d.cool_until > time.time() and d.cool_kind in ("http418", "http429", "http403", "http451")

    def _done(self, route: _Route) -> None:
        if route is self.direct:
            with self.lock:
                self._direct_probe = False

    def _note(self, route: _Route, status: Optional[int], headers: Any, nbytes: int = 0) -> None:
        """Registra una respuesta de Binance (peso usado, aciertos, recuperación)."""
        now = time.time()
        msgs: List[str] = []
        try:
            w = headers.get("X-MBX-USED-WEIGHT-1M") if headers is not None else None
            weight = int(str(w).strip()) if w not in (None, "") else None
        except Exception:
            weight = None
        with self.lock:
            route.last_status = int(status or 0)
            route.last_ts = now
            route.bytes += max(0, int(nbytes))
            if weight is not None:
                mk = int(now // 60)
                if route.min_key == mk:
                    route.min_weight = max(route.min_weight, weight)
                else:
                    route.min_key, route.min_weight = mk, weight
            if status is not None and 200 <= int(status) < 400:
                route.ok += 1
                if route is self.direct:
                    self._direct_ok = True
                    self.stats["direct"] += 1
                else:
                    self.stats["via_proxy"] += 1
                    if route.ok == 1:
                        msgs.append(f"[proxy] {route.name} ({route.label}) funciona: primera descarga OK")
                if route.cool_kind and route.cool_until <= now:
                    if route is self.direct and self.mode == "auto" and self.active():
                        msgs.append("[proxy] La IP del servidor vuelve a responder: el REST sale directo")
                    route.cool_kind = route.cool_reason = ""
        self._emit(msgs)

    def _cool(self, route: _Route, secs: Optional[float], reason: str, kind: str) -> None:
        """Aparta una ruta `secs` segundos (None = hasta el final de la ventana)."""
        now = time.time()
        until = max(self.window_end, now + 6 * 3600.0) if secs is None else now + float(secs)
        msgs: List[str] = []
        with self.lock:
            route.fail += 1
            changed = route.cool_until <= now or route.cool_kind != kind
            route.cool_until = max(route.cool_until, until)
            route.cool_reason, route.cool_kind = reason, kind
            if route is self.direct:
                self._direct_ok = False
            if changed:
                left = route.cool_until - now
                if route is self.direct:
                    tail = " → el REST sale por los proxies" if self.proxies else ""
                    msgs.append(f"[proxy] IP del servidor {reason} durante {_fmt_secs(left)}{tail}")
                else:
                    dur = ("hasta nuevo aviso" if kind in ("http407", "http403", "http451")
                           else f"durante {_fmt_secs(left)}")
                    msgs.append(f"[proxy] {route.name} ({route.label}) fuera {dur}: {reason}")
        self._emit(msgs)

    def _judge_status(self, route: _Route, status: int, retry_after: Optional[float]) -> bool:
        """True si la respuesta es un bloqueo de ESTA salida y hay que probar otra."""
        if status in (418, 429):
            secs = retry_after or (300.0 if status == 418 else 60.0)
            what = ("bloqueada por Binance (HTTP 418)" if status == 418
                    else "limitada por Binance (HTTP 429)")
            if route is not self.direct:
                what = what.replace("bloqueada", "bloqueado").replace("limitada", "limitado")
            self._cool(route, secs, what, f"http{status}")
            return True
        if status in (403, 451):
            if route is self.direct:
                self._cool(route, 600.0, f"rechazada por Binance (HTTP {status})", f"http{status}")
            else:
                self._cool(route, None, f"Binance rechaza la IP de este proxy (HTTP {status}, "
                                        f"p. ej. ubicación restringida)", f"http{status}")
            return True
        if status == 407 and route is not self.direct:
            self._cool(route, None, "el proxy rechazó la conexión (407: credenciales o cuota "
                                    "mensual agotada)", "http407")
            return True
        return False

    def _net_fail(self, route: _Route, exc: BaseException) -> None:
        st = _proxy_status_from_exc(exc)
        if route is not self.direct and st == 407:
            self._judge_status(route, 407, None)
            return
        if route is not self.direct and st in (403, 451):
            self._judge_status(route, st, None)
            return
        detail = str(getattr(exc, "reason", None) or exc or type(exc).__name__)[:120]
        self._cool(route, 30.0 if route is self.direct else 60.0, f"sin conexión ({detail})", "net")

    def _count(self, lib: str, url: str) -> None:
        first = False
        with self.lock:
            self.stats["routed"] += 1
            self.stats["libs"][lib] = self.stats["libs"].get(lib, 0) + 1
            if str(url).split("?", 1)[0].lower().endswith("klines"):
                self.stats["klines"] += 1
                first = self.stats["klines"] == 1
        if first:
            self._emit([f"[proxy] Descarga de velas detectada ({lib}): pasa por el enrutador de arranque"])

    # ── urllib ────────────────────────────────────────────────────────────────

    def _urllib_via_proxy(self, route: _Route, req: urllib.request.Request, timeout: float) -> Any:
        want_gzip = not req.has_header("Accept-encoding")   # ahorra cuota de datos del proxy
        clone = _clone_request(req, {"Accept-Encoding": "gzip"} if want_gzip else None)
        resp = route.opener.open(clone, timeout=timeout)
        if want_gzip and (resp.headers.get("Content-Encoding") or "").lower() == "gzip":
            raw = resp.read()
            body = gzip.decompress(raw)
            resp = _rewrap_response(resp, body)
            resp._router_bytes = len(raw)
        return resp

    def fetch_urllib(self, orig_open, caller_opener, req: urllib.request.Request, timeout: Any) -> Any:
        url = req.full_url
        self._count("urllib", url)
        weight = _estimate_weight(url)
        to = timeout if isinstance(timeout, (int, float)) else self.timeout_s
        tried: set = set()
        last_exc: Optional[BaseException] = None
        waited = 0
        while True:
            route, wait = self._pick(weight, tried)
            if route is None:
                if wait > 0 and waited < 2:
                    waited += 1
                    time.sleep(min(wait, 65.0))
                    continue
                break
            tried.add(route)
            try:
                if route is self.direct:
                    resp = orig_open(caller_opener, _clone_request(req), None, timeout)
                else:
                    resp = self._urllib_via_proxy(route, req, to)
            except urllib.error.HTTPError as exc:
                self._note(route, exc.code, exc.headers)
                if self._judge_status(route, exc.code, _retry_after(exc.headers)):
                    if isinstance(last_exc, urllib.error.HTTPError):
                        try:
                            last_exc.close()
                        except Exception:
                            pass
                    last_exc = exc
                    continue
                raise
            except (urllib.error.URLError, OSError, http.client.HTTPException, EOFError, zlib.error) as exc:
                self._net_fail(route, exc)
                last_exc = exc
                continue
            finally:
                self._done(route)
            self._note(route, getattr(resp, "status", None) or resp.getcode(), resp.headers,
                       getattr(resp, "_router_bytes", 0))
            return resp
        if last_exc is not None:
            raise last_exc
        if self._direct_banned():
            self._starved()
            raise urllib.error.URLError("Binance bloquea la IP del servidor y no hay proxy disponible")
        return orig_open(caller_opener, req, None, timeout)  # nada disponible: comportamiento original

    # ── requests ──────────────────────────────────────────────────────────────

    def fetch_requests(self, orig_send, adapter, request, kwargs: dict) -> Any:
        import requests as _rq
        url = request.url
        self._count("requests", url)
        weight = _estimate_weight(url)
        tried: set = set()
        last_exc: Optional[BaseException] = None
        last_resp = None
        waited = 0
        while True:
            route, wait = self._pick(weight, tried)
            if route is None:
                if wait > 0 and waited < 2:
                    waited += 1
                    time.sleep(min(wait, 65.0))
                    continue
                break
            tried.add(route)
            kw = dict(kwargs)
            if route is not self.direct:
                kw["proxies"] = {"http": route.url, "https": route.url}
                if kw.get("timeout") is None:
                    kw["timeout"] = self.timeout_s
            try:
                resp = orig_send(adapter, request, **kw)
            except (_rq.exceptions.ConnectionError, _rq.exceptions.Timeout) as exc:
                self._net_fail(route, exc)
                last_exc = exc
                continue
            finally:
                self._done(route)
            self._note(route, resp.status_code, resp.headers)
            if self._judge_status(route, resp.status_code, _retry_after(resp.headers)):
                if last_resp is not None:
                    last_resp.close()
                last_resp = resp
                continue
            if last_resp is not None:
                last_resp.close()
            return resp
        if last_resp is not None:
            return last_resp
        if last_exc is not None:
            raise last_exc
        if self._direct_banned():
            self._starved()
            raise _rq.exceptions.ConnectionError("Binance bloquea la IP del servidor y no hay proxy disponible")
        return orig_send(adapter, request, **kwargs)

    # ── aiohttp ───────────────────────────────────────────────────────────────

    async def fetch_aiohttp(self, orig_request, session, method, str_or_url, kwargs: dict) -> Any:
        import aiohttp as _aio
        url = str(str_or_url)
        self._count("aiohttp", url)
        weight = _estimate_weight(url, kwargs.get("params"))
        tried: set = set()
        last_exc: Optional[BaseException] = None
        last_resp = None
        while True:
            route, wait = self._pick(weight, tried)
            if route is None:
                if wait > 0:
                    # Todas las salidas en pausa: NO se devuelve el 418/429 (la caché de
                    # velas se pararía horas con su Retry-After) ni se insiste sobre la IP
                    # bloqueada. Se espera a la primera salida que se libere y se reintenta.
                    if last_resp is not None:
                        last_resp.release()
                        last_resp = None
                    self._starved()
                    await asyncio.sleep(min(wait, 60.0))
                    tried = set()
                    continue
                break
            tried.add(route)
            kw = dict(kwargs)
            if route is not self.direct:
                kw["proxy"] = route.url
                kw.pop("proxy_auth", None)
            try:
                resp = await orig_request(session, method, str_or_url, **kw)
            except _aio.ClientHttpProxyError as exc:          # el proxy respondió con error (407…)
                self._net_fail(route, exc)
                last_exc = exc
                continue
            except _aio.ClientResponseError as exc:           # raise_for_status=True
                self._note(route, exc.status, exc.headers)
                if self._judge_status(route, exc.status, _retry_after(exc.headers)):
                    last_exc = exc
                    continue
                raise
            except (_aio.ClientConnectionError, asyncio.TimeoutError) as exc:
                self._net_fail(route, exc)
                last_exc = exc
                continue
            finally:
                self._done(route)
            self._note(route, resp.status, resp.headers)
            if self._judge_status(route, resp.status, _retry_after(resp.headers)):
                if last_resp is not None:
                    last_resp.release()
                last_resp = resp
                continue
            if last_resp is not None:
                last_resp.release()
            return resp
        if last_resp is not None:
            return last_resp
        if last_exc is not None:
            raise last_exc
        if self._direct_banned():
            raise _aio.ClientConnectionError("Binance bloquea la IP del servidor y no hay proxy disponible")
        return await orig_request(session, method, str_or_url, **kwargs)

    # ── Mantenimiento y vista para la web ─────────────────────────────────────

    def housekeeping(self) -> None:
        if not self.configured:
            return
        now = time.time()
        msgs: List[str] = []
        with self.lock:
            if now >= self.window_end and not self._ended_logged:
                self._ended_logged = True
                per = ", ".join(f"{r.name} {r.ok}" for r in self.proxies)
                msgs.append(f"[proxy] Terminaron las {self.hours:g} h de proxies de arranque "
                            f"({self.stats['via_proxy']} descargas por proxy: {per}). "
                            + ("Desde ahora el REST sale directo y los proxies solo se usan si Binance "
                               "bloquea la IP del servidor." if self.fallback
                               else "Desde ahora el REST de Binance sale directo."))
            if (not self._klines_warned and self.stats["klines"] == 0
                    and now - self.started > 300 and now < self.window_end):
                self._klines_warned = True
                msgs.append("[proxy] En 5 min no ha pasado ninguna descarga de velas por el enrutador. "
                            "Si KlineWebSocketCache_v4 descarga con otra librería (no urllib, "
                            "requests ni aiohttp), esas descargas no van por los proxies.")
        self._emit(msgs)

    def view(self) -> dict:
        now = time.time()
        mk = int(now // 60)
        with self.lock:
            routes = []
            for r in [self.direct] + self.proxies:
                cooling = r.cool_until > now
                used = r.min_weight if r.min_key == mk else 0
                if cooling and r is not self.direct and r.cool_kind in ("http407", "http403", "http451"):
                    state = "off"
                elif cooling:
                    state = "cooling"
                elif used >= self.weight_limit:
                    state = "limit"
                else:
                    state = "ok"
                routes.append({
                    "name": r.name, "label": r.label,
                    "kind": "direct" if r is self.direct else "proxy",
                    "state": state, "ok": r.ok, "fail": r.fail,
                    "last_status": r.last_status, "last_ts": r.last_ts,
                    "cool_left_s": (r.cool_until - now) if cooling else 0.0,
                    "reason": r.cool_reason if cooling else "",
                    "weight": used, "kb": round(r.bytes / 1024.0, 1),
                })
            stats = {k: (dict(v) if isinstance(v, dict) else v) for k, v in self.stats.items()}
        return {
            "configured": self.configured, "mode": self.mode, "hours": self.hours,
            "active": self.configured and now < self.window_end,
            "fallback": self.fallback,
            "window_left_s": max(0.0, self.window_end - now) if self.configured else 0.0,
            "weight_limit": self.weight_limit, "n_proxies": len(self.proxies),
            "routes": routes, **stats,
        }


_ACTIVE_ROUTER: Optional[BinanceRestRouter] = None
_ROUTER_PATCHED = False


def _install_rest_router(router: BinanceRestRouter) -> None:
    """Activa `router` en urllib, requests y aiohttp (los parches se ponen una vez;
    con el router fuera de su ventana pasan la petición tal cual)."""
    global _ACTIVE_ROUTER, _ROUTER_PATCHED
    _ACTIVE_ROUTER = router
    if _ROUTER_PATCHED or not router.configured:
        return
    _ROUTER_PATCHED = True

    orig_open = urllib.request.OpenerDirector.open

    def _open(opener, fullurl, data=None, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):
        r = _ACTIVE_ROUTER
        if r is None or getattr(opener, "_rest_router_internal", False) or not r.active():
            return orig_open(opener, fullurl, data, timeout)
        req = fullurl if isinstance(fullurl, urllib.request.Request) else None
        url = req.full_url if req is not None else str(fullurl)
        method = req.get_method() if req is not None else "GET"
        if data is not None or (req is not None and req.data is not None) or not r.should_route(url, method):
            return orig_open(opener, fullurl, data, timeout)
        return r.fetch_urllib(orig_open, opener, req if req is not None else urllib.request.Request(url),
                              timeout)

    urllib.request.OpenerDirector.open = _open

    try:
        import requests.adapters as _rq_adapters
    except Exception:
        _rq_adapters = None
    if _rq_adapters is not None:
        orig_send = _rq_adapters.HTTPAdapter.send

        def _send(adapter, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
            r = _ACTIVE_ROUTER
            kwargs = dict(stream=stream, timeout=timeout, verify=verify, cert=cert, proxies=proxies)
            if (r is None or not r.active() or request.body
                    or not r.should_route(request.url, request.method)):
                return orig_send(adapter, request, **kwargs)
            return r.fetch_requests(orig_send, adapter, request, kwargs)

        _rq_adapters.HTTPAdapter.send = _send

    try:
        import aiohttp as _aio
    except Exception:
        _aio = None
    if _aio is not None:
        orig_request = _aio.ClientSession._request

        async def _request(session, method, str_or_url, **kwargs):
            r = _ACTIVE_ROUTER
            if (r is None or not r.active() or kwargs.get("data") is not None
                    or kwargs.get("json") is not None
                    or not r.should_route(str(str_or_url), method, kwargs.get("params"))):
                return await orig_request(session, method, str_or_url, **kwargs)
            return await r.fetch_aiohttp(orig_request, session, method, str_or_url, kwargs)

        _aio.ClientSession._request = _request


REST_ROUTER = BinanceRestRouter(
    _parse_proxy_urls(_PROXY_URLS_RAW), PROXY_BOOTSTRAP_HOURS, PROXY_MODE,
    PROXY_WEIGHT_LIMIT, os.getenv("BASE_URL", "https://fapi.binance.com"), PROXY_TIMEOUT_S,
    PROXY_FALLBACK,
)
_install_rest_router(REST_ROUTER)       # antes de importar los módulos que descargan velas
print(REST_ROUTER.startup_message(), flush=True)

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


def _host_of(url: str) -> str:
    try:
        return urlsplit(url).netloc or url
    except Exception:
        return url or ""


class ExecutorBridge:
    """Envía señales de apertura/cierre al Executor y consulta su estado.
    El link se puede cambiar en caliente (set_url) y cada señal puede ir a un
    link concreto (url=…): así una posición recibe su DCA y su cierre en el
    executor donde se abrió aunque luego cambies el link."""

    def __init__(
        self,
        executor_url: str = "",
        signal_secret: str = "clave-secreta-aleatoria",
        poll_secs: int = 5,
        logger=None,
    ) -> None:
        self.config = _ExecutorSignalConfig(
            executor_url=self.normalize_url(executor_url),
            signal_secret=signal_secret,
            poll_secs=int(poll_secs),
        )
        self.logger = logger or print
        self._stats_lock = threading.Lock()
        self.sent_ok = 0
        self.sent_err = 0
        self.last_ok_ts = 0.0
        self.last_err = ""
        self.last_err_ts = 0.0
        self._tasks: set = set()

    @staticmethod
    def normalize_url(url: Optional[str]) -> str:
        return (url or "").strip().rstrip("/")

    def set_url(self, url: str) -> None:
        self.config.executor_url = self.normalize_url(url)

    def _log(self, message: str) -> None:
        try:
            self.logger(message)
        except Exception:
            pass

    def _build_signal_request(self, payload: dict, url: Optional[str] = None) -> urllib.request.Request:
        body = json.dumps(payload).encode("utf-8")
        target = (self.config.executor_url if url is None else self.normalize_url(url))
        return urllib.request.Request(
            f"{target}/signal",
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Signal-Secret": self.config.signal_secret,
            },
            method="POST",
        )

    def send_signal_sync(self, payload: dict, url: Optional[str] = None) -> None:
        """Envía una señal al Executor. No lanza excepción: solo registra el error."""
        target = (self.config.executor_url if url is None else self.normalize_url(url))
        if not target:
            return
        try:
            req = self._build_signal_request(payload, target)
            with urllib.request.urlopen(req, timeout=self.config.timeout_signal) as resp:
                resp.read()
            with self._stats_lock:
                self.sent_ok += 1
                self.last_ok_ts = time.time()
            self._log(
                f"[executor] ✓ señal enviada: {payload.get('action')} {payload.get('symbol')}"
                f" → {_host_of(target)}"
            )
        except Exception as exc:
            with self._stats_lock:
                self.sent_err += 1
                self.last_err = f"{payload.get('action')} {payload.get('symbol')}: {exc}"[:200]
                self.last_err_ts = time.time()
            self._log(
                f"[executor] error enviando {payload.get('action')} "
                f"{payload.get('symbol')} a {_host_of(target)}: {exc}"
            )

    async def send_signal_async(self, payload: dict, url: Optional[str] = None) -> None:
        """Versión no bloqueante para usar desde el event loop."""
        await asyncio.to_thread(self.send_signal_sync, payload, url)

    def stats_view(self) -> dict:
        with self._stats_lock:
            return {"sent_ok": self.sent_ok, "sent_err": self.sent_err,
                    "last_ok_ts": self.last_ok_ts, "last_err": self.last_err,
                    "last_err_ts": self.last_err_ts}

    def probe(self, url: Optional[str] = None, timeout: float = 20.0) -> dict:
        """Comprueba que el link responde (GET /api/state). No envía señales."""
        target = (self.config.executor_url if url is None else self.normalize_url(url))
        if not target:
            return {"ok": False, "error": "No hay link de executor"}
        t0 = time.time()
        try:
            req = urllib.request.Request(f"{target}/api/state", method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                resp.read(4096)
                return {"ok": True, "status": resp.status, "ms": round((time.time() - t0) * 1000)}
        except urllib.error.HTTPError as exc:     # responde, aunque no tenga /api/state
            return {"ok": True, "status": exc.code, "ms": round((time.time() - t0) * 1000)}
        except Exception as exc:
            reason = getattr(exc, "reason", None) or exc
            return {"ok": False, "error": str(reason)[:200] or type(exc).__name__,
                    "ms": round((time.time() - t0) * 1000)}

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
        url: Optional[str] = None,
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
        self.notify_async(payload, url)

    def notify_close(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        reason: str,
        close_price: float,
        pnl: float = 0.0,
        url: Optional[str] = None,
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
        self.notify_async(payload, url)

    def notify_async(self, payload: dict, url: Optional[str] = None) -> None:
        """Dispara el envío sin bloquear el event loop."""
        target = (self.config.executor_url if url is None else self.normalize_url(url))
        if not target:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            threading.Thread(
                target=self.send_signal_sync,
                args=(payload, target),
                daemon=True,
            ).start()
            return
        task = loop.create_task(self.send_signal_async(payload, target))
        self._tasks.add(task)                       # referencia viva hasta que termine
        task.add_done_callback(self._tasks.discard)



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
EMA_FAST               = int(os.getenv("EMA_FAST", "250"))
EMA_SLOW               = int(os.getenv("EMA_SLOW", "500"))
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
# EXECUTOR_URL es el link de arranque; desde la web se puede cambiar en caliente
# (se guarda en SETTINGS_FILE y desde entonces manda el de la web).
EXECUTOR_URL    = os.getenv("EXECUTOR_URL",    "https://executor-5lu0.onrender.com").strip().rstrip("/")
EXECUTOR_SECRET = os.getenv("EXECUTOR_SECRET", "clave-secreta-aleatoria")
PAUSE_MAX_MIN   = 10080                     # pausas con tiempo: de 1 min a 7 días


@dataclass
class ExecutorSettings:
    url:          str   = EXECUTOR_URL
    # Pausa del envío: con el link en pausa las posiciones NUEVAS no se envían al
    # executor; las que ya están en él siguen recibiendo su DCA y su cierre.
    paused:       bool  = False
    pause_reason: str   = ""
    paused_at:    float = 0.0
    pause_until:  float = 0.0               # 0 = hasta que la reanudes a mano


EXEC = ExecutorSettings()


def _v_exec_url(v: Any) -> str:
    """Valida el link del executor. Vacío = sin executor (no se envían señales)."""
    s = str(v or "").strip().rstrip("/")
    if not s:
        return ""
    if len(s) > 300:
        raise ValueError("El link es demasiado largo")
    if any(c.isspace() for c in s):
        raise ValueError("El link no puede tener espacios")
    if "://" not in s:
        s = "https://" + s
    try:
        u = urlsplit(s)
        _ = u.port
    except ValueError:
        raise ValueError("El link no es válido")
    host = u.hostname or ""
    if u.scheme not in ("http", "https") or not host or ("." not in host and host != "localhost"):
        raise ValueError("El link debe ser del tipo https://mi-executor.onrender.com")
    if u.query or u.fragment:
        raise ValueError("El link no debe llevar ? ni #")
    return s


def _v_pause_minutes(v: Any) -> Optional[float]:
    """Minutos de una pausa con tiempo. None/""/0 = hasta reanudar a mano."""
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    try:
        m = float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        raise ValueError("Los minutos de pausa deben ser un número")
    if m != m:
        raise ValueError("Los minutos de pausa deben ser un número")
    if m == 0:
        return None
    if not 1 <= m <= PAUSE_MAX_MIN:
        raise ValueError(f"La pausa va de 1 a {PAUSE_MAX_MIN} minutos (7 días)")
    return m


def _fmt_minutes(m: float) -> str:
    m = round(float(m), 1)
    if m >= 60 and abs(m - round(m)) < 1e-9:
        h, r = divmod(int(round(m)), 60)
        return f"{h} h" + (f" {r} min" if r else "")
    return f"{m:g} min"

# ── Persistencia de estadísticas (MFE/MAE) y ajustes editables desde la web ───
# IMPORTANTE: en hosting con disco efímero (Render free, etc.) apunta estas rutas
# a un disco persistente (STATS_FILE=/data/trade_stats.jsonl) o se perderán al redeploy.
STATS_FILE    = os.getenv("STATS_FILE",    os.path.join(_HERE, "trade_stats.jsonl"))
SETTINGS_FILE = os.getenv("SETTINGS_FILE", os.path.join(_HERE, "bot_settings.json"))
# Operaciones cerradas que se guardan EN MEMORIA para las estadísticas (las más
# recientes). El archivo STATS_FILE conserva todas y el CSV se lee de él.
STATS_MAX_IN_MEMORY = max(100, int(os.getenv("STATS_MAX_IN_MEMORY", "3000")))


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
#    abiertas siguen funcionando). Puede ser hasta reanudarla a mano o durar
#    N minutos (pause_until): al cumplirse se reanuda sola.
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
    # Minutos que dura la pausa del stop global (0 = hasta reanudarla a mano)
    global_stop_pause_min: float = float(os.getenv("GLOBAL_STOP_PAUSE_MIN", "0") or 0)
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
    pause_until:          float = 0.0     # 0 = hasta reanudar a mano; si no, se reanuda sola


RISK = RiskSettings()


# ── Modo invertido (espejo del bot) ───────────────────────────────────────────
# Un solo interruptor (web: POST /api/invert). Activo → las posiciones NUEVAS se
# abren en el lado contrario a la señal (UP → SHORT, DOWN → LONG) y con todas
# las reglas en espejo: su TP es el SL del normal con el signo cambiado, su SL
# es el TP del normal con el signo cambiado, el stop global de −X pasa a ser un
# TP global de +X, y los DCA entran en los mismos precios que en el bot normal.
# Las posiciones ya abiertas siguen con las reglas con las que se abrieron.
@dataclass
class ModeSettings:
    inverted:   bool  = _env_bool("INVERTED_MODE", False)
    changed_at: float = 0.0


MODE = ModeSettings()

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
    "global_stop_pause_min": _v_float(0, PAUSE_MAX_MIN,
                                      f"La pausa tras el stop global va de 0 (hasta reanudarla) "
                                      f"a {PAUSE_MAX_MIN} minutos"),
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


_SETTINGS_SOURCE = "none"     # "file" si al arrancar había ajustes guardados desde la web


def _load_settings() -> None:
    """Restaura los ajustes guardados desde la web (SL global, multiplicador del TP,
    EMA rápida/lenta y gestión de riesgo). Sobrescriben los valores de entorno.
    Si hay memoria en Upstash, lo de Upstash se aplica después y manda."""
    global _SETTINGS_SOURCE
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return
    except Exception:
        _SETTINGS_SOURCE = "error"
        return
    if not isinstance(data, dict):
        _SETTINGS_SOURCE = "error"
        return
    _SETTINGS_SOURCE = "file"
    _apply_settings_dict(data)


def _apply_settings_dict(data: dict) -> None:
    """Aplica un diccionario de ajustes (el que escribe _save_settings), venga del
    disco o de Upstash. Las claves que falten o no sean válidas no se tocan."""
    global DEFAULT_STOP_LOSS_USD, TAKE_PROFIT_FRACTION, EMA_FAST, EMA_SLOW, ENTRY_LADDER
    if not isinstance(data, dict):
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
            RISK.pause_until = float(risk.get("pause_until", 0) or 0)
        except Exception:
            RISK.pause_until = 0.0
    md = data.get("mode")
    if isinstance(md, dict):
        MODE.inverted = bool(md.get("inverted", MODE.inverted))
        try:
            MODE.changed_at = float(md.get("changed_at", 0) or 0)
        except Exception:
            MODE.changed_at = 0.0
    ex = data.get("executor")
    if isinstance(ex, dict):
        try:
            EXEC.url = _v_exec_url(ex.get("url", EXEC.url))
        except ValueError as exc:
            print(f"Link de executor guardado no válido ({exc}); uso EXECUTOR_URL", flush=True)
        EXEC.paused       = bool(ex.get("paused", False))
        EXEC.pause_reason = str(ex.get("pause_reason", "") or "")
        for key in ("paused_at", "pause_until"):
            try:
                setattr(EXEC, key, float(ex.get(key, 0) or 0))
            except Exception:
                setattr(EXEC, key, 0.0)
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
# Lo llama _save_settings tras cada cambio: el bot lo conecta a la memoria externa
# (Upstash) para que el cambio se guarde también allí.
_SETTINGS_SAVED_HOOK: Optional[Any] = None


def _settings_dict() -> dict:
    """TODOS los ajustes editables desde la web (lo que se guarda y se restaura)."""
    return {
        "default_stop_loss_usd": DEFAULT_STOP_LOSS_USD,
        "take_profit_fraction":  TAKE_PROFIT_FRACTION,
        "ema_fast":              EMA_FAST,
        "ema_slow":              EMA_SLOW,
        "ladder": {
            "levels":    [lv for lv, _ in ENTRY_LADDER],
            "notionals": [nt for _, nt in ENTRY_LADDER],
        },
        "risk":                  asdict(RISK),
        "executor":              asdict(EXEC),
        "mode":                  asdict(MODE),
    }


def _save_settings(notify: bool = True) -> Optional[str]:
    """Guarda TODOS los ajustes editables en disco (atómico) y avisa a la memoria
    externa. Devuelve error o None."""
    tmp = f"{SETTINGS_FILE}.tmp"
    err: Optional[str] = None
    try:
        with _SETTINGS_LOCK:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(_settings_dict(), fh)
            os.replace(tmp, SETTINGS_FILE)
    except Exception as exc:
        err = str(exc)
    hook = _SETTINGS_SAVED_HOOK
    if notify and hook is not None:
        try:
            hook()
        except Exception:
            pass
    return err


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


def _opp(side: str) -> str:
    """Lado contrario: LONG ↔ SHORT."""
    return "LONG" if side == "SHORT" else "SHORT"


@dataclass
class BotPosition:
    symbol:       str
    fills:        List[Fill] = field(default_factory=list)
    realized_pnl: float = 0.0
    status:       str   = "OPEN"
    trade_id:     int   = 0
    direction:    str   = "SHORT"     # lado REAL de la posición: "SHORT" | "LONG"
    # Importe fijo en USD (valor negativo): 1 tramo → −notional×0.251 · 2+ → SL
    # estándar · o el fijado a mano. En una posición normal es su STOP LOSS; en
    # una invertida es el espejo de su TAKE PROFIT (TP = −sl_usd).
    sl_usd:       float = DEFAULT_STOP_LOSS_USD
    # True si ese importe lo fijó el usuario desde el dashboard: el bot NO lo toca más.
    sl_manual:    bool  = False
    # Modo invertido (espejo del bot normal): se abrió en el lado CONTRARIO a la
    # señal y todas sus reglas son las del bot normal con el signo cambiado, así
    # que opera en los mismos precios con el resultado opuesto:
    #   • DCA en los mismos precios que el normal (para ella, a favor)
    #   • TP = −(SL del normal)   · SL = −(TP del normal) = −notional × multiplicador
    inverted:     bool  = False
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
    # Link del executor al que se envió la apertura ("" = no se envió: link en
    # pausa o sin link). Su DCA y su cierre van SIEMPRE a ese mismo link.
    exec_url:     str   = ""

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

    @property
    def signal_dir(self) -> str:
        """Lado que habría abierto el bot normal (el de la señal)."""
        return _opp(self.direction) if self.inverted else self.direction

    # ── Salidas en PnL REAL de la posición ───────────────────────────────────
    def exit_tp(self) -> float:
        """Take profit en USD (positivo). Normal: notional × multiplicador del TP.
        Invertida: espejo del SL del normal (1 tramo → notional×0.251, 2+ → |SL estándar|)."""
        if self.inverted:
            return -self.sl_usd
        return self.notional * TAKE_PROFIT_FRACTION

    def exit_sl(self) -> float:
        """Stop loss en USD (negativo). Normal: sl_usd. Invertida: espejo del TP
        del normal = −notional × multiplicador del TP."""
        if self.inverted:
            return -self.notional * TAKE_PROFIT_FRACTION
        return self.sl_usd

    def price_at(self, pnl: float) -> float:
        """Precio al que el PnL no realizado vale `pnl`."""
        q = self.qty
        if q <= 0:
            return 0.0
        return self.avg_entry - self.pnl_sign * pnl / q

    def sl_price(self) -> float:
        """Precio del stop loss (short: por encima; long: por debajo)."""
        return self.price_at(self.exit_sl())

    def tp_price(self) -> float:
        """Precio del take profit."""
        return self.price_at(self.exit_tp())

    def adverse_pct(self, price: float) -> float:
        """% que el precio se movió EN CONTRA de la posición respecto a la 1.ª entrada."""
        if not self.fills or price <= 0:
            return 0.0
        p0 = self.fills[0].entry_price
        if p0 <= 0:
            return 0.0
        return (price / p0 - 1.0) * 100.0 * self.pnl_sign

    def trigger_pct(self, price: float) -> float:
        """% que manda en el DCA: lo que el precio se movió en contra de la SEÑAL
        desde la 1.ª entrada. Normal = en contra de la posición; invertida = a
        favor (el bot normal añadiría justo en esos precios)."""
        a = self.adverse_pct(price)
        return -a if self.inverted else a

    def opened_levels(self) -> set:
        return {f.level for f in self.fills}

    # ── Importe fijo automático por tramos (SL normal / TP invertida) ────────
    def auto_sl_usd(self) -> float:
        """1 solo tramo → -(notional del primer fill × 0.251).
        2 o más tramos → SL estándar (DEFAULT_STOP_LOSS_USD).
        En una posición invertida este valor con el signo cambiado es su TP."""
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


_POS_FIELDS = {f.name for f in fields(BotPosition)}
_FILL_FIELDS = {f.name for f in fields(Fill)}
# Datos de una posición que cambian con cada tick (no obligan a guardar al instante)
_POS_VOLATILE = ("mfe_usd", "mae_usd", "mfe_pct", "mae_pct", "mfe_ts", "mae_ts", "low_price", "high_price")


def _position_from_dict(d: Any) -> Optional[BotPosition]:
    """Reconstruye una posición abierta guardada (None si no es válida)."""
    if not isinstance(d, dict) or not d.get("symbol") or not isinstance(d.get("fills"), list):
        return None                         # (los archivos de versiones viejas solo traían un resumen)
    fills: List[Fill] = []
    for f in d["fills"]:
        if not isinstance(f, dict):
            continue
        try:
            fills.append(Fill(**{k: float(v) for k, v in f.items() if k in _FILL_FIELDS}))
        except (TypeError, ValueError):
            continue
    if not fills:
        return None
    kw = {k: v for k, v in d.items() if k in _POS_FIELDS and k not in ("fills", "symbol")}
    try:
        pos = BotPosition(symbol=str(d["symbol"]).upper(), fills=fills, **kw)
        pos.status = "OPEN"
        pos.trade_id = int(pos.trade_id or 0)
        for k in ("sl_usd", "realized_pnl", "opened_ts") + _POS_VOLATILE:
            setattr(pos, k, float(getattr(pos, k) or 0.0))
        pos.sl_manual, pos.inverted = bool(pos.sl_manual), bool(pos.inverted)
        if pos.direction not in ("LONG", "SHORT"):
            return None
        return pos
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# MEMORIA EXTERNA: Upstash Redis (estado) + QStash (que Render no lo duerma)
# ─────────────────────────────────────────────────────────────────────────────
# En Render free el disco se borra en cada reinicio, así que con
# UPSTASH_REDIS_REST_URL y UPSTASH_REDIS_REST_TOKEN el bot guarda en Upstash
# Redis (gratis) todo lo necesario para seguir donde estaba:
#   {STORE_PREFIX}:config  ajustes de la web: EMAs, SL, TP, escalera DCA, riesgo,
#                          pausas, link del executor y modo invertido
#   {STORE_PREFIX}:state   posiciones abiertas con TODOS sus datos (tramos, SL o TP
#                          manual, trade_id, executor, MFE/MAE…), cooldowns,
#                          contador de trade_id, PnL realizado y stop global
#   {STORE_PREFIX}:closed  últimas 500 operaciones cerradas (historial del panel)
#   {STORE_PREFIX}:stats   últimas STATS_MAX_IN_MEMORY operaciones con MFE/MAE
#   {STORE_PREFIX}:lock    qué instancia manda: en un deploy Render arranca la
#                          nueva antes de parar la vieja; la nueva espera a que
#                          la vieja guarde y suelte, así nunca operan dos a la vez.
# Cada cambio (apertura, DCA, cierre, SL/TP, ajuste, cooldown) se guarda en ~1 s;
# el MFE/MAE de las abiertas, cada minuto. Gasto: ~4.000 comandos al día (la capa
# gratis de Upstash da 500.000 al mes).
# QStash NO guarda datos (es una cola de mensajes HTTP). Con QSTASH_TOKEN el bot
# crea en QStash un horario que visita /health cada 10 min: así Render free no lo
# duerme por falta de visitas (dormido no vigila precios ni gestiona posiciones).

UPSTASH_URL   = (os.getenv("UPSTASH_REDIS_REST_URL","https://merry-camel-217854.upstash.io") or os.getenv("KV_REST_API_URL") or "").strip().rstrip("/")
UPSTASH_TOKEN = (os.getenv("UPSTASH_REDIS_REST_TOKEN", "gQAAAAAAA1L-AAIgcDEyYWRiYmMwZDczMDc0YTYzOTYwMjllNjM0ZmMwNzgzMA") or os.getenv("KV_REST_API_TOKEN") or "").strip()
STORE_PREFIX  = re.sub(r"[^A-Za-z0-9_.:-]", "", os.getenv("STORE_PREFIX", "botema") or "") or "botema"
STORE_LOCK_TTL_S  = max(30.0, float(os.getenv("STORE_LOCK_TTL_S", "75")))
STORE_RENEW_S     = max(5.0, min(STORE_LOCK_TTL_S / 3.0, float(os.getenv("STORE_RENEW_S", "25"))))
STORE_HEARTBEAT_S = max(10.0, float(os.getenv("STORE_HEARTBEAT_S", "60")))   # MFE/MAE de las abiertas
STORE_MIN_GAP_S   = 1.0                                                       # agrupa ráfagas de cambios
STORE_BOOT_WAIT_S = max(5.0, float(os.getenv("STORE_BOOT_WAIT_S", "20")))
STORE_CLOSED_MAX  = 500
# Sin conexión con Upstash el bot no sabe qué posiciones tenía: por defecto NO abre
# posiciones nuevas hasta conectar (sí gestiona TP/SL/DCA de las que tenga en memoria).
STORE_OFFLINE_BLOCKS_ENTRIES = _env_bool("STORE_OFFLINE_BLOCKS_ENTRIES", True)

QSTASH_TOKEN   = (os.getenv("QSTASH_TOKEN") or "").strip()
QSTASH_URL     = (os.getenv("QSTASH_URL") or "https://qstash.upstash.io").strip().rstrip("/")
KEEPALIVE_URL  = (os.getenv("KEEPALIVE_URL") or "").strip()
if not KEEPALIVE_URL and os.getenv("RENDER_EXTERNAL_URL"):
    KEEPALIVE_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip().rstrip("/") + "/health"
KEEPALIVE_CRON = (os.getenv("KEEPALIVE_CRON") or "*/10 * * * *").strip()

# Guardado atómico con cerrojo: solo escribe quien tiene el lock (si nadie lo tiene,
# lo recupera). ARGV: 1 id · 2 ttl · 3 config · 4 state · 5 reemplazar listas ·
# 6 máx. cerrados · 7 máx. estadísticas · 8 nº cerrados + cerrados · nº stats + stats
_LUA_SAVE = """
local cur = redis.call('GET', KEYS[1])
if cur and cur ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
if ARGV[3] ~= '' then redis.call('SET', KEYS[2], ARGV[3]) end
if ARGV[4] ~= '' then redis.call('SET', KEYS[3], ARGV[4]) end
if ARGV[5] == '1' then redis.call('DEL', KEYS[4], KEYS[5]) end
local i = 8
local n = tonumber(ARGV[i])
i = i + 1
for j = 1, n do
  redis.call('LPUSH', KEYS[4], ARGV[i])
  i = i + 1
end
if n > 0 then redis.call('LTRIM', KEYS[4], 0, tonumber(ARGV[6]) - 1) end
n = tonumber(ARGV[i])
i = i + 1
for j = 1, n do
  redis.call('RPUSH', KEYS[5], ARGV[i])
  i = i + 1
end
if n > 0 then redis.call('LTRIM', KEYS[5], -tonumber(ARGV[7]), -1) end
return 1
"""
_LUA_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


class UpstashError(Exception):
    def __init__(self, msg: str, status: int = 0, fatal: bool = False) -> None:
        super().__init__(msg)
        self.status = status
        self.fatal = fatal          # credenciales o URL mal puestas: reintentar no sirve


class UpstashRedis:
    """Cliente mínimo de la API REST de Upstash Redis (solo librería estándar)."""

    def __init__(self, url: str, token: str, timeout: float = 6.0) -> None:
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout
        self.calls = 0
        self.bytes_out = 0
        self.bytes_in = 0

    def _post(self, path: str, payload: Any) -> Any:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        req = urllib.request.Request(self.url + path, data=body, method="POST", headers={
            "Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
        self.calls += 1
        self.bytes_out += len(body)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            try:
                detail = json.loads(exc.read() or b"{}").get("error", "")
            except Exception:
                detail = ""
            msg = f"HTTP {exc.code}" + (f": {detail}" if detail else "")
            if exc.code in (401, 403):
                msg += " (revisa UPSTASH_REDIS_REST_TOKEN)"
            elif exc.code == 404:
                msg += " (revisa UPSTASH_REDIS_REST_URL)"
            raise UpstashError(msg, exc.code, fatal=exc.code in (401, 403, 404))
        except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError, OSError) as exc:
            reason = getattr(exc, "reason", None) or exc
            raise UpstashError(f"sin conexión con Upstash: {reason}", 0)
        self.bytes_in += len(raw)
        try:
            return json.loads(raw)
        except Exception:
            raise UpstashError("respuesta no válida de Upstash", 0)

    def cmd(self, *args: Any) -> Any:
        data = self._post("", [a if isinstance(a, (str, int, float)) else str(a) for a in args])
        if isinstance(data, dict) and data.get("error"):
            raise UpstashError(str(data["error"]), 400)
        return data.get("result") if isinstance(data, dict) else None

    def pipeline(self, cmds: List[List[Any]]) -> List[Any]:
        data = self._post("/pipeline", cmds)
        if isinstance(data, dict) and data.get("error"):
            raise UpstashError(str(data["error"]), 400)
        out = []
        for item in data if isinstance(data, list) else []:
            if isinstance(item, dict) and item.get("error"):
                raise UpstashError(str(item["error"]), 400)
            out.append(item.get("result") if isinstance(item, dict) else None)
        return out


def _json_loads_safe(raw: Any) -> Any:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _num(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if v == v else default
    except (TypeError, ValueError):
        return default


class StateStore:
    """Memoria del bot en Upstash Redis. Un hilo propio guarda los cambios y
    renueva el cerrojo; el bot solo avisa (mark_dirty / mark_config / push_closed)."""

    STATUS_TEXT = {
        "off":        "Desactivada",
        "connecting": "Conectando",
        "active":     "Activa",
        "waiting":    "En espera",
        "standby":    "En espera",
        "degraded":   "Sin conexión",
        "error":      "Error de configuración",
    }

    def __init__(self, url: str, token: str, prefix: str) -> None:
        bad_url = bool(url) and not url.lower().startswith(("https://", "http://"))
        self.enabled = bool(url and token) and not bad_url
        self.client = UpstashRedis(url, token) if self.enabled else None
        self.prefix = prefix
        self.k_lock, self.k_config, self.k_state, self.k_closed, self.k_stats = (
            f"{prefix}:{n}" for n in ("lock", "config", "state", "closed", "stats"))
        self.host = (os.getenv("RENDER_INSTANCE_ID") or socket.gethostname() or "local").replace("|", "_")
        self.instance_id = f"{self.host}|{os.getpid()}|{int(time.time())}|{os.urandom(3).hex()}"
        self.status = "connecting" if self.enabled else ("error" if bad_url else "off")
        self.detail = ("UPSTASH_REDIS_REST_URL debe ser la URL REST (https://…upstash.io), no la redis://"
                       if bad_url else
                       "" if self.enabled else
                       "sin UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN: lo guardado se pierde al reiniciar")
        self.holder = ""
        self.bot: Any = None
        self.stopping = False
        self.loaded_info = ""
        self.last_save_ts = 0.0
        self.last_ok_ts = 0.0
        self.fail_since = 0.0
        self.saves = 0
        self.errors = 0
        self.last_error = ""
        self.keepalive: Dict[str, Any] = {"status": "off", "detail": "", "url": KEEPALIVE_URL,
                                          "cron": KEEPALIVE_CRON, "schedule_id": ""}
        self._lock = threading.Lock()           # colas y marcas
        self._io = threading.Lock()             # una petición a Upstash a la vez
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._pending_closed: deque = deque()
        self._pending_stats: deque = deque()
        self._config_gen = 0
        self._config_saved_gen = 0
        self._replace_lists = False
        self._last_struct: Optional[str] = None
        self._last_full: Optional[str] = None
        self._last_state_ts = 0.0
        self._last_renew = 0.0
        self._wait_since = 0.0
        self._fails = 0
        self._shut = False
        self._takeover_lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._recheck = False
        self._last_hb_check = 0.0

    # ── Estado para el bot y la web ───────────────────────────────────────
    @property
    def manage_ok(self) -> bool:
        """¿Puede esta instancia gestionar posiciones (TP/SL/DCA, cierres, ajustes)?"""
        if not self.enabled:
            return True
        if self.stopping:
            return False
        return self.status in ("active", "degraded", "error")

    @property
    def entries_ok(self) -> bool:
        """¿Puede abrir posiciones nuevas?"""
        if not self.enabled:
            return True
        if self.stopping:
            return False
        if self.status == "degraded":
            return not STORE_OFFLINE_BLOCKS_ENTRIES
        return self.status in ("active", "error")

    def block_text(self) -> str:
        if self.stopping:
            return "el bot se está deteniendo (reinicio de Render)"
        if self.status in ("waiting", "standby"):
            who = self.holder.split("|")[0] if self.holder else "otra instancia"
            return (f"otra instancia del bot ({who}) tiene el control de la memoria; "
                    "esta espera para no operar dos veces")
        if self.status == "degraded":
            return "sin conexión con la memoria Upstash: no se abren posiciones nuevas hasta conectar"
        if self.status == "connecting":
            return "recuperando el estado guardado en Upstash"
        return ""

    def view(self) -> dict:
        now = time.time()
        c = self.client
        return {
            "enabled":      self.enabled,
            "status":       self.status,
            "text":         self.STATUS_TEXT.get(self.status, self.status),
            "detail":       self.detail,
            "block":        self.block_text() if not self.manage_ok or not self.entries_ok else "",
            "holder":       self.holder.split("|")[0] if self.holder else "",
            "instance":     self.host,
            "prefix":       self.prefix,
            "loaded":       self.loaded_info,
            "last_save_ts": self.last_save_ts,
            "last_save_ago": (now - self.last_save_ts) if self.last_save_ts else None,
            "fail_s":       (now - self.fail_since) if self.fail_since else 0.0,
            "saves":        self.saves,
            "errors":       self.errors,
            "last_error":   self.last_error,
            "calls":        c.calls if c else 0,
            "kb_out":       round(c.bytes_out / 1024.0, 1) if c else 0.0,
            "keepalive":    dict(self.keepalive),
        }

    # ── Avisos del bot (no bloquean) ──────────────────────────────────────
    def mark_dirty(self) -> None:
        if self.enabled:
            self._wake.set()

    def mark_config(self) -> None:
        if self.enabled:
            with self._lock:
                self._config_gen += 1
            self._wake.set()

    def push_closed(self, closed_rec: Optional[dict], stat_rec: Optional[dict]) -> None:
        if not self.enabled:
            return
        with self._lock:
            if closed_rec is not None:
                self._pending_closed.append(json.dumps(closed_rec, ensure_ascii=False,
                                                       separators=(",", ":"), default=str))
            if stat_rec is not None:
                self._pending_stats.append(json.dumps(stat_rec, ensure_ascii=False,
                                                      separators=(",", ":"), default=str))
            for q_ in (self._pending_closed, self._pending_stats):
                while len(q_) > 5000:           # sin conexión mucho tiempo: se descartan los más viejos
                    q_.popleft()
        self._wake.set()

    def request_full_resync(self) -> None:
        """Tras unir memoria local y Upstash: reescribe las listas enteras."""
        with self._lock:
            self._replace_lists = True
            self._config_gen += 1
        self._last_struct = None
        self._wake.set()

    # ── Arranque (lo llama el bot ANTES de operar) ────────────────────────
    def boot(self) -> dict:
        """Toma el cerrojo y lee todo lo guardado. Reintenta los errores de red
        hasta STORE_BOOT_WAIT_S. Nunca lanza excepciones."""
        deadline = time.time() + STORE_BOOT_WAIT_S
        attempt = 0
        while True:
            try:
                with self._io:
                    acquired, holder = self._acquire(force=False)
                    data = self._load_all()
                self._ok()
                self.holder = "" if acquired else (holder or "")
                return {"ok": True, "acquired": acquired, "holder": holder, "data": data}
            except UpstashError as exc:
                self._fail(exc)
                if exc.fatal:
                    return {"ok": False, "fatal": True, "error": str(exc)}
                attempt += 1
                if time.time() >= deadline:
                    return {"ok": False, "fatal": False, "error": str(exc)}
                time.sleep(min(5.0, 1.0 + attempt))
            except Exception as exc:                     # nunca tumbar el arranque
                self._fail(exc)
                return {"ok": False, "fatal": False, "error": repr(exc)}

    def _acquire(self, force: bool) -> Tuple[bool, str]:
        """(cerrojo tomado, quién lo tiene). Con force lo toma aunque sea de otro.
        Requiere self._io."""
        c = self.client
        ttl = int(STORE_LOCK_TTL_S)
        if force:
            c.cmd("SET", self.k_lock, self.instance_id, "EX", ttl)
            return True, self.instance_id
        for _ in range(3):
            if c.cmd("SET", self.k_lock, self.instance_id, "NX", "EX", ttl) == "OK":
                return True, self.instance_id
            holder = c.cmd("GET", self.k_lock)
            if holder is None:
                continue                                 # expiró justo ahora: reintenta
            if holder == self.instance_id:
                return True, holder
            parts = str(holder).split("|")
            # Proceso anterior de ESTE mismo contenedor que ya no existe (se cayó): se toma ya
            if len(parts) >= 2 and parts[0] == self.host and parts[1].isdigit() \
                    and int(parts[1]) != os.getpid() and not _pid_alive(int(parts[1])):
                c.cmd("SET", self.k_lock, self.instance_id, "EX", ttl)
                return True, self.instance_id
            return False, str(holder)
        return False, ""

    def _load_all(self) -> dict:
        """Lee config, estado, cerrados y estadísticas. Requiere self._io."""
        c = self.client
        cfg_raw, st_raw, closed_raw, n_stats = c.pipeline([
            ["GET", self.k_config], ["GET", self.k_state],
            ["LRANGE", self.k_closed, 0, STORE_CLOSED_MAX - 1], ["LLEN", self.k_stats]])
        stats_raw: List[Any] = []
        n_stats = int(n_stats or 0)
        if n_stats:
            chunks = [["LRANGE", self.k_stats, i, i + 999] for i in range(0, n_stats, 1000)]
            for part in c.pipeline(chunks):
                stats_raw.extend(part or [])
        closed = [x for x in (_json_loads_safe(r) for r in closed_raw or []) if isinstance(x, dict)]
        stats = [x for x in (_json_loads_safe(r) for r in stats_raw) if isinstance(x, dict)]
        cfg = _json_loads_safe(cfg_raw)
        st = _json_loads_safe(st_raw)
        return {"config": cfg if isinstance(cfg, dict) else None,
                "state": st if isinstance(st, dict) else None,
                "closed": closed, "stats": stats}

    def start(self, status: str) -> None:
        """Arranca el hilo de guardado con el estado decidido en el arranque."""
        if not self.enabled or self._thread is not None:
            return
        self.status = status
        if status in ("waiting", "standby"):
            self._wait_since = time.time()
        self._thread = threading.Thread(target=self._run, daemon=True, name="StateStore")
        self._thread.start()

    # ── Hilo de guardado ──────────────────────────────────────────────────
    def _run(self) -> None:
        while not self.stopping:
            st = self.status
            if st == "active":
                timeout = 1.0
            elif st in ("waiting", "standby"):
                timeout = 3.0 if time.time() - self._wait_since < 300 else 20.0
            elif st == "degraded":
                timeout = min(60.0, 5.0 * (2 ** min(self._fails, 4)))
            else:
                timeout = 30.0
            woke = self._wake.wait(timeout)
            self._wake.clear()
            if self.stopping:
                break
            try:
                if st == "active":
                    self.flush(woke=woke)
                elif st in ("waiting", "standby"):
                    self.takeover(force=False)
                elif st == "degraded":
                    self._reconnect()
            except UpstashError as exc:
                self._fail(exc)
                if exc.fatal:
                    self._set_error(str(exc))
            except Exception as exc:                      # el hilo no debe morir nunca
                self._fail(exc)

    def flush(self, final: bool = False, woke: bool = True) -> bool:
        """Guarda lo que haya cambiado y renueva el cerrojo. True si quedó guardado."""
        with self._flush_lock:                            # hilo y SIGTERM nunca a la vez
            return self._flush_locked(final, woke)

    def _flush_locked(self, final: bool, woke: bool) -> bool:
        bot = self.bot
        if bot is None or self.status != "active":
            return False
        now = time.time()
        with self._lock:
            pending = bool(self._pending_closed or self._pending_stats or self._replace_lists
                           or self._config_gen != self._config_saved_gen)
        hb_due = now - self._last_hb_check >= STORE_HEARTBEAT_S
        need_renew = now - self._last_renew >= STORE_RENEW_S
        if not (final or woke or pending or hb_due or need_renew or self._recheck):
            return True                                  # nada nuevo: ni siquiera se calcula
        if hb_due:
            self._last_hb_check = now
        state = bot._collect_state()
        struct_sig, full_sig, state_json = bot._state_signature(state)
        with self._lock:
            cfg_gen = self._config_gen
            cfg_dirty = cfg_gen != self._config_saved_gen
            closed = list(self._pending_closed)
            stats = list(self._pending_stats)
            replace = self._replace_lists
        struct_changed = struct_sig != self._last_struct
        need_state = (final or replace or bool(closed) or struct_changed
                      or (full_sig != self._last_full and now - self._last_state_ts >= STORE_HEARTBEAT_S))
        if not (need_state or cfg_dirty or stats or need_renew):
            self._recheck = False
            return True
        # Ráfagas de cambios (p. ej. varios tramos DCA seguidos): se agrupan ~1 s
        if (struct_changed and not (final or closed or replace or need_renew)
                and now - self._last_state_ts < STORE_MIN_GAP_S):
            self._recheck = True                         # el hilo vuelve en 1 s y lo guarda
            return True
        self._recheck = False
        cfg_json = (json.dumps(_settings_dict(), ensure_ascii=False, separators=(",", ":"), default=str)
                    if (cfg_dirty or replace) else "")
        if replace:
            closed, stats = bot._history_for_resync()
        argv: List[Any] = [self.instance_id, int(STORE_LOCK_TTL_S), cfg_json,
                           state_json if (need_state or replace) else "",
                           "1" if replace else "0", STORE_CLOSED_MAX, STATS_MAX_IN_MEMORY,
                           len(closed), *closed, len(stats), *stats]
        with self._io:
            if self.status != "active":
                return False
            res = self.client.cmd("EVAL", _LUA_SAVE, 5, self.k_lock, self.k_config, self.k_state,
                                  self.k_closed, self.k_stats, *argv)
        if res in (0, "0", None):
            self._lost()
            return False
        self._ok()
        self._last_renew = now
        if need_state or replace:
            self._last_struct, self._last_full, self._last_state_ts = struct_sig, full_sig, now
            self.last_save_ts = now
            self.saves += 1
        with self._lock:
            if cfg_json:
                self._config_saved_gen = cfg_gen
            if replace:
                self._replace_lists = False
                self._pending_closed.clear()
                self._pending_stats.clear()
            else:
                for _ in range(len(closed)):
                    if self._pending_closed:
                        self._pending_closed.popleft()
                for _ in range(len(stats)):
                    if self._pending_stats:
                        self._pending_stats.popleft()
        return True

    def takeover(self, force: bool) -> bool:
        """Toma el control (si el cerrojo está libre, o a la fuerza), recarga TODO
        de Upstash y lo aplica en el bot. True si esta instancia pasa a mandar."""
        bot = self.bot
        if bot is None:
            return False
        # Un solo intento a la vez (hilo de la memoria y botón de la web)
        if not self._takeover_lock.acquire(timeout=30.0 if force else 0.0):
            return False
        try:
            return self._takeover_locked(force)
        finally:
            self._takeover_lock.release()

    def _takeover_locked(self, force: bool) -> bool:
        bot = self.bot
        if self.status == "active":
            return True
        with self._io:
            acquired, holder = self._acquire(force=force)
            if not acquired:
                self.holder = holder or self.holder
                self._ok()
                return False
            data = self._load_all()
        self._ok()
        self.holder = ""
        ok = bot._store_apply_threadsafe(data, "takeover")
        if not ok:
            with self._io:
                self.client.cmd("EVAL", _LUA_RELEASE, 1, self.k_lock, self.instance_id)
            return False
        self._last_struct = None
        self._last_renew = time.time()
        self.status = "active"
        self.detail = ""
        bot.log("🔐 Memoria Upstash: esta instancia toma el control y continúa con lo guardado")
        self._wake.set()
        return True

    def _reconnect(self) -> None:
        """Arrancó sin conexión: al volver Upstash une lo de allí con lo de aquí."""
        bot = self.bot
        with self._io:
            acquired, holder = self._acquire(force=False)
            if not acquired:
                self._ok()
                self.holder = holder
                self.status, self._wait_since = "standby", time.time()
                bot.log(f"⚠️ Memoria Upstash: otra instancia ({holder.split('|')[0]}) tiene el control; "
                        "esta queda en espera")
                return
            data = self._load_all()
        self._ok()
        if not bot._store_apply_threadsafe(data, "merge"):
            with self._io:
                self.client.cmd("EVAL", _LUA_RELEASE, 1, self.k_lock, self.instance_id)
            return
        self.status, self.detail = "active", ""
        self.request_full_resync()
        bot.log("🔐 Memoria Upstash conectada: lo guardado y lo de esta sesión quedan unidos")

    def _lost(self) -> None:
        holder = ""
        try:
            with self._io:
                holder = self.client.cmd("GET", self.k_lock) or ""
        except Exception:
            pass
        self.holder = str(holder)
        self.status, self._wait_since = "standby", time.time()
        if self.bot is not None:
            self.bot.log(f"⚠️ Memoria Upstash: otra instancia ({self.holder.split('|')[0] or '?'}) tomó el "
                         "control. Este bot deja de operar para no duplicar órdenes (en espera)")

    def _set_error(self, msg: str) -> None:
        self.status, self.detail = "error", msg
        if self.bot is not None:
            self.bot.log(f"❌ Memoria Upstash desactivada: {msg}. El bot sigue operando SIN memoria")

    def _ok(self) -> None:
        self.last_ok_ts = time.time()
        self.fail_since = 0.0
        self._fails = 0

    def _fail(self, exc: BaseException) -> None:
        self.errors += 1
        self._fails += 1
        self.last_error = str(exc)[:300]
        if not self.fail_since:
            self.fail_since = time.time()
        if self.bot is not None:
            self.bot._log_throttled("store_err", f"Memoria Upstash: {self.last_error}", 120.0)

    # ── Parada (SIGTERM de Render) ─────────────────────────────────────────
    def shutdown(self, reason: str) -> None:
        """Deja de operar, guarda lo último y suelta el cerrojo para la siguiente
        instancia. Se llama una sola vez (señal o salida)."""
        if not self.enabled or self._shut:
            return
        self._shut = True
        was_active = self.status == "active"
        self.stopping = True
        self._wake.set()
        bot = self.bot
        if not was_active or bot is None:
            return
        t_end = time.time() + 3.0
        while time.time() < t_end and bot._orders_in_flight():
            time.sleep(0.05)
        try:
            self.status = "active"                       # flush exige estado activo
            ok = self.flush(final=True)
            bot.log(f"💾 {reason}: estado guardado en Upstash" if ok else
                    f"⚠️ {reason}: no pude guardar el estado final en Upstash")
        except Exception as exc:
            bot.log(f"⚠️ {reason}: no pude guardar el estado final en Upstash: {exc}")
        try:
            with self._io:
                self.client.cmd("EVAL", _LUA_RELEASE, 1, self.k_lock, self.instance_id)
        except Exception:
            pass
        self.status = "standby"


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except Exception:
        return True
    return True


def _qstash_keepalive(store: "StateStore", log) -> None:
    """Crea (o actualiza) en QStash el horario que visita /health cada 10 min.
    Usa siempre el mismo id de horario: no se duplica en cada arranque."""
    ka = store.keepalive
    if not QSTASH_TOKEN:
        ka.update(status="off", detail="sin QSTASH_TOKEN")
        return
    if not KEEPALIVE_URL:
        ka.update(status="error", detail="falta KEEPALIVE_URL (Render la deduce de RENDER_EXTERNAL_URL)")
        log("⚠️ QStash: no sé la URL pública del bot; pon KEEPALIVE_URL=https://tu-bot.onrender.com/health")
        return
    sched_id = f"{STORE_PREFIX}-keepalive"
    req = urllib.request.Request(
        f"{QSTASH_URL}/v2/schedules/{KEEPALIVE_URL}", data=b"", method="POST",
        headers={"Authorization": "Bearer " + QSTASH_TOKEN, "Upstash-Cron": KEEPALIVE_CRON,
                 "Upstash-Method": "GET", "Upstash-Schedule-Id": sched_id,
                 "Upstash-Retries": "1", "Content-Type": "text/plain"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                body = json.loads(resp.read() or b"{}")
            ka.update(status="active", schedule_id=body.get("scheduleId", sched_id),
                      detail=f"visita {KEEPALIVE_URL} ({KEEPALIVE_CRON})")
            every = "cada 10 min" if KEEPALIVE_CRON == "*/10 * * * *" else f"con el horario {KEEPALIVE_CRON}"
            log(f"⏰ QStash: keep-alive activo → {KEEPALIVE_URL} {every} "
                f"(horario {ka['schedule_id']}); Render free no dormirá el bot")
            return
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = json.loads(exc.read() or b"{}").get("error", "")
            except Exception:
                pass
            ka.update(status="error", detail=f"HTTP {exc.code} {detail}".strip())
            if exc.code in (400, 401, 403):
                log(f"❌ QStash: no pude crear el keep-alive (HTTP {exc.code} {detail}). Revisa QSTASH_TOKEN"
                    + (" y QSTASH_URL (la región de tu QStash)" if exc.code != 400 else ""))
                return
        except Exception as exc:
            ka.update(status="error", detail=str(exc)[:200])
        time.sleep(5 * (attempt + 1))
    log(f"⚠️ QStash: no pude crear el keep-alive ({ka.get('detail')})")


# ─────────────────────────────────────────────────────────────────────────────
# CLIENTE BINANCE FUTURES
# ─────────────────────────────────────────────────────────────────────────────

class BinanceFuturesClient:
    def __init__(self) -> None:
        self.exchange_filters: Dict[str, Dict[str, float]] = {}
        # Última respuesta de exchangeInfo (al arrancar se reutiliza para la lista
        # de símbolos en vez de pedirla dos veces: una petición menos al proxy)
        self.last_exchange_info: Tuple[float, Optional[dict]] = (0.0, None)

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
        self.last_exchange_info = (time.time(), data)
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
        self._persist_sig: Optional[tuple] = None      # lo último escrito (no se reescribe igual)

        # ── Memoria (RSS) ─────────────────────────────────────────────────
        self.mem_rss_mb:  float = 0.0
        self.mem_peak_mb: float = 0.0
        self.mem_trims:   int   = 0
        self._mem_last_log: float = 0.0

        # ── Guardia anti doble cierre ─────────────────────────────────────
        self._closing_symbols: set[str] = set()

        # ── Cierre masivo "una a una" (estado visible en la web) ──────────
        self._close_all_active = False        # mientras es True no se abren entradas/DCA nuevos
        self.close_all_state: Dict[str, Any] = self._empty_close_all()

        # ── Executor bridge ───────────────────────────────────────────────
        self._trade_id_seq: int = 0
        self.executor = ExecutorBridge(
            executor_url=EXEC.url,              # el guardado desde la web, o EXECUTOR_URL
            signal_secret=EXECUTOR_SECRET,
        )
        # Cambios de pausa (entradas y executor): manual, con tiempo y reanudación automática
        self._pause_lock = threading.RLock()
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

        # ── Memoria externa (Upstash Redis): config, posiciones, cierres… ──
        self.store = StateStore(UPSTASH_URL, UPSTASH_TOKEN, STORE_PREFIX)
        self.store.bot = self
        self._pnl_at_boot = 0.0
        self._config_changed_here = False      # ajustes cambiados en esta sesión (para unir memorias)
        global _SETTINGS_SAVED_HOOK
        _SETTINGS_SAVED_HOOK = self._on_settings_saved

        # ── Estadísticas MFE/MAE por operación cerrada (se cargan del disco) ──
        # Acotadas a las STATS_MAX_IN_MEMORY más recientes: antes la lista crecía
        # sin límite (≈3 KB por operación) y se cargaba entera del disco.
        self.trade_stats: deque = deque(maxlen=STATS_MAX_IN_MEMORY)
        self._stats_lock = threading.Lock()
        self._load_trade_stats()
        self._restore_history()
        self._pnl_at_boot = self.total_realized_pnl

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
        # Conectar el logger del executor y del enrutador REST al sistema de log del bot
        self.executor.logger = self.log
        REST_ROUTER.logger = self.log
        self.log(REST_ROUTER.startup_message())
        if EXEC.url:
            src = "guardado desde la web" if EXEC.url != EXECUTOR_URL else "EXECUTOR_URL"
            self.log(f"[executor] Bridge configurado → {EXEC.url} ({src})")
            if self._exec_paused_now():
                self.log("[executor] ⏸ El envío de operaciones nuevas al executor sigue en pausa "
                         + self._pause_left_txt(EXEC.pause_until))
        else:
            self.log("[executor] Sin link de executor — señales desactivadas")

        self.log("Bot iniciado — modo " + (
            "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL"
        ))
        self.log(f"[mem] {_MALLOC_NOTE} · RSS al arrancar {self._note_rss():.0f} MB · "
                 f"límite de la instancia {MEM_LIMIT_MB:.0f} MB · aviso a partir de {MEM_WARN_MB:.0f} MB")
        if _SETTINGS_SOURCE == "file":
            self.log(f"Ajustes restaurados de {SETTINGS_FILE} (los guardados desde la web) · "
                     f"EMA {EMA_FAST}/{EMA_SLOW}")
        elif not self.store.enabled:
            self.log(f"⚠️ Sin ajustes guardados en {SETTINGS_FILE}"
                     + (" (archivo ilegible)" if _SETTINGS_SOURCE == "error" else "")
                     + f": se usan las variables de entorno (EMA {EMA_FAST}/{EMA_SLOW}). En Render free "
                       "el disco se borra en cada reinicio: activa la memoria Upstash "
                       "(UPSTASH_REDIS_REST_URL y UPSTASH_REDIS_REST_TOKEN) para que todo sobreviva.")

        # Memoria externa: ajustes, posiciones abiertas, cierres, estadísticas y
        # cooldowns ANTES de conectar con Binance y de evaluar nada.
        await self._store_boot()
        if QSTASH_TOKEN:
            threading.Thread(target=_qstash_keepalive, args=(self.store, self.log),
                             daemon=True, name="QStashKeepAlive").start()
        else:
            self.store.keepalive.update(status="off", detail="sin QSTASH_TOKEN")
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
        self.client.last_exchange_info = (0.0, None)     # libera la copia del arranque

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
        En modo invertido abre el lado CONTRARIO (UP → SHORT, DOWN → LONG).
        Si ya hay posición abierta el cruce se ignora (el DCA/TP/SL la gestionan)."""
        if not self._is_tradable(symbol):
            return
        if self._close_all_active:            # cierre masivo en curso: no abrir nada nuevo
            return
        signal_side = "LONG" if direction == "UP" else "SHORT"
        inverted = MODE.inverted
        side = _opp(signal_side) if inverted else signal_side
        now = time.time()
        if not self.store.entries_ok:
            if self.store.manage_ok:           # sin conexión con Upstash: se registra el freno
                self._record_block(symbol, side, "memoria", self.store.block_text(), is_dca=False,
                                   extra=f"cruce {direction}")
            return
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
        # igual que siempre; aquí solo se decide si puede abrir. El filtro BTC mira
        # el lado de la SEÑAL: en modo invertido solo se abre el espejo de lo que el
        # bot normal habría abierto.
        first_level, first_notional = ENTRY_LADDER[0]
        blocked = self._gate_check(signal_side, first_notional, is_dca=False)
        if blocked is not None:
            self._record_block(symbol, side, blocked[0], blocked[1], is_dca=False,
                               extra=f"cruce {direction}" + (", invertida" if inverted else ""))
            return
        self.log(f"CRUCE EMA{EMA_FAST}/{EMA_SLOW} {direction} {symbol} → {side}"
                 + (f" (INVERTIDO: la señal era {signal_side})" if inverted else "")
                 + f" (cierre vela={cross_price:.6f} | px={price:.6f})")
        self._entry_inflight.add(symbol)
        self._spawn(self._enter_levels(symbol, [(0, first_level, first_notional)], side, inverted))

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
                         price: float, inverted: bool = False) -> Optional[Tuple[str, str]]:
        """Si la condición EMA está activa y la posición ya tiene ≥ dca_ema_after
        tramos, el tramo idx (0 = 1.er tramo) solo entra si LONG: precio > EMA y
        SHORT: precio < EMA. `direction` es el lado de la SEÑAL (en una posición
        invertida, el que tendría el bot normal: así el DCA es su espejo exacto).
        Devuelve ("ema_dca", motivo) si lo frena."""
        if not RISK.dca_ema_enabled or idx < RISK.dca_ema_after:
            return None
        p = RISK.dca_ema_period
        ema = self._trend_ema(symbol, p)
        if ema is None:
            return "ema_dca", f"EMA{p} de {symbol} sin velas suficientes (condición activa)"
        tail = f"; espejo de la señal {direction}, posición invertida" if inverted else ""
        if direction == "LONG" and not price > ema:
            return "ema_dca", (f"tramo {idx + 1}: precio {price:.6g} ≤ EMA{p} {ema:.6g} "
                               f"(LONG exige precio por encima{tail})")
        if direction == "SHORT" and not price < ema:
            return "ema_dca", (f"tramo {idx + 1}: precio {price:.6g} ≥ EMA{p} {ema:.6g} "
                               f"(SHORT exige precio por debajo{tail})")
        return None

    @staticmethod
    def _pause_active(paused: bool, until: float, now: Optional[float] = None) -> bool:
        """Una pausa con tiempo deja de contar en cuanto vence (aunque el
        mantenimiento aún no la haya quitado)."""
        if not paused:
            return False
        return not (until and (now or time.time()) >= until)

    def _entries_paused_now(self, now: Optional[float] = None) -> bool:
        return self._pause_active(RISK.entries_paused, RISK.pause_until, now)

    def _exec_paused_now(self, now: Optional[float] = None) -> bool:
        return self._pause_active(EXEC.paused, EXEC.pause_until, now)

    @staticmethod
    def _hhmm_utc(ts: float) -> str:
        return datetime.fromtimestamp(ts, timezone.utc).strftime("%H:%M UTC")

    @classmethod
    def _pause_left_txt(cls, until: float) -> str:
        if not until:
            return "hasta que la reanudes"
        return f"hasta las {cls._hhmm_utc(until)} (quedan {_fmt_secs(until - time.time())})"

    def _gate_check(self, side: str, notional: float, is_dca: bool,
                    exposure: Optional[float] = None) -> Optional[Tuple[str, str]]:
        """Devuelve (tipo, detalle) si la entrada NO puede abrirse, o None.
        `side` es el lado de la SEÑAL (el que abriría el bot normal), que es el
        que mira el filtro BTC también en modo invertido.
        NO toma self.lock salvo para calcular la exposición cuando no se pasa."""
        if not is_dca and self._entries_paused_now():
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
            info_ts, info = self.client.last_exchange_info
            if info and time.time() - info_ts < 120:
                data = info                          # recién descargado al arrancar
                self.log("REST: lista de símbolos tomada del exchangeInfo recién descargado")
            else:
                self.log("REST: obteniendo lista completa de símbolos de futuros USDT-M...")
                data = await self.client.request("GET", "/fapi/v1/exchangeInfo")
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
        if not self.store.manage_ok:          # otra instancia tiene el control (o se está deteniendo)
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
            target = pos.exit_tp()            # normal: notional×TP · invertida: espejo del SL
            sl_usd = pos.exit_sl()            # normal: sl_usd     · invertida: −notional×TP
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
        En una posición invertida se abren en los MISMOS precios que en el bot
        normal, es decir, cuando el precio va a favor de ella (espejo exacto).
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
            trigger   = pos.trigger_pct(price)
            direction = pos.direction
            inverted  = pos.inverted
            sig_dir   = pos.signal_dir
            n_fills   = len(pos.fills)
        ladder = ENTRY_LADDER
        due = []
        for idx in range(n_fills, len(ladder)):
            lvl, nt = ladder[idx]
            if lvl <= 0 or trigger < lvl or (symbol, idx) in self._entry_reserved:
                break
            due.append((idx, lvl, nt))
        if not due:
            return
        first_idx, first_lvl, first_nt = due[0]
        # Límite de exposición aplicado al DCA (solo si así se configuró en la web)
        blocked = self._gate_check(sig_dir, first_nt, is_dca=True)
        # Condición EMA del DCA (a partir de N tramos), evaluada sobre la señal
        if blocked is None:
            blocked = self._dca_trend_block(symbol, sig_dir, first_idx, price, inverted)
        if blocked is not None:
            self._record_block(symbol, direction, blocked[0], blocked[1], is_dca=True,
                               extra=f"tramo {first_idx + 1} al {first_lvl:g}%"
                                     + (" a favor (invertida)" if inverted else ""))
            return
        self._entry_inflight.add(symbol)
        self._spawn(self._enter_levels(symbol, due, direction, inverted))

    def _block_by_price(self, symbol: str, price: float) -> None:
        with self.lock:
            newly = symbol not in self.price_blocked
            self.price_blocked.add(symbol)
        if newly:
            self.log(f"BLOQUEADO permanente {symbol}: precio {price:.4f} > {MAX_PRICE_BLOCK} USD")

    async def _enter_levels(self, symbol: str, due: list, direction: str,
                            inverted: bool = False) -> None:
        """Abre, en orden, los tramos vencidos [(índice, % de la escalera, notional), ...].
        `direction` es el lado REAL; `inverted` dice si la posición es espejo.
        Corre como tarea: el motor no espera a la orden."""
        opened_any = False
        sig_dir = _opp(direction) if inverted else direction
        try:
            for idx, level, notional in due:
                price = self._price_for(symbol)
                if price is None:
                    break
                if idx > 0:                  # DCA: reconfirma que el precio sigue en el nivel
                    with self.lock:
                        pos = self.positions.get(symbol)
                        trigger = pos.trigger_pct(price) if pos else 0.0
                    if trigger < level:
                        break
                    trend = self._dca_trend_block(symbol, sig_dir, idx, price, inverted)
                    if trend is not None:    # p. ej. el tramo 3 entra y el 4 ya exige la EMA
                        self._record_block(symbol, direction, trend[0], trend[1], is_dca=True,
                                           extra=f"tramo {idx + 1} al {level:g}%"
                                                 + (" a favor (invertida)" if inverted else ""))
                        break
                if MAX_PRICE_BLOCK > 0 and price > MAX_PRICE_BLOCK:
                    self._block_by_price(symbol, price)
                    break
                if not await self._ensure_position(symbol, idx, level, notional, price, direction,
                                                   inverted):
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
                               price: float, direction: str, inverted: bool = False) -> bool:
        """Abre el tramo de índice `idx` (0 = entrada del cruce). True si se abrió.
        `direction` es el lado REAL de la orden; `inverted` marca la posición espejo.
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
                  or pos.inverted != inverted
                  or len(pos.fills) != idx):              # el tramo idx solo sigue al idx-1
                return False
            # Comprobación definitiva de las guardas (con la exposición real, incluidas
            # las órdenes en vuelo de otros símbolos) justo antes de reservar.
            blocked = self._gate_check(_opp(direction) if inverted else direction, notional,
                                       is_dca, exposure=self._exposure_locked())
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
            exec_note = ""
            with self.lock:
                pos = self.positions.get(symbol)
                if pos is None:
                    pos = BotPosition(symbol=symbol, trade_id=trade_id, direction=direction,
                                      inverted=inverted)
                    # Posición NUEVA: va al executor salvo que su envío esté en pausa
                    pos.exec_url, exec_note = self._exec_target_new()
                    self.positions[symbol] = pos
                elif pos.trade_id == 0:
                    pos.trade_id = trade_id
                else:
                    trade_id = pos.trade_id
                pos.fills.append(fill)
                pos.refresh_auto_sl()           # 1 tramo → notional×0.251 | 2+ → estándar | manual → intacto
                tp_now, sl_now = pos.exit_tp(), pos.exit_sl()
                sl_mode, n_fills = pos.sl_mode, len(pos.fills)
                exec_target = pos.exec_url      # DCA → al mismo executor que la apertura

            self._watch_add(symbol)             # la posición abierta siempre queda vigilada
            if inverted:
                exits = f"TP={tp_now:+.4f} USD ({sl_mode}) | SL={sl_now:.4f} USD (−notional×TP)"
                where = f"{level:g}% a favor" if idx > 0 else "entrada"
            else:
                exits = f"SL={sl_now:.4f} USD ({sl_mode})"
                where = f"{level:g}% en contra"
            self.log(
                f"{direction} {symbol}{' INVERTIDA' if inverted else ''}: tramo {idx + 1} ({where}) | "
                f"{notional:.2f} USDT | qty={qty} | px={price:.6f} | trade_id={trade_id} | "
                f"tramos={n_fills} | {exits}"
                + ("" if exec_target else
                   f" | sin executor ({exec_note or 'la posición no se envió al executor'})")
            )
            if exec_target:
                self.executor.notify_open(
                    trade_id=trade_id,
                    symbol=symbol,
                    direction=direction,
                    price=price,
                    quantity=qty,
                    notional=notional,
                    level=level,
                    url=exec_target,
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
                inverted  = pos.inverted
                sl_usd    = pos.exit_sl()       # SL y TP en PnL real (en una invertida, en espejo)
                target    = pos.exit_tp()
                exec_url  = pos.exec_url        # el cierre va al executor donde se abrió
                pnl       = pos.pnl_sign * sum((f.entry_price - price) * f.qty for f in snapshot) if price > 0 else 0.0
                # Re-verifica la condición con el estado actual (pudo cambiar el SL/TP desde el dashboard)
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
                    "inverted":      inverted,
                    "signal_dir":    _opp(direction) if inverted else direction,
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
                         "GLOBAL": "stop global", "GLOBAL_TP": "TP global"}.get(reason, reason)
                self.log(f"Error cerrando {label} {symbol}: {exc}")
                return False

            unblock_str = ""
            unblock_ts  = 0.0
            leftover = 0
            closed_rec: Optional[dict] = None
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
                    closed_rec = {
                        "symbol":      symbol,
                        "trade_id":    trade_id,
                        "direction":   direction,
                        "inverted":    inverted,
                        "fills_n":     fills_n,
                        "pnl":         pnl,
                        "target":      target,
                        "qty":         qty,
                        "avg_entry":   avg_ent,
                        "close_price": price,
                        "notional":    notional,
                        "closed_at":   datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                        "closed_ts":   now_ts,
                        "unblock_at":  unblock_str,
                        "unblock_ts":  unblock_ts,
                        "reason":      reason,
                        **exc,
                    }
                    self.closed_trades.insert(0, closed_rec)
                    self.closed_trades = self.closed_trades[:500]

            self._record_trade_stat(stat_rec)
            self.store.push_closed(closed_rec, stat_rec)       # a la memoria externa (Upstash)
            if exec_url:
                self.executor.notify_close(
                    trade_id=trade_id,
                    symbol=symbol,
                    direction=direction,
                    # Al Executor el stop/TP global le llega como "MANUAL" (motivo que ya conoce)
                    reason="MANUAL" if reason in ("GLOBAL", "GLOBAL_TP") else reason,
                    close_price=price,
                    pnl=pnl,
                    url=exec_url,
                )
            tag = " (invertida)" if inverted else ""
            if reason == "TP":
                self.log(
                    f"CIERRE TP {symbol}{tag}: PnL={pnl:.4f} | objetivo={target:.4f} | "
                    f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
                )
            elif reason == "SL":
                self.log(
                    f"⛔ STOP LOSS {symbol}{tag}: PnL={pnl:.4f} | SL configurado={sl_usd:.4f} | "
                    f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
                )
            elif reason == "GLOBAL":
                self.log(
                    f"🛑 CIERRE POR STOP GLOBAL {symbol}{tag}: PnL={pnl:.4f} | "
                    f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
                )
            elif reason == "GLOBAL_TP":
                self.log(
                    f"🎯 CIERRE POR TP GLOBAL {symbol}{tag}: PnL={pnl:.4f} | "
                    f"px={price:.6f} | bloqueado {COOLDOWN_SECONDS // 3600}h hasta {unblock_str}"
                )
            else:
                self.log(
                    f"✋ CIERRE MANUAL {symbol}{tag}: PnL={pnl:.4f} | "
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
        """Carga el histórico de operaciones (una línea JSON por cierre). En memoria
        quedan solo las STATS_MAX_IN_MEMORY más recientes; el trade_id se sigue
        calculando sobre TODO el archivo."""
        try:
            if not os.path.exists(STATS_FILE):
                return
            max_id = 0
            with open(STATS_FILE, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    if not isinstance(rec, dict):
                        continue
                    try:
                        max_id = max(max_id, int(rec.get("trade_id", 0) or 0))
                    except (TypeError, ValueError):
                        pass
                    self.trade_stats.append(rec)   # deque(maxlen): se descartan las más viejas
            if max_id > self._trade_id_seq:
                self._trade_id_seq = max_id        # evita repetir trade_id tras reinicios
        except Exception as exc:
            print(f"No pude leer {STATS_FILE}: {exc}", flush=True)

    def _restore_history(self) -> None:
        """Tras un reinicio recupera del estado guardado en disco el historial, el PnL
        realizado y (si el archivo es de esta versión) las posiciones abiertas, los
        cooldowns y el contador de trade_id. En Render free el disco se borra al
        reiniciar: allí lo que vale es la memoria de Upstash, que se aplica después."""
        try:
            if not os.path.exists(STATE_FILE):
                return
            with open(STATE_FILE, "r", encoding="utf-8") as fh:
                persisted = json.load(fh)
        except Exception as exc:
            print(f"No pude restaurar historial de {STATE_FILE}: {exc}", flush=True)
            return
        if not isinstance(persisted, dict):
            return
        # Los archivos de versiones anteriores no traen "v": de ellos solo el PnL y el historial
        state = persisted if persisted.get("v") else {
            "total_realized_pnl": persisted.get("total_realized_pnl", 0.0)}
        try:
            info = self._apply_restore({"config": None, "state": state,
                                        "closed": persisted.get("closed_trades") or [], "stats": []},
                                       "boot")
            if info:
                self.log(f"♻️ Recuperado del disco ({STATE_FILE}): {info}")
        except Exception as exc:
            print(f"No pude restaurar el estado de {STATE_FILE}: {exc}", flush=True)

    # ── Memoria externa: qué se guarda y cómo se recupera ────────────────────

    def _on_settings_saved(self) -> None:
        """Cada _save_settings(): el cambio de ajustes va también a Upstash."""
        self._config_changed_here = True
        self.store.mark_config()

    def _collect_state(self) -> dict:
        """Estado vivo que se guarda (Upstash y disco) para continuar tras un reinicio."""
        now = time.time()
        with self.lock:
            positions = [asdict(p) for _, p in sorted(self.positions.items())
                         if p.status == "OPEN" and p.fills]
            cooldowns = {s: ts for s, ts in self.symbol_cooldown.items() if ts > now}
            total = self.total_realized_pnl
        with self._trade_id_lock:
            seq = self._trade_id_seq
        return {
            "v":                  1,
            "positions":          positions,
            "cooldowns":          cooldowns,
            "trade_id_seq":       seq,
            "total_realized_pnl": total,
            "global_stop":        {"triggers": self.global_stop_triggers,
                                   "last": dict(self.global_stop_last),
                                   "rearm_at": self._gstop_rearm_at},
        }

    def _state_signature(self, state: dict) -> Tuple[str, str, str]:
        """(firma sin MFE/MAE, firma completa, JSON a guardar). Un cambio en la
        primera (apertura, DCA, cierre, SL, cooldown…) se guarda al momento; si solo
        cambia la segunda, como mucho cada STORE_HEARTBEAT_S."""
        full = json.dumps(state, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
        struct = dict(state)
        struct["positions"] = [{k: v for k, v in p.items() if k not in _POS_VOLATILE}
                               for p in state.get("positions") or []]
        s_json = json.dumps(struct, sort_keys=True, separators=(",", ":"), default=str)
        payload = json.dumps(dict(state, saved_at=time.time(), by=self.store.host),
                             ensure_ascii=False, separators=(",", ":"), default=str)
        return (hashlib.sha1(s_json.encode("utf-8")).hexdigest(),
                hashlib.sha1(full.encode("utf-8")).hexdigest(), payload)

    def _history_for_resync(self) -> Tuple[List[str], List[str]]:
        """Cierres (del más viejo al más nuevo) y estadísticas, ya en JSON."""
        with self.lock:
            closed = list(self.closed_trades[:STORE_CLOSED_MAX])
        with self._stats_lock:
            stats = list(self.trade_stats)
        dump = lambda r: json.dumps(r, ensure_ascii=False, separators=(",", ":"), default=str)  # noqa: E731
        return [dump(r) for r in reversed(closed)], [dump(r) for r in stats]

    def _orders_in_flight(self) -> bool:
        with self.lock:
            return bool(self._entry_inflight or self._closing_symbols or self._entry_reserved)

    @staticmethod
    def _closed_key(c: dict) -> tuple:
        if c.get("trade_id") and c.get("closed_ts"):
            return ("id", c.get("trade_id"), round(_num(c.get("closed_ts")), 3))
        return ("txt", c.get("symbol"), c.get("closed_at"), round(_num(c.get("pnl")), 6))

    @staticmethod
    def _stat_key(s: dict) -> tuple:
        return (s.get("trade_id"), round(_num(s.get("closed_at_ts")), 3), s.get("symbol"))

    def _apply_restore(self, data: dict, mode: str) -> str:
        """Aplica lo guardado y devuelve un resumen ("" si no había nada).
        mode "boot": al arrancar · "takeover": otra instancia soltó el control y se
        sustituye todo · "merge": se une con lo de esta sesión (arrancó sin conexión)."""
        cfg = data.get("config")
        state = data.get("state")
        closed_in = [c for c in (data.get("closed") or []) if isinstance(c, dict)]
        stats_in = [s for s in (data.get("stats") or []) if isinstance(s, dict)]
        if cfg is None and not isinstance(state, dict) and not closed_in and not stats_in:
            return ""
        state = state if isinstance(state, dict) else {}
        merge = mode == "merge"
        parts: List[str] = []

        # 1) Ajustes de la web
        if isinstance(cfg, dict) and not (merge and self._config_changed_here):
            _apply_settings_dict(cfg)
            self.executor.set_url(EXEC.url)
            _save_settings(notify=False)                 # deja también la copia local al día
            pause = ""
            if self._entries_paused_now():
                pause = " · entradas EN PAUSA " + self._pause_left_txt(RISK.pause_until)
            parts.append(f"ajustes (EMA {EMA_FAST}/{EMA_SLOW} · SL {DEFAULT_STOP_LOSS_USD:g} USD · "
                         f"TP {TAKE_PROFIT_FRACTION * 100:g}% · DCA {len(ENTRY_LADDER)} tramos · modo "
                         f"{'invertido' if MODE.inverted else 'normal'}{pause})")

        # 2) Posiciones abiertas, cooldowns, PnL realizado y contadores
        now = time.time()
        restored: Dict[str, BotPosition] = {}
        pos_in = state.get("positions")
        for d in pos_in if isinstance(pos_in, list) else []:
            pos = _position_from_dict(d)
            if pos is not None:
                restored[pos.symbol] = pos
        cds: Dict[str, float] = {}
        cd_in = state.get("cooldowns")
        for s, ts in (cd_in.items() if isinstance(cd_in, dict) else []):
            try:
                ts = float(ts)
            except (TypeError, ValueError):
                continue
            if ts > now:
                cds[str(s).upper()] = ts
        has_state = bool(state)
        with self.lock:
            if merge:
                kept = [s for s in restored if s in self.positions]
                for s, p in restored.items():
                    self.positions.setdefault(s, p)
                for s, ts in cds.items():
                    self.symbol_cooldown[s] = max(ts, self.symbol_cooldown.get(s, 0.0))
                if "total_realized_pnl" in state:
                    self.total_realized_pnl = (float(state.get("total_realized_pnl") or 0.0)
                                               + (self.total_realized_pnl - self._pnl_at_boot))
                if kept:
                    parts.append(f"{', '.join(kept)} ya estaba(n) abierta(s) aquí: se conserva lo de esta sesión")
            elif has_state:
                self.positions = restored
                self.symbol_cooldown = cds
                if "total_realized_pnl" in state:
                    self.total_realized_pnl = float(state.get("total_realized_pnl") or 0.0)
            self._pnl_at_boot = self.total_realized_pnl
            merged = {self._closed_key(c): c for c in self.closed_trades}
            for c in closed_in:
                merged.setdefault(self._closed_key(c), c)
            self.closed_trades = sorted(
                merged.values(),
                key=lambda c: (_num(c.get("closed_ts")), str(c.get("closed_at") or "")),
                reverse=True)[:500]
            n_open = sum(1 for p in self.positions.values() if p.status == "OPEN" and p.fills)
            pos_txt = ", ".join(f"{p.symbol} {p.direction}{' inv.' if p.inverted else ''} "
                                f"{len(p.fills)} tramo{'s' if len(p.fills) != 1 else ''}"
                                for p in list(self.positions.values())[:6])
            n_closed = len(self.closed_trades)
            n_cd = sum(1 for ts in self.symbol_cooldown.values() if ts > now)
        gs = state.get("global_stop") if isinstance(state.get("global_stop"), dict) else {}
        if gs:
            self.global_stop_triggers = max(self.global_stop_triggers if merge else 0,
                                            int(_num(gs.get("triggers"))))
            if isinstance(gs.get("last"), dict) and gs.get("last"):
                self.global_stop_last = dict(gs["last"])
            self._gstop_rearm_at = max(self._gstop_rearm_at, _num(gs.get("rearm_at")))

        # 3) Estadísticas MFE/MAE (se unen sin duplicar)
        with self._stats_lock:
            st_map = {self._stat_key(s): s for s in self.trade_stats}
            for s in stats_in:
                st_map.setdefault(self._stat_key(s), s)
            ordered = sorted(st_map.values(), key=lambda s: _num(s.get("closed_at_ts")))
            self.trade_stats.clear()
            self.trade_stats.extend(ordered)          # deque(maxlen): quedan las más recientes
            n_stats = len(self.trade_stats)
            max_stat_id = int(max((_num(s.get("trade_id")) for s in self.trade_stats), default=0))
        self._sync_stats_file()

        # 4) Contador de trade_id: nunca repetir uno ya usado
        with self.lock:
            max_pos_id = max((p.trade_id for p in self.positions.values()), default=0)
        saved_seq = int(_num(state.get("trade_id_seq")))
        with self._trade_id_lock:
            self._trade_id_seq = max(self._trade_id_seq, saved_seq, max_pos_id, max_stat_id)

        if has_state or merge:
            parts.append(f"{n_open} posición(es) abierta(s)" + (f": {pos_txt}" if pos_txt else "")
                         + ("…" if n_open > 6 else ""))
            parts.append(f"{n_cd} en cooldown")
        parts.append(f"{n_closed} cierres · {n_stats} estadísticas · PnL realizado "
                     f"{self.total_realized_pnl:+.2f} USD")
        return " · ".join(parts)

    def _sync_stats_file(self) -> None:
        """Si el archivo local de estadísticas tiene menos operaciones que la memoria
        (Render borró el disco), se reescribe para que el CSV las incluya todas."""
        try:
            n_file = 0
            if os.path.exists(STATS_FILE):
                with open(STATS_FILE, "rb") as fh:
                    n_file = sum(1 for line in fh if line.strip())
            with self._stats_lock:
                rows = list(self.trade_stats)
            if n_file >= len(rows):
                return
            tmp = f"{STATS_FILE}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                for r in rows:
                    fh.write(json.dumps(r, ensure_ascii=False) + "\n")
            os.replace(tmp, STATS_FILE)
        except Exception as exc:
            print(f"No pude reescribir {STATS_FILE}: {exc}", flush=True)

    def _store_apply_threadsafe(self, data: dict, mode: str) -> bool:
        """Desde el hilo de la memoria: aplica lo guardado dentro del loop del bot."""
        loop = self.loop
        if loop is None or not loop.is_running():
            return False
        fut = asyncio.run_coroutine_threadsafe(self._apply_restore_async(data, mode), loop)
        try:
            fut.result(timeout=60)
            return True
        except Exception as exc:
            self.log(f"⚠️ Memoria Upstash: no pude aplicar lo guardado ({exc!r})")
            return False

    async def _apply_restore_async(self, data: dict, mode: str) -> None:
        info = self._apply_restore(data, mode)
        if info:
            self.log(f"♻️ Recuperado de Upstash{self._saved_ago_txt(data)}: {info}")
        kc = self.kline_cache
        if kc is not None and (kc.fast_period, kc.slow_period) != (EMA_FAST, EMA_SLOW):
            try:
                await asyncio.to_thread(kc.set_periods, EMA_FAST, EMA_SLOW)
            except Exception as exc:
                self.log(f"⚠️ No pude aplicar EMA {EMA_FAST}/{EMA_SLOW} al detector: {exc}")
        for sym in self._open_position_symbols():
            self._watch_add(sym)
            self._enqueue(sym)                       # TP/SL con el precio de ahora mismo
        self.persist_state()

    @staticmethod
    def _saved_ago_txt(data: dict) -> str:
        st = data.get("state") if isinstance(data, dict) else None
        try:
            ts = float((st or {}).get("saved_at") or 0)
        except (TypeError, ValueError):
            ts = 0.0
        if not ts:
            return ""
        who = (st or {}).get("by") or ""
        return f" (guardado hace {_fmt_secs(max(0.0, time.time() - ts))}" + (f" por {who}" if who else "") + ")"

    async def _store_boot(self) -> None:
        """Antes de operar: recupera de Upstash ajustes, posiciones, cierres,
        estadísticas y cooldowns, y toma el control (solo opera una instancia)."""
        st = self.store
        if not st.enabled:
            if st.status == "error":
                self.log(f"❌ Memoria Upstash mal configurada: {st.detail}")
            else:
                self.log("ℹ️ Memoria Upstash desactivada (faltan UPSTASH_REDIS_REST_URL y "
                         "UPSTASH_REDIS_REST_TOKEN): en Render free lo que no esté en Environment "
                         "se pierde al reiniciar")
            return
        self.log(f"🔐 Memoria Upstash: conectando (prefijo {st.prefix})…")
        res = await asyncio.to_thread(st.boot)
        if res.get("ok"):
            data = res["data"]
            info = self._apply_restore(data, "boot")
            if info:
                st.loaded_info = info
                self.log(f"♻️ Recuperado de Upstash{self._saved_ago_txt(data)}: {info}")
            else:
                st.loaded_info = "Upstash estaba vacío (primer arranque con memoria)"
                self.log("🔐 Memoria Upstash vacía: primer arranque con memoria; se guarda lo actual")
                st.request_full_resync()
            if res.get("acquired"):
                st.start("active")
                self.log("🔐 Memoria Upstash activa: cada cambio se guarda en ~1 s")
            else:
                st.holder = res.get("holder") or ""
                st.start("waiting")
                self.log(f"⏳ Memoria Upstash: otra instancia ({st.holder.split('|')[0] or '?'}) tiene el "
                         "control (¿deploy en curso?). Esta espera sin operar y sigue sola en cuanto la "
                         "otra guarde y suelte el control")
        elif res.get("fatal"):
            st._set_error(res.get("error", "error"))
        else:
            st.detail = f"sin conexión al arrancar: {res.get('error')}"
            st.start("degraded")
            self.log(f"⚠️ Memoria Upstash sin conexión ({res.get('error')}). Reintento en segundo plano"
                     + ("; mientras tanto NO se abren posiciones nuevas" if STORE_OFFLINE_BLOCKS_ENTRIES else ""))

    def store_takeover(self) -> dict:
        """Botón de la web: esta instancia toma el control aunque otra lo tenga."""
        st = self.store
        if not st.enabled:
            return {"ok": False, "error": "La memoria Upstash no está configurada", "code": 404}
        if st.status == "active":
            return {"ok": True, "msg": "Esta instancia ya tiene el control"}
        if st.status not in ("waiting", "standby"):
            return {"ok": False, "error": f"No aplica en el estado actual ({st.STATUS_TEXT.get(st.status)})",
                    "code": 409}
        try:
            ok = st.takeover(force=True)
        except Exception as exc:
            return {"ok": False, "error": str(exc), "code": 502}
        if not ok:
            return {"ok": False, "error": "No pude tomar el control", "code": 502}
        self.log("🔐 Control tomado a mano desde la web: la otra instancia dejará de operar "
                 "en su próximo guardado (≤ 25 s)")
        return {"ok": True, "msg": "Control tomado"}

    # ── Cooldowns: liberar a mano desde la web ───────────────────────────────

    def release_cooldowns(self, symbols: Optional[List[str]] = None) -> List[str]:
        """Saca símbolos del cooldown (None = todos). Devuelve los liberados."""
        now = time.time()
        with self.lock:
            active = {s: ts for s, ts in self.symbol_cooldown.items() if ts > now}
            if symbols is None:
                targets = sorted(active)
            else:
                targets = sorted({s.upper().strip() for s in symbols} & set(active))
            for s in targets:
                self.symbol_cooldown.pop(s, None)
        if not targets:
            return []
        if len(targets) == 1:
            s = targets[0]
            self.log(f"🔓 COOLDOWN LIBERADO a mano: {s} (quedaban {self._fmt_cooldown(active[s] - now)}). "
                     f"Puede volver a abrir con el próximo cruce EMA")
        else:
            self.log(f"🔓 COOLDOWN LIBERADO a mano: {len(targets)} símbolos "
                     f"({', '.join(targets[:10])}{'…' if len(targets) > 10 else ''}). "
                     f"Pueden volver a abrir con el próximo cruce EMA")
        self.persist_state()
        return targets

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
            ("ALL",       recs),
            ("TP",        [r for r in recs if r.get("reason") == "TP"]),
            ("SL",        [r for r in recs if r.get("reason") == "SL"]),
            ("MANUAL",    [r for r in recs if r.get("reason") == "MANUAL"]),
            ("GLOBAL",    [r for r in recs if r.get("reason") == "GLOBAL"]),
            ("GLOBAL_TP", [r for r in recs if r.get("reason") == "GLOBAL_TP"]),
            ("NORMAL",    [r for r in recs if not r.get("inverted")]),
            ("INVERTED",  [r for r in recs if r.get("inverted")]),
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
        # Solo posiciones normales: en las invertidas el importe fijo es el TP.
        tp_mae = [float(r.get("mae_usd", 0)) for r in recs
                  if r.get("reason") == "TP" and not r.get("inverted")]
        tps_normal = len(tp_mae)
        sl_sim = []
        for sl in (-1.0, -2.0, -3.0, -4.0, -5.0, -6.0, -8.0, -10.0, -12.0, -15.0, -20.0):
            stopped = sum(1 for m in tp_mae if m <= sl)
            sl_sim.append({
                "sl": sl, "tp_stopped": stopped, "tp_total": tps_normal,
                "pct": (stopped / tps_normal * 100.0) if tps_normal else 0.0,
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
        mirror = (f" | TP de las invertidas = {-DEFAULT_STOP_LOSS_USD:+.4f} USD"
                  if (MODE.inverted or self._has_open_book(True)) else "")
        self.log(f"SL GLOBAL = {DEFAULT_STOP_LOSS_USD:.4f} USD{mirror} "
                 f"(posiciones actualizadas: {', '.join(updated) or 'ninguna'}"
                 f"{' | manuales sobrescritos' if override_manual else ''})")
        self.persist_state()
        for sym in self._open_position_symbols():
            self._enqueue(sym)          # reevalúa ya con el nuevo SL
        return {"default_stop_loss_usd": DEFAULT_STOP_LOSS_USD, "updated": updated}

    def set_stop_loss(self, symbol: str, sl_usd: float) -> bool:
        """Fija un stop loss MANUAL (USD, negativo) para una posición NORMAL abierta.
        Queda marcado como manual: el SL automático por tramos ya no lo pisa.
        ValueError si la posición es invertida (en ella lo editable es el TP)."""
        symbol = symbol.upper().strip()
        with self.lock:
            pos = self.positions.get(symbol)
            if not pos or pos.status != "OPEN":
                return False
            if pos.inverted:
                raise ValueError(f"{symbol} es una posición invertida: lo que se edita a mano es "
                                 f"su take profit (POST /api/set-tp/{symbol})")
            pos.sl_usd    = sl_usd
            pos.sl_manual = True
        self.log(f"Stop loss MANUAL para {symbol}: {sl_usd:.4f} USD")
        self.persist_state()
        # Reevalúa ya con el nuevo SL (por si el precio actual ya lo cruza)
        self._enqueue(symbol)
        return True

    def set_take_profit_manual(self, symbol: str, tp_usd: float) -> bool:
        """Fija un take profit MANUAL (USD, positivo) para una posición INVERTIDA
        abierta (el espejo del SL manual de una normal: el bot ya no lo cambia).
        ValueError si la posición es normal."""
        symbol = symbol.upper().strip()
        with self.lock:
            pos = self.positions.get(symbol)
            if not pos or pos.status != "OPEN":
                return False
            if not pos.inverted:
                raise ValueError(f"{symbol} es una posición normal: su take profit es notional × "
                                 f"multiplicador; lo que se edita a mano es su stop loss")
            pos.sl_usd    = -abs(float(tp_usd))      # se guarda en espejo (TP = −sl_usd)
            pos.sl_manual = True
        self.log(f"Take profit MANUAL para {symbol} (invertida): {abs(tp_usd):+.4f} USD")
        self.persist_state()
        self._enqueue(symbol)                    # por si el precio actual ya lo alcanza
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
        mirror = (f" | SL de las invertidas = −{fraction * 100:g}% del notional"
                  if (MODE.inverted or self._has_open_book(True)) else "")
        self.log(f"TP MULTIPLICADOR = {fraction:g} ({fraction * 100:g}% del notional){mirror} — "
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

    def set_pause(self, paused: bool, reason: str = "", minutes: Optional[float] = None,
                  auto: bool = False) -> dict:
        """Pausa / reanuda la apertura de posiciones NUEVAS (DCA/TP/SL siguen activos).
        minutes=None → hasta reanudar a mano; N → se reanuda sola a los N minutos."""
        paused = bool(paused)
        with self._pause_lock:
            now = time.time()
            prev_at, prev_until = RISK.paused_at, RISK.pause_until
            RISK.entries_paused = paused
            RISK.pause_reason   = (reason or "Pausa manual") if paused else ""
            RISK.paused_at      = now if paused else 0.0
            RISK.pause_until    = (now + float(minutes) * 60.0) if (paused and minutes) else 0.0
            err = _save_settings()
        if err:
            self.log(f"No pude guardar ajustes: {err}")
        if paused:
            dur = (f"durante {_fmt_minutes(minutes)} (hasta las {self._hhmm_utc(RISK.pause_until)})"
                   if minutes else "hasta que las reanudes")
            self.log(f"⏸ ENTRADAS PAUSADAS {dur} — {RISK.pause_reason}")
        elif auto:
            self.log(f"▶ ENTRADAS REANUDADAS: terminó la pausa de "
                     f"{_fmt_minutes(max(0.0, prev_until - prev_at) / 60.0)}")
        else:
            self.log("▶ ENTRADAS REANUDADAS")
        self.persist_state()
        return {"paused": RISK.entries_paused, "pause_reason": RISK.pause_reason,
                "paused_at": RISK.paused_at, "pause_until": RISK.pause_until}

    def _check_pause_timers(self) -> None:
        """Reanuda las pausas con tiempo que ya vencieron (lo llama el mantenimiento)."""
        if not self.store.manage_ok:          # lo hace la instancia que tiene el control
            return
        now = time.time()
        with self._pause_lock:
            entries_due = RISK.entries_paused and RISK.pause_until and now >= RISK.pause_until
            exec_due    = EXEC.paused and EXEC.pause_until and now >= EXEC.pause_until
            if entries_due:
                self.set_pause(False, auto=True)
            if exec_due:
                self.set_executor_pause(False, auto=True)

    # ── Executor: link en caliente y pausa del envío ──────────────────────────

    def _exec_target_new(self) -> Tuple[str, str]:
        """(link, motivo si no se envía) para una posición NUEVA."""
        url = self.executor.config.executor_url
        if not url:
            return "", "sin link de executor"
        if self._exec_paused_now():
            return "", "envío al executor en pausa"
        return url, ""

    def executor_view(self) -> dict:
        now = time.time()
        paused = self._exec_paused_now(now)
        cur = self.executor.config.executor_url
        with self.lock:
            open_pos = [p for p in self.positions.values() if p.status == "OPEN" and p.fills]
        on_cur   = sum(1 for p in open_pos if p.exec_url and p.exec_url == cur)
        on_other = sum(1 for p in open_pos if p.exec_url and p.exec_url != cur)
        return {
            "url":             cur,
            "host":            _host_of(cur) if cur else "",
            "env_url":         EXECUTOR_URL,
            "paused":          paused,
            "pause_reason":    EXEC.pause_reason if paused else "",
            "paused_at":       EXEC.paused_at if paused else 0.0,
            "pause_until":     EXEC.pause_until if paused else 0.0,
            "pause_left_s":    max(0.0, EXEC.pause_until - now) if (paused and EXEC.pause_until) else 0.0,
            "open_on_current": on_cur,
            "open_on_other":   on_other,
            "open_not_sent":   len(open_pos) - on_cur - on_other,
            **self.executor.stats_view(),
        }

    def set_executor_url(self, url: Any, move_open: bool = False) -> dict:
        """Cambia el link del executor sin reiniciar. Las posiciones NUEVAS van al
        link nuevo; las abiertas siguen recibiendo su DCA y su cierre en el link
        donde se abrieron, salvo move_open=True (mismo executor con otra dirección).
        ValueError si el link no es válido."""
        new = _v_exec_url(url)
        old = self.executor.config.executor_url
        moved: List[str] = []
        with self.lock:
            if move_open and old and new and new != old:
                for sym, p in self.positions.items():
                    if p.status == "OPEN" and p.fills and p.exec_url == old:
                        p.exec_url = new
                        moved.append(sym)
            staying = sorted(sym for sym, p in self.positions.items()
                             if p.status == "OPEN" and p.fills and p.exec_url and p.exec_url != new)
        if new == old:
            return {"changed": False, "moved": [], "staying": staying, "executor": self.executor_view()}
        EXEC.url = new
        self.executor.set_url(new)
        err = _save_settings()
        if err:
            self.log(f"No pude guardar ajustes: {err}")
        extra = ""
        if moved:
            one = len(moved) == 1
            extra = (f" | {'1 posición abierta pasa' if one else f'{len(moved)} posiciones abiertas pasan'}"
                     f" al link nuevo: {', '.join(sorted(moved))}")
        elif staying:
            one = len(staying) == 1
            extra = (f" | {'1 posición abierta sigue' if one else f'{len(staying)} posiciones abiertas siguen'}"
                     f" recibiendo DCA y cierre en su link anterior: {', '.join(staying)}")
        self.log(f"[executor] LINK CAMBIADO: {old or '(ninguno)'} → "
                 f"{new or '(ninguno: no se envían señales)'}{extra}")
        self.persist_state()
        return {"changed": True, "moved": sorted(moved), "staying": staying,
                "executor": self.executor_view()}

    def set_executor_pause(self, paused: bool, minutes: Optional[float] = None,
                           reason: str = "", auto: bool = False) -> dict:
        """Pausa / reanuda el envío de posiciones NUEVAS al executor. El bot sigue
        operando igual; lo que ya está en el executor sigue con su DCA y su cierre."""
        paused = bool(paused)
        with self._pause_lock:
            now = time.time()
            prev_at, prev_until = EXEC.paused_at, EXEC.pause_until
            EXEC.paused       = paused
            EXEC.pause_reason = (reason or "Pausa manual") if paused else ""
            EXEC.paused_at    = now if paused else 0.0
            EXEC.pause_until  = (now + float(minutes) * 60.0) if (paused and minutes) else 0.0
            err = _save_settings()
        if err:
            self.log(f"No pude guardar ajustes: {err}")
        host = _host_of(self.executor.config.executor_url) or "sin link"
        if paused:
            dur = (f"durante {_fmt_minutes(minutes)} (hasta las {self._hhmm_utc(EXEC.pause_until)})"
                   if minutes else "hasta que lo reanudes")
            self.log(f"⏸ EXECUTOR EN PAUSA ({host}) {dur}: las posiciones nuevas no se le envían; "
                     f"las que ya tiene siguen con su DCA y su cierre")
        elif auto:
            self.log(f"▶ EXECUTOR REANUDADO ({host}): terminó la pausa de "
                     f"{_fmt_minutes(max(0.0, prev_until - prev_at) / 60.0)}")
        else:
            self.log(f"▶ EXECUTOR REANUDADO ({host}): las posiciones nuevas se vuelven a enviar")
        self.persist_state()
        return {"executor": self.executor_view()}

    # ── Modo invertido (un solo interruptor) ──────────────────────────────────

    @staticmethod
    def mirror_rules() -> dict:
        """Reglas que tendrá una posición invertida con los ajustes actuales."""
        first_nt = ENTRY_LADDER[0][1] if ENTRY_LADDER else 0.0
        return {
            "tp_first":     abs(first_nt) * FIRST_TRANCHE_SL_FRACTION,   # TP con 1 tramo
            "tp_first_nt":  first_nt,
            "tp_fraction":  FIRST_TRANCHE_SL_FRACTION,
            "tp_std":       -DEFAULT_STOP_LOSS_USD,                      # TP desde el 2.º tramo
            "sl_fraction":  TAKE_PROFIT_FRACTION,                        # SL = −notional × esto
            "global_tp":    -RISK.global_stop_usd,                       # TP global
            "global_on":    RISK.global_stop_enabled,
        }

    def set_inverted(self, enabled: bool) -> dict:
        """Activa / desactiva el modo invertido. Solo afecta a las posiciones NUEVAS:
        las abiertas siguen con las reglas con las que se abrieron."""
        enabled = bool(enabled)
        changed = enabled != MODE.inverted
        MODE.inverted = enabled
        if changed:
            MODE.changed_at = time.time()
        err = _save_settings()
        if err:
            self.log(f"No pude guardar ajustes: {err}")
        with self.lock:
            others = sorted(sym for sym, p in self.positions.items()
                            if p.status == "OPEN" and p.fills and p.inverted != enabled)
        if changed:
            r = self.mirror_rules()
            keep = ""
            if others:
                kind = "normales" if enabled else "invertidas"
                keep = (f". Siguen con sus reglas {len(others)} posición(es) {kind} abiertas: "
                        f"{', '.join(others[:8])}{'…' if len(others) > 8 else ''}")
            if enabled:
                self.log(
                    f"🔄 MODO INVERTIDO ACTIVADO: las posiciones nuevas se abren al revés de la señal "
                    f"(UP → SHORT, DOWN → LONG). TP = +{r['tp_first']:.4f} USD con 1 tramo, "
                    f"+{r['tp_std']:.2f} USD desde el 2.º | SL = −{r['sl_fraction'] * 100:g}% del notional"
                    f" | TP global = " + (f"+{r['global_tp']:.2f} USD" if r["global_on"] else "apagado")
                    + " | DCA en los mismos precios que el modo normal (a favor de la posición)" + keep)
            else:
                self.log("🔄 MODO NORMAL: las posiciones nuevas se abren en la dirección de la señal "
                         "(UP → LONG, DOWN → SHORT)" + keep)
        self.persist_state()
        return {"inverted_mode": MODE.inverted, "changed": changed, "others_open": others,
                "rules": self.mirror_rules()}

    def _unrealized_total_locked(self) -> Tuple[float, int]:
        """(PnL no realizado total, nº de posiciones abiertas). Requiere self.lock."""
        total, n_open = 0.0, 0
        for sym, pos in self.positions.items():
            if pos.status != "OPEN" or not pos.fills:
                continue
            n_open += 1
            total += pos.unrealized_pnl(self._display_price(sym))
        return total, n_open

    def _unrealized_books_locked(self) -> Tuple[float, int, float, int]:
        """(PnL normales, nº normales, PnL invertidas, nº invertidas). Requiere self.lock."""
        tot_n = tot_i = 0.0
        n_n = n_i = 0
        for sym, pos in self.positions.items():
            if pos.status != "OPEN" or not pos.fills:
                continue
            pnl = pos.unrealized_pnl(self._display_price(sym))
            if pos.inverted:
                tot_i += pnl
                n_i += 1
            else:
                tot_n += pnl
                n_n += 1
        return tot_n, n_n, tot_i, n_i

    def _has_open_book(self, inverted: bool) -> bool:
        with self.lock:
            return any(p.status == "OPEN" and p.fills and p.inverted == inverted
                       for p in self.positions.values())

    def _open_symbols_in_book(self, book: Optional[bool]) -> List[str]:
        """Símbolos abiertos: todos (book=None), solo normales (False) o solo invertidas (True)."""
        with self.lock:
            return [sym for sym, p in self.positions.items()
                    if p.status == "OPEN" and p.fills and (book is None or p.inverted == book)]

    def _check_global_stop(self) -> None:
        """Dos "libros", cada uno con su regla (mismo valor, signo opuesto):
          • Normales:   PnL no realizado ≤ global_stop_usd (−5)  → STOP GLOBAL: las cierra.
          • Invertidas: PnL no realizado ≥ −global_stop_usd (+5) → TP GLOBAL:   las cierra.
        Tras dispararse, si así está configurado, pausa las entradas nuevas.
        Corre en el loop del bot."""
        if not RISK.global_stop_enabled or self._close_all_active or not self.store.manage_ok:
            return
        now = time.time()
        if now - self._gstop_last_check < 0.2 or now < self._gstop_rearm_at:
            return
        self._gstop_last_check = now
        with self.lock:
            tot_n, n_n, tot_i, n_i = self._unrealized_books_locked()
        thr = RISK.global_stop_usd                       # negativo (ej. −5)
        if n_n and tot_n <= thr:
            kind, total, n_open, book, target = "SL", tot_n, n_n, False, thr
        elif n_i and tot_i >= -thr:
            kind, total, n_open, book, target = "TP", tot_i, n_i, True, -thr
        else:
            return
        self._gstop_rearm_at = now + GLOBAL_STOP_REARM_S
        self.global_stop_triggers += 1
        self.global_stop_last = {
            "ts": now, "pnl": total, "threshold": target, "positions": n_open, "kind": kind,
            "at": datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
        if kind == "SL":
            self.log(f"🛑 STOP GLOBAL: PnL no realizado {total:.4f} ≤ {thr:.2f} USD "
                     f"→ cierre de {n_open} posición(es)" + (" normales" if n_i else ""))
            why = f"Stop global disparado ({total:.2f} USD ≤ {thr:.2f})"
        else:
            self.log(f"🎯 TP GLOBAL: PnL no realizado de las invertidas {total:+.4f} ≥ {-thr:+.2f} USD "
                     f"→ cierre de {n_open} posición(es) invertida(s)")
            why = f"TP global alcanzado ({total:+.2f} USD ≥ {-thr:+.2f})"
        if RISK.global_stop_pause:
            mins = RISK.global_stop_pause_min or None
            want_until = (now + mins * 60.0) if mins else 0.0
            # Pausa si no la hay, o si la que hay es con tiempo y acaba antes que esta
            if (not self._entries_paused_now(now)
                    or (RISK.pause_until and (not want_until or want_until > RISK.pause_until))):
                self.set_pause(True, why, minutes=mins)
        res = self.start_close_all(origin="GLOBAL" if kind == "SL" else "GLOBAL_TP", book=book)
        if not res.get("ok"):
            self.log(f"{'Stop' if kind == 'SL' else 'TP'} global: no pude lanzar el cierre masivo: "
                     f"{res.get('error')}")

    def gate_view(self, full: bool = True) -> dict:
        """Estado de las guardas de entrada para la web."""
        with self.lock:
            exposure = sum(p.notional for p in self.positions.values()
                           if p.status == "OPEN" and p.fills)
            unreal_n, n_n, unreal_i, n_i = self._unrealized_books_locked()
        unreal, n_open = unreal_n + unreal_i, n_n + n_i
        btc_chg, btc_fresh = self.btc_change()
        blk_long, blk_short, btc_reasons = self._btc_blocks()     # por lado de la SEÑAL
        inv_mode = MODE.inverted
        # Lados REALES que pueden abrir: en modo invertido un LONG real sale de una
        # señal SHORT (y al revés), así que lo frena lo que frene a esa señal.
        real_blk_long, real_blk_short = (blk_short, blk_long) if inv_mode else (blk_long, blk_short)
        first_nt = ENTRY_LADDER[0][1] if ENTRY_LADDER else 0.0
        exp_blocks = bool(RISK.exposure_enabled
                          and exposure + first_nt > RISK.max_exposure_usd + 1e-9)
        now = time.time()
        paused = self._entries_paused_now(now)
        store_block = "" if self.store.entries_ok else self.store.block_text()
        common = paused or exp_blocks or self._close_all_active or bool(store_block)
        view: Dict[str, Any] = {
            "store_block":       store_block,
            "paused":            paused,
            "pause_reason":      RISK.pause_reason if paused else "",
            "paused_at":         RISK.paused_at if paused else 0.0,
            "pause_until":       RISK.pause_until if paused else 0.0,
            "pause_left_s":      max(0.0, RISK.pause_until - now) if (paused and RISK.pause_until) else 0.0,
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
            "gstop_pause_min":   RISK.global_stop_pause_min,
            "gstop_triggers":    self.global_stop_triggers,
            "gstop_last":        dict(self.global_stop_last),
            "unrealized":        unreal,
            "open_positions":    n_open,
            "unreal_normal":     unreal_n,
            "open_normal":       n_n,
            "unreal_inverted":   unreal_i,
            "open_inverted":     n_i,
            "inverted_mode":     inv_mode,
            "inverted_since":    MODE.changed_at,
            "can_open_long":     not (common or real_blk_long),
            "can_open_short":    not (common or real_blk_short),
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
                "started": 0.0, "finished": 0.0, "origin": "", "book": None}

    def close_all_view(self) -> dict:
        with self.lock:
            v = dict(self.close_all_state)
            v["failed_symbols"] = list(v.get("failed_symbols", []))
        return v

    def start_close_all(self, origin: str = "MANUAL", book: Optional[bool] = None) -> dict:
        """Lanza el cierre secuencial de las posiciones abiertas (desde Flask o desde
        el propio loop: stop/TP global). Devuelve al instante; el progreso se lee en
        close_all_view(). origin: "MANUAL" | "GLOBAL" | "GLOBAL_TP".
        book: None = todas · False = solo normales · True = solo invertidas."""
        if not self.loop or not self.loop.is_running():
            return {"ok": False, "error": "Bot loop no está activo", "code": 503}
        if not self.store.manage_ok:
            return {"ok": False, "error": "No se puede: " + self.store.block_text(), "code": 409}
        with self.lock:
            if self.close_all_state.get("running"):
                return {"ok": False, "error": "Ya hay un cierre masivo en curso", "code": 409}
            n_open = sum(1 for p in self.positions.values() if p.status == "OPEN" and p.fills
                         and (book is None or p.inverted == book))
            if n_open == 0:
                return {"ok": False, "error": "No hay posiciones abiertas", "code": 404}
            self.close_all_state = self._empty_close_all()
            self.close_all_state.update(running=True, total=n_open, started=time.time(),
                                        origin=origin, book=book)
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
        origin = st.get("origin")
        reason = origin if origin in ("GLOBAL", "GLOBAL_TP") else "MANUAL"
        book = st.get("book")
        label = {"GLOBAL": "stop global", "GLOBAL_TP": "TP global"}.get(reason, "manual")
        which = "" if book is None else (" invertida(s)" if book else " normal(es)")
        self.log(f"CIERRE MASIVO iniciado ({label}): {st.get('total', 0)} posición(es){which}, una a una")
        try:
            while not st["cancel"]:
                todo = [s for s in self._open_symbols_in_book(book) if attempts.get(s, 0) < 2]
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
        last_rate = last_prune = last_persist = last_mem = time.time()
        mem_every = MEM_TRIM_SECS if MEM_TRIM_SECS > 0 else 60.0
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

                # Pausas con tiempo que ya vencieron + ventana de proxies de arranque
                self._check_pause_timers()
                REST_ROUTER.housekeeping()

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

                if now - last_mem >= mem_every:
                    last_mem = now
                    await self._memory_housekeeping()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                self._log_throttled("maint_err", f"Error en mantenimiento: {exc!r}")

    # ── Memoria ───────────────────────────────────────────────────────────────

    def _note_rss(self) -> float:
        rss = _rss_mb()
        if rss > 0:
            self.mem_rss_mb = rss
            if rss > self.mem_peak_mb:
                self.mem_peak_mb = rss
        return rss

    def _mem_summary(self) -> str:
        ks: dict = {}
        try:
            ks = self.kline_cache.get_stats() if self.kline_cache else {}
        except Exception:
            pass
        with self.lock:
            n_open = sum(1 for p in self.positions.values() if p.status == "OPEN" and p.fills)
            n_closed = len(self.closed_trades)
        return (f"{ks.get('tracked_symbols', 0)} símbolos EMA con {ks.get('stored_candles', 0)} velas · "
                f"{n_open} posiciones · {n_closed} cierres en memoria · {threading.active_count()} hilos")

    async def _memory_housekeeping(self) -> None:
        """Devuelve al sistema la memoria libre (malloc_trim), mide el RSS, lo apunta
        en el log cada MEM_LOG_MIN minutos y avisa si se acerca al límite de la
        instancia (en Render free, al pasar de 512 MB Render reinicia el bot)."""
        before = self._note_rss()
        if MEM_TRIM_SECS > 0 and await asyncio.to_thread(_malloc_trim):
            self.mem_trims += 1
        rss = self._note_rss()
        now = time.time()
        if MEM_WARN_MB > 0 and rss >= MEM_WARN_MB:
            self._log_throttled("mem_warn", f"⚠️ MEMORIA ALTA: {rss:.0f} MB de {MEM_LIMIT_MB:.0f} MB "
                                            f"(pico {self.mem_peak_mb:.0f} MB) · {self._mem_summary()}", 300.0)
        if MEM_LOG_MIN > 0 and now - self._mem_last_log >= MEM_LOG_MIN * 60.0:
            self._mem_last_log = now
            freed = before - rss
            self.log(f"[mem] RSS {rss:.0f} MB de {MEM_LIMIT_MB:.0f} MB (pico {self.mem_peak_mb:.0f} MB"
                     + (f", malloc_trim devolvió {freed:.0f} MB" if freed >= 1 else "") + ") · "
                     + self._mem_summary())

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
                    "slu": pos.exit_sl(),
                    "tp":  pos.exit_tp(),
                    "mfe": max(pos.mfe_usd, pnl),
                    "mae": min(pos.mae_usd, pnl),
                }
        winners = [{"s": w["symbol"], "p": w["price"], "c": w["change"]}
                   for w in self._winners_now(set(pos_out))]
        ex_paused = self._exec_paused_now(now)
        return {
            "ts":               now,
            "positions":        pos_out,
            "winners":          winners,
            "total_unrealized": total_unreal,
            "total_notional":   total_notional,
            "gate":             self.gate_view(full=False),
            "executor":         {"url": self.executor.config.executor_url, "paused": ex_paused,
                                 "pause_until": EXEC.pause_until if ex_paused else 0.0,
                                 "pause_left_s": max(0.0, EXEC.pause_until - now)
                                 if (ex_paused and EXEC.pause_until) else 0.0},
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
        exec_cur       = self.executor.config.executor_url
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
            sig_dir = pos.signal_dir             # la condición EMA mira la señal (espejo)
            if ema_val is not None and price > 0:
                ema_ok = price > ema_val if sig_dir == "LONG" else price < ema_val
            dca_next = {
                "idx":         nxt,
                "level":       ladder[nxt][0] if nxt < len(ladder) else None,
                "notional":    ladder[nxt][1] if nxt < len(ladder) else None,
                # % que manda en el DCA: en contra (normal) o a favor (invertida)
                "adverse":     pos.trigger_pct(price),
                "ema_dir":     sig_dir,
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
                "inverted":        pos.inverted,
                "signal_dir":      pos.signal_dir,
                "target":          pos.exit_tp(),        # TP real (USD)
                "take_profit_price": pos.tp_price(),
                "unrealized_pnl":  pnl,
                "stop_loss_price": sl_price,
                "stop_loss_usd":   pos.exit_sl(),        # SL real (USD)
                # Origen del importe fijo (1er tramo / estándar / manual): en una
                # normal es el del SL; en una invertida, el del TP.
                "sl_mode":         pos.sl_mode,
                "trade_id":        pos.trade_id,
                "exec_url":        pos.exec_url,
                "exec_host":       _host_of(pos.exec_url) if pos.exec_url else "",
                "exec_current":    bool(pos.exec_url) and pos.exec_url == exec_cur,
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
                "unblock_ts":    round(ts, 1),
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
            "executor_url":      exec_cur or "",
            "executor":          self.executor_view(),
            "inverted_mode":     MODE.inverted,
            "inverted_since":    MODE.changed_at,
            "mirror_rules":      self.mirror_rules(),
            "rest_proxy":        REST_ROUTER.view(),
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
                "reconnects":      kl_stats.get("reconnects", 0),
                "gap_fills":       kl_stats.get("gap_fills", 0),
                "full_warmups":    kl_stats.get("full_warmups", 0),
            },
            "memory": {
                "rss_mb":   round(self._note_rss(), 1),
                "peak_mb":  round(self.mem_peak_mb, 1),
                "limit_mb": MEM_LIMIT_MB,
                "warn_mb":  MEM_WARN_MB,
                "trims":    self.mem_trims,
                "malloc":   _MALLOC_NOTE,
            },
            "store": self.store.view(),
            "ts": now,
        }


    def _state_payload(self) -> Tuple[Optional[tuple], Optional[dict]]:
        """(firma, contenido) de STATE_FILE: el mismo estado que se guarda en Upstash
        (posiciones completas, cooldowns, contadores, PnL) más los últimos 130
        cierres. Sirve de memoria cuando el disco no se borra (VPS, PC local)."""
        state = self._collect_state()
        _, full_sig, _ = self._state_signature(state)
        with self.lock:
            closed = list(self.closed_trades[:130])
        if not (closed or state["positions"] or state["cooldowns"] or state["total_realized_pnl"]):
            return None, None
        sig = (full_sig, len(closed), closed[0].get("closed_at") if closed else None)
        return sig, dict(state, saved_at=time.time(), closed_trades=closed)

    def _write_state_file(self, state: Tuple[Optional[tuple], Optional[dict]]) -> None:
        '''Escribe el estado en disco de forma atómica (solo si cambió).'''
        sig, payload = state
        if payload is None or sig == self._persist_sig:
            return
        tmp = f"{STATE_FILE}.tmp"
        try:
            data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(data)
            os.replace(tmp, STATE_FILE)
            self._persist_sig = sig
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

                state = self._state_payload()
                if state[1] is None or state[0] == self._persist_sig:
                    continue                    # nada que guardar o sin cambios desde la última vez

                await asyncio.to_thread(self._write_state_file, state)

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
        """Solicita persistencia asíncrona del estado sin bloquear el loop
        (disco y memoria externa de Upstash)."""
        self.store.mark_dirty()
        if self._persist_event is None:
            self._write_state_file(self._state_payload())
            return

        try:
            if self.loop and self.loop.is_running():
                self.loop.call_soon_threadsafe(self._persist_event.set)
            else:
                self._persist_event.set()
        except Exception:
            self._write_state_file(self._state_payload())

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


def _install_shutdown_handlers() -> None:
    """Render para el servicio con SIGTERM (deploy, reinicio o al dormirlo): el bot
    deja de operar, guarda lo último en Upstash y suelta el control para la
    instancia nueva. También al salir normalmente (atexit)."""
    import atexit
    import signal as _signal

    def _final(reason: str) -> None:
        # En un hilo aparte y con tiempo máximo: la señal puede llegar mientras este
        # mismo hilo tiene tomado el lock del bot (p. ej. gunicorn sync en una petición)
        t = threading.Thread(target=bot.store.shutdown, args=(reason,), daemon=True,
                             name="StoreShutdown")
        t.start()
        t.join(12.0)

    atexit.register(_final, "Salida del proceso")
    try:
        prev = _signal.getsignal(_signal.SIGTERM)

        def _on_term(signum, frame):
            _final("SIGTERM (Render detiene esta instancia)")
            if callable(prev) and prev not in (_signal.SIG_IGN, _signal.SIG_DFL):
                return prev(signum, frame)          # p. ej. el cierre ordenado de gunicorn
            if prev == _signal.SIG_IGN:
                return None
            raise SystemExit(0)

        _signal.signal(_signal.SIGTERM, _on_term)
    except (ValueError, OSError, AttributeError):
        pass                                       # no es el hilo principal: queda solo atexit


_install_shutdown_handlers()

app = Flask(__name__)


@app.before_request
def _store_gate():
    """Mientras otra instancia tiene el control de la memoria (deploy en curso o
    una copia del bot abierta en otro sitio) esta no acepta cambios: se perderían
    al tomar el control y podrían duplicar órdenes."""
    if request.method != "POST" or bot.store.manage_ok:
        return None
    if request.path.startswith("/api/store/"):
        return None
    return jsonify({"ok": False, "error": "No se puede ahora: " + bot.store.block_text()}), 409


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
      --inv: #b48cff;
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
    .lamp.inv  { background: var(--inv); box-shadow: 0 0 0 3px rgba(180,140,255,.18), 0 0 10px rgba(180,140,255,.55); }
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
    .alert.warn { border-color: rgba(242,179,61,.55); background: rgba(242,179,61,.07); }
    .alert .alert-body { display: flex; flex-wrap: wrap; gap: 8px 14px; align-items: center; flex: 1; min-width: 0; }
    .alert .alert-body p { margin: 0; flex: 1 1 260px; line-height: 1.5; }

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
    .tag.gtp { color: #06301c; background: rgba(61,214,140,.8); }

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
    .btn.violet { color: #ddd0ff; border-color: rgba(180,140,255,.6); background: rgba(180,140,255,.1); }
    .btn.violet:hover { background: rgba(180,140,255,.18); }
    .btn.block { width: 100%; }
    .btn.sm { min-height: 32px; padding: 5px 11px; font-size: 13px; }
    .btn[hidden] { display: none; }
    .cd-link { background: none; border: 0; padding: 0; color: inherit; font: inherit; cursor: pointer;
               text-decoration: underline dotted var(--dim); text-underline-offset: 5px; }
    .cd-link:hover { color: var(--blue); }

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
    input[type=number], input[type=url], select {
      width: 100%; background: var(--ink); border: 1px solid var(--rule2); border-radius: var(--r-md);
      padding: 10px 12px; font-size: 16px; font-family: var(--font-c); font-weight: 600; outline: none; min-height: 42px;
    }
    input[type=url] { font-family: var(--font); font-weight: 500; min-width: 0; }
    select { font-family: var(--font); font-weight: 500; font-size: 15px; }
    input[type=number]:focus, input[type=url]:focus, select:focus { border-color: var(--blue); }
    input[data-dirty] { border-color: var(--warn); }
    /* Duración de una pausa: "hasta que la reanude" | "durante N min" */
    .seg { display: grid; grid-template-columns: 1fr 1fr; border: 1px solid var(--rule2); border-radius: var(--r-md); background: var(--ink); overflow: hidden; }
    .seg[hidden] { display: none; }
    .seg label { display: flex; align-items: center; justify-content: center; gap: 6px; min-height: 42px; padding: 5px 8px; cursor: pointer; font-size: 14px; color: var(--muted); font-weight: 500; text-align: center; transition: background .15s, color .15s; }
    .seg label + label { border-left: 1px solid var(--rule2); }
    .seg label:hover { color: var(--txt); }
    .seg label.on { background: rgba(106,156,255,.14); color: var(--txt); box-shadow: inset 0 0 0 1px var(--blue); }
    .seg input[type=radio] { position: absolute; opacity: 0; width: 1px; height: 1px; }
    .seg input[type=radio]:focus-visible + span { outline: 2px solid var(--blue); outline-offset: 2px; border-radius: 3px; }
    .seg input[type=radio][data-dirty] + span { text-decoration: underline 2px var(--warn); text-underline-offset: 4px; }
    .seg input.mins { width: 58px; min-height: 32px; padding: 3px 4px; text-align: center; -moz-appearance: textfield; appearance: textfield; }
    .seg input.mins::-webkit-inner-spin-button, .seg input.mins::-webkit-outer-spin-button { -webkit-appearance: none; margin: 0; }
    .seg.dim { opacity: .45; }
    .inline { display: flex; gap: 8px; }
    .inline input { flex: 1; }
    .ctl-title .host { margin-left: auto; font-family: var(--font-c); font-weight: 600; font-size: 13.5px; color: var(--muted); max-width: 62%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .ctl-sub { display: grid; gap: 10px; padding-top: 12px; border-top: 1px dashed var(--rule); }
    .ctl .field p { font-size: 13px; }
    .switch[hidden] { display: none; }
    @media (min-width: 641px) {
      table.proxytbl th:nth-child(2), table.proxytbl td:nth-child(2) { text-align: left; white-space: normal; min-width: 240px; }
    }
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
    /* Modo invertido */
    .mode.inv { color: var(--inv); border-color: rgba(180,140,255,.6); background: rgba(180,140,255,.1); }
    .mode[hidden] { display: none; }
    .side.inv { box-shadow: inset 0 0 0 1.5px var(--inv); }
    .inv-tag { font-family: var(--font-c); font-weight: 700; font-size: 11px; color: var(--inv); border: 1px solid rgba(180,140,255,.5); border-radius: 4px; padding: 0 4px; margin-left: 6px; vertical-align: 1px; }
    .ctl.inv-on { background: linear-gradient(180deg, rgba(180,140,255,.09), rgba(180,140,255,0) 75%); }
    .inv-map { list-style: none; margin: 0; padding: 0; border: 1px solid var(--rule); border-radius: var(--r-md); font-size: 13.5px; }
    .inv-map li { display: grid; grid-template-columns: 92px minmax(0, 1fr); gap: 10px; padding: 7px 12px; border-bottom: 1px solid var(--rule); }
    .inv-map li:last-child { border-bottom: 0; }
    .inv-map span { color: var(--muted); }
    .inv-map b { font-weight: 600; }
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
    .sub-h-row { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin: 0 0 8px; min-height: 32px; }
    .sub-h-row .sub-h { margin: 0; }
    .sr-only { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; }
    td.act { width: 1%; padding-top: 5px; padding-bottom: 5px; }
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
      .rt td small { text-align: right; }
      table.rt thead { display: none; }
      table.rt, .rt tbody, .rt tr, .rt td { display: block; width: 100%; }
      .rt tr { border-bottom: 1px solid var(--rule); padding: 8px 14px; }
      .rt td { display: flex; justify-content: space-between; gap: 12px; padding: 3px 0; border: 0; white-space: normal; text-align: right; }
      .rt td::before { content: attr(data-label); color: var(--muted); font-size: 13px; text-align: left; }
      .rt td:first-child { text-align: right; }
    }
    @media (max-width: 380px) {
      .seg { grid-template-columns: 1fr; }
      .seg label + label { border-left: 0; border-top: 1px solid var(--rule2); }
    }
  </style>
</head>
<body>
<header class="top">
  <div class="top-row">
    <div class="brand">Bot Short<span>Binance USDT-M, cruce EMA</span></div>
    <span id="modeBadge" class="mode paper">—</span>
    <span id="invBadge" class="mode inv" hidden title="Las posiciones nuevas se abren al revés de la señal">Invertido</span>
    <div class="grow"></div>
    <div class="conn" aria-label="Estado de conexión">
      <span title="Entradas nuevas"><i id="lampEntries" class="lamp off"></i><span class="lbl" id="lblEntries">Entradas</span></span>
      <span title="Envío de operaciones al executor"><i id="lampExec" class="lamp off"></i><span class="lbl" id="lblExec">Executor</span></span>
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
        <div><dt>En cooldown</dt><dd><button type="button" class="cd-link" id="cdCount" title="Ver y liberar los símbolos en cooldown">—</button></dd></div>
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
        <span class="il-head"><i class="lamp off"></i><span id="ilGstopName">Stop global</span></span>
        <span class="il-val" data-v>—</span>
        <span class="il-bar"><i data-b></i></span>
        <span class="il-sub" data-s></span>
      </button>
    </div>
  </section>

  <div id="errorBox" class="alert" role="alert"><i class="lamp bad"></i><pre id="lastError"></pre></div>
  <div id="storeBox" class="alert warn" role="status">
    <i id="storeLamp" class="lamp warn"></i>
    <div class="alert-body">
      <p><b id="storeTitle"></b> <span id="storeMsg"></span></p>
      <button id="storeTake" class="btn sm amber" type="button" hidden>Tomar el control</button>
    </div>
  </div>

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
        <div class="ctl" id="ctlInv">
          <div class="ctl-title"><i id="lampInv" class="lamp off"></i>Modo invertido<span class="host" id="invState">—</span></div>
          <p id="invText">—</p>
          <ul class="inv-map" id="invMap" aria-label="Reglas de las posiciones invertidas"></ul>
          <button id="invBtn" class="btn violet block">Activar modo invertido</button>
        </div>
        <div class="ctl">
          <div class="ctl-title"><i id="lampPause" class="lamp off"></i>Entradas nuevas</div>
          <p id="pauseText">—</p>
          <div class="seg" id="pauseSeg" role="radiogroup" aria-label="Duración de la pausa de entradas">
            <label><input type="radio" name="pauseDur" value="manual" checked><span>Hasta que la reanude</span></label>
            <label><input type="radio" name="pauseDur" value="timed"><span>Durante</span><input class="mins" id="pauseMin" type="number" min="1" max="10080" step="1" inputmode="numeric" value="25" aria-label="Minutos de pausa de las entradas"><span>min</span></label>
          </div>
          <button id="pauseBtn" class="btn amber block">Pausar entradas</button>
        </div>
        <div class="ctl" id="ctlExec">
          <div class="ctl-title"><i id="lampExecCard" class="lamp off"></i>Executor<span class="host" id="execHost">—</span></div>
          <p id="execText">—</p>
          <div class="seg" id="execSeg" role="radiogroup" aria-label="Duración de la pausa del envío al executor">
            <label><input type="radio" name="execDur" value="manual" checked><span>Hasta que lo reanude</span></label>
            <label><input type="radio" name="execDur" value="timed"><span>Durante</span><input class="mins" id="execMin" type="number" min="1" max="10080" step="1" inputmode="numeric" value="25" aria-label="Minutos de pausa del envío al executor"><span>min</span></label>
          </div>
          <button id="execPauseBtn" class="btn amber block">Pausar envío al executor</button>
          <div class="ctl-sub">
            <div class="field">
              <label for="execUrl">Link del executor</label>
              <div class="inline">
                <input id="execUrl" type="url" inputmode="url" autocomplete="off" autocapitalize="off" spellcheck="false" placeholder="https://mi-executor.onrender.com">
                <button class="btn" id="execTest" type="button" title="Comprueba que el link responde, sin enviar señales">Probar</button>
              </div>
              <p id="execHint" class="muted" style="margin:0"></p>
            </div>
            <label class="switch" id="execMoveWrap" hidden><input type="checkbox" id="execMove"><span class="track"></span><span id="execMoveTxt">—</span></label>
            <div id="execMsg" class="msg"></div>
            <button class="btn primary block" id="execSave" disabled>Guardar link</button>
          </div>
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
        <div class="panel-head" style="border-top:1px solid var(--rule)"><h2>Ajustes</h2><span class="muted" style="font-size:13.5px" id="cfgPersist">—</span></div>

        <details class="acc" id="cfg-gstop">
          <summary><span class="acc-name" id="gsName">Stop global por PnL</span><span class="acc-val" id="cvGstop">—</span><span class="chev"></span></summary>
          <div class="acc-body" data-panel>
            <p class="hint" id="gsHint">Si la suma del PnL no realizado de las posiciones llega a este valor, el bot las cierra todas a mercado, una a una.</p>
            <label class="switch"><input type="checkbox" id="gsEnabled"><span class="track"></span><span id="gsEnabledTxt">Stop global activado</span></label>
            <div class="field">
              <label for="gsUsd" id="gsUsdLbl">Cerrar todo cuando el PnL no realizado sea igual o menor que (USD)</label>
              <input id="gsUsd" type="number" step="0.5" placeholder="-5">
            </div>
            <label class="switch"><input type="checkbox" id="gsPause"><span class="track"></span>Pausar entradas nuevas tras dispararse</label>
            <div class="seg" id="gsSeg" role="radiogroup" aria-label="Duración de la pausa tras el stop global">
              <label><input type="radio" name="gsDur" value="manual" checked><span>Hasta que la reanude</span></label>
              <label><input type="radio" name="gsDur" value="timed"><span>Durante</span><input class="mins" id="gsPauseMin" type="number" min="1" max="10080" step="1" inputmode="numeric" value="30" aria-label="Minutos de pausa tras el stop global"><span>min</span></label>
            </div>
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
          <summary><span class="acc-name" id="slName">Stop loss por posición</span><span class="acc-val on" id="cvSl">—</span><span class="chev"></span></summary>
          <div class="acc-body" data-panel>
            <p class="hint" id="slHint">—</p>
            <div class="field">
              <label for="gslInput" id="gslLbl">Stop loss estándar para 2 o más tramos (USD)</label>
              <input id="gslInput" type="number" step="0.5" placeholder="-8">
            </div>
            <label class="switch"><input type="checkbox" id="gslOverride"><span class="track"></span><span id="gslOverrideTxt">Sobrescribir también los SL fijados a mano</span></label>
            <button class="btn primary" id="gslSave">Guardar stop loss</button>
          </div>
        </details>

        <details class="acc" id="cfg-tp">
          <summary><span class="acc-name" id="tpName">Take profit</span><span class="acc-val on" id="cvTp">—</span><span class="chev"></span></summary>
          <div class="acc-body" data-panel>
            <p class="hint" id="tpHint">Objetivo de cada posición = notional × multiplicador. Se aplica al instante a las posiciones abiertas.</p>
            <div class="field">
              <label for="tpInput" id="tpLbl">Multiplicador (0.07 = 7 % del notional)</label>
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
        <button data-f="GLOBAL_TP" aria-pressed="false">TP global</button>
        <button data-f="INV" aria-pressed="false">Invertidas</button>
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
              <thead><tr><th>Grupo</th><th>N</th><th>PnL total</th><th>MFE med</th><th>MFE p90</th><th>MAE med</th><th>MAE p90</th><th>MFE %</th><th>MAE %</th><th>Duración</th></tr></thead>
              <tbody id="tbStatGroups"><tr><td colspan="10" class="muted">Sin datos aún</td></tr></tbody>
            </table>
          </div>
        </div>
        <div>
          <h3 class="sub-h" id="slSimTitle">Take profits que cada SL habría cortado</h3>
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
        <div class="sub-h-row">
          <h3 class="sub-h">En cooldown tras cerrar</h3>
          <button class="btn sm" id="cdReleaseAll" type="button" hidden>Liberar todos</button>
        </div>
        <div class="boxed tablewrap">
          <table class="rt">
            <thead><tr><th>Símbolo</th><th>Tiempo restante</th><th>Se libera (UTC)</th><th><span class="sr-only">Acción</span></th></tr></thead>
            <tbody id="tbCooldown"><tr><td colspan="4" class="muted">Ningún símbolo en cooldown.</td></tr></tbody>
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
        <div><span>Proxies de arranque</span><b id="proxyStatus">—</b></div>
        <div><span>Tiempo activo</span><b id="uptime">—</b></div>
        <div><span>Memoria (RSS)</span><b id="memRss">—</b></div>
        <div><span>Reconexiones de velas</span><b id="klResync">—</b></div>
        <div><span>Memoria Upstash</span><b id="storeState">—</b></div>
        <div><span>Keep-alive QStash</span><b id="kaState">—</b></div>
      </div>
      <div id="proxyBox" hidden>
        <h3 class="sub-h" id="proxySum">Proxies de arranque</h3>
        <div class="boxed tablewrap">
          <table class="rt proxytbl">
            <thead><tr><th>Salida</th><th>Estado</th><th>OK</th><th>Fallos</th><th>Peso del minuto</th><th>Última respuesta</th></tr></thead>
            <tbody id="tbProxy"></tbody>
          </table>
        </div>
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
    <h3 id="slModalTitle"><span id="slModalWhat">Stop loss</span> de <span id="slModalSymbol">—</span></h3>
    <div class="field">
      <label for="slModalInput" id="slModalLbl">Pérdida máxima en USD. Un SL fijado a mano no lo cambia el bot.</label>
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
function lsGet(k, d) { try { const v = localStorage.getItem(k); return v === null ? d : v; } catch (e) { return d; } }
function lsSet(k, v) { try { localStorage.setItem(k, v); } catch (e) { /* sin almacenamiento */ } }
function fmtMin(m) {
  m = Math.round(n(m) * 10) / 10;
  if (m >= 60 && Number.isInteger(m)) { const h = Math.floor(m / 60), r = m % 60; return `${h} h` + (r ? ` ${r} min` : ''); }
  return `${m} min`;
}
// Pausa con tiempo: "hasta las 10:42 (quedan 23 min 05 s)" o "hasta que la reanudes"
function untilTxt(untilTs, leftS, manual) {
  return n(untilTs) ? `hasta las ${hhmm(untilTs)} (quedan ${fmtDur(leftS)})` : manual;
}

// ── Selector de duración: "hasta que la reanude" | "durante N min" ──────────
function segSync(segId) {
  q(segId).querySelectorAll('label').forEach(l => l.classList.toggle('on', l.querySelector('input[type=radio]').checked));
}
function segInit(segId, minId, lsKey) {
  const seg = q(segId), mi = q(minId), timed = seg.querySelector('input[value="timed"]');
  if (lsKey) { const v = parseFloat(lsGet(lsKey, '')); if (v >= 1 && v <= 10080) mi.value = v; }
  seg.addEventListener('change', () => segSync(segId));
  ['focus', 'input'].forEach(ev => mi.addEventListener(ev, () => {
    if (!timed.checked) { timed.checked = true; timed.dispatchEvent(new Event('change', { bubbles: true })); }
  }));
  mi.addEventListener('keydown', e => { if (e.key === 'Enter') e.preventDefault(); });
  segSync(segId);
}
function segValue(segId, minId) {
  if (!q(segId).querySelector('input[value="timed"]').checked) return { minutes: null };
  const m = numVal(minId);
  if (!(m >= 1 && m <= 10080)) return { error: 'Escribe los minutos de la pausa, de 1 a 10080 (7 días).' };
  return { minutes: m };
}
function fillSeg(segId, minId, minutes) {
  const seg = q(segId), mi = q(minId);
  if (seg.querySelector('[data-dirty]') || isEditing(mi)) return;
  const timed = n(minutes) > 0;
  seg.querySelector(`input[value="${timed ? 'timed' : 'manual'}"]`).checked = true;
  if (timed && String(mi.value) !== String(n(minutes))) mi.value = n(minutes);
  segSync(segId);
}
const hostOf = u => String(u || '').replace(/^https?:\/\//, '');
function normUrl(v) {
  v = String(v || '').trim().replace(/\/+$/, '');
  if (v && !/^[a-z][a-z0-9+.-]*:\/\//i.test(v)) v = 'https://' + v;
  return v;
}

// ── Campos editables: no se pisan mientras el usuario los está cambiando ────
function isEditing(el) { return el && (document.activeElement === el || el.dataset.dirty); }
function fillVal(id, v) { const el = q(id); if (el && !isEditing(el) && String(el.value) !== String(v)) el.value = v; }
function fillChk(id, v) { const el = q(id); if (el && !isEditing(el)) el.checked = !!v; }
function clearDirty(scope) { scope.querySelectorAll('[data-dirty]').forEach(el => delete el.dataset.dirty); }
function busy(btn, on, label) { if (!btn) return; if (on) { btn.dataset.label = btn.textContent; btn.textContent = 'Guardando…'; btn.disabled = true; } else { btn.textContent = label || btn.dataset.label || btn.textContent; btn.disabled = false; } }

// ── Estado del cliente ──────────────────────────────────────────────────────
let _d = null, _gate = null, _closeAll = {};
let _skew = 0;           // hora del servidor − hora del navegador (s)
function tickUntil() {
  const now = Date.now() / 1000 + _skew;
  document.querySelectorAll('[data-until]').forEach(td => {
    const r = n(td.dataset.until) - now; setTxt(td, r > 0 ? fmtDur(r) : 'liberado');
  });
}
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
  const inv = !!g.inverted_mode;
  if (inv !== _inv) renderMode(inv, null, g);
  // 1) Entradas nuevas
  let st = 'ok', val = 'Permitidas', sub = inv ? 'Un cruce EMA abre la posición contraria' : 'Un cruce EMA puede abrir posición';
  const globalRun = !!(_closeAll.running && (_closeAll.origin === 'GLOBAL' || _closeAll.origin === 'GLOBAL_TP'));
  if (g.store_block) {
    st = 'bad'; val = 'Bloqueadas';
    sub = g.store_block.charAt(0).toUpperCase() + g.store_block.slice(1);
  }
  else if (g.close_all_active) {
    st = 'bad'; val = 'Cerrando todo';
    sub = !globalRun ? 'Cierre masivo en curso' : _closeAll.origin === 'GLOBAL_TP' ? 'TP global alcanzado' : 'Stop global disparado';
  }
  else if (g.paused) {
    st = 'warn'; val = 'En pausa';
    sub = (g.pause_reason || 'Pausa manual') + (n(g.pause_until) ? `, hasta las ${hhmm(g.pause_until)} (quedan ${fmtDur(g.pause_left_s)})`
        : n(g.paused_at) ? `, desde las ${hhmm(g.paused_at)}` : '');
  }
  else if (!g.can_open_long && !g.can_open_short) {
    st = 'bad'; val = 'Bloqueadas';
    sub = g.exposure_blocks ? 'Exposición al límite'
        : (g.btc_change === null || !g.btc_fresh) ? 'Filtro BTC sin dato fresco' : 'El filtro BTC frena todo';
  } else if (!g.can_open_long || !g.can_open_short) {
    st = 'warn'; val = 'Solo ' + (g.can_open_long ? 'LONG' : 'SHORT');
    const realBlocked = g.can_open_long ? 'SHORT' : 'LONG';
    sub = inv ? `El filtro BTC frena las señales ${realBlocked === 'LONG' ? 'SHORT' : 'LONG'}, que abrirían ${realBlocked}`
              : `El filtro BTC frena ${realBlocked}`;
  }
  tile('ilEntries', st, val, sub);
  setHTML(q('ilEntries').querySelector('[data-sides]'),
    `<span class="${g.can_open_long ? 'side-ok' : 'side-no'}">LONG</span><span class="${g.can_open_short ? 'side-ok' : 'side-no'}">SHORT</span>`);
  setLamp(q('lampEntries'), st);
  setTxt('lblEntries', 'Entradas: ' + val.toLowerCase());

  // Panel de pausa
  setLamp(q('lampPause'), g.paused ? 'warn' : 'ok');
  setTxt('pauseText', g.paused
    ? `En pausa ${untilTxt(g.pause_until, g.pause_left_s, 'hasta que la reanudes')}${n(g.pause_until) ? ', luego se reanudan solas' : ''}. Motivo: ${g.pause_reason || 'pausa manual'}. Las posiciones abiertas siguen con su TP, SL y DCA.`
    : 'Sin pausa. Pausar frena solo las posiciones nuevas; las abiertas siguen con su TP, SL y DCA.');
  q('pauseSeg').hidden = !!g.paused;
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
    sub = `${who} ${inv ? 'las señales ' : ''}${bL && bS ? 'LONG y SHORT' : bL ? 'LONG' : 'SHORT'}`;
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

  // 4) Stop global (normales: −X) · TP global (invertidas: +X)
  const stop = n(g.gstop_usd), tgt = -stop;
  const unN = n(g.unreal_normal), unI = n(g.unreal_inverted);
  const last = g.gstop_last || {};
  const trig = n(g.gstop_triggers)
    ? `${g.gstop_triggers} disparo${g.gstop_triggers === 1 ? '' : 's'}, el último a las ${hhmm(last.ts)}` : '';
  let bar;
  if (!inv) {
    bar = (unN < 0 && stop < 0) ? unN / stop : 0;
    st = !g.gstop_enabled ? 'off' : globalRun ? 'bad' : bar >= 0.6 ? 'warn' : 'ok';
    val = `<span class="${cls(unN)}">${sgn(unN, 2)}</span><small>/ ${fx(stop, 2)} USD</small>`;
    sub = !g.gstop_enabled ? 'Desactivado' : trig || `Cierra todo al llegar a ${fx(stop, 2)} USD`;
    if (g.gstop_enabled && n(g.open_inverted)) sub += `. Invertidas: ${sgn(unI, 2)} de +${fx(tgt, 2)}`;
  } else {
    bar = (unI > 0 && tgt > 0) ? unI / tgt : 0;
    st = !g.gstop_enabled ? 'off' : globalRun ? 'warn' : 'ok';
    val = `<span class="${cls(unI)}">${sgn(unI, 2)}</span><small>/ +${fx(tgt, 2)} USD</small>`;
    sub = !g.gstop_enabled ? 'Desactivado' : trig || `Cierra las invertidas al llegar a +${fx(tgt, 2)} USD`;
    if (g.gstop_enabled && n(g.open_normal)) sub += `. Normales: ${sgn(unN, 2)} de ${fx(stop, 2)}`;
  }
  tile('ilGstop', st, val, sub, g.gstop_enabled ? bar : 0);
  setTxt('cvGstop', !g.gstop_enabled ? 'Apagado' : inv ? `+${fx(tgt, 2)} USD` : `${fx(stop, 2)} USD`);
  q('cvGstop').classList.toggle('on', !!g.gstop_enabled);
  setTxt('gsLast', n(g.gstop_triggers)
    ? `Último disparo${last.kind === 'TP' ? ' (TP global)' : last.kind === 'SL' ? ' (stop global)' : ''}: ${last.at || hhmm(last.ts)} con PnL ${sgn(last.pnl, 2)} USD sobre ${n(last.positions)} posición(es).`
    : 'Aún no se ha disparado desde el arranque.');

  // Formularios de riesgo (en modo invertido el stop global se muestra como TP global: +X)
  fillChk('gsEnabled', g.gstop_enabled); fillVal('gsUsd', fx(inv ? tgt : stop, 2)); fillChk('gsPause', g.gstop_pause);
  fillSeg('gsSeg', 'gsPauseMin', g.gstop_pause_min);
  q('gsSeg').classList.toggle('dim', !q('gsPause').checked);
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
  const sym = p.symbol, side = p.direction === 'LONG' ? 'LONG' : 'SHORT', inv = !!p.inverted;
  const fills = Array.isArray(p.fills) ? p.fills : [];
  const pnl = n(p.unrealized_pnl);
  const [m, z] = rangePos(pnl, p.stop_loss_usd, p.target);
  const rows = fills.map((f, i) => `<tr><td>${i + 1}</td><td>${fmtN(f.level)} %</td><td>${fx(f.notional, 2)}</td><td>${px(f.entry_price)}</td><td>${f.qty}</td><td>${hhmm(f.opened_at)}</td></tr>`).join('');
  const slPct = n(p.notional) > 0 ? fx(-n(p.stop_loss_usd) / n(p.notional) * 100, 2) : '0';
  return `<details id="prow_${sym}" data-sym="${sym}"${_openPos.has(sym) ? ' open' : ''}>
    <summary>
      <span class="side ${side.toLowerCase()}${inv ? ' inv' : ''}"${inv ? ` title="Invertida: la señal era ${esc(p.signal_dir)}"` : ''}>${side}</span>
      <span class="pos-id"><b>${sym}${inv ? '<span class="inv-tag">INV</span>' : ''}</b><small>${fills.length} tramo${fills.length === 1 ? '' : 's'}, ${fx(p.notional, 2)} USDT${inv ? `, señal ${esc(p.signal_dir)}` : ''}${p.exec_url ? '' : ', solo bot'}</small></span>
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
        <div><dt>Take profit</dt><dd><span class="pos">+${fx(p.target, 3)} USD</span> <small>a ${px(p.take_profit_price)}${inv ? ', ' + esc(p.sl_mode || '') : ''}</small></dd></div>
        <div><dt>Stop loss</dt><dd><span id="psl_${sym}">${px(p.stop_loss_price)}</span> <small>${sgn(p.stop_loss_usd, 3)} USD, ${inv ? `−${slPct} % del notional` : esc(p.sl_mode || '')}</small></dd></div>
        <div><dt>MFE (mejor)</dt><dd id="pmfe_${sym}" class="pos">${sgn(p.mfe_usd)}</dd></div>
        <div><dt>MAE (peor)</dt><dd id="pmae_${sym}" class="neg">${sgn(p.mae_usd)}</dd></div>
        <div><dt>Abierta hace</dt><dd id="pdur_${sym}">${fmtDur(Date.now() / 1000 - n(p.opened_ts))}</dd></div>
        <div><dt>Trade</dt><dd>#${n(p.trade_id)}</dd></div>
        <div><dt>Executor</dt><dd>${p.exec_url ? esc(p.exec_host) + (p.exec_current ? '' : ' <small>link anterior</small>')
          : '<small>no se envió (link en pausa o sin link)</small>'}</dd></div>
      </dl>
      <p class="dca-next" id="pdca_${sym}" style="margin:0">${dcaHtml(p)}</p>
      <div class="tablewrap"><table class="fills">
        <thead><tr><th>#</th><th>${inv ? 'A favor' : 'En contra'}</th><th>Notional</th><th>Precio</th><th>Cantidad</th><th>Hora</th></tr></thead>
        <tbody>${rows}</tbody>
      </table></div>
      <div class="actions">
        ${inv ? `<button class="btn" data-act="tp" data-sym="${sym}" data-tp="${n(p.target)}">Editar take profit</button>`
              : `<button class="btn" data-act="sl" data-sym="${sym}" data-sl="${n(p.stop_loss_usd)}">Editar stop loss</button>`}
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
  const inv = !!p.inverted;
  let s = `Próximo DCA: tramo ${n(d.idx) + 1}, <b>${fx(d.notional, 2)} USDT</b> al <b>${fmtN(d.level)} %</b> ${inv ? 'a favor' : 'en contra'} (ahora ${sgn(d.adverse, 2)} %)`
    + (inv ? ', donde añadiría el bot normal.' : '.');
  if (d.ema_applies) {
    const where = (d.ema_dir || p.direction) === 'LONG' ? 'por encima' : 'por debajo';
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
    p.stop_loss_usd, p.sl_mode, (p.fills || []).length, p.exec_url, p.exec_current, p.inverted]));
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
// Un cooldown liberado a mano ya no está activo aunque su hora no haya llegado
function cdUntilTxt(t) {
  const until = n(t.unblock_ts);
  if (until && until > Date.now() / 1000 + _skew && !((_d && _d.cooldowns) || {})[t.symbol]) return 'liberado a mano';
  return t.unblock_at || '—';
}
const REASONS = { TP: ['tp', 'Take profit'], SL: ['sl', 'Stop loss'], MANUAL: ['manual', 'Manual'],
                  GLOBAL: ['global', 'Stop global'], GLOBAL_TP: ['gtp', 'TP global'] };
function renderHistory() {
  const closed = (_d && Array.isArray(_d.closed_trades)) ? _d.closed_trades : [];
  const rows = closed.filter(t => _histFilter === 'ALL' || (_histFilter === 'INV' ? !!t.inverted : t.reason === _histFilter));
  const html = rows.length ? rows.map(t => {
    const r = REASONS[t.reason] || ['', t.reason || '—'];
    return `<tr>
      <td data-label="Símbolo" class="sym">${esc(t.symbol)}</td>
      <td data-label="Lado"><span>${esc(t.direction || '—')}${t.inverted ? '<span class="inv-tag" title="Posición invertida">INV</span>' : ''}</span></td>
      <td data-label="Motivo"><span class="tag ${r[0]}">${esc(r[1])}</span></td>
      <td data-label="PnL" class="num ${cls(t.pnl)}">${sgn(t.pnl)}</td>
      <td data-label="MFE" class="pos">${t.mfe_usd === undefined ? '—' : sgn(t.mfe_usd)}</td>
      <td data-label="MAE" class="neg">${t.mae_usd === undefined ? '—' : sgn(t.mae_usd)}</td>
      <td data-label="Duración">${t.duration_s === undefined ? '—' : fmtDur(t.duration_s)}</td>
      <td data-label="Entrada">${px(t.avg_entry)}</td>
      <td data-label="Cierre">${px(t.close_price)}</td>
      <td data-label="Cooldown hasta" class="muted">${esc(cdUntilTxt(t))}</td>
      <td data-label="Cerrada" class="muted">${esc(t.closed_at || '')}</td>
    </tr>`;
  }).join('') : `<tr><td colspan="11" class="muted">${!closed.length ? 'Todavía no hay cierres.' : _histFilter === 'INV' ? 'Ninguna posición invertida cerrada.' : 'Ningún cierre con ese motivo.'}</td></tr>`;
  setHTML(q('tbClosed'), html);
  const rp = n(_d && _d.total_realized_pnl);
  setHTML(q('histSum'), closed.length ? `${closed.length} cierres, realizado <b class="${cls(rp)}">${sgn(rp)} USDT</b>` : 'Sin cierres');
}

// ── Memoria Upstash (aviso, diagnóstico y texto de Ajustes) ─────────────────
function renderStore(s) {
  if (!s) return;
  const holder = s.holder ? `la instancia ${s.holder}` : 'otra instancia';
  let show = false, lamp = 'warn', title = '', msg = '', take = false;
  if (s.status === 'waiting') {
    show = true; take = true; title = 'Esta instancia espera:';
    msg = `${holder} tiene el control de la memoria. Suele ser un deploy en curso: esta seguirá sola, con todo lo guardado, en cuanto la otra guarde y se detenga (1-2 min). Mientras tanto no opera.`;
  } else if (s.status === 'standby') {
    show = true; take = true; title = 'Esta instancia no opera:';
    msg = `${holder} tomó el control de la memoria y esta se detuvo para no duplicar órdenes. Si la otra ya no debería estar funcionando, toma el control aquí.`;
  } else if (s.status === 'degraded') {
    show = true; lamp = 'bad'; title = 'Memoria Upstash sin conexión:';
    msg = `${s.last_error || s.detail || 'sin respuesta'}. Reintentando sola; mientras tanto el bot gestiona lo abierto pero no abre posiciones nuevas.`;
  } else if (s.status === 'error') {
    show = true; lamp = 'bad'; title = 'Memoria Upstash desactivada:';
    msg = `${s.detail || s.last_error}. El bot opera, pero lo que cambie se perderá si Render lo reinicia.`;
  } else if (s.status === 'active' && n(s.fail_s) > 60) {
    show = true; title = 'Memoria Upstash:';
    msg = `no puedo guardar desde hace ${fmtDur(s.fail_s)} (${s.last_error}). Reintentando; lo último guardado sigue a salvo.`;
  }
  const box = q('storeBox');
  box.style.display = show ? 'flex' : 'none';
  box.classList.toggle('warn', lamp === 'warn');
  setLamp(q('storeLamp'), lamp);
  setTxt('storeTitle', title); setTxt('storeMsg', msg);
  q('storeTake').hidden = !take;

  const st = q('storeState');
  const ago = s.last_save_ago === null || s.last_save_ago === undefined ? '' : `, guardado hace ${fmtDur(s.last_save_ago)}`;
  const TXT = { off: 'Desactivada', connecting: 'Conectando', active: 'Activa' + ago, waiting: 'En espera',
                standby: 'En espera', degraded: 'Sin conexión', error: 'Error' };
  setTxt(st, TXT[s.status] || s.status);
  st.className = s.status === 'active' ? 'pos' : (s.status === 'off' ? 'muted' : (s.status === 'degraded' || s.status === 'error') ? 'neg' : 'warn-t');
  st.title = s.status === 'off' ? s.detail : `prefijo ${s.prefix} · ${n(s.saves)} guardados · ${n(s.calls)} peticiones · ${s.loaded || ''}`;
  const ka = s.keepalive || {}, kb = q('kaState');
  setTxt(kb, ka.status === 'active' ? 'Activo, ' + (ka.cron === '*/10 * * * *' ? 'cada 10 min' : ka.cron)
           : ka.status === 'error' ? 'Error' : 'Desactivado');
  kb.className = ka.status === 'active' ? 'pos' : ka.status === 'error' ? 'neg' : 'muted';
  kb.title = ka.detail || '';

  setTxt('cfgPersist', s.status === 'active' ? 'se guardan en Upstash y sobreviven a reinicios'
    : s.status === 'degraded' ? 'Upstash sin conexión: se guardarán al reconectar'
    : (s.status === 'waiting' || s.status === 'standby') ? 'otra instancia tiene el control'
    : 'se guardan en el servidor; en Render free se pierden al reiniciar');
}
async function releaseCooldown(sym, btn) {
  btn.disabled = true; btn.textContent = 'Liberando…';
  try {
    await postJSON(`/api/cooldown/release/${encodeURIComponent(sym)}`);
    toast(`${sym} liberado: puede abrir con el próximo cruce EMA`, 'ok');
  } catch (e) { btn.disabled = false; btn.textContent = 'Liberar'; toast(`No se liberó ${sym}: ${e.message}`, 'bad'); }
  finally { requestFull(true); }
}
async function releaseAllCooldowns() {
  const k = q('tbCooldown').querySelectorAll('button[data-cd-release]').length;
  if (!k) return;
  if (!confirm(`¿Liberar los ${k} símbolos en cooldown?\n\nPodrán volver a abrir posición con el próximo cruce EMA.`)) return;
  const btn = q('cdReleaseAll'); btn.disabled = true;
  try {
    const r = await postJSON('/api/cooldown/release-all');
    toast(r.count === 1 ? '1 símbolo liberado del cooldown' : `${r.count} símbolos liberados del cooldown`, 'ok');
  } catch (e) { toast('No se liberaron: ' + e.message, 'bad'); }
  finally { btn.disabled = false; requestFull(true); }
}
async function storeTakeover() {
  if (!confirm('¿Tomar el control en esta instancia?\n\nLa otra dejará de operar en unos segundos. Hazlo solo si la otra ya no debería estar funcionando (por ejemplo, una copia vieja del bot).')) return;
  const btn = q('storeTake'); btn.disabled = true;
  try { await postJSON('/api/store/takeover'); toast('Esta instancia tiene ahora el control', 'ok'); }
  catch (e) { toast('No se pudo tomar el control: ' + e.message, 'bad'); }
  finally { btn.disabled = false; requestFull(true); }
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
  const origin = ca.origin === 'GLOBAL' ? 'Stop global: ' : ca.origin === 'GLOBAL_TP' ? 'TP global: ' : '';
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
  if (n(d.ts)) _skew = n(d.ts) - Date.now() / 1000;
  renderStore(d.store);
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

  // Modo invertido (tarjeta, insignia y rótulos de los ajustes en espejo)
  renderMode(!!d.inverted_mode, d.mirror_rules, d.gate || {});
  const inv = !!d.inverted_mode;

  // Ajustes: SL, TP, EMA, DCA (en modo invertido se muestran en espejo: mismo valor, otro signo)
  const frac = n(d.first_tranche_sl_fraction || 0.251), defSl = n(d.default_stop_loss_usd ?? -8);
  setTxt('cvSl', inv ? `+${fx(-defSl, 2)} USD` : `${fx(defSl, 2)} USD`);
  setTxt('slHint', inv
    ? `Con 1 tramo el TP es el notional del tramo × ${frac} (por ejemplo, 5 USDT → +${fx(5 * frac, 3)} USD). Desde el 2.º tramo se usa este TP estándar. Es el stop loss del modo normal con el signo cambiado: el mismo valor sigue siendo el SL de las posiciones normales.`
    : `Con 1 tramo el SL es el notional del tramo × ${frac} (por ejemplo, 5 USDT → ${fx(-5 * frac, 3)} USD). Desde el 2.º tramo se usa este SL estándar. Se aplica al instante a las posiciones abiertas.`);
  fillVal('gslInput', fx(inv ? -defSl : defSl, 2));

  const tpf = n(d.take_profit_fraction || 0.07);
  setTxt('cvTp', inv ? `−${+(tpf * 100).toFixed(2)} % del notional` : `${+(tpf * 100).toFixed(2)} % del notional`);
  fillVal('tpInput', inv ? -(+tpf.toFixed(4)) : +tpf.toFixed(4));
  setHTML(q('tpPreview'), [5, 10, 20, 50, 100].map(v => inv
    ? `<span class="chip">${v} USDT pierde <b>−${+(v * tpf).toFixed(3)}</b></span>`
    : `<span class="chip">${v} USDT gana <b>+${+(v * tpf).toFixed(3)}</b></span>`).join(''));

  const kw = d.kline_ws || {};
  if (d.ema_fast !== undefined) {
    setTxt('cvEma', `${n(d.ema_fast)} / ${n(d.ema_slow)}, velas de ${d.ema_interval || ''}`);
    fillVal('emaFastInput', n(d.ema_fast)); fillVal('emaSlowInput', n(d.ema_slow));
    q('emaFastInput').max = q('emaSlowInput').max = n(d.ema_max_period || 500);
    setTxt('emaNote', `La EMA lenta admite hasta ${n(d.ema_max_period)} (se guardan ${n(d.ema_max_candles)} velas por símbolo; ${n(kw.pairs_with_data)} símbolos listos). Al cambiarlas se recalculan con esas velas, sin descargar nada y sin cruces falsos. Las posiciones abiertas no cambian. Para que sigan así tras un reinicio de Render, ponlas también en Environment como EMA_FAST y EMA_SLOW.`);
  }
  q('edPeriod').max = n(d.ema_max_period || 500);
  ladderSync(d.ladder);

  // Señales frenadas
  const g = d.gate || {};
  const KIND = { pausa: 'Pausa', exposicion: 'Exposición', btc: 'Filtro BTC', btc_down: 'Filtro BTC bajista',
                 btc_up: 'Filtro BTC alcista', ema_dca: 'Condición EMA', memoria: 'Memoria Upstash' };
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
  // Filas estables (solo cambian si entra o sale un símbolo): el reloj local
  // actualiza el tiempo restante y los botones no se rehacen bajo el dedo.
  const cds = Object.entries(d.cooldowns || {}).sort((a, b) => n(a[1].unblock_ts) - n(b[1].unblock_ts));
  setHTML(q('tbCooldown'), cds.length ? cds.map(([s, i]) => `<tr><td data-label="Símbolo" class="sym">${esc(s)}</td>
      <td data-label="Restante" class="warn-t" data-until="${n(i.unblock_ts)}">…</td><td data-label="Se libera" class="muted">${esc(i.unblock_utc)}</td>
      <td data-label="" class="act"><button class="btn sm" type="button" data-cd-release="${esc(s)}" title="Quitar ${esc(s)} del cooldown: podrá abrir con el próximo cruce EMA">Liberar</button></td></tr>`).join('')
    : '<tr><td colspan="4" class="muted">Ningún símbolo en cooldown.</td></tr>');
  tickUntil();
  const ra = q('cdReleaseAll');
  ra.hidden = cds.length < 2;
  setTxt(ra, `Liberar los ${cds.length}`);
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
  setTxt('executorStatus', d.executor_url ? hostOf(d.executor_url).split('/')[0] + ((d.executor || {}).paused ? ' (en pausa)' : '') : 'No configurado');
  renderExecutor(d.executor);
  renderProxy(d.rest_proxy);
  setTxt('uptime', fmtDur(d.uptime_seconds));
  const mem = d.memory || {};
  if (n(mem.rss_mb)) {
    setTxt('memRss', `${fx(mem.rss_mb, 0)} MB de ${fx(mem.limit_mb, 0)} (pico ${fx(mem.peak_mb, 0)})`);
    q('memRss').className = n(mem.rss_mb) >= n(mem.warn_mb) ? 'neg' : n(mem.rss_mb) >= 0.6 * n(mem.limit_mb) ? 'warn-t' : 'pos';
  }
  setTxt('klResync', `${n(kw.reconnects)} cortes, ${n(kw.gap_fills)} huecos rellenados, ${n(kw.full_warmups)} siembras completas`);
  setTxt('diagSum', `${d.ws_connected ? 'WebSocket conectado' : 'WebSocket desconectado'}, ${fx(d.eval_rate, 0)} evaluaciones/s`
    + (n(mem.rss_mb) ? `, memoria ${fx(mem.rss_mb, 0)} MB` : ''));

  const evs = Array.isArray(d.events) ? d.events : [];
  setHTML(q('events'), evs.map(line => {
    const i = line.indexOf(' | ');
    const dt = new Date(line.slice(0, 19).replace(' ', 'T') + 'Z');
    const stamp = (i > 0 && !isNaN(dt)) ? dt.toLocaleTimeString('es-CO', { hour12: false }) : '';
    const text = i > 0 ? line.slice(i + 3) : line;
    const k = /STOP GLOBAL|STOP LOSS|Error|error|falló|⛔|🛑/.test(text) ? 'bad'
      : /CIERRE TP|REANUDAD|vuelve a responder|TP GLOBAL/.test(text) ? 'good'
      : /PAUSA|frenada|⚠️|CIERRE MASIVO|LINK CAMBIADO|MODO INVERTIDO|MODO NORMAL|bloquead|limitad|fuera (durante|hasta)|\(407:/.test(text) ? 'warn' : '';
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
  if (l.executor) renderExecutor(l.executor, true);
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
  const names = { ALL: 'Todas', TP: 'Take profit', SL: 'Stop loss', MANUAL: 'Manual', GLOBAL: 'Stop global',
                  GLOBAL_TP: 'TP global', NORMAL: 'Normales', INVERTED: 'Invertidas' };
  const G = s.groups || {}, hasInv = !!(G.INVERTED && G.INVERTED.n);
  const keys = ['ALL', 'TP', 'SL', 'MANUAL', 'GLOBAL', 'GLOBAL_TP'].concat(hasInv ? ['NORMAL', 'INVERTED'] : []);
  setTxt('slSimTitle', 'Take profits que cada SL habría cortado' + (hasInv ? ' (solo posiciones normales)' : ''));
  const rows = keys.filter(k => G[k] && G[k].n).map(k => {
    const g = G[k], tot = n(g.pnl && g.pnl.mean) * n(g.n);
    return `<tr><td>${names[k]}</td><td>${g.n}</td><td class="num ${cls(tot)}">${sgn(tot, 2)}</td>
      <td class="pos">${fx(g.mfe_usd.median, 3)}</td><td class="pos">${fx(g.mfe_usd.p90, 3)}</td>
      <td class="neg">${fx(g.mae_usd.median, 3)}</td><td class="neg">${fx(g.mae_usd.p90, 3)}</td>
      <td>${fx(g.mfe_pct.median, 1)} %</td><td>${fx(g.mae_pct.median, 1)} %</td><td>${fmtDur(g.duration_s.median)}</td></tr>`;
  });
  setHTML(q('tbStatGroups'), rows.length ? rows.join('') : '<tr><td colspan="10" class="muted">Sin datos aún</td></tr>');
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
  const body = { paused: !resume, reason: 'Pausa manual' };
  let okMsg = 'Entradas reanudadas';
  if (!resume) {
    const dv = segValue('pauseSeg', 'pauseMin');
    if (dv.error) { toast(dv.error, 'bad'); q('pauseMin').focus(); return; }
    const how = dv.minutes ? `durante ${fmtMin(dv.minutes)} (luego se reanudan solas)` : 'hasta que las reanudes';
    if (!confirm(`¿Pausar la apertura de posiciones nuevas ${how}?\n\nLas posiciones abiertas siguen con su TP, SL y DCA.`)) return;
    body.minutes = dv.minutes;
    if (dv.minutes) lsSet('pauseMin', String(dv.minutes));
    okMsg = dv.minutes ? `Entradas en pausa durante ${fmtMin(dv.minutes)}` : 'Entradas en pausa hasta que las reanudes';
  }
  busy(btn, true);
  try {
    const data = await postJSON('/api/pause', body);
    if (_gate) Object.assign(_gate, { paused: data.paused, pause_until: data.pause_until });
    toast(okMsg, 'ok');
    requestFull(true);
  } catch (e) { toast('No se pudo cambiar la pausa: ' + e.message, 'bad'); }
  finally { busy(btn, false, resume ? 'Pausar entradas' : 'Reanudar entradas'); }
}

// ── Executor: estado, link en caliente y pausa del envío ────────────────────
let _ex = null;
function renderExecutor(ex, partial) {
  if (!ex) return;
  _ex = partial ? { ...(_ex || {}), ...ex } : ex;
  const e = _ex, url = e.url || '';
  const errRecent = n(e.sent_err) > 0 && n(e.last_err_ts) > n(e.last_ok_ts);
  const st = !url ? 'off' : e.paused ? 'warn' : errRecent ? 'bad' : 'ok';
  setLamp(q('lampExec'), st);
  setLamp(q('lampExecCard'), st);
  setTxt('lblExec', 'Executor: ' + (!url ? 'sin link' : e.paused ? 'en pausa' : errRecent ? 'con errores' : 'enviando'));
  setTxt('execHost', url ? hostOf(url) : 'sin link');
  q('execHost').title = url;
  let txt;
  if (!url) txt = 'Sin link: el bot opera pero no envía señales a ningún executor.';
  else if (e.paused) txt = `En pausa ${untilTxt(e.pause_until, e.pause_left_s, 'hasta que lo reanudes')}: las posiciones nuevas no se envían al executor. Las que ya están en él siguen recibiendo su DCA y su cierre.`;
  else txt = 'Enviando las posiciones nuevas al executor.';
  if (url && e.sent_ok !== undefined) {
    const ok = n(e.sent_ok), bad = n(e.sent_err);
    txt += ` ${ok} señal${ok === 1 ? '' : 'es'} enviada${ok === 1 ? '' : 's'} desde el arranque`
      + (bad ? `, ${bad} con error` + (errRecent ? ` (la última a las ${hhmm(e.last_err_ts)}: ${e.last_err})` : '') : '') + '.';
  }
  const k = n(e.open_on_other);
  if (k) txt += k === 1 ? ' 1 posición abierta sigue en el link anterior hasta cerrarse.'
                        : ` ${k} posiciones abiertas siguen en el link anterior hasta cerrarse.`;
  setTxt('execText', txt);
  q('execSeg').hidden = !!e.paused || !url;
  const pb = q('execPauseBtn');
  if (!pb.dataset.busy) {
    setTxt(pb, e.paused ? 'Reanudar envío al executor' : 'Pausar envío al executor');
    pb.className = 'btn block ' + (e.paused ? 'green' : 'amber');
    pb.disabled = !url && !e.paused;
  }
  if (e.env_url !== undefined) {
    setTxt('execHint', e.env_url && e.env_url !== url ? `El de arranque (EXECUTOR_URL) es ${hostOf(e.env_url)}.` : '');
  }
  fillVal('execUrl', url);
  execDirty();
}
function execDirty() {
  const inp = q('execUrl'), cur = (_ex && _ex.url) || '', val = normUrl(inp.value);
  const changed = val !== cur;
  if (changed) inp.dataset.dirty = '1'; else delete inp.dataset.dirty;
  const btn = q('execSave');
  if (!btn.dataset.busy) btn.disabled = !changed;
  const nOpen = n(_ex && _ex.open_on_current);
  const showMove = changed && !!cur && !!val && nOpen > 0;
  q('execMoveWrap').hidden = !showMove;
  if (showMove) setTxt('execMoveTxt', `Mandar también el DCA y el cierre de ${nOpen === 1 ? 'la posición abierta' : `las ${nOpen} posiciones abiertas`} al link nuevo (solo si es el mismo executor con otra dirección)`);
}
async function saveExecUrl() {
  const inp = q('execUrl'), btn = q('execSave'), val = normUrl(inp.value), cur = (_ex && _ex.url) || '';
  if (val === cur) return;
  const move = !q('execMoveWrap').hidden && q('execMove').checked;
  const nOpen = n(_ex && _ex.open_on_current);
  let msg = val ? `¿Cambiar el link del executor a\n${val}?\n\nLas posiciones nuevas se enviarán ahí.`
                : '¿Quitar el link del executor?\n\nEl bot seguirá operando pero no enviará señales.';
  if (cur && val && nOpen) msg += move ? `\nLas ${nOpen} posiciones abiertas también pasan al link nuevo (su DCA y su cierre irán ahí).`
                                       : `\nLas ${nOpen} posiciones abiertas seguirán recibiendo su DCA y su cierre en el link anterior.`;
  if (!confirm(msg)) return;
  btn.dataset.busy = '1';
  busy(btn, true);
  try {
    const data = await postJSON('/api/executor/url', { url: val, move_open: move });
    delete inp.dataset.dirty;
    q('execMove').checked = false;
    setTxt('execMsg', '');
    renderExecutor(data.executor);
    toast(!data.changed ? 'El link no cambió' : val ? `Link del executor: ${hostOf(val)}` : 'Link del executor quitado', 'ok');
    requestFull(true);
  } catch (e) { toast('No se guardó el link: ' + e.message, 'bad'); }
  finally { delete btn.dataset.busy; busy(btn, false, 'Guardar link'); execDirty(); }
}
async function testExecUrl() {
  const btn = q('execTest'), val = normUrl(q('execUrl').value) || ((_ex && _ex.url) || ''), m = q('execMsg');
  if (!val) { toast('Escribe un link para probarlo.', 'bad'); return; }
  btn.disabled = true; setTxt(btn, 'Probando…');
  m.className = 'msg muted'; setTxt(m, `Probando ${hostOf(val)}…`);
  try {
    const d = await postJSON('/api/executor/test', { url: val });
    if (d.reachable) { m.className = 'msg pos'; setTxt(m, `${hostOf(val)} responde (HTTP ${d.status}, ${n(d.ms)} ms).`); }
    else {
      m.className = 'msg neg';
      setTxt(m, `${hostOf(val)} no responde: ${d.detail || 'sin respuesta'}.`
        + (/timed out|timeout/i.test(d.detail || '') ? ' Si estaba dormido, vuelve a probar en un minuto.' : ''));
    }
  } catch (e) { m.className = 'msg neg'; setTxt(m, e.message); }
  finally { btn.disabled = false; setTxt(btn, 'Probar'); }
}
async function toggleExecPause() {
  const btn = q('execPauseBtn'), resume = !!(_ex && _ex.paused);
  const body = { paused: !resume, reason: 'Pausa manual' };
  let okMsg = 'Envío al executor reanudado';
  if (!resume) {
    const dv = segValue('execSeg', 'execMin');
    if (dv.error) { toast(dv.error, 'bad'); q('execMin').focus(); return; }
    const how = dv.minutes ? `durante ${fmtMin(dv.minutes)} (luego se reanuda solo)` : 'hasta que lo reanudes';
    if (!confirm(`¿Pausar el envío de posiciones nuevas al executor ${how}?\n\nEl bot sigue operando. Las posiciones que ya están en el executor siguen recibiendo su DCA y su cierre.`)) return;
    body.minutes = dv.minutes;
    if (dv.minutes) lsSet('execMin', String(dv.minutes));
    okMsg = dv.minutes ? `Envío al executor en pausa durante ${fmtMin(dv.minutes)}` : 'Envío al executor en pausa hasta que lo reanudes';
  }
  btn.dataset.busy = '1';
  busy(btn, true);
  try {
    const data = await postJSON('/api/executor/pause', body);
    delete btn.dataset.busy;
    busy(btn, false);
    renderExecutor(data.executor);
    toast(okMsg, 'ok');
    requestFull(true);
  } catch (e) { toast('No se pudo cambiar la pausa del executor: ' + e.message, 'bad'); }
  finally { if (btn.dataset.busy) { delete btn.dataset.busy; busy(btn, false); renderExecutor(_ex); } }
}

// ── Proxies de arranque (diagnóstico) ───────────────────────────────────────
function renderProxy(rp) {
  if (!rp) return;
  const box = q('proxyBox'), el = q('proxyStatus');
  if (!rp.configured) { setTxt(el, 'Sin proxies'); el.className = 'muted'; box.hidden = true; return; }
  box.hidden = false;
  setTxt(el, rp.active ? `Activos, quedan ${fmtDur(rp.window_left_s)}`
    : rp.fallback ? 'De respaldo (si Binance bloquea la IP)' : 'Terminados, REST directo');
  el.className = rp.active ? 'pos' : 'muted';
  setTxt('proxySum', `Proxies de arranque: ${n(rp.n_proxies)} proxies, ${rp.mode === 'always' ? 'siempre por proxy' : 'modo automático'}, `
    + (rp.active ? `quedan ${fmtDur(rp.window_left_s)} de ${n(rp.hours)} h`
       : `ventana de ${n(rp.hours)} h terminada` + (rp.fallback ? ', ahora de respaldo si Binance bloquea la IP' : ''))
    + `. ${n(rp.via_proxy)} descargas por proxy y ${n(rp.direct)} directas (${n(rp.klines)} de velas).`);
  const ST = { ok: ['pos', 'Disponible'], cooling: ['warn-t', 'En pausa'], off: ['neg', 'Fuera'], limit: ['warn-t', 'Al tope de peso'] };
  setHTML(q('tbProxy'), (rp.routes || []).map(r => {
    const s = ST[r.state] || ['', r.state];
    return `<tr>
      <td data-label="Salida"><span><b>${esc(r.name)}</b> <span class="muted">${esc(r.label)}</span></span></td>
      <td data-label="Estado"><span><span class="${s[0]}">${s[1]}${r.state === 'cooling' ? ' ' + fmtDur(r.cool_left_s) : ''}</span>${r.reason ? ` <small class="muted">${esc(r.reason)}</small>` : ''}</span></td>
      <td data-label="OK">${n(r.ok)}</td>
      <td data-label="Fallos">${n(r.fail)}</td>
      <td data-label="Peso del minuto">${n(r.weight)} / ${n(rp.weight_limit)}</td>
      <td data-label="Última respuesta">${n(r.last_status) ? `HTTP ${n(r.last_status)}, ${hhmm(r.last_ts)}` : '—'}</td>
    </tr>`;
  }).join(''));
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
  // Se guarda siempre como pérdida (−X). En modo invertido el campo muestra +X (TP global):
  // negVal acepta 5, +5 o −5 y devuelve −5 en los dos casos.
  const inv = !!_inv, usd = negVal('gsUsd'), enabled = q('gsEnabled').checked;
  if (isNaN(usd)) {
    toast(inv ? 'Escribe la ganancia a la que se cierran las invertidas, por ejemplo 5.'
              : 'Escribe la pérdida a la que se cierra todo, por ejemplo 5 o -5.', 'bad');
    return;
  }
  const pause = q('gsPause').checked, dv = segValue('gsSeg', 'gsPauseMin');
  if (pause && dv.error) { toast(dv.error, 'bad'); q('gsPauseMin').focus(); return; }
  const g = _gate || {};
  const unN = n(g.unreal_normal), nN = n(g.open_normal), unI = n(g.unreal_inverted), nI = n(g.open_inverted);
  if (enabled && nN > 0 && unN <= usd &&
      !confirm(`El PnL de las posiciones normales ya es ${sgn(unN, 2)} USD, así que el stop global las cerrará de inmediato.\n\n¿Guardar igualmente?`)) return;
  if (enabled && nI > 0 && unI >= -usd &&
      !confirm(`El PnL de las posiciones invertidas ya es ${sgn(unI, 2)} USD, así que el TP global las cerrará de inmediato.\n\n¿Guardar igualmente?`)) return;
  const body = { global_stop_enabled: enabled, global_stop_usd: usd, global_stop_pause: pause };
  if (!dv.error) body.global_stop_pause_min = dv.minutes || 0;
  const what = inv ? 'TP global' : 'Stop global';
  saveRisk('gsSave', body, !enabled ? `${what} desactivado`
    : `${what} guardado en ${inv ? '+' + fx(-usd, 2) : fx(usd, 2)} USD` + (!pause ? ', sin pausa'
      : dv.minutes ? `, pausa de ${fmtMin(dv.minutes)}` : ', pausa hasta reanudar'));
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
  // Mismo valor para los dos modos: SL estándar de las normales (−X) = TP estándar de las invertidas (+X)
  const inv = !!_inv, v = negVal('gslInput'), btn = q('gslSave');
  if (isNaN(v)) { toast(inv ? 'Escribe el take profit en USD, por ejemplo 8.' : 'Escribe el stop loss en USD, por ejemplo 8 o -8.', 'bad'); return; }
  busy(btn, true);
  try {
    const data = await postJSON('/api/set-default-sl', { sl_usd: v, override_manual: q('gslOverride').checked });
    clearDirty(btn.closest('[data-panel]'));
    toast(`${inv ? 'Take profit estándar en +' + fx(-v, 2) : 'Stop loss estándar en ' + fx(v, 2)} USD, ${(data.updated || []).length} posición(es) actualizada(s)`, 'ok');
    requestFull(true);
  } catch (e) { toast('No se guardó: ' + e.message, 'bad'); }
  finally { busy(btn, false); }
}
async function saveTp() {
  // Mismo multiplicador: TP de las normales (+notional×f) = SL de las invertidas (−notional×f)
  const inv = !!_inv, v = Math.abs(numVal('tpInput')), btn = q('tpSave');
  if (isNaN(v) || v < 0.001 || v > 5) {
    toast(inv ? 'El multiplicador va de 0.001 a 5, por ejemplo -0.1.' : 'El multiplicador va de 0.001 a 5, por ejemplo 0.07.', 'bad');
    return;
  }
  busy(btn, true);
  try {
    await postJSON('/api/set-take-profit', { fraction: v });
    clearDirty(btn.closest('[data-panel]'));
    toast(inv ? `Stop loss en −${+(v * 100).toFixed(2)} % del notional` : `Take profit en ${+(v * 100).toFixed(2)} % del notional`, 'ok');
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
let _slSym = null, _slKind = 'sl';
// kind 'sl': SL manual de una posición normal · 'tp': TP manual de una invertida (su espejo)
function editStopLoss(sym, cur, kind = 'sl') {
  _slSym = sym; _slKind = kind; setTxt('slModalSymbol', sym);
  setTxt('slModalWhat', kind === 'tp' ? 'Take profit' : 'Stop loss');
  setTxt('slModalLbl', kind === 'tp' ? 'Ganancia objetivo en USD. Un TP fijado a mano no lo cambia el bot.'
                                     : 'Pérdida máxima en USD. Un SL fijado a mano no lo cambia el bot.');
  setTxt('slModalSave', kind === 'tp' ? 'Guardar take profit' : 'Guardar stop loss');
  q('slModalInput').value = kind === 'tp' ? +n(cur).toFixed(4) : cur; q('slModalError').style.display = 'none';
  q('slModalOverlay').style.display = 'flex';
  setTimeout(() => q('slModalInput').focus(), 50);
}
function closeSlModal() { q('slModalOverlay').style.display = 'none'; _slSym = null; }
async function saveSlModal() {
  if (!_slSym) return;
  const btn = q('slModalSave'), err = q('slModalError'), sym = _slSym;
  if (_slKind === 'tp') {
    const raw = numVal('slModalInput'), v = Math.abs(raw);
    if (!(v > 0)) { setTxt(err, 'Escribe la ganancia objetivo, por ejemplo 8.'); err.style.display = 'block'; return; }
    busy(btn, true);
    try { await postJSON(`/api/set-tp/${sym}`, { tp_usd: v }); toast(`Take profit de ${sym} en +${fx(v, 2)} USD`, 'ok'); closeSlModal(); requestFull(true); }
    catch (e) { setTxt(err, e.message); err.style.display = 'block'; }
    finally { busy(btn, false); }
    return;
  }
  const v = negVal('slModalInput');
  if (isNaN(v)) { setTxt(err, 'Escribe la pérdida máxima, por ejemplo 5 o -5.'); err.style.display = 'block'; return; }
  busy(btn, true);
  try { await postJSON(`/api/set-sl/${sym}`, { sl_usd: v }); toast(`Stop loss de ${sym} en ${fx(v, 2)} USD`, 'ok'); closeSlModal(); requestFull(true); }
  catch (e) { setTxt(err, e.message); err.style.display = 'block'; }
  finally { busy(btn, false); }
}

// ── Modo invertido: un solo botón ───────────────────────────────────────────
let _inv = null, _rules = null;
function setBtnLabel(id, txt) {
  const b = q(id); if (!b) return;
  if (b.disabled && b.dataset.label) b.dataset.label = txt; else setTxt(b, txt);
}
// Rótulos de los ajustes: en modo invertido se ven en espejo (TP ↔ SL, stop global → TP global)
function applyModeLabels(inv) {
  setTxt('gsName', inv ? 'Take profit global por PnL' : 'Stop global por PnL');
  setTxt('gsEnabledTxt', inv ? 'TP global activado' : 'Stop global activado');
  setTxt('gsHint', inv
    ? 'Si la suma del PnL no realizado de las posiciones invertidas llega a este valor, el bot las cierra todas a mercado, una a una. Es el stop global del modo normal con el signo cambiado.'
    : 'Si la suma del PnL no realizado de las posiciones normales llega a este valor, el bot las cierra todas a mercado, una a una.');
  setTxt('gsUsdLbl', inv ? 'Cerrar las invertidas cuando su PnL no realizado sea igual o mayor que (USD)'
                         : 'Cerrar todo cuando el PnL no realizado sea igual o menor que (USD)');
  q('gsUsd').placeholder = inv ? '5' : '-5';
  setBtnLabel('gsSave', inv ? 'Guardar TP global' : 'Guardar stop global');
  setTxt('slName', inv ? 'Take profit por posición' : 'Stop loss por posición');
  setTxt('gslLbl', inv ? 'Take profit estándar para 2 o más tramos (USD)' : 'Stop loss estándar para 2 o más tramos (USD)');
  setTxt('gslOverrideTxt', inv ? 'Sobrescribir también los TP fijados a mano' : 'Sobrescribir también los SL fijados a mano');
  q('gslInput').placeholder = inv ? '8' : '-8';
  setBtnLabel('gslSave', inv ? 'Guardar take profit' : 'Guardar stop loss');
  setTxt('tpName', inv ? 'Stop loss' : 'Take profit');
  setTxt('tpHint', inv
    ? 'Stop loss de cada posición = −(notional × multiplicador). Es el take profit del modo normal con el signo cambiado; se aplica al instante a las posiciones abiertas.'
    : 'Objetivo de cada posición = notional × multiplicador. Se aplica al instante a las posiciones abiertas.');
  setTxt('tpLbl', inv ? 'Multiplicador (−0.1 = −10 % del notional)' : 'Multiplicador (0.07 = 7 % del notional)');
  q('tpInput').placeholder = inv ? '-0.1' : '0.07';
  setBtnLabel('tpSave', inv ? 'Guardar stop loss' : 'Guardar take profit');
  setTxt('ilGstopName', inv ? 'TP global' : 'Stop global');
  // El signo de esos campos cambia: se descarta lo que estuviera a medio editar
  ['gsUsd', 'gslInput', 'tpInput'].forEach(id => { const el = q(id); delete el.dataset.dirty; if (document.activeElement === el) el.blur(); });
}
function renderMode(inv, rules, g) {
  if (rules) _rules = rules;
  if (inv !== _inv) { _inv = inv; applyModeLabels(inv); }
  const r = _rules || {};
  g = g || _gate || {};
  setLamp(q('lampInv'), inv ? 'inv' : 'off');
  setTxt('invState', inv ? 'Activo' : 'Apagado');
  q('ctlInv').classList.toggle('inv-on', inv);
  q('invBadge').hidden = !inv;
  const others = inv ? n(g.open_normal) : n(g.open_inverted);
  const kind = inv ? 'normal' : 'invertida';
  setTxt('invText', (inv
    ? 'Activo: cada posición nueva hace lo contrario del bot normal, en los mismos precios.'
    : 'Apagado. Al activarlo, cada posición nueva hará lo contrario del bot normal, en los mismos precios:')
    + (others ? ` ${others} posición${others === 1 ? '' : 'es'} ${kind}${others === 1 ? '' : 'es'} abierta${others === 1 ? '' : 's'} sigue${others === 1 ? '' : 'n'} con sus reglas hasta cerrarse.` : ''));
  if (r.tp_std !== undefined) {
    const items = [
      ['Señal', 'UP → SHORT · DOWN → LONG'],
      ['Take profit', `+${fx(r.tp_first, 3)} USD con 1 tramo, +${fx(r.tp_std, 2)} USD desde el 2.º`],
      ['Stop loss', `−${+(n(r.sl_fraction) * 100).toFixed(2)} % del notional (−${+n(r.sl_fraction).toFixed(4)})`],
      ['TP global', r.global_on ? `+${fx(r.global_tp, 2)} USD` : 'apagado (el stop global está desactivado)'],
      ['DCA', 'en los mismos precios que el normal: cuando el precio va a favor'],
    ];
    setHTML(q('invMap'), items.map(([k, v]) => `<li><span>${k}</span><b>${esc(v)}</b></li>`).join(''));
  }
  const b = q('invBtn');
  if (!b.disabled) { setTxt(b, inv ? 'Volver al modo normal' : 'Activar modo invertido'); b.className = 'btn block ' + (inv ? 'green' : 'violet'); }
}
async function toggleInvert() {
  const btn = q('invBtn'), on = !_inv, r = _rules || {};
  const msg = on
    ? '¿Activar el modo invertido?\n\nLas posiciones NUEVAS harán lo contrario del bot normal, en los mismos precios:\n'
      + '• Señal UP → SHORT, DOWN → LONG\n'
      + `• Take profit +${fx(r.tp_first, 3)} USD con 1 tramo, +${fx(r.tp_std, 2)} USD desde el 2.º\n`
      + `• Stop loss −${+(n(r.sl_fraction) * 100).toFixed(2)} % del notional\n`
      + `• TP global ${r.global_on ? '+' + fx(r.global_tp, 2) + ' USD' : 'apagado'}\n`
      + '• DCA en los mismos precios que el normal (cuando el precio va a favor)\n\n'
      + 'Las posiciones abiertas no cambian.'
    : '¿Volver al modo normal?\n\nLas posiciones nuevas se abrirán en la dirección de la señal. Las invertidas abiertas siguen con sus reglas hasta cerrarse.';
  if (!confirm(msg)) return;
  busy(btn, true);
  try {
    const data = await postJSON('/api/invert', { inverted: on });
    if (_gate) _gate.inverted_mode = !!data.inverted_mode;
    btn.disabled = false;
    renderMode(!!data.inverted_mode, data.rules, _gate);
    toast(on ? 'Modo invertido activado: las posiciones nuevas se abren al revés' : 'Modo normal: las posiciones nuevas siguen la señal', 'ok');
    requestFull(true);
  } catch (e) { toast('No se pudo cambiar el modo: ' + e.message, 'bad'); }
  finally { busy(btn, false, _inv ? 'Volver al modo normal' : 'Activar modo invertido'); }
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
  q('invBtn').addEventListener('click', toggleInvert);
  q('caStart').addEventListener('click', closeAll);
  q('caCancel').addEventListener('click', cancelCloseAll);

  // Cooldowns: liberar uno (botón de su fila) o todos de golpe; control de la memoria
  q('tbCooldown').addEventListener('click', e => {
    const b = e.target.closest('button[data-cd-release]');
    if (b && !b.disabled) releaseCooldown(b.dataset.cdRelease, b);
  });
  q('cdReleaseAll').addEventListener('click', releaseAllCooldowns);
  q('storeTake').addEventListener('click', storeTakeover);
  q('cdCount').addEventListener('click', () => {
    const s = q('sec-radar'); s.open = true; s.scrollIntoView({ block: 'start' });
  });

  // Pausas con duración y executor
  segInit('pauseSeg', 'pauseMin', 'pauseMin');
  segInit('execSeg', 'execMin', 'execMin');
  segInit('gsSeg', 'gsPauseMin', '');
  q('gsPause').addEventListener('change', () => q('gsSeg').classList.toggle('dim', !q('gsPause').checked));
  q('execPauseBtn').addEventListener('click', toggleExecPause);
  q('execSave').addEventListener('click', saveExecUrl);
  q('execTest').addEventListener('click', testExecUrl);
  const eu = q('execUrl');
  eu.addEventListener('input', () => { setTxt('execMsg', ''); execDirty(); });
  eu.addEventListener('keydown', e => {
    if (e.key === 'Enter') { e.preventDefault(); if (!q('execSave').disabled) saveExecUrl(); }
    if (e.key === 'Escape') { eu.value = (_ex && _ex.url) || ''; execDirty(); eu.blur(); }
  });

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
    if (b.dataset.act === 'tp') editStopLoss(b.dataset.sym, n(b.dataset.tp), 'tp');
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
  tickUntil();
}, 1000);

// Con la pestaña en segundo plano se refresca mucho menos: el servidor sigue
// recibiendo visitas (Render no lo duerme) sin gastar CPU en datos que nadie ve.
const HIDDEN_LIVE_MS = 5000, HIDDEN_STATUS_MS = 15000;

// ── Bucle 1: precios en vivo ────────────────────────────────────────────────
let liveTimer = null, liveFails = 0, liveInFlight = false;
async function livePoll() {
  if (liveInFlight) return;
  liveInFlight = true;
  clearTimeout(liveTimer);
  const t0 = performance.now();
  try {
    const r = await fetch('/api/live', { cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    applyLive(await r.json());
    liveFails = 0; liveOk = true;
  } catch (e) { liveFails++; liveOk = false; }
  finally {
    liveInFlight = false;
    setLamp(q('lampPoll'), (pollOk && liveOk) ? 'ok' : (pollOk || liveOk) ? 'warn' : 'bad');
    const spent = performance.now() - t0, backoff = liveFails ? Math.min(3000, liveFails * 300) : 0;
    const base = document.hidden ? HIDDEN_LIVE_MS : livePollMs;
    liveTimer = setTimeout(livePoll, Math.max(0, base - spent) + backoff);
  }
}
document.addEventListener('visibilitychange', () => {
  if (document.hidden) return;
  livePoll();
  requestFull(true);
});

// ── Bucle 2: estructura completa ────────────────────────────────────────────
let statusTimer = null, statusDelay = 1500, statusInFlight = false;
async function pollStatus() {
  if (statusInFlight) return;
  statusInFlight = true; _lastFull = Date.now();
  try {
    const r = await fetch('/api/status', { cache: 'no-store' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    render(await r.json());
    pollOk = true; statusDelay = document.hidden ? HIDDEN_STATUS_MS : statusPollMs;
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

class _JsonCache:
    """Respuesta JSON ya serializada que comparten las peticiones que llegan en el
    mismo instante: con el panel abierto en el PC y en el móvil el servidor ya no
    construye el snapshot dos veces. Con una sola pestaña no cambia nada (el TTL es
    la mitad del intervalo de refresco) y cualquier POST lo invalida."""

    def __init__(self, ttl_s: float) -> None:
        self.ttl = max(0.0, float(ttl_s))
        self._lock = threading.Lock()
        self._body: Optional[bytes] = None
        self._ts = 0.0
        self._gen = 0

    def invalidate(self) -> None:
        with self._lock:
            self._body = None
            self._gen += 1

    def get(self, build) -> bytes:
        with self._lock:
            if self._body is not None and time.monotonic() - self._ts < self.ttl:
                return self._body
            gen = self._gen
        body = json.dumps(build(), ensure_ascii=False, separators=(",", ":"),
                          default=str).encode("utf-8")
        with self._lock:
            if gen == self._gen:                  # nadie lo invalidó mientras se construía
                self._body, self._ts = body, time.monotonic()
        return body


_STATUS_CACHE = _JsonCache(min(1.0, STATUS_POLL_MS / 2000.0))
_LIVE_CACHE   = _JsonCache(min(0.5, LIVE_POLL_MS / 2000.0))


def _json_response(body: bytes):
    resp = Response(body, mimetype="application/json")
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.after_request
def _fresh_after_write(resp):
    """Tras cualquier cambio (POST) el siguiente refresco del panel sale nuevo."""
    if request.method == "POST":
        _STATUS_CACHE.invalidate()
        _LIVE_CACHE.invalidate()
    return resp


@app.get("/")
def index():
    resp = make_response(HTML)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/api/status")
def api_status():
    return _json_response(_STATUS_CACHE.get(bot.snapshot))


@app.get("/api/live")
def api_live():
    """Payload mínimo (precio, cambio y PnL) para el refresco en vivo del navegador."""
    return _json_response(_LIVE_CACHE.get(bot.live_payload))


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
    try:
        ok = bot.set_stop_loss(symbol, sl_usd)
    except ValueError as exc:
        return jsonify({"ok": False, "symbol": symbol, "error": str(exc)}), 400
    if ok:
        return jsonify({"ok": True, "symbol": symbol, "sl_usd": sl_usd})
    return jsonify({"ok": False, "symbol": symbol, "error": "Posición no encontrada o cerrada"}), 404


@app.post("/api/set-tp/<symbol>")
def api_set_tp(symbol: str):
    """Take profit MANUAL (USD, positivo) de una posición INVERTIDA abierta:
    {"tp_usd": 8}. Es el espejo del SL manual de una posición normal."""
    symbol = symbol.upper().strip()
    data = request.get_json(silent=True) or {}
    try:
        tp_usd = float(str(data.get("tp_usd")).replace(",", "."))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "tp_usd inválido"}), 400
    if not (0 < tp_usd <= 100000):
        return jsonify({"ok": False, "error": "tp_usd debe ser un valor positivo (ganancia)"}), 400
    try:
        ok = bot.set_take_profit_manual(symbol, tp_usd)
    except ValueError as exc:
        return jsonify({"ok": False, "symbol": symbol, "error": str(exc)}), 400
    if ok:
        return jsonify({"ok": True, "symbol": symbol, "tp_usd": tp_usd})
    return jsonify({"ok": False, "symbol": symbol, "error": "Posición no encontrada o cerrada"}), 404


@app.post("/api/invert")
def api_invert():
    """Modo invertido: {"inverted": true} lo activa y {"inverted": false} lo apaga.
    Solo cambia las posiciones NUEVAS."""
    data = request.get_json(silent=True) or {}
    try:
        enabled = _v_bool(data.get("inverted"))
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, **bot.set_inverted(enabled)})


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
        rows = list(bot.trade_stats)[-limit:]
    rows.reverse()
    resp = jsonify(rows)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.get("/api/trades.csv")
def api_trades_csv():
    """Descarga CSV de todo el histórico (para analizarlo en Excel / pandas).
    Se lee del archivo línea a línea (en memoria solo están las más recientes)."""
    cols = [
        "trade_id", "symbol", "direction", "inverted", "signal_dir", "reason", "opened_at_ts", "closed_at_ts",
        "duration_s", "avg_entry", "close_price", "qty", "notional", "fills_n", "levels",
        "sl_usd", "target", "pnl", "pnl_pct", "mfe_usd", "mae_usd", "mfe_pct", "mae_pct",
        "time_to_mfe_s", "time_to_mae_s", "low_price", "high_price",
    ]

    def records():
        if os.path.exists(STATS_FILE):
            try:
                with open(STATS_FILE, "r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except Exception:
                            continue
                        if isinstance(rec, dict):
                            yield rec
                return
            except OSError:
                pass
        with bot._stats_lock:
            mem = list(bot.trade_stats)
        yield from mem

    def generate():
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(cols)
        n = 0
        for r in records():
            out = []
            for c in cols:
                v = r.get(c, "")
                out.append("|".join(str(x) for x in v) if isinstance(v, list) else v)
            writer.writerow(out)
            n += 1
            if n % 200 == 0:
                yield buf.getvalue()
                buf.seek(0)
                buf.truncate(0)
        yield buf.getvalue()

    resp = Response(generate(), mimetype="text/csv")
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


@app.post("/api/cooldown/release/<symbol>")
def api_cooldown_release(symbol: str):
    """Libera UN símbolo del cooldown: podrá abrir con el próximo cruce EMA."""
    symbol = symbol.upper().strip()
    released = bot.release_cooldowns([symbol])
    if not released:
        return jsonify({"ok": False, "error": f"{symbol} no está en cooldown"}), 404
    return jsonify({"ok": True, "released": released, "count": len(released)})


@app.post("/api/cooldown/release-all")
def api_cooldown_release_all():
    """Libera TODOS los símbolos en cooldown de una vez."""
    released = bot.release_cooldowns(None)
    return jsonify({"ok": True, "released": released, "count": len(released)})


@app.get("/api/store")
def api_store():
    """Estado de la memoria externa (Upstash) y del keep-alive (QStash)."""
    resp = jsonify(bot.store.view())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.post("/api/store/takeover")
def api_store_takeover():
    """Esta instancia toma el control de la memoria aunque otra lo tenga."""
    res = bot.store_takeover()
    if res.get("ok"):
        return jsonify(res)
    return jsonify({"ok": False, "error": res.get("error", "error")}), res.get("code", 500)


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
    """Pausa ({"paused": true}) o reanuda ({"paused": false}) las entradas nuevas.
    {"paused": true, "minutes": 25} → se reanudan solas a los 25 min;
    sin "minutes" (o 0/null) → hasta reanudarlas a mano."""
    data = request.get_json(silent=True) or {}
    try:
        paused = _v_bool(data.get("paused", True))
        minutes = _v_pause_minutes(data.get("minutes")) if paused else None
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    reason = str(data.get("reason", "") or "").strip()[:120]
    return jsonify({"ok": True, **bot.set_pause(paused, reason or "Pausa manual", minutes=minutes)})


@app.get("/api/executor")
def api_executor():
    """Link del executor, estado de la pausa del envío y contadores de señales."""
    resp = jsonify(bot.executor_view())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.post("/api/executor/url")
def api_executor_url():
    """Cambia el link del executor en caliente: {"url": "https://…", "move_open": false}.
    move_open=true → el DCA y el cierre de las posiciones abiertas también van al
    link nuevo (solo si es el mismo executor con otra dirección)."""
    data = request.get_json(silent=True) or {}
    try:
        move_open = _v_bool(data.get("move_open", False))
        result = bot.set_executor_url(data.get("url", ""), move_open=move_open)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    return jsonify({"ok": True, **result})


@app.post("/api/executor/pause")
def api_executor_pause():
    """Pausa ({"paused": true, "minutes": 25 | null}) o reanuda ({"paused": false}) el
    envío de posiciones NUEVAS al executor."""
    data = request.get_json(silent=True) or {}
    try:
        paused = _v_bool(data.get("paused", True))
        minutes = _v_pause_minutes(data.get("minutes")) if paused else None
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    if paused and not bot.executor.config.executor_url:
        return jsonify({"ok": False, "error": "No hay link de executor que pausar"}), 400
    reason = str(data.get("reason", "") or "").strip()[:120]
    return jsonify({"ok": True, **bot.set_executor_pause(paused, minutes=minutes,
                                                         reason=reason or "Pausa manual")})


@app.post("/api/executor/test")
def api_executor_test():
    """Comprueba si un link (o el actual) responde, sin enviar señales: {"url": "…"}."""
    data = request.get_json(silent=True) or {}
    raw = data.get("url")
    try:
        url = _v_exec_url(raw) if raw not in (None, "") else None
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    res = bot.executor.probe(url)
    target = url or bot.executor.config.executor_url
    return jsonify({"ok": True, "reachable": bool(res.get("ok")), "url": target,
                    "status": res.get("status"), "ms": res.get("ms"),
                    "detail": res.get("error", "")})


@app.get("/api/rest-proxy")
def api_rest_proxy():
    """Estado de los proxies de arranque del REST de Binance."""
    resp = jsonify(REST_ROUTER.view())
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


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
    """Ligero a propósito (Render o un pinger lo llaman a menudo): antes construía
    el snapshot completo del panel en cada llamada."""
    ws_ok, universe = False, 0
    pc = bot.price_cache
    if pc is not None:
        try:
            st = pc.get_stats()
            ws_ok, universe = bool(st.get("connected", False)), int(st.get("active_tickers", 0) or 0)
        except Exception:
            pass
    now = time.time()
    with bot.lock:
        cooldowns = sum(1 for ts in bot.symbol_cooldown.values() if ts > now)
    return jsonify({
        "ok":                True,
        "running":           bot.running,
        "mode":              "PAPER" if PAPER_MODE or not LIVE_TRADING else "REAL",
        "ws_connected":      ws_ok,
        "scan_count":        bot.scan_count,
        "eval_rate":         bot.eval_rate,
        "latency_avg_ms":    bot.latency_avg_ms,
        "all_symbols_count": len(bot.all_symbols),
        "universe_count":    universe,
        "subscribed_count":  len(bot.watch),
        "last_error":        bot.last_error,
        "cooldown_count":    cooldowns,
        "rss_mb":            round(bot._note_rss(), 1),
        "peak_mb":           round(bot.mem_peak_mb, 1),
        "uptime_s":          round(now - bot.started_at),
        "store":             bot.store.status,
        "keepalive":         bot.store.keepalive.get("status", "off"),
    })


# El servidor de Flask escribe una línea de log por petición: con el panel abierto
# son 4-5 por segundo (/api/live cada 250 ms), que tapan los mensajes del bot en el
# log de Render y gastan CPU. Se omiten las respuestas correctas de las rutas de
# refresco; los errores y el resto de rutas se siguen registrando.
_QUIET_PATHS = frozenset({"/api/live", "/api/status", "/api/stats", "/health"})

try:
    from werkzeug.serving import WSGIRequestHandler as _WSGIRequestHandler

    class _QuietRequestHandler(_WSGIRequestHandler):
        def log_request(self, code="-", size="-"):
            try:
                c = int(getattr(code, "value", code))
                if (self.command == "GET" and 200 <= c < 400
                        and self.path.split("?", 1)[0] in _QUIET_PATHS):
                    return
            except Exception:
                pass
            super().log_request(code, size)
except Exception:                                   # werkzeug sin esa clase: log normal
    _QuietRequestHandler = None


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    run_opts: Dict[str, Any] = {"threaded": True}
    if _QuietRequestHandler is not None:
        run_opts["request_handler"] = _QuietRequestHandler
    app.run(host="0.0.0.0", port=port, **run_opts)
