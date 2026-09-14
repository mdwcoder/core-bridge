#!/usr/bin/env python3
"""Core Bridge — puente SQL genérico con drivers y sesión temporal.

Dependencias: psycopg2-binary (Postgres), PyMySQL (MySQL). HTTP y JSON con
la stdlib de Python.

Uso:
    python bridge.py [--key CLAVE] [--host 127.0.0.1] [--port 8000] [--ttl 7200]

Arranca, pide una clave de autenticación (si no se pasa --key genera una) y
sirve  POST /execute  con body:
    {"key": "...", "engine": "postgres|mysql", "host":..., "port":...,
     "user":..., "password":..., "database":..., "sql": "..."}

La sesión dura --ttl segundos (defecto 7200 = 2h). Al expirar cierra las
conexiones abiertas y pregunta en la consola:
    ¿Sigues necesitando esta sesión? (Y/y/s/S):
Hasta responder Y/y/s/S no se reabren conexiones ni se sirven peticiones
(devuelve 503).
"""

from __future__ import annotations

import argparse
import datetime
import decimal
import json
import os
import secrets
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import psycopg2
import pymysql

# --- estado global ----------------------------------------------------------
_state_lock = threading.Lock()
_connections: dict[str, tuple[Any, threading.Lock]] = {}
_paused = threading.Event()  # set => en pausa
_session_start: float = 0.0
_auth_key: str = ""


def _json_default(o: Any) -> Any:
    if isinstance(o, (datetime.datetime, datetime.date, datetime.time)):
        return o.isoformat()
    if isinstance(o, datetime.timedelta):
        return o.total_seconds()
    if isinstance(o, decimal.Decimal):
        return str(o)
    if isinstance(o, bytes):
        return o.decode("utf-8", errors="replace")
    if isinstance(o, (set, frozenset)):
        return list(o)
    raise TypeError(f"no serializable: {type(o)}")


def _conn_key(engine: str, p: dict) -> str:
    return f"{engine}|{p.get('host')}|{p.get('port')}|{p.get('user')}|{p.get('database')}"


def _connect(engine: str, p: dict):
    port = p.get("port")
    port = int(port) if port not in (None, "") else None
    if engine in ("postgres", "postgresql"):
        return psycopg2.connect(
            host=p.get("host"), port=port, user=p.get("user"),
            password=p.get("password"), dbname=p.get("database"),
        )
    if engine == "mysql":
        return pymysql.connect(
            host=p.get("host"), port=port, user=p.get("user"),
            password=p.get("password"), database=p.get("database"),
        )
    raise ValueError(f"engine no soportado: {engine}")


def _get_conn(engine: str, p: dict):
    """Devuelve (conn, lock) reutilizando la conexión abierta para esa config."""
    key = _conn_key(engine, p)
    with _state_lock:
        entry = _connections.get(key)
        if entry is None:
            conn = _connect(engine, p)
            entry = (conn, threading.Lock())
            _connections[key] = entry
        return entry


def _close_all() -> None:
    with _state_lock:
        for conn, _ in _connections.values():
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        _connections.clear()


# --- ejecución SQL ----------------------------------------------------------
def _execute(engine: str, p: dict) -> tuple[int, dict]:
    conn, lock = _get_conn(engine, p)
    try:
        with lock:
            with conn.cursor() as cur:
                cur.execute(p["sql"])
                if cur.description is not None:
                    columns = [d[0] for d in cur.description]
                    rows = [list(r) for r in cur.fetchall()]
                    conn.commit()
                    return 200, {"columns": columns, "rows": rows, "rowcount": len(rows)}
                rc = cur.rowcount
                conn.commit()
                return 200, {"rowcount": rc}
    except Exception as exc:  # noqa: BLE001
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        # si la conexión se rompió, la quitamos del pool para reabrir luego
        with _state_lock:
            _connections.pop(_conn_key(engine, p), None)
        return 500, {"error": str(exc)}


# --- HTTP -------------------------------------------------------------------
_CORS_ORIGIN = os.environ.get("BRIDGE_CORS_ORIGIN", "*")
_CORS_METHODS = "GET, POST, OPTIONS"
_CORS_HEADERS = "Content-Type, Authorization"


class Handler(BaseHTTPRequestHandler):
    def _cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", _CORS_ORIGIN)
        self.send_header("Access-Control-Allow-Methods", _CORS_METHODS)
        self.send_header("Access-Control-Allow-Headers", _CORS_HEADERS)

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        # preflight CORS
        self.send_response(204)
        self._cors_headers()
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send(200, {"status": "ok", "paused": _paused.is_set()})
        else:
            self._send(404, {"error": "no encontrado"})

    def do_POST(self) -> None:
        if self.path != "/execute":
            self._send(404, {"error": "no encontrado"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b""
            payload = json.loads(raw.decode("utf-8")) if raw else None
        except Exception as exc:  # noqa: BLE001
            self._send(400, {"error": f"JSON inválido: {exc}"})
            return
        if not isinstance(payload, dict):
            self._send(400, {"error": "se esperaba un JSON objeto"})
            return

        if _paused.is_set():
            self._send(503, {"error": "sesión en pausa, confirma en la consola"})
            return

        if not _auth_key or payload.get("key") != _auth_key:
            self._send(401, {"error": "clave de autenticación incorrecta"})
            return

        engine = payload.get("engine")
        sql = payload.get("sql")
        if not engine or not sql:
            self._send(400, {"error": "faltan campos requeridos: 'engine' y/o 'sql'"})
            return
        engine = str(engine).lower()
        if engine not in ("postgres", "postgresql", "mysql"):
            self._send(400, {"error": f"engine '{engine}' no soportado",
                            "available": ["mysql", "postgres", "postgresql"]})
            return
        try:
            status, result = _execute(engine, payload)
        except Exception as exc:  # noqa: BLE001
            status, result = 502, {"error": str(exc)}
        self._send(status, result)

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write(f"{self.address_string()} - {fmt % args}\n")


# --- bucle de sesión (hilo principal) ---------------------------------------
def _fmt_remaining(secs: float) -> str:
    secs = max(0, int(secs))
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def session_loop(ttl: int) -> None:
    global _session_start
    _session_start = time.monotonic()
    while True:
        # esperar hasta que expire la sesión
        while True:
            remaining = ttl - (time.monotonic() - _session_start)
            if remaining <= 0:
                break
            time.sleep(min(remaining, 1.0))
        # expiró: pausar y cerrar conexiones
        _paused.set()
        _close_all()
        print("\n" + "=" * 50, flush=True)
        print("Sesión expirada (2h). Conexiones cerradas.", flush=True)
        print("=" * 50, flush=True)
        while True:
            try:
                ans = input("¿Sigues necesitando esta sesión? (Y/y/s/S): ").strip()
            except EOFError:
                print("\nNo hay terminal para confirmar. Cerrando.", flush=True)
                os._exit(0)
            if ans in ("Y", "y", "s", "S"):
                break
            print("Respuesta no válida. Usa Y, y, s o S.", flush=True)
        _paused.clear()
        _session_start = time.monotonic()
        print("Sesión reanudada por 2h más.\n", flush=True)


def main() -> None:
    global _auth_key
    ap = argparse.ArgumentParser(description="Core Bridge — puente SQL genérico")
    ap.add_argument("--key", help="clave de autenticación (si se omite, se genera/pide)")
    ap.add_argument("--host", default=os.environ.get("BRIDGE_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("BRIDGE_PORT", "8000")))
    ap.add_argument("--ttl", type=int, default=int(os.environ.get("BRIDGE_TTL", "7200")),
                    help="duración de la sesión en segundos (defecto 7200 = 2h)")
    args = ap.parse_args()

    if args.key:
        _auth_key = args.key
    else:
        _auth_key = secrets.token_urlsafe(18)
        print(f"Clave de autenticación generada: {_auth_key}", flush=True)
        print("(Guarda esta clave; debe ir en el campo 'key' de cada petición.)\n", flush=True)

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()
    print(f"core-bridge escuchando en http://{args.host}:{args.port}", flush=True)
    print(f"Sesión activa durante {_fmt_remaining(args.ttl)}. "
          f"Al expirar pedirá confirmación en esta consola.\n", flush=True)

    try:
        session_loop(args.ttl)
    except KeyboardInterrupt:
        print("\nCerrando...", flush=True)
    finally:
        _close_all()
        server.shutdown()


if __name__ == "__main__":
    main()
