# core-bridge

Puente SQL genérico por HTTP. Recibe una conexión a base de datos y un SQL, lo ejecuta tal cual y devuelve columnas/filas o el error. Sin lógica de negocio.

## Instalar

```bash
pip install -r requirements.txt
```

Funciona en macOS, Windows y Linux (dependencias con wheels precompiladas).

## Ejecutar

```bash
python bridge.py                      # genera una clave y la muestra
python bridge.py --key MI_CLAVE --port 8000 --ttl 7200
```

Opciones: `--key` (clave de auth), `--host`, `--port`, `--ttl` (duración de la sesión en segundos, defecto 7200 = 2h).

## Usar

`POST /execute` con body:

```json
{
  "key": "MI_CLAVE",
  "engine": "postgres",
  "host": "db", "port": 5432,
  "user": "u", "password": "p", "database": "app",
  "sql": "SELECT id, nombre FROM usuarios"
}
```

- `engine`: `postgres` / `postgresql` / `mysql`
- Respuesta SELECT → `{"columns": [...], "rows": [[...], ...], "rowcount": N}`
- Respuesta INSERT/UPDATE/DELETE → `{"rowcount": N}`
- Error de key → `401` · Error de conexión → `502` · Error de SQL → `500`

```bash
curl -X POST http://127.0.0.1:8000/execute \
  -H 'Content-Type: application/json' \
  -d '{"key":"MI_CLAVE","engine":"postgres","host":"db","port":5432,"user":"u","password":"p","database":"app","sql":"SELECT 1 AS uno"}'
```

`GET /health` → `{"status": "ok", "paused": false}`

## Sesión

La sesión dura 2h (o `--ttl`). Al expirar, el puente cierra las conexiones, deja de servir peticiones (`503`) y pregunta en la consola:

```
¿Sigues necesitando esta sesión? (Y/y/s/S):
```

Hasta responder `Y`, `y`, `s` o `S` no reabre conexiones. Tras confirmar, reanuda por 2h más.
