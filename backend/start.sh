#!/usr/bin/env bash
# CARTA startup — creates venv, installs deps, launches server on 0.0.0.0
set -euo pipefail
cd "$(dirname "$0")"

VENV=".venv"
PORT="${CARTA_PORT:-8787}"
HOST="${CARTA_HOST:-0.0.0.0}"

if [ ! -d "$VENV" ]; then
  echo "[carta] creating virtualenv…"
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q --upgrade pip
  "$VENV/bin/pip" install -q -r requirements.txt
fi

# Soft check for Ollama — warn only
if ! curl -fsS --max-time 2 "${OLLAMA_URL:-http://127.0.0.1:11434}/api/tags" >/dev/null 2>&1; then
  echo "[carta] WARN  Ollama is not reachable at ${OLLAMA_URL:-http://127.0.0.1:11434}."
  echo "        CARTA chat & draft features will return an offline notice until Ollama starts."
fi

echo "[carta] listening on http://$HOST:$PORT  (model=${OLLAMA_MODEL:-llama3.2:1b})"
exec "$VENV/bin/uvicorn" server:app --host "$HOST" --port "$PORT" "$@"
