#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker was not found in PATH."
  exit 1
fi

if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "Error: docker compose was not found."
  exit 1
fi

if [[ "${1:-}" == "--volumes" ]]; then
  echo "Stopping platform and removing volumes..."
  "${COMPOSE_CMD[@]}" down --volumes --remove-orphans
else
  echo "Stopping platform..."
  "${COMPOSE_CMD[@]}" down --remove-orphans
fi

echo "Platform stopped."
