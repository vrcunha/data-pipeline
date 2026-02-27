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

AIRFLOW_DIRS=(
  "./airflow/dags"
  "./airflow/logs"
  "./airflow/plugins"
  "./airflow/config"
)

DATA_DIRS=(
  "./bucket_data/minio"
)

echo "Preparing directories..."
mkdir -p "${AIRFLOW_DIRS[@]}" "${DATA_DIRS[@]}"

if command -v sudo >/dev/null 2>&1; then
  echo "Applying permissions with sudo..."
  sudo chown -R "$(id -u):0" "${AIRFLOW_DIRS[@]}" "${DATA_DIRS[@]}"
  sudo chmod -R ug+rwx,o+rx "${AIRFLOW_DIRS[@]}" "${DATA_DIRS[@]}"
else
  echo "Applying permissions without sudo..."
  chown -R "$(id -u):$(id -g)" "${AIRFLOW_DIRS[@]}" "${DATA_DIRS[@]}"
  chmod -R ug+rwx,o+rx "${AIRFLOW_DIRS[@]}" "${DATA_DIRS[@]}"
fi

echo "Building service images..."
"${COMPOSE_CMD[@]}" build

echo "Starting platform..."
"${COMPOSE_CMD[@]}" up -d

echo "Platform started successfully."
