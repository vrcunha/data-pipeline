#!/usr/bin/env bash

set -euo pipefail

LAYER="${1:-gold}"
TABLE_NAME="${2:-breweries}"
TABLE_LOCATION="s3a://${LAYER}/${TABLE_NAME}"
CATALOG="lakehouse"

echo "Registering Delta table in Trino catalog..."
echo "Layer: ${LAYER}"
echo "Table: ${TABLE_NAME}"
echo "Location: ${TABLE_LOCATION}"


TRINO_SQL="
CALL ${CATALOG}.system.register_table(
  schema_name => '${LAYER}',
  table_name => '${TABLE_NAME}',
  table_location => '${TABLE_LOCATION}'
);
SHOW TABLES FROM ${CATALOG}.${LAYER};
"

run_trino_query() {
  local sql_query="$1"

  trino_container_id="$(docker ps -q --filter label=com.docker.compose.service=trino | head -n1)"
  if [[ -z "${trino_container_id}" ]]; then
    echo "Could not find Trino container via Docker labels." >&2
    exit 1
  fi

  docker exec -i "${trino_container_id}" trino --execute "${sql_query}"
}


run_trino_query "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${LAYER};" >/dev/null

set +e
register_output="$(run_trino_query "${TRINO_SQL}" 2>&1)"
register_exit_code=$?
set -e

echo "${register_output}"

if [[ ${register_exit_code} -ne 0 ]]; then
  if echo "${register_output}" | grep -qi "Table already exists"; then
    echo "Table ${CATALOG}.${LAYER}.${TABLE_NAME} already exists. Skipping registration."
    run_trino_query "SHOW TABLES FROM ${CATALOG}.${LAYER};"
    echo "Registration completed (no-op)."
    exit 0
  fi

  echo "Registration failed with unexpected error." >&2
  exit ${register_exit_code}
fi

echo "Registration completed."
