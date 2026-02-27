#!/usr/bin/env bash

set -euo pipefail

mkdir -p .ci
COVERAGE_MIN=70
export SPARK_VERSION="${SPARK_VERSION:-3.5}"
exit_code=0

run_step() {
  local step_name="$1"
  shift

  echo "${step_name}..."
  set +e
  "$@"
  local step_exit=$?
  set -e

  if [ "${step_exit}" -ne 0 ]; then
    echo "${step_name} failed with exit code ${step_exit}."
    exit_code=1
  fi
}

run_step "Running Ruff lint checks" poetry run ruff check --fix .

run_step "Running Ruff format" poetry run ruff format .

echo "Running pytest with coverage..."
set +e
poetry run coverage run --source=data_pipeline -m pytest tests -v -q \
  2>&1 | tee .ci/pytest_output.txt
test_exit_code=${PIPESTATUS[0]}
set -e
if [ "${test_exit_code}" -ne 0 ]; then
  echo "Pytest failed with exit code ${test_exit_code}."
  exit_code=1
fi

echo "Generating coverage reports..."
set +e
poetry run coverage report -m | tee .ci/coverage.txt
coverage_report_exit=${PIPESTATUS[0]}
poetry run coverage xml -o coverage.xml
coverage_xml_exit=$?
poetry run coverage json -o .ci/coverage.json
coverage_json_exit=$?
set -e
if [ "${coverage_report_exit}" -ne 0 ] || [ "${coverage_xml_exit}" -ne 0 ] || [ "${coverage_json_exit}" -ne 0 ]; then
  echo "Coverage report generation failed."
  exit_code=1
fi

echo "Validating minimum coverage gate (${COVERAGE_MIN}%)..."
set +e
MIN_COVERAGE="${COVERAGE_MIN}" python - <<'PY'
import json
import os
import sys

MIN_COVERAGE = float(os.environ["MIN_COVERAGE"])

with open(".ci/coverage.json", "r", encoding="utf-8") as file:
    data = json.load(file)

total = float(data["totals"]["percent_covered"])
print(f"Coverage total (data_pipeline): {total:.2f}%")
if total < MIN_COVERAGE:
    print(
        f"Coverage gate failed: {total:.2f}% < {MIN_COVERAGE:.2f}%",
        file=sys.stderr,
    )
    sys.exit(1)
PY
coverage_gate_exit=$?
set -e
if [ "${coverage_gate_exit}" -ne 0 ]; then
  exit_code=1
fi

if [ "${exit_code}" -eq 0 ]; then
  echo "CI checks completed successfully."
else
  echo "CI checks finished with failures."
fi

exit "${exit_code}"
