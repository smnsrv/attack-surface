#!/usr/bin/env bash

# Simple smoke test for scope generation from Mongo targets.
# Usage (inside API container or with network access to it):
#   ./scripts/smoke_scope.sh <api_base_url> <target_id>
# Example:
#   ./scripts/smoke_scope.sh http://localhost:8080 acme-prod

set -euo pipefail

API_BASE_URL="${1:-http://localhost:8080}"
TARGET_ID="${2:-smoke-target}"

echo "[*] Creating/upserting target '${TARGET_ID}' via /mvp/targets ..."

curl -sS -X POST "${API_BASE_URL}/mvp/targets" \
  -H "Content-Type: application/json" \
  -d "{
    \"id\": \"${TARGET_ID}\",
    \"name\": \"Smoke Test Target\",
    \"domains\": [\"example.com\", \"WWW.EXAMPLE.COM\"],
    \"enabled\": true
  }" || true

echo
echo "[*] Launching scan via /api/${TARGET_ID}/launch_scan ..."

curl -sS "${API_BASE_URL}/api/${TARGET_ID}/launch_scan" || true

echo
echo "[*] Checking generated scope file at /app/scope/${TARGET_ID} ..."

if [ -f "/app/scope/${TARGET_ID}" ]; then
  echo "[+] Scope file exists:"
  cat "/app/scope/${TARGET_ID}"
else
  echo "[-] Scope file /app/scope/${TARGET_ID} not found."
  exit 1
fi

echo "[*] Smoke test completed."

