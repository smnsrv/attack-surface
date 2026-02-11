#!/usr/bin/env bash

# Simple smoke test for scan metadata tracking in Mongo `scans`.
#
# Usage:
#   ./scripts/test_scans_metadata.sh [API_BASE_URL] [TARGET_ID]
# Examples:
#   ./scripts/test_scans_metadata.sh
#   ./scripts/test_scans_metadata.sh http://localhost smoke-meta
#
# Assumptions:
#   - Docker Compose project name is `asm-mvp` (default when running from this folder),
#   - Mongo container is named `asm-mvp-mongo-1`,
#   - `mongosh` is available in the Mongo container image.

set -euo pipefail

API_BASE_URL="${1:-http://localhost}"
TARGET_ID="${2:-scan-meta-test}"

echo "[*] Creating target '${TARGET_ID}' via /mvp/targets ..."

curl -sS -X POST "${API_BASE_URL}/mvp/targets" \
  -H "Content-Type: application/json" \
  -d "{
    \"id\": \"${TARGET_ID}\",
    \"name\": \"Scan Metadata Test Target\",
    \"domains\": [\"example.com\"],
    \"enabled\": true
  }" || true

echo
echo "[*] Launching scan via /api/${TARGET_ID}/launch_scan ..."

curl -sS "${API_BASE_URL}/api/${TARGET_ID}/launch_scan" || true

echo
echo "[*] Waiting a bit for worker to process scan (this may still fail if Axiom is not configured) ..."
sleep 20

echo "[*] Querying Mongo 'scans' collection for target_id='${TARGET_ID}' ..."

docker exec asm-mvp-mongo-1 mongosh --quiet --eval "
db = db.getSiblingDB('asm');
const docs = db.scans.find({ target_id: '${TARGET_ID}' }).toArray();
printjson(docs);
" || echo '[-] Failed to query Mongo scans collection (container or mongosh missing).'

echo "[*] If metadata tracking works, you should see documents with fields like:"
echo "    { scan_id, target_id, started_at, finished_at?, status, subs_count?, http_count? }"

