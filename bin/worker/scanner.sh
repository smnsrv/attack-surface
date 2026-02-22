#!/usr/bin/env bash

set -euo pipefail

echo "Scanning job: $1"

# 1) Parse job "<target>:<spinup>:<module>"
job="$1"
target_id="$(echo "$job" | cut -d: -f 1)"
instances="$(echo "$job" | cut -d: -f 2)"  # currently unused in local MVP
module="$(echo "$job" | cut -d: -f 3)"     # currently unused in local MVP

ppath="/app"
scan_id="${target_id}-$(date +%s)"
scan_path="${ppath}/scans/${scan_id}"
scope_src="${ppath}/scope/${target_id}"
log_file="${scan_path}/scanner.log"

# Scan metadata: record start (failure of subfinder or subs import will call finish failed with counts)
python3 /app/bin/parser/scan_meta.py "$scan_id" "$target_id" start
trap 'subs_count=0; http_count=0; [ -f "$scan_path/subs.json" ] && subs_count=$(wc -l < "$scan_path/subs.json" 2>/dev/null || echo 0); [ -f "$scan_path/http.json" ] && http_count=$(wc -l < "$scan_path/http.json" 2>/dev/null || echo 0); python3 /app/bin/parser/scan_meta.py "$scan_id" "$target_id" finish failed "$subs_count" "$http_count"; exit 1' ERR

mkdir -p "$scan_path"

# Log everything to scanner.log as well as stdout
exec > >(tee -a "$log_file") 2>&1

echo "[+] Starting LOCAL MVP scan"
echo "    target_id = ${target_id}"
echo "    scan_id   = ${scan_id}"
echo "    scan_path = ${scan_path}"

cd "$scan_path"

# 3) Copy scope file into scan_path/scope.txt
if [ ! -f "$scope_src" ]; then
  echo "[!] Scope file not found at ${scope_src}"
  exit 1
fi

cp "$scope_src" "$scan_path/scope.txt"
echo "[+] Copied scope to ${scan_path}/scope.txt"

# 4) Run subfinder against scope file
echo "[+] Running subfinder ..."
subfinder -silent -dL "$scan_path/scope.txt" > "$scan_path/subs.txt"
echo "[+] subfinder completed. Output: ${scan_path}/subs.txt"

# 5) Produce minimal JSONL subs file
echo "[+] Generating subs.json ..."
subs_json="$scan_path/subs.json"
: > "$subs_json"
while IFS= read -r fqdn; do
  if [ -n "$fqdn" ]; then
    printf '{"input":"%s"}\n' "$fqdn" >> "$subs_json"
  fi
done < "$scan_path/subs.txt"
echo "[+] Generated ${subs_json}"

# 6) Run httpx over discovered hosts
echo "[+] Running httpx ..."
cat "$scan_path/subs.txt" | httpx -silent -json > "$scan_path/http.json"
echo "[+] httpx completed. Output: ${scan_path}/http.json"

# Counts for scan metadata (after subs.json and http.json exist)
subs_count=$(wc -l < "$scan_path/subs.json" 2>/dev/null || echo 0)
http_count=$(wc -l < "$scan_path/http.json" 2>/dev/null || echo 0)

# 7) Import results into Mongo
echo "[+] Importing results into Mongo ..."
python3 /app/bin/parser/import.py "$scan_path/subs.json" "$scan_id" "$target_id"
python3 /app/bin/parser/import.py "$scan_path/http.json" "$scan_id" "$target_id" || echo "[!] http import failed (best-effort)"
echo "[+] Import complete."

# After successful imports: build assets; on failure mark scan failed and exit non-zero
echo "[+] Building assets ..."
python3 /app/bin/parser/build_assets.py "$scan_id" "$target_id" || {
  python3 /app/bin/parser/scan_meta.py "$scan_id" "$target_id" finish failed "$subs_count" "$http_count"
  exit 1
}
echo "[+] Assets built."

# Scan metadata: record finish success
python3 /app/bin/parser/scan_meta.py "$scan_id" "$target_id" finish success "$subs_count" "$http_count"

echo "[+] Scan completed. subs.json: ${subs_count} lines, http.json: ${http_count} lines"
