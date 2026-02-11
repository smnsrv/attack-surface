#!/bin/bash

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

mkdir -p "$scan_path"

# Log everything to scanner.log as well as stdout
exec > >(tee -a "$log_file") 2>&1

echo "[+] Starting LOCAL MVP scan"
echo "    target_id = ${target_id}"
echo "    scan_id   = ${scan_id}"
echo "    scan_path = ${scan_path}"

# Fail handler: mark scan as failed in Mongo
trap 'python3 "'"$ppath"'"/bin/parser/scan_meta.py "'"$scan_id"'" "'"$target_id"'" "failed"' ERR

# Record scan start in Mongo
python3 "$ppath/bin/parser/scan_meta.py" "$scan_id" "$target_id" "start"

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

# 7) Import results into Mongo
echo "[+] Importing results into Mongo ..."
python3 "$ppath/bin/parser/import.py" "$scan_path/subs.json" "$scan_id" "$target_id"
python3 "$ppath/bin/parser/import.py" "$scan_path/http.json" "$scan_id" "$target_id"
echo "[+] Import complete."

# 8 & 9) Mark scan success and rely on scan_meta for metadata
python3 "$ppath/bin/parser/scan_meta.py" "$scan_id" "$target_id" "success"
echo "[+] Scan completed successfully."
