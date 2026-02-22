#!/usr/bin/env python3

"""
Port discovery after build_assets. Uses naabu; IPs from alive assets (last_http.ip).
Upserts results into services collection (org+ip+port). Subprocess watchdog timeout prevents hangs.
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from pymongo import MongoClient

# Watchdog timeout per IP (safety net; no nmap --host-timeout)
TIMEOUT_FULL_SCAN_PER_IP = 3600
TIMEOUT_TOP1000_PER_IP = 1200

NAABU_RATE = int(os.environ.get("NAABU_RATE", "500"))
NAABU_C = int(os.environ.get("NAABU_C", "50"))


def get_target_config(target_id, org_id):
    client = MongoClient("mongodb://mongo:27017")
    doc = client.asm.targets.find_one({"_id": target_id, "organization_id": org_id})
    if not doc:
        return None
    return {
        "port_scan_mode": doc.get("port_scan_mode", "top1000"),
        "full_scan_day": int(doc.get("full_scan_day", 6)),
    }


def weekday_today():
    """0 = Monday, 6 = Sunday (Python weekday())."""
    return datetime.utcnow().weekday()


def should_deep_scan(port_scan_mode, full_scan_day):
    """top1000 = daily (top-ports); full = weekly (all ports on full_scan_day)."""
    if port_scan_mode == "full":
        return True
    if port_scan_mode == "top1000":
        return weekday_today() == full_scan_day
    return False


def unique_ips_from_alive_assets(org_id, target_id):
    """
    Collect unique IPs from alive assets with last_http.ip.
    Returns (sorted_ip_list, ip_to_asset_value_map).
    """
    client = MongoClient("mongodb://mongo:27017")
    cursor = client.asm.assets.find(
        {
            "organization_id": org_id,
            "target_id": target_id,
            "alive": True,
            "last_http.ip": {"$exists": True, "$ne": None},
        },
        {"value": 1, "last_http.ip": 1},
    )
    ips = set()
    ip_to_asset = {}
    for doc in cursor:
        ip = (doc.get("last_http") or {}).get("ip")
        if not ip or not isinstance(ip, str):
            continue
        ip = ip.strip()
        if ip:
            ips.add(ip)
            if ip not in ip_to_asset:
                ip_to_asset[ip] = (doc.get("value") or "").strip() or None
    return sorted(ips), ip_to_asset


def run_cmd(args, timeout):
    try:
        r = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return r.returncode == 0, r.stdout, r.stderr or ""
    except subprocess.TimeoutExpired as e:
        raise e
    except (FileNotFoundError, OSError):
        return False, "", ""


def parse_ip_port_lines(stdout):
    """Parse naabu output lines 'ip:port'. Returns list of (ip, port_str)."""
    out = []
    for line in (stdout or "").strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if ":" in line:
            parts = line.rsplit(":", 1)
            if len(parts) == 2 and parts[1].isdigit():
                out.append((parts[0].strip(), parts[1]))
    return out


def ensure_services_index(db):
    """Unique index on (organization_id, ip, port) for upserts."""
    coll = db.services
    key = [("organization_id", 1), ("ip", 1), ("port", 1)]
    for idx in coll.list_indexes():
        if idx.get("key") == dict(key):
            return
    coll.create_index(key, name="org_ip_port_1", unique=True)


def scan_ip(ip, deep_scan, discovery_timeout, rate, concurrency):
    """
    Run naabu: deep -> -p-; else -top-ports 1000. -silent, -rate, -c.
    Returns (list of (ip, port_str), success). Raises TimeoutExpired on timeout.
    """
    base = ["naabu", "-host", ip, "-silent", "-rate", str(rate), "-c", str(concurrency)]
    if deep_scan:
        args = base + ["-p", "0-65535"]
    else:
        args = base + ["-top-ports", "1000"]

    ok, out, _ = run_cmd(args, timeout=discovery_timeout)
    if not ok:
        return [], False
    return parse_ip_port_lines(out), True


def upsert_services(db, org_id, target_id, scan_id, now_iso, ip_port_list, ip_to_asset):
    """Upsert into services (org+ip+port); update last_seen/last_scan_id/target_id/asset_value; first_seen on insert."""
    coll = db.services
    for ip, port in ip_port_list:
        asset_value = ip_to_asset.get(ip) or ""
        coll.update_one(
            {"organization_id": org_id, "ip": ip, "port": port},
            {
                "$set": {
                    "last_seen": now_iso,
                    "last_scan_id": scan_id,
                    "target_id": target_id,
                    "asset_value": asset_value,
                },
                "$setOnInsert": {
                    "organization_id": org_id,
                    "ip": ip,
                    "port": port,
                    "first_seen": now_iso,
                },
            },
            upsert=True,
        )


def main():
    parser = argparse.ArgumentParser(description="Port discovery with naabu; IPs from alive assets; upsert services.")
    parser.add_argument("--scan-id", required=True, help="Scan id")
    parser.add_argument("--target-id", required=True, help="Target id")
    parser.add_argument("--org-id", default="default", help="Organization id for target lookup")
    parser.add_argument("--scan-path", required=True, help="Path to scan dir (unused; kept for CLI compatibility)")
    args = parser.parse_args()

    target_id = args.target_id.strip()
    scan_id = args.scan_id.strip()
    org_id = (args.org_id or "").strip() or "default"

    # Check naabu availability before any subprocess call to avoid hanging
    if shutil.which("naabu") is None:
        print("[WARN] naabu not installed, skipping port scan", file=sys.stderr)
        client = MongoClient("mongodb://mongo:27017")
        client.asm.scans.update_one(
            {"_id": scan_id},
            {"$set": {"port_scan_status": "failed", "port_scan_error": "naabu_not_installed"}},
        )
        sys.exit(0)

    client = MongoClient("mongodb://mongo:27017")
    db = client.asm

    def update_scan_port_stats(ports_scanned_total, open_ports_total, deep_scan_used, port_scan_status, port_scan_error=None):
        setters = {
            "port_scan_status": port_scan_status,
            "ports_scanned_total": ports_scanned_total,
            "open_ports_total": open_ports_total,
            "deep_scan_used": bool(deep_scan_used),
        }
        if port_scan_error is not None:
            setters["port_scan_error"] = port_scan_error
        db.scans.update_one({"_id": scan_id}, {"$set": setters})

    config = get_target_config(target_id, org_id)
    if not config:
        print("[!] port_scan: target not found, skipping", file=sys.stderr)
        update_scan_port_stats(0, 0, False, "failed", "target not found")
        sys.exit(0)

    port_scan_mode = config["port_scan_mode"]
    full_scan_day = config["full_scan_day"]
    weekday = weekday_today()
    deep_scan = should_deep_scan(port_scan_mode, full_scan_day)
    mode_str = "full" if deep_scan else "top1000"
    discovery_timeout = TIMEOUT_FULL_SCAN_PER_IP if deep_scan else TIMEOUT_TOP1000_PER_IP

    ips, ip_to_asset = unique_ips_from_alive_assets(org_id, target_id)
    if not ips:
        print("[+] open_ports_found=0 (no alive assets with last_http.ip)")
        ensure_services_index(db)
        update_scan_port_stats(0, 0, deep_scan, "ok")
        sys.exit(0)

    N = len(ips)
    first_5 = ips[:5]
    print(f"[+] port_scan_mode={port_scan_mode} full_scan_day={full_scan_day}")
    print(f"[+] weekday={weekday} deep_scan_triggered={str(deep_scan).lower()}")
    print(f"[+] unique_ip_count={N} first_5_ips={first_5}")

    ensure_services_index(db)
    now_iso = datetime.utcnow().isoformat() + "Z"

    total_open = 0
    ips_ok = 0
    ips_failed = 0
    port_scan_error = None
    all_ip_ports = []

    for i, ip in enumerate(ips, 1):
        print(f"[+] naabu start ip {i}/{N}: {ip} mode={mode_str}")
        t0 = time.monotonic()
        try:
            ip_ports, ok = scan_ip(ip, deep_scan, discovery_timeout, NAABU_RATE, NAABU_C)
            duration_s = round(time.monotonic() - t0, 1)
            if ok:
                all_ip_ports.extend(ip_ports)
                total_open += len(ip_ports)
                ips_ok += 1
                print(f"[+] naabu done ip {i}/{N}: {ip} duration={duration_s}s open_ports={len(ip_ports)}")
            else:
                ips_failed += 1
                port_scan_error = f"naabu failed for ip={ip} duration_s={duration_s}"
                print(f"[WARN] {port_scan_error}", file=sys.stderr)
        except subprocess.TimeoutExpired as e:
            duration_s = round(time.monotonic() - t0, 1)
            ips_failed += 1
            port_scan_error = f"timeout for ip={ip} duration_s={duration_s}: {e}"
            print(f"[WARN] port_scan {port_scan_error}", file=sys.stderr)
        except Exception as e:
            duration_s = round(time.monotonic() - t0, 1)
            ips_failed += 1
            port_scan_error = f"failed ip={ip}: {e}"
            print(f"[WARN] port_scan {port_scan_error}", file=sys.stderr)

    upsert_services(db, org_id, target_id, scan_id, now_iso, all_ip_ports, ip_to_asset)

    ports_per_ip = 65536 if deep_scan else 1000
    ports_scanned_total = (ips_ok + ips_failed) * ports_per_ip
    if ips_failed == 0:
        port_scan_status = "ok"
    elif ips_ok == 0:
        port_scan_status = "failed"
    else:
        port_scan_status = "partial"
    update_scan_port_stats(ports_scanned_total, total_open, deep_scan, port_scan_status, port_scan_error)

    scanned_ips = ips_ok + ips_failed
    print(f"[+] port_scan summary: scanned_ips={scanned_ips} open_ports_total={total_open}")
    sys.exit(0)


if __name__ == "__main__":
    main()
