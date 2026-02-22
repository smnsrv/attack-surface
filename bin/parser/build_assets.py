#!/usr/bin/env python3

"""
Build inventory assets from raw scan results (subs + http).
Usage: build_assets.py <scan_id> <target_id>
"""

import sys
from datetime import datetime

from pymongo import MongoClient


def iso_utc_now():
    return datetime.utcnow().isoformat() + "Z"


def main():
    if len(sys.argv) < 3:
        print("Usage: build_assets.py <scan_id> <target_id>", file=sys.stderr)
        sys.exit(1)

    scan_id = sys.argv[1]
    target_id = sys.argv[2]
    now = iso_utc_now()

    client = MongoClient("mongodb://mongo:27017")
    db = client.asm

    # Ensure unique index for target_id + type + value
    db.assets.create_index(
        [("target_id", 1), ("type", 1), ("value", 1)],
        unique=True,
    )

    subs_count = 0
    http_count = 0

    # Process subs: upsert assets with type=subdomain
    for doc in db.subs.find({"scan_id": scan_id, "target_id": target_id}):
        fqdn = (doc.get("input") or "").strip().lower()
        if not fqdn:
            continue
        asset_id = f"{target_id}|subdomain|{fqdn}"
        db.assets.update_one(
            {"_id": asset_id},
            {
                "$set": {
                    "last_seen": now,
                    "last_scan_id": scan_id,
                },
                "$setOnInsert": {
                    "first_seen": now,
                    "target_id": target_id,
                    "type": "subdomain",
                    "value": fqdn,
                    "alive": False,
                },
            },
            upsert=True,
        )
        subs_count += 1

    # Process http: update assets with last_http and alive
    for doc in db.http.find({"scan_id": scan_id, "target_id": target_id}):
        fqdn = (doc.get("host") or doc.get("input") or "").strip().lower()
        if not fqdn:
            continue
        asset_id = f"{target_id}|subdomain|{fqdn}"
        alive = doc.get("failed") is False
        port = doc.get("port")
        if port is not None:
            try:
                port = int(port)
            except (TypeError, ValueError):
                port = None
        last_http = {
            "url": doc.get("url"),
            "status_code": doc.get("status_code"),
            "scheme": doc.get("scheme"),
            "port": port,
            "ip": doc.get("host_ip") or doc.get("ip"),
            "failed": doc.get("failed"),
            "timestamp": doc.get("timestamp"),
        }
        # Remove None values so we don't overwrite with null
        last_http = {k: v for k, v in last_http.items() if v is not None}

        db.assets.update_one(
            {"_id": asset_id},
            {
                "$set": {
                    "alive": alive,
                    "last_http": last_http,
                },
            },
        )
        http_count += 1

    print(f"Subs processed: {subs_count}")
    print(f"HTTP processed: {http_count}")


if __name__ == "__main__":
    main()
