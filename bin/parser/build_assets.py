#!/usr/bin/env python3

"""
Build inventory assets from raw collections subs/http.
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

    db.assets.create_index(
        [("target_id", 1), ("type", 1), ("value", 1)],
        unique=True,
    )

    subs_processed = 0
    http_processed = 0

    # 1) Subs: upsert assets by target_id, type, value
    for doc in db.subs.find({"scan_id": scan_id, "target_id": target_id}):
        fqdn = (doc.get("input") or "").strip().lower()
        if not fqdn:
            continue
        db.assets.update_one(
            {"target_id": target_id, "type": "subdomain", "value": fqdn},
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
        subs_processed += 1

    # 2) Http: update assets with last_http and alive (upsert if missing)
    for doc in db.http.find({"scan_id": scan_id, "target_id": target_id}):
        fqdn = (doc.get("host") or doc.get("input") or "").strip().lower()
        if not fqdn:
            continue
        port = doc.get("port")
        port = int(port) if port is not None else None
        last_http = {
            "url": doc.get("url"),
            "status_code": doc.get("status_code"),
            "scheme": doc.get("scheme"),
            "port": port,
            "ip": doc.get("host_ip"),
            "failed": doc.get("failed"),
            "timestamp": doc.get("timestamp"),
        }
        db.assets.update_one(
            {"target_id": target_id, "type": "subdomain", "value": fqdn},
            {
                "$set": {
                    "alive": (doc.get("failed") is False),
                    "last_http": last_http,
                },
                "$setOnInsert": {
                    "first_seen": now,
                    "last_seen": now,
                    "last_scan_id": scan_id,
                    "target_id": target_id,
                    "type": "subdomain",
                    "value": fqdn,
                },
            },
            upsert=True,
        )
        http_processed += 1

    assets_total_for_target = db.assets.count_documents({"target_id": target_id})

    print(f"subs_processed={subs_processed} http_processed={http_processed} assets_total_for_target={assets_total_for_target}")


if __name__ == "__main__":
    main()
