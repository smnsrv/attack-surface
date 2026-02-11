#!/usr/bin/env python3

import sys
from datetime import datetime
from pymongo import MongoClient


def iso_utc_now():
    return datetime.utcnow().isoformat() + "Z"


def main():
    if len(sys.argv) < 4:
        print("Usage: scan_meta.py <scan_id> <target_id> <action>")
        sys.exit(1)

    scan_id = sys.argv[1]
    target_id = sys.argv[2]
    action = sys.argv[3]

    client = MongoClient("mongodb://mongo:27017")
    db = client.asm
    scans = db.scans

    if action == "start":
        # Insert or update metadata when scan starts
        scans.update_one(
            {"scan_id": scan_id, "target_id": target_id},
            {
                "$setOnInsert": {
                    "scan_id": scan_id,
                    "target_id": target_id,
                },
                "$set": {
                    "started_at": iso_utc_now(),
                    "status": "running",
                },
            },
            upsert=True,
        )
        return

    # For completion / failure, compute basic stats and mark finished
    finished_at = iso_utc_now()

    subs_count = db.subs.count_documents({"scan_id": scan_id, "target_id": target_id})
    http_count = db.http.count_documents({"scan_id": scan_id, "target_id": target_id})

    status = "success" if action == "success" else "failed"

    scans.update_one(
        {"scan_id": scan_id, "target_id": target_id},
        {
            "$setOnInsert": {
                "scan_id": scan_id,
                "target_id": target_id,
            },
            "$set": {
                "finished_at": finished_at,
                "status": status,
                "subs_count": subs_count,
                "http_count": http_count,
            },
        },
        upsert=True,
    )


if __name__ == "__main__":
    main()

