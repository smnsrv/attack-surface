#!/usr/bin/env python3

import sys
from datetime import datetime

from pymongo import MongoClient


def iso_utc_now():
    return datetime.utcnow().isoformat() + "Z"


def main():
    if len(sys.argv) < 4:
        print("Usage: scan_meta.py <scan_id> <target_id> <event> [status] [subs_count] [http_count]")
        print("  event: start | finish")
        print("  status, subs_count, http_count: optional, for event=finish")
        sys.exit(1)

    scan_id = sys.argv[1]
    target_id = sys.argv[2]
    event = sys.argv[3].lower()
    status = sys.argv[4] if len(sys.argv) > 4 else None
    subs_count = int(sys.argv[5]) if len(sys.argv) > 5 else None
    http_count = int(sys.argv[6]) if len(sys.argv) > 6 else None

    client = MongoClient("mongodb://mongo:27017")
    db = client.asm
    scans = db.scans  # collection created on first write

    if event == "start":
        scans.update_one(
            {"_id": scan_id},
            {
                "$set": {
                    "target_id": target_id,
                    "started_at": iso_utc_now(),
                    "status": "running",
                },
            },
            upsert=True,
        )
        return

    if event == "finish":
        update = {
            "finished_at": iso_utc_now(),
        }
        if status is not None:
            update["status"] = status
        if subs_count is not None:
            update["subs_count"] = subs_count
        if http_count is not None:
            update["http_count"] = http_count
        scans.update_one(
            {"_id": scan_id},
            {"$set": update},
            upsert=False,
        )
        return

    print("Unknown event: %s (use start or finish)" % event, file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
