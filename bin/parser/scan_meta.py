#!/usr/bin/env python3

import sys
from datetime import datetime

from pymongo import MongoClient


def iso_utc_now():
    return datetime.utcnow().isoformat() + "Z"


def main():
    if len(sys.argv) < 4:
        print("Usage: scan_meta.py <scan_id> <target_id> start", file=sys.stderr)
        print("       scan_meta.py <scan_id> <target_id> finish <status> <subs_count> <http_count>", file=sys.stderr)
        sys.exit(2)

    scan_id = sys.argv[1]
    target_id = sys.argv[2]
    event = sys.argv[3].lower()

    client = MongoClient("mongodb://mongo:27017")
    db = client.asm
    scans = db.scans

    if event == "start":
        if len(sys.argv) != 4:
            print("Usage: scan_meta.py <scan_id> <target_id> start", file=sys.stderr)
            sys.exit(2)
        now = iso_utc_now()
        scans.update_one(
            {"_id": scan_id},
            {
                "$set": {
                    "target_id": target_id,
                    "started_at": now,
                    "status": "running",
                },
            },
            upsert=True,
        )
        print(f"scan_meta: start scan_id={scan_id} target_id={target_id} status=running")
        return

    if event == "finish":
        if len(sys.argv) != 7:
            print("Usage: scan_meta.py <scan_id> <target_id> finish <status> <subs_count> <http_count>", file=sys.stderr)
            sys.exit(2)
        status = sys.argv[4]
        try:
            subs_count = int(sys.argv[5])
            http_count = int(sys.argv[6])
        except ValueError:
            print("subs_count and http_count must be integers", file=sys.stderr)
            sys.exit(2)
        now = iso_utc_now()
        scans.update_one(
            {"_id": scan_id},
            {
                "$set": {
                    "finished_at": now,
                    "status": status,
                    "subs_count": subs_count,
                    "http_count": http_count,
                },
            },
            upsert=False,
        )
        print(f"scan_meta: finish scan_id={scan_id} status={status} subs_count={subs_count} http_count={http_count}")
        return

    print(f"Unknown event: {event} (use start or finish)", file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
