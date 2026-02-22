#!/usr/bin/env python3

"""
Build inventory assets from raw collections subs/http.
Emit asset_events for change detection and update scan with event counters.
Usage: build_assets.py --scan-id <string> --target-id <string> --org-id <string>
"""

import argparse
import sys
from datetime import datetime

from pymongo import MongoClient
from pymongo import ReturnDocument


def iso_utc_now():
    return datetime.utcnow().isoformat() + "Z"


def _ensure_index(collection, keys, name):
    """Create index only if one with the same key pattern does not already exist (idempotent)."""
    want_dict = dict(keys) if isinstance(keys, list) else dict(list(keys))
    for idx in collection.list_indexes():
        if idx.get("key") == want_dict:
            return
    collection.create_index(keys, name=name)


def ensure_asset_events_indexes(db):
    """Ensure asset_events indexes exist; compare key pattern to avoid IndexOptionsConflict."""
    events = db.asset_events
    _ensure_index(events, [("organization_id", 1), ("created_at", -1)], "org_created_at_-1")
    _ensure_index(events, [("organization_id", 1), ("scan_id", 1)], "org_scan_id_1")
    _ensure_index(events, [("organization_id", 1), ("type", 1), ("created_at", -1)], "org_type_created_at_-1")


def _asset_snapshot(doc):
    """Minimal snapshot for event old/new (alive, last_http.status_code, risk_level)."""
    if not doc:
        return None
    lh = doc.get("last_http") or {}
    return {
        "alive": doc.get("alive"),
        "status_code": lh.get("status_code"),
        "risk_level": doc.get("risk_level"),
    }


def _detect_event_type(previous_snapshot, new_snapshot):
    """
    Determine event type: new | dead | status_changed | risk_changed.
    First match wins. Snapshots have keys: alive, status_code, risk_level.
    """
    if previous_snapshot is None:
        return "new"
    prev_alive = previous_snapshot.get("alive")
    new_alive = new_snapshot.get("alive")
    if prev_alive is True and new_alive is False:
        return "dead"
    if previous_snapshot.get("status_code") != new_snapshot.get("status_code"):
        return "status_changed"
    if previous_snapshot.get("risk_level") is not None or new_snapshot.get("risk_level") is not None:
        if previous_snapshot.get("risk_level") != new_snapshot.get("risk_level"):
            return "risk_changed"
    return None


def main():
    parser = argparse.ArgumentParser(description="Build assets from subs/http and emit asset_events.")
    parser.add_argument("--scan-id", required=True, help="Scan id (required)")
    parser.add_argument("--target-id", required=True, help="Target id (required)")
    parser.add_argument("--org-id", required=True, help="Organization id slug, e.g. default (required)")
    args = parser.parse_args()
    scan_id = (args.scan_id or "").strip()
    target_id = (args.target_id or "").strip()
    org_id = (args.org_id or "").strip()
    if not scan_id:
        print("build_assets: --scan-id is required", file=sys.stderr)
        sys.exit(1)
    if not target_id:
        print("build_assets: --target-id is required", file=sys.stderr)
        sys.exit(1)
    if not org_id:
        print("build_assets: --org-id is required", file=sys.stderr)
        sys.exit(1)

    now_iso = datetime.utcnow().isoformat() + "Z"

    client = MongoClient("mongodb://mongo:27017")
    db = client.asm

    ensure_asset_events_indexes(db)

    db.assets.create_index(
        [("target_id", 1), ("type", 1), ("value", 1)],
        unique=True,
    )

    events_coll = db.asset_events
    new_assets = 0
    dead_assets = 0
    status_changed = 0
    high_risk_new = 0
    events_inserted = 0

    def write_event(asset_type, asset_value, event_type, old_subset, new_subset):
        nonlocal new_assets, dead_assets, status_changed, high_risk_new, events_inserted
        event_doc = {
            "organization_id": org_id,
            "scan_id": scan_id,
            "target_id": target_id,
            "type": event_type,
            "asset": {"type": asset_type, "value": asset_value},
            "old": old_subset,
            "new": new_subset,
            "created_at": now_iso,
        }
        events_coll.insert_one(event_doc)
        events_inserted += 1
        if event_type == "new":
            new_assets += 1
            if (new_subset or {}).get("risk_level") == "high":
                high_risk_new += 1
        elif event_type == "dead":
            dead_assets += 1
        elif event_type == "status_changed":
            status_changed += 1

    subs_processed = 0
    http_processed = 0

    # 1) Subs: find previous BEFORE update, upsert, compare and emit events
    asset_type = "subdomain"
    for doc in db.subs.find({"scan_id": scan_id, "target_id": target_id}):
        fqdn = (doc.get("input") or "").strip().lower()
        if not fqdn:
            continue
        previous = db.assets.find_one({"organization_id": org_id, "type": asset_type, "value": fqdn})
        # state is immutable during scans; only set on insert. State fields live only in $setOnInsert to avoid path conflict (Mongo 40).
        res = db.assets.find_one_and_update(
            {"target_id": target_id, "type": asset_type, "value": fqdn},
            {
                "$set": {
                    "last_seen": now_iso,
                    "last_scan_id": scan_id,
                },
                "$setOnInsert": {
                    "first_seen": now_iso,
                    "target_id": target_id,
                    "type": asset_type,
                    "value": fqdn,
                    "alive": False,
                    "organization_id": org_id,
                    "state": "discovered",
                    "state_changed_at": now_iso,
                    "state_changed_by": "system",
                    "notes": "",
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if res:
            new_snapshot = _asset_snapshot(res)
            event_type = _detect_event_type(_asset_snapshot(previous) if previous else None, new_snapshot)
            if event_type:
                write_event(asset_type, fqdn, event_type, _asset_snapshot(previous) if previous else None, new_snapshot)
        subs_processed += 1

    # 2) Http: find previous BEFORE update, upsert, compare and emit events
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
        alive = doc.get("failed") is False
        previous = db.assets.find_one({"organization_id": org_id, "type": asset_type, "value": fqdn})
        # state is immutable during scans; only set on insert. State fields live only in $setOnInsert to avoid path conflict (Mongo 40).
        res = db.assets.find_one_and_update(
            {"target_id": target_id, "type": asset_type, "value": fqdn},
            {
                "$set": {
                    "alive": alive,
                    "last_http": last_http,
                },
                "$setOnInsert": {
                    "first_seen": now_iso,
                    "last_seen": now_iso,
                    "last_scan_id": scan_id,
                    "target_id": target_id,
                    "type": asset_type,
                    "value": fqdn,
                    "organization_id": org_id,
                    "state": "discovered",
                    "state_changed_at": now_iso,
                    "state_changed_by": "system",
                    "notes": "",
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if res:
            new_snapshot = _asset_snapshot(res)
            event_type = _detect_event_type(_asset_snapshot(previous) if previous else None, new_snapshot)
            if event_type:
                write_event(asset_type, fqdn, event_type, _asset_snapshot(previous) if previous else None, new_snapshot)
        http_processed += 1

    # Store event counters on scan document (strict: scan must exist)
    scan_result = db.scans.update_one(
        {"_id": scan_id, "organization_id": org_id},
        {
            "$set": {
                "new_assets": new_assets,
                "dead_assets": dead_assets,
                "status_changed": status_changed,
                "high_risk_new": high_risk_new,
            },
        },
    )
    if scan_result.matched_count == 0:
        print(f"build_assets: scan_id '{scan_id}' not found or org mismatch; cannot update counters", file=sys.stderr)
        sys.exit(1)

    assets_total_for_target = db.assets.count_documents({"target_id": target_id})
    print(
        f"subs_processed={subs_processed} http_processed={http_processed} assets_total_for_target={assets_total_for_target} "
        f"new_assets={new_assets} dead_assets={dead_assets} status_changed={status_changed} high_risk_new={high_risk_new}"
    )
    print(
        f"[+] asset_events inserted: {events_inserted} (new={new_assets} dead={dead_assets} status_changed={status_changed} high_risk_new={high_risk_new})"
    )


if __name__ == "__main__":
    main()
