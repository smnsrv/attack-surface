#!/usr/bin/env python3

"""
Build inventory assets from raw collections subs/http.
Emit asset_events for change detection and update scan with event counters.
Usage: build_assets.py <scan_id> <target_id>
"""

import sys
from datetime import datetime

from pymongo import MongoClient
from pymongo import ReturnDocument


def iso_utc_now():
    return datetime.utcnow().isoformat() + "Z"


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
    if previous_snapshot.get("risk_level") != new_snapshot.get("risk_level"):
        return "risk_changed"
    return None


def main():
    if len(sys.argv) < 3:
        print("Usage: build_assets.py <scan_id> <target_id>", file=sys.stderr)
        sys.exit(1)

    scan_id = (sys.argv[1] or "").strip()
    target_id = sys.argv[2]
    if not scan_id:
        print("build_assets: scan_id is required; cannot write partial events", file=sys.stderr)
        sys.exit(1)

    now_dt = datetime.utcnow()
    now_iso = now_dt.isoformat() + "Z"

    client = MongoClient("mongodb://mongo:27017")
    db = client.asm

    target_doc = db.targets.find_one({"_id": target_id})
    if not target_doc:
        print(f"build_assets: target '{target_id}' not found", file=sys.stderr)
        sys.exit(1)
    organization_id = target_doc.get("organization_id")
    if not organization_id:
        print(f"build_assets: target '{target_id}' has no organization_id; org isolation required", file=sys.stderr)
        sys.exit(1)

    db.assets.create_index(
        [("target_id", 1), ("type", 1), ("value", 1)],
        unique=True,
    )

    # Cache previous assets by (type, value) for this target (single query)
    prev_assets = {}
    for a in db.assets.find({"organization_id": organization_id, "target_id": target_id}):
        key = (a.get("type"), a.get("value"))
        if key:
            prev_assets[key] = a

    events_coll = db.asset_events
    new_assets = 0
    dead_assets = 0
    status_changed = 0
    high_risk_new = 0
    events_inserted = 0

    def write_event(asset_type, asset_value, event_type, old_subset, new_subset):
        nonlocal new_assets, dead_assets, status_changed, high_risk_new, events_inserted
        event_doc = {
            "organization_id": organization_id,
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

    # 1) Subs: upsert assets; detect "new"; write events
    for doc in db.subs.find({"scan_id": scan_id, "target_id": target_id}):
        fqdn = (doc.get("input") or "").strip().lower()
        if not fqdn:
            continue
        key = ("subdomain", fqdn)
        previous = prev_assets.get(key)
        new_snapshot = {"alive": False, "status_code": None, "risk_level": (previous or {}).get("risk_level")}
        event_type = _detect_event_type(_asset_snapshot(previous) if previous else None, new_snapshot)

        res = db.assets.find_one_and_update(
            {"target_id": target_id, "type": "subdomain", "value": fqdn},
            {
                "$set": {
                    "last_seen": now_iso,
                    "last_scan_id": scan_id,
                    "organization_id": organization_id,
                },
                "$setOnInsert": {
                    "first_seen": now_iso,
                    "target_id": target_id,
                    "type": "subdomain",
                    "value": fqdn,
                    "alive": False,
                    "organization_id": organization_id,
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if res:
            prev_assets[key] = res
        if event_type and res:
            write_event("subdomain", fqdn, event_type, _asset_snapshot(previous) if previous else None, new_snapshot)
        subs_processed += 1

    # 2) Http: update assets with last_http and alive; detect dead/status_changed/risk_changed
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
        key = ("subdomain", fqdn)
        previous = prev_assets.get(key)
        new_snapshot = {
            "alive": alive,
            "status_code": last_http.get("status_code"),
            "risk_level": (previous or {}).get("risk_level"),
        }
        event_type = _detect_event_type(_asset_snapshot(previous) if previous else None, new_snapshot)

        res = db.assets.find_one_and_update(
            {"target_id": target_id, "type": "subdomain", "value": fqdn},
            {
                "$set": {
                    "alive": alive,
                    "last_http": last_http,
                    "organization_id": organization_id,
                },
                "$setOnInsert": {
                    "first_seen": now_iso,
                    "last_seen": now_iso,
                    "last_scan_id": scan_id,
                    "target_id": target_id,
                    "type": "subdomain",
                    "value": fqdn,
                    "organization_id": organization_id,
                },
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if res:
            prev_assets[key] = res
        if event_type and res:
            old_snap = _asset_snapshot(previous) if previous else None
            write_event("subdomain", fqdn, event_type, old_snap, new_snapshot)
        http_processed += 1

    # Store event counters on scan document (strict: scan must exist)
    scan_result = db.scans.update_one(
        {"_id": scan_id, "organization_id": organization_id},
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
    print(f"[DEBUG] asset_events: {events_inserted} events inserted for scan_id={scan_id}", file=sys.stderr)


if __name__ == "__main__":
    main()
