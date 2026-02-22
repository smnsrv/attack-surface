#!/usr/bin/env python3

from pymongo import MongoClient
import redis
from flask import Flask
from flask import request
from flask import jsonify
from flask import redirect
from flask import render_template
from flask import session
from flask import url_for
from datetime import datetime
import os

import click

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-secret-change-in-production")
r = redis.Redis(host='redis', port=6379, db=0)
client = MongoClient("mongodb://mongo:27017")
db = client.asm

def _ensure_index(collection, coll_name, keys, name, **kwargs):
    """Create index only if one with the same key pattern does not already exist."""
    want_key = list(keys) if isinstance(keys, (list, tuple)) else [(k, v) for k, v in keys.items()]
    want_dict = dict(want_key)
    for idx in collection.list_indexes():
        if idx.get("key") == want_dict:
            print(f"[+] index exists: {coll_name} {name}")
            return
    collection.create_index(keys, name=name, **kwargs)
    print(f"[+] created: {coll_name} {name}")


# Ensure organizations collection has indexes (slug unique, is_active)
def ensure_organizations_indexes():
    """Create indexes on organizations collection if not present (idempotent)."""
    orgs = db.organizations
    _ensure_index(orgs, "organizations", [("slug", 1)], "slug_1", unique=True)
    _ensure_index(orgs, "organizations", [("is_active", 1)], "is_active_1")


def ensure_org_isolation_indexes():
    """Create MongoDB indexes for multi-organization isolation; idempotent (skip if key exists)."""
    # Targets
    targets = db.targets
    _ensure_index(targets, "targets", [("organization_id", 1)], "org_1")
    _ensure_index(targets, "targets", [("organization_id", 1), ("domain", 1)], "org_domain_1")

    # Assets
    assets = db.assets
    _ensure_index(assets, "assets", [("organization_id", 1)], "org_1")
    _ensure_index(assets, "assets", [("organization_id", 1), ("value", 1)], "org_value_1")
    _ensure_index(assets, "assets", [("organization_id", 1), ("risk_score", 1)], "org_risk_score_1")
    _ensure_index(assets, "assets", [("organization_id", 1), ("alive", 1)], "org_alive_1")

    # Scans
    scans = db.scans
    _ensure_index(scans, "scans", [("organization_id", 1)], "org_1")
    _ensure_index(scans, "scans", [("organization_id", 1), ("started_at", -1)], "org_started_at_-1")


def ensure_asset_events_indexes():
    """Create indexes on asset_events for change detection. Idempotent: _ensure_index compares key pattern first to avoid IndexOptionsConflict."""
    events = db.asset_events
    # {organization_id:1, created_at:-1}
    _ensure_index(events, "asset_events", [("organization_id", 1), ("created_at", -1)], "org_created_at_-1")
    # {organization_id:1, scan_id:1}
    _ensure_index(events, "asset_events", [("organization_id", 1), ("scan_id", 1)], "org_scan_id_1")
    # {organization_id:1, type:1, created_at:-1}
    _ensure_index(events, "asset_events", [("organization_id", 1), ("type", 1), ("created_at", -1)], "org_type_created_at_-1")


def ensure_all_indexes():
    """Run all index ensure functions (call at app startup, not at import)."""
    ensure_organizations_indexes()
    ensure_org_isolation_indexes()
    ensure_asset_events_indexes()


class NoOrganizationsError(Exception):
    """Raised when no active organization exists (run seed)."""


def get_current_org_id():
    """
    Return the current organization id for the request.
    - If session has org_id, return it.
    - Else set session from first active organization in DB and return it.
    - If no organizations exist, raise NoOrganizationsError (suggests running seed).
    """
    org_id = session.get("org_id")
    if org_id is not None:
        return org_id
    org = db.organizations.find_one({"is_active": True})
    if org is None:
        raise NoOrganizationsError(
            "No organizations found. Run the seed script: python scripts/seed_organizations.py"
        )
    org_id = org["slug"]
    session["org_id"] = org_id
    return org_id


def org_filter():
    """Return query filter for the current organization (strict: only matching organization_id)."""
    return {"organization_id": get_current_org_id()}


@app.errorhandler(NoOrganizationsError)
def handle_no_organizations(err):
    return error_response(str(err), 503)


@app.context_processor
def inject_org_context():
    """Provide active_organizations and current_org_id to all templates."""
    try:
        current_org_id = get_current_org_id()
    except NoOrganizationsError:
        return {"active_organizations": [], "current_org_id": None}
    orgs = list(db.organizations.find({"is_active": True}))
    active_organizations = [{"slug": o["slug"], "name": o.get("name", o["slug"])} for o in orgs]
    return {"current_org_id": current_org_id, "active_organizations": active_organizations}


def error_response(message, status_code):
    """
    Return a consistent JSON error response.
    """
    return jsonify({"error": message}), status_code


def normalize_domains(domains):
    """
    Validate and normalize domains:
    - Must be a non-empty list
    - Lowercase
    - Deduplicate
    - Strip whitespace
    """
    if not isinstance(domains, list):
        return None

    normalized = []
    for d in domains:
        if not isinstance(d, str):
            continue
        d = d.strip().lower()
        if d:
            normalized.append(d)

    # Remove duplicates while preserving order
    seen = set()
    unique = []
    for d in normalized:
        if d not in seen:
            seen.add(d)
            unique.append(d)

    if not unique:
        return None

    return unique


def serialize_target(doc):
    """
    Convert a MongoDB target document to an API-friendly dict.
    Expose `id` instead of internal `_id`.
    """
    return {
        "id": doc.get("_id"),
        "name": doc.get("name"),
        "domains": doc.get("domains", []),
        "enabled": doc.get("enabled", True),
        "created_at": doc.get("created_at"),
    }

@app.route("/api/<target>/<datatype>")
def get_subdomains(target, datatype):
    slug = target.strip().lower()
    # Ensure target belongs to current org
    target_doc = db.targets.find_one({"_id": slug, **org_filter()})
    if target_doc is None:
        return error_response("Target not found", 404)
    scan_id = request.args.get("scan_id")
    query = {"target_id": slug}
    if scan_id is not None:
        query["scan_id"] = scan_id
    collection = db[datatype]
    res = collection.find(query)
    data = []

    for row in res:
        row.pop('_id')
        data.append(row)

    return jsonify(data)


@app.route("/api/<target>/launch_scan")
def start_scan(target):
    instances = request.args.get("spinup")
    module = request.args.get("module")
    req = target

    # Ensure target exists, is in current org, and is enabled
    targets = db.targets
    slug = target.strip().lower()
    target_doc = targets.find_one({"_id": slug, **org_filter()})
    if target_doc is None:
        app.logger.error(f"launch_scan: target '{slug}' not found")
        return error_response("Target not found", 404)
    if not target_doc.get("enabled", True):
        app.logger.error(f"launch_scan: target '{slug}' is disabled")
        return error_response("Target is disabled", 409)

    # Generate scope file from target domains for the worker pipeline
    domains = target_doc.get("domains")
    if not isinstance(domains, list) or len(domains) == 0:
        app.logger.error(f"launch_scan: target '{slug}' has no domains configured")
        return error_response("Target has no domains configured", 400)

    scope_dir = "/app/scope"
    tmp_path = os.path.join(scope_dir, f"{slug}.tmp")
    final_path = os.path.join(scope_dir, slug)
    try:
        os.makedirs(scope_dir, exist_ok=True)
        with open(tmp_path, "w") as f:
            for d in domains:
                f.write(f"{d}\n")
        os.replace(tmp_path, final_path)
        app.logger.info(f"launch_scan: wrote scope file '{final_path}' with {len(domains)} domains")
    except OSError as e:
        app.logger.error(f"launch_scan: failed to write scope file for '{slug}': {str(e)}")
        return error_response(f"Failed to write scope file: {str(e)}", 500)

    if instances == None:
        instances = "0"

    if module == None:
        module="asm"

    r.rpush('queue', req+":"+str(instances)+":"+str(module))

    data = {"message":"Scan launched!"}
    return jsonify(data)


@app.route("/api/<target>/spinup")
def spinup(target):
    slug = target.strip().lower()
    target_doc = db.targets.find_one({"_id": slug, **org_filter()})
    if target_doc is None:
        return error_response("Target not found", 404)
    instances = request.args.get("instances")
    if instances is None:
        instances = "3"
    module = "spinup"
    r.rpush("queue", slug + ":" + str(instances) + ":" + module)

    data = {"message":"Fleet queued for initializing!"}
    return jsonify(data)


# MVP: Target management API
@app.route("/mvp/targets", methods=["POST"])
def create_target():
    """
    Create a new target.
    Expected JSON body: {id, name, domains, enabled}
    - id: slug used as MongoDB _id
    - name: human readable name
    - domains: non-empty array of strings
    - enabled: bool (optional, defaults to True)
    """
    payload = request.get_json(silent=True)
    if payload is None:
        return error_response("Invalid or missing JSON body", 400)

    target_id = payload.get("id")
    name = payload.get("name")
    domains = payload.get("domains")
    enabled = payload.get("enabled", True)

    if not isinstance(target_id, str) or not target_id.strip():
        return error_response("Field 'id' is required and must be a non-empty string", 400)

    if not isinstance(name, str) or not name.strip():
        return error_response("Field 'name' is required and must be a non-empty string", 400)

    normalized_domains = normalize_domains(domains)
    if normalized_domains is None:
        return error_response("Field 'domains' must be a non-empty array of domain strings", 400)

    if not isinstance(enabled, bool):
        return error_response("Field 'enabled' must be a boolean", 400)

    slug = target_id.strip().lower()

    # Ensure id is unique (globally) and add to current org
    targets = db.targets
    existing = targets.find_one({"_id": slug})
    if existing is not None:
        return error_response("Target with this id already exists", 409)

    org_id = get_current_org_id()
    if not org_id:
        return error_response("organization_id required for org isolation", 400)

    doc = {
        "_id": slug,
        "name": name.strip(),
        "domains": normalized_domains,
        "enabled": enabled,
        "organization_id": org_id,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }

    targets.insert_one(doc)

    return jsonify(serialize_target(doc)), 201


# --- Mini UI (server-rendered) ---
@app.route("/mvp/set-org", methods=["POST"])
def set_org():
    """Set current organization in session; redirect back. Expects form field org_id (slug)."""
    org_id = request.form.get("org_id", "").strip()
    if not org_id:
        return error_response("org_id required", 400)
    doc = db.organizations.find_one({"slug": org_id, "is_active": True})
    if doc is None:
        return error_response("Organization not found or inactive", 404)
    session["org_id"] = doc["slug"]
    redirect_url = request.referrer if request.referrer and request.referrer.startswith(request.host_url) else None
    return redirect(redirect_url or url_for("ui_dashboard"))


@app.route("/mvp")
def ui_dashboard():
    """Dashboard: stats and recent scans (scoped to current org)."""
    q = org_filter()
    total_targets = db.targets.count_documents(q)
    total_assets = db.assets.count_documents(q)
    alive_assets = db.assets.count_documents({**q, "alive": True})
    recent_scans = list(
        db.scans.find(q).sort("started_at", -1).limit(10)
    )
    return render_template(
        "dashboard.html",
        total_targets=total_targets,
        total_assets=total_assets,
        alive_assets=alive_assets,
        recent_scans=recent_scans,
    )


@app.route("/mvp/targets", methods=["GET"])
def list_targets():
    """
    List all targets in current org. Returns HTML for browser (Accept: text/html), JSON otherwise.
    """
    targets_cursor = db.targets.find(org_filter())
    targets_list = [serialize_target(t) for t in targets_cursor]
    if request.accept_mimetypes.best_match(["application/json", "text/html"]) == "text/html":
        return render_template("targets.html", targets=targets_list)
    return jsonify(targets_list)


@app.route("/mvp/targets/<target_id>", methods=["GET"], endpoint="ui_target_assets")
def get_target(target_id):
    """
    Get a single target by id (slug) in current org. Returns HTML (assets table) or JSON.
    """
    slug = target_id.strip().lower()
    targets = db.targets
    doc = targets.find_one({"_id": slug, **org_filter()})
    if doc is None:
        return error_response("Target not found", 404)
    if request.accept_mimetypes.best_match(["application/json", "text/html"]) == "text/html":
        assets = list(
            db.assets.find({**org_filter(), "target_id": slug}).sort("last_seen", -1)
        )
        return render_template(
            "target_assets.html",
            target_id=slug,
            assets=assets,
        )
    return jsonify(serialize_target(doc))


@app.route("/mvp/targets/<target_id>", methods=["DELETE"])
def delete_target(target_id):
    """
    Delete a target by id (slug) in current org.
    """
    slug = target_id.strip().lower()
    targets = db.targets
    res = targets.delete_one({"_id": slug, **org_filter()})

    if res.deleted_count == 0:
        return error_response("Target not found", 404)

    return jsonify({"message": "Target deleted"}), 200


# --- CLI ---
@app.cli.command("backfill-org")
def backfill_org():
    """
    One-time backfill: set organization_id on all documents that lack it,
    using the organization with slug="default". Collections: targets, assets, scans.
    """
    default_org = db.organizations.find_one({"slug": "default"})
    if default_org is None:
        raise click.ClickException(
            "Organization with slug='default' not found. Run the seed first: python scripts/seed_organizations.py"
        )
    # Use slug so filtering matches session/UI (get_current_org_id returns slug)
    default_oid = default_org["slug"]
    for coll_name in ("targets", "assets", "scans"):
        result = db[coll_name].update_many(
            {"organization_id": {"$exists": False}},
            {"$set": {"organization_id": default_oid}},
        )
        click.echo(f"{coll_name}: {result.modified_count} documents updated")


# Index initialization: run once at first request (not at import) to avoid crashes when indexes already exist
_indexes_ensured = False


@app.before_request
def _ensure_indexes_once():
    global _indexes_ensured
    if _indexes_ensured:
        return
    ensure_all_indexes()
    _indexes_ensured = True

