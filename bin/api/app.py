#!/usr/bin/env python3

from pymongo import MongoClient
import redis
from flask import Flask
from flask import request
from flask import jsonify
from flask import render_template
from datetime import datetime
import os
app = Flask(__name__)

r = redis.Redis(host='redis', port=6379, db=0)
client = MongoClient("mongodb://mongo:27017")
db = client.asm


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
    scan_id = request.args.get("scan_id")
    query = {'target_id':target}

    if scan_id != None:
        query['scan_id'] = scan_id

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

    # Ensure target exists and is enabled in MongoDB before launching scan
    targets = db.targets
    slug = target.strip().lower()
    target_doc = targets.find_one({"_id": slug})

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
    instances = request.args.get("instances")
    req = target

    if instances == None:
        instances = "3"
    
    module = "spinup"

    r.rpush('queue', req+":"+str(instances)+":"+module)

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

    # Ensure collection exists and id is unique
    targets = db.targets
    existing = targets.find_one({"_id": slug})
    if existing is not None:
        return error_response("Target with this id already exists", 409)

    doc = {
        "_id": slug,
        "name": name.strip(),
        "domains": normalized_domains,
        "enabled": enabled,
        "created_at": datetime.utcnow().isoformat() + "Z",
    }

    targets.insert_one(doc)

    return jsonify(serialize_target(doc)), 201


# --- Mini UI (server-rendered) ---
@app.route("/mvp")
def ui_dashboard():
    """Dashboard: stats and recent scans."""
    total_targets = db.targets.count_documents({})
    total_assets = db.assets.count_documents({})
    alive_assets = db.assets.count_documents({"alive": True})
    recent_scans = list(
        db.scans.find().sort("started_at", -1).limit(10)
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
    List all targets. Returns HTML for browser (Accept: text/html), JSON otherwise.
    """
    targets_cursor = db.targets.find()
    targets_list = [serialize_target(t) for t in targets_cursor]
    if request.accept_mimetypes.best_match(["application/json", "text/html"]) == "text/html":
        return render_template("targets.html", targets=targets_list)
    return jsonify(targets_list)


@app.route("/mvp/targets/<target_id>", methods=["GET"], endpoint="ui_target_assets")
def get_target(target_id):
    """
    Get a single target by id (slug). Returns HTML (assets table) for browser, JSON otherwise.
    """
    slug = target_id.strip().lower()
    targets = db.targets
    doc = targets.find_one({"_id": slug})

    if doc is None:
        return error_response("Target not found", 404)

    if request.accept_mimetypes.best_match(["application/json", "text/html"]) == "text/html":
        assets = list(
            db.assets.find({"target_id": slug}).sort("last_seen", -1)
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
    Delete a target by id (slug).
    """
    slug = target_id.strip().lower()
    targets = db.targets
    res = targets.delete_one({"_id": slug})

    if res.deleted_count == 0:
        return error_response("Target not found", 404)

    return jsonify({"message": "Target deleted"}), 200

