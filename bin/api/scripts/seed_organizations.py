#!/usr/bin/env python3
"""
Seed the organizations collection: create indexes and insert Default Org if missing.

Usage (from repo root):

  # Local Mongo (or Mongo reachable on localhost):
  pip install pymongo
  python scripts/seed_organizations.py
  # or: MONGO_URI=mongodb://localhost:27017 python scripts/seed_organizations.py

  # With Docker Compose (mount repo, use mongo service):
  docker compose run --rm -v "$(pwd):/workspace" -w /workspace api \\
    sh -c "pip install -q pymongo && MONGO_URI=mongodb://mongo:27017 python scripts/seed_organizations.py"

Expects Mongo at MONGO_URI (default: mongodb://mongo:27017).
"""

import os
import sys
from datetime import datetime

try:
    from pymongo import MongoClient
except ImportError:
    print("pymongo not installed. Install with: pip install pymongo", file=sys.stderr)
    sys.exit(1)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
DB_NAME = "asm"

def default_org_doc():
    return {
        "name": "Default Org",
        "slug": "default",
        "contact_email": None,
        "created_at": datetime.utcnow(),
        "is_active": True,
    }


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    orgs = db.organizations

    # Create indexes: slug unique, is_active
    orgs.create_index("slug", unique=True)
    orgs.create_index("is_active")
    print("[+] organizations indexes ensured (slug unique, is_active)")

    # Insert Default Org if not exists
    result = orgs.update_one(
        {"slug": "default"},
        {"$setOnInsert": default_org_doc()},
        upsert=True,
    )
    if result.upserted_id:
        print("[+] Created organization: Default Org (slug=default)")
    else:
        print("[.] Organization 'default' already exists, skipped")

    return 0


if __name__ == "__main__":
    sys.exit(main())
