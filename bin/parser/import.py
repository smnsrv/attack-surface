#!/usr/bin/env python3

"""
Import JSONL file (subs.json or http.json) into Mongo.
Skips empty lines and bad JSON; does not crash. Prints imported_count and bad_lines for scanner.
"""

import json
import sys
from pymongo import MongoClient

client = MongoClient("mongodb://mongo:27017")
db = client.asm

filename = sys.argv[1]
scan_id = sys.argv[2]
collection_name = filename.split(".")[0].split("/")[-1]
collection = db[collection_name]
target_id = sys.argv[3]


def jsonf_to_lines(filename):
    parsed_lines = []
    bad_count = 0
    with open(filename, "r") as reader:
        for line in reader:
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                bad_count += 1
                continue
            parsed["scan_id"] = scan_id
            parsed["target_id"] = target_id
            parsed_lines.append(parsed)
    return parsed_lines, bad_count


parsed_lines, bad_count = jsonf_to_lines(filename)
if parsed_lines:
    collection.insert_many(parsed_lines)
# stdout: counts for scanner (imported_count, bad_lines); scanner prints combined WARN if any bad
print(f"imported_count={len(parsed_lines)}")
print(f"bad_lines={bad_count}")
