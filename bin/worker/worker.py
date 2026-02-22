#!/usr/bin/env python3

import logging
import os
import subprocess
import threading
import time

import redis

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

r = redis.Redis(host="redis", port=6379, db=0)

# LOCAL MVP: no Axiom; only run axiom-account if present (e.g. legacy image)
_axiom_account = "/root/.axiom/interact/axiom-account"
if os.path.isfile(_axiom_account):
    try:
        subprocess.call([_axiom_account, "default"], timeout=10)
    except (subprocess.SubprocessError, OSError) as e:
        log.warning("axiom-account skipped or failed: %s", e)
else:
    log.info("Axiom not installed; running in LOCAL MVP mode.")

log.info("worker started")


def run_job(job_bytes):
    job_str = job_bytes.decode("utf-8")
    log.info("job: %s", job_str)

    parts = job_str.split(":")
    if len(parts) >= 3 and parts[2] == "spinup":
        log.info("spinup ignored in LOCAL MVP (no Axiom fleet)")
        return

    ret = subprocess.call(
        ["bash", "/app/bin/worker/scanner.sh", job_str],
        cwd="/",
    )
    if ret != 0:
        log.error("scanner.sh exited with code %s for job: %s", ret, job_str)


while True:
    res = r.rpop("queue")
    if res is not None:
        t = threading.Thread(target=run_job, args=(res,))
        t.start()
    time.sleep(1)
