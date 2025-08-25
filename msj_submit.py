#!/usr/bin/env python3
"""
MSJ v0.01 – Mass Job Submit to Rescale

Usage:
    python msj_submit.py [--dry-run] [--config FILE]

Features:
    • Reads msj_config.csv & job_inputs_matrix.csv in cwd
    • Uploads input files via rescale-cli
    • Builds job JSON on‑the‑fly (supports multi‑analysis)
    • Burst/throttle logic with short & long gaps
    • Parallel uploads / submits (thread pool)
    • Retries & ledger logging
"""

import argparse
import csv
import datetime as dt
import json
import logging
import os
import queue
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import requests


# -----------------------------------------------------------
# Config & constants
# -----------------------------------------------------------
PLATFORM_URL = "https://platform.rescale.com/api/v2/jobs/"
UPLOAD_ID_REGEX = re.compile(r"File ID (\w+)")
LEDGER_FILE = "msj_ledger.csv"
LOG_FILE = "msj.log"
SUBMIT_URL_TMPL = "https://platform.rescale.com/api/v2/jobs/{}/submit/"


# -----------------------------------------------------------
# Helpers
# -----------------------------------------------------------
def init_logging(level: str = "INFO"):
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, mode="a"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def load_config(path: Path) -> dict:
    if not path.exists():
        logging.error("Config file %s not found.", path)
        sys.exit(1)

    cfg = {}
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["key"].strip()
            val = row["value"].strip()
            cfg[key] = val

    # provide defaults
    defaults = {
        "x_jobs_per_burst": "25",
        "short_gap_seconds": "2",
        "long_gap_minutes": "10",
        "max_concurrent_submissions": "10",
        "max_retries": "3",
        "default_command_secondary": "#none",
        "log_level": "INFO",
    }
    for k, v in defaults.items():
        cfg.setdefault(k, v)

    return cfg


def parse_matrix(path: Path) -> list[dict]:
    required = [
        "job_name",
        "input_files",
        "num_cores",
        "walltime",
        "analysis_codes",
        "analysis_versions",
        "hardware_type",
        "command",
    ]
    if not path.exists():
        logging.error("Job matrix %s not found.", path)
        sys.exit(1)

    jobs = []
    with path.open() as f:
        reader = csv.DictReader(f)
        missing = [c for c in required if c not in reader.fieldnames]
        if missing:
            logging.error("CSV missing required columns: %s", missing)
            sys.exit(1)

        for i, row in enumerate(reader, start=2):
            codes = row["analysis_codes"].split(";")
            vers = row["analysis_versions"].split(";")
            if len(codes) != len(vers):
                logging.error(
                    "Row %d: analysis_codes count != analysis_versions count", i
                )
                sys.exit(1)

            jobs.append(row)
    return jobs


def ensure_env_token() -> str:
    token = os.getenv("RESCALE_API_KEY")
    if not token:
        logging.error("RESCALE_API_KEY is not set. Aborting.")
        sys.exit(1)
    return token


# -----------------------------------------------------------
# File upload
# -----------------------------------------------------------
def upload_file(local_path: str, token: str, dry_run: bool = False) -> str:
    """
    Returns Rescale fileId for uploaded file.
    """
    if dry_run:
        fake_id = f"DRY{hash(local_path) & 0xFFFFF:X}"
        logging.info("[dry‑run] Would upload %s → id %s", local_path, fake_id)
        return fake_id

    cmd = ["rescale-cli", "upload", "-p", token, "-f", local_path]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Upload failed: {proc.stderr or proc.stdout}")

    m = UPLOAD_ID_REGEX.search(proc.stdout + proc.stderr)
    if not m:
        raise RuntimeError("Could not parse fileId from upload output")
    file_id = m.group(1)
    logging.info("Uploaded %s → %s", local_path, file_id)
    return file_id


def upload_files(paths: list[str], token: str, dry_run: bool = False) -> list[str]:
    ids = []
    for p in paths:
        p = p.strip()
        if not Path(p).exists() and not dry_run:
            raise FileNotFoundError(f"Input file not found: {p}")
        ids.append(upload_file(p, token, dry_run))
    return ids


# -----------------------------------------------------------
# Job JSON builder
# -----------------------------------------------------------
def build_job_json(row: dict, file_ids: list[str], cfg: dict) -> dict:
    codes = [c.strip() for c in row["analysis_codes"].split(";")]
    vers = [v.strip() for v in row["analysis_versions"].split(";")]
    hardware_block = {
        "coresPerSlot": int(row["num_cores"]),
        "coreType": row["hardware_type"],
        "walltime": int(row["walltime"]),
    }
    input_files_json = [{"id": fid} for fid in file_ids]

    jobanalyses = []
    for idx, (code, ver) in enumerate(zip(codes, vers)):
        jobanalyses.append(
            {
                "analysis": {"code": code, "version": ver},
                "command": row["command"] if idx == 0 else cfg["default_command_secondary"],
                "hardware": hardware_block,
                "inputFiles": input_files_json,
            }
        )

    return {"name": row["job_name"], "jobanalyses": jobanalyses}


# -----------------------------------------------------------
# Job submitter
# -----------------------------------------------------------
def post_job(payload: dict, token: str, dry_run: bool = False) -> tuple[str, str]:
    """
    Create the job then immediately submit/launch it.
    Returns (job_id, submit_time).
    """
    if dry_run:
        fake_id = f"JOB{hash(json.dumps(payload)) & 0xFFFFFF:X}"
        now = dt.datetime.utcnow().isoformat()
        logging.info("[dry‑run] Would POST job %s", payload["name"])
        return fake_id, now

    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }

    # ----- 1. CREATE (draft) -----
    resp = requests.post(PLATFORM_URL, headers=headers, json=payload)
    if resp.status_code != 201:
        raise RuntimeError(f"Job CREATE failed: {resp.status_code} {resp.text}")

    data = resp.json()
    job_id = data["id"]
    submit_time = data["dateInserted"]

    # Debug dump if requested
    if logging.getLogger().level == logging.DEBUG:
        logging.debug("Payload sent:\n%s", json.dumps(payload, indent=2))
        logging.debug("Create response %s: %s", resp.status_code, resp.text)

    # ----- 2. SUBMIT (launch) -----
    submit_resp = requests.post(
        SUBMIT_URL_TMPL.format(job_id),
        headers={"Authorization": f"Token {token}"},
        json={},  # body empty is fine
    )
    logging.debug("Submit response %s: %s", submit_resp.status_code, submit_resp.text)

    if submit_resp.status_code not in (200, 201, 202):
        raise RuntimeError(
            f"Job SUBMIT failed for {job_id}: "
            f"{submit_resp.status_code} {submit_resp.text}"
        )

    return job_id, submit_time



# -----------------------------------------------------------
# Ledger
# -----------------------------------------------------------
_LEDGER_LOCK = threading.Lock()


def init_ledger():
    if not Path(LEDGER_FILE).exists():
        with open(LEDGER_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "job_id",
                    "job_name",
                    "submit_time",
                    "input_files",
                    "hardware_type",
                    "num_retries",
                ]
            )


def write_ledger(job_id: str, row: dict, submit_time: str, retries: int):
    with _LEDGER_LOCK, open(LEDGER_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                job_id,
                row["job_name"],
                submit_time,
                row["input_files"],
                row["hardware_type"],
                retries,
            ]
        )


# -----------------------------------------------------------
# Worker & scheduler
# -----------------------------------------------------------
def worker(
    q: queue.Queue,
    token: str,
    cfg: dict,
    dry_run: bool = False,
):
    max_retries = int(cfg["max_retries"])
    while True:
        row = q.get()
        if row is None:
            q.task_done()
            break
        retries = 0
        try:
            while True:
                try:
                    paths = row["input_files"].split(";")
                    ids = upload_files(paths, token, dry_run)
                    payload = build_job_json(row, ids, cfg)
                    job_id, submit_time = post_job(payload, token, dry_run)
                    write_ledger(job_id, row, submit_time, retries)
                    logging.info("Submitted job %s (%s)", row["job_name"], job_id)
                    break
                except Exception as e:
                    retries += 1
                    logging.warning(
                        "Job %s failed attempt %d/%d: %s",
                        row["job_name"],
                        retries,
                        max_retries,
                        e,
                    )
                    if retries >= max_retries:
                        logging.error("Giving up on job %s", row["job_name"])
                        break
                    time.sleep(5 * (2 ** (retries - 1)))  # exponential back‑off
        finally:
            q.task_done()


def main():
    ap = argparse.ArgumentParser(description="MSJ v0.01 – Mass Job Submit")
    ap.add_argument("--dry-run", action="store_true", help="Do everything except network")
    ap.add_argument("--config", default="msj_config.csv", help="Config CSV path")
    args = ap.parse_args()

    cfg = load_config(Path(args.config))
    init_logging(cfg["log_level"])
    # Reduce urllib3 chatter unless we need it
    if cfg["log_level"].upper() == "DEBUG":
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    token = ensure_env_token()
    jobs = parse_matrix(Path("job_inputs_matrix.csv"))
    init_ledger()

    # Thread pool
    num_workers = int(cfg["max_concurrent_submissions"])
    q: queue.Queue = queue.Queue(maxsize=num_workers * 2)
    threads = []
    for _ in range(num_workers):
        t = threading.Thread(
            target=worker, args=(q, token, cfg, args.dry_run), daemon=True
        )
        t.start()
        threads.append(t)

    # Burst scheduler
    x_jobs = int(cfg["x_jobs_per_burst"])
    short_gap = float(cfg["short_gap_seconds"])
    long_gap = float(cfg["long_gap_minutes"]) * 60

    counter = 0
    for row in jobs:
        q.put(row)
        counter += 1
        if counter >= x_jobs:
            logging.info("Burst limit reached: resting %.0f s", long_gap)
            counter = 0
            time.sleep(long_gap)
        else:
            time.sleep(short_gap)

    # Finish
    q.join()
    for _ in threads:
        q.put(None)  # sentinel
    for t in threads:
        t.join()

    logging.info("All done! Ledger written to %s", LEDGER_FILE)


if __name__ == "__main__":
    main()
