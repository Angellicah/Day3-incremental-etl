"""
Day 3 - Incremental ETL Pipeline
Squad Python Specialization - Data Engineering

Run it twice in a row. The second run must change nothing.
"""
import time
import os
import csv
import json
import logging
from datetime import datetime
from pathlib import Path

WAREHOUSE_PATH = Path("data") / "transactions.csv"
BATCH_DIR = Path("incoming")
CHECKPOINT_PATH = Path("state") / "checkpoint.json"
LOG_PATH = Path("logs") / "etl.log"

KEY = "transaction_id"
COMPARED_FIELDS = ("customer_id", "merchant", "amount_kes", "status", "channel", "updated_at")

REQUIRED_FIELDS = (KEY,) + COMPARED_FIELDS
VALID_STATUSES = {"PENDING", "COMPLETED", "FAILED"}


def setup_logging(path: Path = LOG_PATH) -> logging.Logger:
    """Configure a logger that writes to both the console and a log file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("etl")
    logger.setLevel(logging.INFO)

    if logger.handlers:  # already configured, don't add duplicates
        return logger

    formatter = logging.Formatter("%(asctime)s %(levelname)-5s %(message)s")

    file_handler = logging.FileHandler(path, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def load_existing(path: Path = WAREHOUSE_PATH) -> dict[str, dict]:
    """Read the loaded transactions into a dict keyed by transaction_id."""
    existing = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["amount_kes"] = float(row["amount_kes"])
            existing[row[KEY]] = row
    return existing


def load_batch(path: Path) -> tuple[str, list[dict]] | None:
    """Read one JSON batch file and return (batch_id, transactions), or None if unusable."""
    log = logging.getLogger("etl")
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        batch_id = data["batch_id"]
        transactions = data["transactions"]
        if not isinstance(transactions, list):
            raise ValueError("'transactions' is not a list")
    except FileNotFoundError:
        log.error("Batch file not found: %s - skipping", path)
        return None
    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
        log.error("Malformed batch file %s: %r - skipping", path, e)
        return None
    return batch_id, transactions


def validate_record(record: dict) -> list[str]:
    """Return the reasons a batch record cannot be loaded (empty list means valid)."""
    if not isinstance(record, dict):
        return ["record is not a JSON object"]

    reasons = []

    for field in REQUIRED_FIELDS:
        value = record.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            reasons.append(f"missing {field}")

    amount = record.get("amount_kes")
    if amount is not None and amount != "":
        if isinstance(amount, bool) or not isinstance(amount, (int, float)):
            reasons.append(f"amount_kes is not a number: {amount!r}")
        elif amount <= 0:
            reasons.append(f"amount_kes must be positive: {amount}")

    status = record.get("status")
    if status and status not in VALID_STATUSES:
        reasons.append(f"unknown status: {status!r}")

    updated_at = record.get("updated_at")
    if updated_at:
        try:
            datetime.fromisoformat(updated_at)
        except (ValueError, TypeError):
            reasons.append(f"updated_at is not a valid timestamp: {updated_at!r}")

    return reasons

def _ts(record: dict) -> datetime:
    return datetime.fromisoformat(record["updated_at"])


def classify(existing: dict[str, dict], batch: list[dict]) -> dict[str, list[dict]]:
    """Split the batch into the buckets 'new', 'updated', 'duplicate' and 'invalid'."""
    buckets = {"new": [], "updated": [], "duplicate": [], "invalid": []}

    # 1. Validate, and keep only the newest version of each ID within the batch
    candidates = {}
    for index, record in enumerate(batch):
        reasons = validate_record(record)
        if reasons:
            buckets["invalid"].append({"index": index, "record": record, "reasons": reasons})
            continue

        record = {**record, "amount_kes": float(record["amount_kes"])}
        key = record[KEY]
        if key not in candidates:
            candidates[key] = record
        elif _ts(record) >= _ts(candidates[key]):
            buckets["duplicate"].append(candidates[key])  # older copy is superseded
            candidates[key] = record
        else:
            buckets["duplicate"].append(record)

    # 2. Set operations on the IDs
    batch_ids = set(candidates)
    existing_ids = set(existing)
    new_ids = batch_ids - existing_ids
    overlap_ids = batch_ids & existing_ids

    for key in sorted(new_ids):
        buckets["new"].append(candidates[key])

    # 3. Compare field values only where the IDs overlap
    for key in sorted(overlap_ids):
        incoming, stored = candidates[key], existing[key]
        if all(incoming[f] == stored[f] for f in COMPARED_FIELDS):
            buckets["duplicate"].append(incoming)
        elif _ts(incoming) > _ts(stored):
            buckets["updated"].append(incoming)
        else:
            buckets["duplicate"].append(incoming)  # stale: older or same-time conflict

    return buckets


def upsert(existing: dict[str, dict], new: list[dict], updated: list[dict]) -> dict[str, dict]:
    """Return the merged state: inserts for new keys, replacements for updated keys."""
    merged = dict(existing)  # copy, so we never mutate the caller's data
    for record in new:
        merged[record[KEY]] = record
    for record in updated:
        merged[record[KEY]] = record
    return merged


def read_checkpoint(path: Path = CHECKPOINT_PATH) -> dict:
    """Return the saved checkpoint, or a fresh one if none exists yet."""
    fresh = {
        "last_run_at": None,
        "processed_batches": [],
        "high_watermark": None,
        "row_count": 0,
    }
    if not path.exists():
        return fresh
    try:
        with open(path, encoding="utf-8") as f:
            checkpoint = json.load(f)
        if not isinstance(checkpoint.get("processed_batches"), list):
            raise ValueError("processed_batches missing or not a list")
        return {**fresh, **checkpoint}
    except (json.JSONDecodeError, ValueError, AttributeError) as e:
        logging.getLogger("etl").warning(
            "Checkpoint unreadable (%r) - starting from a fresh one", e
        )
        return fresh


def write_checkpoint(checkpoint: dict, path: Path = CHECKPOINT_PATH) -> None:
    """Persist the checkpoint so an interrupted run never leaves a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".json.tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=2)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, path)


def save_warehouse(state: dict[str, dict], path: Path = WAREHOUSE_PATH) -> None:
    """Write the merged state back to CSV in a stable order."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".csv.tmp")

    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[KEY, *COMPARED_FIELDS])
        writer.writeheader()
        for key in sorted(state):
            row = dict(state[key])
            row["amount_kes"] = f"{row['amount_kes']:.2f}"
            writer.writerow(row)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp_path, path)  # atomic swap


def run() -> dict:
    """Process every batch not yet recorded in the checkpoint and return run statistics."""
    log = setup_logging()
    started = time.perf_counter()
    log.info("Run started")

    state = load_existing()
    log.info("Existing rows loaded: %d", len(state))

    checkpoint = read_checkpoint()
    stats = {"batches_processed": 0, "new": 0, "updated": 0, "duplicate": 0, "invalid": 0}

    for batch_path in sorted(BATCH_DIR.glob("*.json")):
        loaded = load_batch(batch_path)
        if loaded is None:
            continue  # already logged; never mark an unreadable batch as done
        batch_id, transactions = loaded

        if batch_id in checkpoint["processed_batches"]:
            log.info("Batch %s already processed - skipping", batch_id)
            log.info("New: 0 | Updated: 0 | Duplicates: 0 | Invalid: 0")
            continue

        log.info("Batch %s: %d records read", batch_id, len(transactions))
        buckets = classify(state, transactions)
        log.info(
            "New: %d | Updated: %d | Duplicates: %d | Invalid: %d",
            len(buckets["new"]), len(buckets["updated"]),
            len(buckets["duplicate"]), len(buckets["invalid"]),
        )
        for item in buckets["invalid"]:
            record = item["record"]
            record_id = record.get(KEY) if isinstance(record, dict) else None
            label = record_id or f"index {item['index']}"
            log.warning("Invalid record %s: %s", label, "; ".join(item["reasons"]))

        # 1) data first...
        state = upsert(state, buckets["new"], buckets["updated"])
        save_warehouse(state)

        # 2) ...checkpoint only after the save succeeded
        checkpoint["processed_batches"].append(batch_id)
        checkpoint["last_run_at"] = datetime.now().isoformat(timespec="seconds")
        checkpoint["high_watermark"] = max(
            (r["updated_at"] for r in state.values()), default=None
        )
        checkpoint["row_count"] = len(state)
        write_checkpoint(checkpoint)

        stats["batches_processed"] += 1
        for name in ("new", "updated", "duplicate", "invalid"):
            stats[name] += len(buckets[name])

    log.info("Rows after load: %d", len(state))
    log.info("Run finished in %.2fs", time.perf_counter() - started)
    return stats


if __name__ == "__main__":
    run()