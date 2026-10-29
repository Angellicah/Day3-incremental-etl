# Lipa Haraka Incremental ETL

An idempotent loader that merges the daily payment-gateway JSON batch into `data/transactions.csv`. It keeps a checkpoint of completed batches and logs every decision. Built with the Python standard library only (no pandas).

## Approach

Last month a manual re-run counted a whole batch twice and inflated merchant settlements. This loader fixes that by keying everything on `transaction_id`. It only inserts IDs it hasn't seen, and only replaces a stored record when the incoming one is genuinely different and newer. Everything else is left alone, so running it any number of times lands on the same table.

Pipeline order per batch:

1. Load the existing rows into a dict keyed by `transaction_id`
2. Load the batch JSON (a missing or malformed file is logged and skipped, never a crash)
3. Validate each record, then classify using set operations on the IDs
4. Upsert into the table and save the CSV (written to a temp file, then swapped in atomically)
5. Only after the save succeeds, write the checkpoint

## How to run

Requires Python 3.9+ (tested on 3.13.2). No dependencies to install.

```bash
python incremental_etl.py
```

- **Input:** `incoming/*.json` batches
- **Outputs:** `data/transactions.csv`, `state/checkpoint.json`, `logs/etl.log`

## Classification rules

| Bucket | Rule |
|---|---|
| **new** | Valid record whose `transaction_id` is not in the table |
| **updated** | ID exists, at least one field differs, and the incoming `updated_at` is **newer** |
| **duplicate** | ID exists and all fields match; or a repeated ID inside the batch (the older copy); or a differing record that is **not newer** (stale) |
| **invalid** | Fails validation: missing or blank field, non-numeric or non-positive amount, unknown status, or unparseable `updated_at` |

### Why stale records are "duplicate"

`MP-50012` in the batch says COMPLETED at 13:00 on 20 Oct, but the table already holds a newer version of that transaction. Overwriting it would replace fresh data with old data, so I don't. I put it in `duplicate` rather than `invalid` because the record is well-formed, just late, and applying it would change nothing, same as an exact repeat. Flagging it as invalid would raise a warning for what is really just an out-of-order message.

## Assumptions

- If an incoming record differs from the stored one but has the exact same `updated_at`, I treat it as a duplicate and keep the stored version, because there is no way to tell which one is correct.
- Amounts must be real numbers greater than zero. The string `"3,450.00"` is rejected rather than guessed at, because with money it is safer to flag than to clean silently.
- Valid statuses are `PENDING`, `COMPLETED` and `FAILED`.
- The checkpoint is written only after the CSV save succeeds. A crash in between means the batch is reprocessed next run, which is safe because the load is idempotent.

## Evidence

I ran the loader three times and took a SHA-256 hash of `data/transactions.csv` after each run.

| Run | Situation | Result |
|---|---|---|
| 1 | First load | 6 new, 3 updated, 5 duplicates, 3 invalid. 20 rows became 26. |
| 2 | Same batch, checkpoint present | Batch skipped, 0 changes. |
| 3 | Checkpoint deleted, run again | Batch reprocessed, 0 new and 0 updated, data unchanged. |

Full log (`cat logs/etl.log`):

```
INFO  Run started
INFO  Existing rows loaded: 20
INFO  Batch BATCH-2026-10-21-01: 17 records read
INFO  New: 6 | Updated: 3 | Duplicates: 5 | Invalid: 3
WARNING Invalid record index 9: missing transaction_id
WARNING Invalid record MP-50023: amount_kes is not a number: '3,450.00'
WARNING Invalid record MP-50026: amount_kes must be positive: -350.0
INFO  Rows after load: 26
INFO  Run finished in 0.0Xs

INFO  Run started
INFO  Existing rows loaded: 26
INFO  Batch BATCH-2026-10-21-01 already processed - skipping
INFO  New: 0 | Updated: 0 | Duplicates: 0 | Invalid: 0
INFO  Rows after load: 26
INFO  Run finished in 0.0Xs

INFO  Run started
INFO  Existing rows loaded: 26
INFO  Batch BATCH-2026-10-21-01: 17 records read
INFO  New: 0 | Updated: 0 | Duplicates: 14 | Invalid: 3
(same 3 WARNING lines)
INFO  Rows after load: 26
INFO  Run finished in 0.0Xs
```

Hash after each run:

```
5c6df7e47e3e39c053f0d2b5cd7119ed66f2a15cd328eb090d351933cd421926 *data/transactions.csv   (run 1)
5c6df7e47e3e39c053f0d2b5cd7119ed66f2a15cd328eb090d351933cd421926 *data/transactions.csv   (run 2)
5c6df7e47e3e39c053f0d2b5cd7119ed66f2a15cd328eb090d351933cd421926 *data/transactions.csv   (run 3)
```

All three hashes are identical, so re-running changes nothing, even with the checkpoint deleted.

## Why this is idempotent

The table is keyed by `transaction_id`, so a transaction can only exist once. When I re-run a batch (even after deleting the checkpoint, like in run 3), every record in it is compared to what is already stored. If it is identical, or older than what is stored, it is classified as a duplicate and never inserted, so nothing gets counted twice. The checkpoint is only a shortcut to skip work. The real protection is that applying the same batch again produces the same table every time.