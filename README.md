# SkyPoints member and redemption ETL assessment

This submission treats the PDF as the assignment specification and the three CSVs as sample source feeds. It includes Snowflake DDL and transformation patterns, plus a small standard-library Python reference pipeline that can be run locally against the supplied files.

## Design summary

- Land each delivered file unchanged with file name, row number, load timestamp, and batch ID. Parse typed data in a separate layer so bad source rows remain auditable.
- Normalize each country feed through an explicit adapter. CSV dates are not guessed: Australia uses ISO `YYYY-MM-DD`, India uses `M/D/YYYY`, and the USA feed uses unpadded `MMDDYYYY` strings.
- Validate required fields, dates, tier codes, and keys. Send invalid rows to quarantine with all reasons; never silently coerce bad dates or identifiers.
- Deduplicate using the source's authoritative `source_updated_at` (or a documented source sequence). The example CSVs do not contain one, so the local runner uses file load time as a clearly marked demonstration fallback. Flight date is not a reliable update timestamp.
- Keep one canonical current-member table keyed by globally unique `member_id`, and expose country-specific views. This avoids maintaining millions of physically separate tables. The SQL also shows how to create physical country tables if a consumer contract requires them.
- Flatten JSON redemptions to one row per transaction and retain the original VARIANT payload. Join on `member_id`; use transaction ID plus partner as a uniqueness key if the source does not guarantee globally unique transaction IDs.

## Run the local reference pipeline

```powershell
python src/skypoints_pipeline.py `
  --aus "path\to\AUS(Sheet1).csv" `
  --ind "path\to\IND(in).csv" `
  --usa "path\to\USA(in).csv" `
  --as-of 2026-11-24 `
  --out output
```

It writes `members_normalized.csv`, `rejections.csv`, and (when `--redemptions` is supplied) `redemptions_flat.csv`. Country membership in the USA and Australia input files is taken from the file's feed scope because those files have no country column. The runner creates a scoped key such as `AUS:1` to avoid collisions between country-local IDs; replace this with the source's globally unique member ID in production.

The supplied data contains one invalid Australian enrollment date (`2021-13-13`). It is quarantined. Missing DOB is permitted by the assessment specification and yields a null age. The example DOB values are synthetic and should not be used for real member decisions.

The PDF's sample metadata marks Member Name as a key while the problem's identity and latest-country logic require a stable member identifier. This implementation treats `member_id` as the business key and name as a required attribute; production owners should confirm this correction with the source-system contract.

## Snowflake implementation

Run scripts in order from `sql/`. `01_ddl.sql` creates raw, staging, canonical, country-view, and redemption objects. `02_transform.sql` contains the parsing, validation, merge, and JSON-flattening patterns. The SQL uses placeholders for the cloud stage and file format; set them to the organization's secured storage integration and feed naming contract.

## Tests

```powershell
python -m unittest discover -s tests -v
```

The tests cover each source date convention, impossible dates, age and stale-member derivation, country-scoped keys, and JSON flattening. They are reference-code tests; Snowflake SQL should also be run in a development account with representative volume and file-format settings before production deployment.

## Scale and operations

At billions of rows/day, use partitioned files in cloud object storage, Snowpipe or scheduled `COPY INTO` with a manifest/batch control table, and parallel micro-batches. Keep raw data immutable, use idempotent batch IDs, and MERGE only changed records into the canonical table. Cluster/partition choices should follow measured access patterns (commonly member ID and country); avoid one table per member or per tiny feed. Monitor ingest lag, row counts, reject rates, duplicate keys, freshness, and reconciliation totals. Reprocess by batch ID without duplicating target rows.
