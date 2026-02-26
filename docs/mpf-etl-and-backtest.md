# MPF Server Logic and ETL (MPFA Official Dataset)

## Scope

This implementation uses MPFA official dataset:

- `net_asset_values_of_approved_constituent_funds02_en.csv`

The dataset contains **category-level NAV totals** (HKD million), not individual fund unit prices.

## Data Model

Tables added in `packages/db/src/schema.ts`:

- `mpf_category_nav`
  - Key: `(as_of_date, category)`
  - Stores NAV series by category.
- `mpf_ingestion_run`
  - ETL run history, status, row counts, and payload hash.
- `mpf_contribution`
  - User contribution ledger for personal performance and XIRR.

## ETL Flow

`mpf.ingestOfficialDataset`:

1. Insert `running` row in `mpf_ingestion_run`.
2. Fetch MPFA CSV.
3. Parse and map columns to internal categories.
4. Upsert to `mpf_category_nav`.
5. Mark ingestion run `success` (or `failed` with error).

## Scheduled Job (Supabase Edge Function + Cron)

Files:

- `supabase/functions/mpf-ingest/index.ts`
- `supabase/migrations/20260226124136_schedule_mpf_ingest.sql`

Deploy and schedule:

1. `supabase functions deploy mpf-ingest`
2. Save required vault secrets:
   - `project_url` = `https://<project-ref>.supabase.co`
   - `service_role_key` = your Supabase service role key
3. Run migration to create the cron job.

Schedule configured in migration:

- Daily check at `02:35 UTC` (`10:35 HKT`).
- Job is idempotent by CSV hash (`payload_hash`), so unchanged data is skipped.

## Compare Logic

`mpf.compare`:

- Filters by categories/date range.
- Returns NAV and cumulative return from first data point for each category.

## Backtest + XIRR Logic

`mpf.backtest`:

- Accepts category weights, initial amount, date range, optional synthetic monthly contribution.
- Optionally merges saved user contributions from `mpf_contribution`.
- Computes portfolio path based on weighted category-period returns.
- Computes XIRR from contribution cashflows and terminal portfolio value.

## Important Limitation

MPFA official dataset here is NAV totals, which are influenced by both market movement and net contributions/withdrawals.
So this backtest is a **macro proxy**, not a strict pure price-return backtest at constituent-fund level.
