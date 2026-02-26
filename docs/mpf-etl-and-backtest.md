# MPF Server Logic and ETL (Trustee-Level Feeds)

## Scope

This implementation uses trustee-level fund feeds for individual fund prices and metadata.

## Data Model

Tables added in `packages/db/src/schema.ts`:

- `mpf_fund`
  - Trustee/scheme/fund metadata with official EN + ZH-HK names.
- `mpf_fund_price`
  - Daily fund price rows keyed by `(fund_id, price_date)`.
- `mpf_ingestion_run`
  - ETL run history, status, row counts, and payload hash.
- `mpf_contribution`
  - User contribution ledger for personal performance and XIRR.

## ETL Flow

Cloudflare Worker orchestrator:

1. Insert `running` row in `mpf_ingestion_run`.
2. Fetch each trustee source (HSBC, Hang Seng, Manulife, Sun Life browser flow).
3. Normalize fund metadata and latest valuation date rows.
4. Upsert to `mpf_fund` and `mpf_fund_price`.
5. Mark ingestion run `success` (or `failed` with error).

## Scheduled Job (Cloudflare Worker Cron)

ETL orchestration now runs on Cloudflare Worker Cron, with Supabase as storage.

Worker docs:

- `docs/cloudflare-etl-setup.md`

Schedule:

- Daily at `02:35 UTC` (`10:35 HKT`) via Worker cron trigger.

Jobs in Cloudflare Worker:

- AIA trustee metadata sync
- HSBC latest fund prices
- Hang Seng latest fund prices
- Manulife latest fund prices
- Sun Life latest fund prices (browser session adapter, no fallback source)

## Backtest + XIRR Logic

`mpf.backtest`:

- Accepts fund allocations, initial amount, date range, optional synthetic monthly contribution.
- Optionally merges saved user contributions from `mpf_contribution`.
- Computes portfolio path based on weighted fund-period returns.
- Computes XIRR from contribution cashflows and terminal portfolio value.
