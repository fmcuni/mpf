# Cloudflare ETL Setup (Workers Cron + Supabase)

This runbook sets up Cloudflare as the full ETL runtime and keeps Supabase as storage.

## 1) What This Delivers

- A Cloudflare Worker cron job that runs once daily.
- All ingestion jobs run inside Cloudflare Worker:
  - AIA trustee metadata sync (official MPFA trustee list source)
  - HSBC daily latest prices
  - Hang Seng daily latest prices
  - Manulife daily latest prices
  - Sun Life browser-session daily latest prices (browser-only; no MPFA fallback)
- Manual trigger endpoint for ad-hoc runs.

Implemented files:

- `cloudflare/workers/mpf-etl/src/index.ts`
- `cloudflare/workers/mpf-etl/wrangler.toml.example`
- `cloudflare/workers/mpf-etl/package.json`

## 2) Prerequisites

- Cloudflare account with Workers enabled.
- Cloudflare Browser Rendering enabled (for Sun Life job).
- Local `pnpm` available.

## 3) Cloudflare Worker Bootstrap

From repo root:

```bash
cd cloudflare/workers/mpf-etl
cp wrangler.toml.example wrangler.toml
pnpm install
pnpm dlx wrangler@latest login
```

## 4) Configure Secrets

Set secrets in Cloudflare Worker:

```bash
pnpm dlx wrangler@latest secret put SUPABASE_URL
pnpm dlx wrangler@latest secret put SUPABASE_SERVICE_ROLE_KEY
pnpm dlx wrangler@latest secret put MANUAL_TRIGGER_TOKEN
```

Expected values:

- `SUPABASE_URL`: `https://<project-ref>.supabase.co`
- `SUPABASE_SERVICE_ROLE_KEY`: Supabase service role key
- `MANUAL_TRIGGER_TOKEN`: random long token for `/run`

## 5) Source Toggles

You can turn specific jobs on/off in `wrangler.toml`:

```toml
[vars]
ENABLE_AIA_ETL = "true"
ENABLE_HSBC_ETL = "true"
ENABLE_HANGSENG_ETL = "true"
ENABLE_MANULIFE_ETL = "true"
ENABLE_SUNLIFE_BROWSER_ETL = "true"
```

Recommended for initial rollout:

1. Keep all `true`.
2. If a source is unstable, set only that source to `"false"` temporarily.

## 6) Deploy

```bash
pnpm dlx wrangler@latest deploy
```

After deploy, note your Worker URL:

- `https://<worker-name>.<account-subdomain>.workers.dev`

## 7) Verify

Health check:

```bash
curl -s https://<worker-url>/health
```

Manual run:

```bash
curl -s -X POST https://<worker-url>/run \
  -H "Authorization: Bearer <MANUAL_TRIGGER_TOKEN>"
```

Check Worker logs:

```bash
pnpm dlx wrangler@latest tail
```

Expected `/run` response format:

- `jobs[0]`: `hsbc.daily`
- `jobs[1]`: `hangseng.daily`
- `jobs[2]`: `manulife.daily`
- `jobs[3]`: `aia.daily`
- `jobs[4]`: `sunlife.browser.daily` (or `skipped=true` when disabled)

## 8) Avoid Double Scheduling

Disable old Supabase `pg_cron` job after Cloudflare cron is verified:

```sql
select cron.unschedule(jobid)
from cron.job
where jobname = 'mpf-ingest-daily';
```

Supabase Edge Function `mpf-ingest` can remain deployed, but it is no longer required in the Cloudflare-first pipeline.
