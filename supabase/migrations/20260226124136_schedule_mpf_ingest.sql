-- Supabase scheduled ingestion for MPFA official dataset.
-- Prerequisites:
-- 1) Deploy edge function: supabase functions deploy mpf-ingest
-- 2) Store secrets in vault:
--    select vault.create_secret('https://<project-ref>.supabase.co', 'project_url');
--    select vault.create_secret('<service-role-key>', 'service_role_key');

create extension if not exists pg_cron;
create extension if not exists pg_net;

do $$
declare
  project_url text;
  service_role_key text;
  existing_job_id bigint;
begin
  select decrypted_secret into project_url
  from vault.decrypted_secrets
  where name = 'project_url'
  limit 1;

  select decrypted_secret into service_role_key
  from vault.decrypted_secrets
  where name = 'service_role_key'
  limit 1;

  if project_url is null or service_role_key is null then
    raise exception 'Missing vault secrets project_url or service_role_key';
  end if;

  select jobid into existing_job_id
  from cron.job
  where jobname = 'mpf-ingest-daily'
  limit 1;

  if existing_job_id is not null then
    perform cron.unschedule(existing_job_id);
  end if;

  perform cron.schedule(
    'mpf-ingest-daily',
    '35 2 * * *', -- 10:35 HKT (UTC+8), daily check
    format(
      $f$
      select net.http_post(
        url := %L,
        headers := jsonb_build_object(
          'Content-Type', 'application/json',
          'Authorization', %L
        ),
        body := %L::jsonb
      ) as request_id;
      $f$,
      project_url || '/functions/v1/mpf-ingest',
      'Bearer ' || service_role_key,
      '{"sourceUrl":"https://www.mpfa.org.hk/en/-/media/files/information-centre/research-and-statistics/other-reports/statistics/net_asset_values_of_approved_constituent_funds02_en.csv"}'
    )
  );
end $$;
