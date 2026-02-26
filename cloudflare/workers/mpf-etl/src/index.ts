import puppeteer from "@cloudflare/puppeteer";

type TriggerSource = "scheduled" | "manual";
type Trustee = "hsbc" | "hangseng" | "manulife" | "sunlife" | "aia";

interface Env {
  SUPABASE_URL: string;
  SUPABASE_SERVICE_ROLE_KEY: string;
  MANUAL_TRIGGER_TOKEN?: string;
  ENABLE_AIA_ETL?: string;
  ENABLE_HSBC_ETL?: string;
  ENABLE_HANGSENG_ETL?: string;
  ENABLE_MANULIFE_ETL?: string;
  ENABLE_SUNLIFE_BROWSER_ETL?: string;
  MYBROWSER?: Fetcher;
}

interface JobResult {
  job: string;
  ok: boolean;
  skipped?: boolean;
  detail?: string;
  rowsRead?: number;
  rowsUpserted?: number;
  latestDataDate?: string | null;
  payload?: unknown;
}

interface OrchestratorResult {
  trigger: TriggerSource;
  startedAt: string;
  finishedAt: string;
  ok: boolean;
  jobs: JobResult[];
}

interface IngestionOutcome {
  skipped?: boolean;
  rowsRead: number;
  rowsUpserted: number;
  latestDataDate: string | null;
  payloadHash: string | null;
  payload?: unknown;
}

interface TrusteeFundUpsertRow {
  trustee: Trustee;
  scheme_code: string;
  scheme_identifier?: string | null;
  fund_code: string;
  fund_identifier?: string | null;
  fund_name_en: string;
  fund_name_zh_hk?: string | null;
  risk_level?: string | null;
  dis_fund_indicator?: boolean;
  dis_related_fund_code?: string | null;
}

interface TrusteePriceSeedRow {
  schemeCode: string;
  fundCode: string;
  priceDate: string;
  bidPrice: number | null;
  offerPrice: number | null;
  currencyCode: string;
  source: string;
}

const SUPABASE_REST_BASE_PATH = "/rest/v1";

const HSBC_FUNDS_API_URL =
  "https://rbwm-api.hsbc.com.hk/wpb-gpbw-mmw-hk-hbap-pa-p-wpp-mpf-market-data-prod-proxy/v1/funds?schemeCodes=HB&includes=fundPrice";
const HANG_SENG_FUNDS_API_URL =
  "https://rbwm-api.hsbc.com.hk/wpb-gpbw-mmw-hk-hase-pa-p-wpp-mpf-market-data-prod-proxy/v1/funds?schemeCodes=HS&includes=fundPrice";
const MANULIFE_FUNDS_LIST_API_URL =
  "https://www.manulife.com.hk/bin/funds/fundslist?productLine=mpf&overrideLocale=en_HK";
const MPFA_MPP_LIST_EN_URL = "https://mfp.mpfa.org.hk/eng/mpp_list.jsp";
const MPFA_MPP_LIST_ZH_HK_URL = "https://mfp.mpfa.org.hk/tch/mpp_list.jsp";
const MPFA_AIA_TRUSTEE_ID = "3";
const AIA_SCHEME_CODE = "AIA_PVC";

const SUNLIFE_PAGE_URL =
  "https://www.sunlife.com.hk/en/investments/mpf-orso-fund-prices-performance/mpf-fund-prices-performance/";
const SUNLIFE_SOURCE_URL =
  "https://www.sunlife.com.hk/webservice/getmpforsofunddata";

const HSBC_API_HEADERS = {
  "x-hsbc-channel-id": "WEB",
  client_id: "5eca677638ab454086052a18da4e2cb0",
  client_secret: "d35073Cf96B64b1E9CE25f4E07746300",
} as const;

function json(body: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(body, null, 2), {
    ...init,
    headers: {
      "content-type": "application/json; charset=utf-8",
      ...(init?.headers ?? {}),
    },
  });
}

function isEnabled(value: string | undefined, defaultValue = true): boolean {
  if (value === undefined) {
    return defaultValue;
  }
  const normalized = value.trim().toLowerCase();
  return (
    normalized === "1" ||
    normalized === "true" ||
    normalized === "yes" ||
    normalized === "on"
  );
}

function assertRequiredEnv(env: Env): void {
  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
  }
}

function sanitizeOfficialFundName(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  const sanitized = value
    .replace(/\s*<%\([^)]*\)%>\s*$/g, "")
    .replace(/\s*[#*]+\s*$/g, "")
    .trim();
  return sanitized.length > 0 ? sanitized : null;
}

function parseNav(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string") {
    const parsed = Number.parseFloat(value.replaceAll(",", "").trim());
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function parseHsbcPriceDate(value: string): string {
  return value.slice(0, 10);
}

function stripHtmlTags(input: string): string {
  return input
    .replace(/<[^>]*>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

function parseMpfaMppFundRows(html: string): {
  cfId: string;
  schemeName: string | null;
  fundName: string | null;
}[] {
  const rows: { cfId: string; schemeName: string | null; fundName: string | null }[] = [];
  const rowRegex = /<tr class="wr"[\s\S]*?<\/tr>/g;
  const cfIdRegex = /name="sortlist_checkbox"[\s\S]*?value="(\d+)"/;
  const tds = (row: string) =>
    [...row.matchAll(/<td[^>]*class="(?:txt|table)"[^>]*>([\s\S]*?)<\/td>/g)].map(
      (m) => sanitizeOfficialFundName(stripHtmlTags(m[1] ?? "")),
    );

  const rowBlocks = html.match(rowRegex) ?? [];
  for (const row of rowBlocks) {
    const cfId = cfIdRegex.exec(row)?.[1];
    if (!cfId) {
      continue;
    }
    const columns = tds(row);
    rows.push({
      cfId,
      schemeName: columns[0] ?? null,
      fundName: columns[1] ?? null,
    });
  }
  return rows;
}

async function hashText(text: string): Promise<string> {
  const bytes = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

async function fetchWithRetry(
  input: string | URL | Request,
  init: RequestInit | undefined,
  options?: { attempts?: number; initialDelayMs?: number },
): Promise<Response> {
  const attempts = options?.attempts ?? 4;
  const initialDelayMs = options?.initialDelayMs ?? 600;
  let lastError: unknown;

  for (let i = 0; i < attempts; i++) {
    try {
      const response = await fetch(input, init);
      if (response.ok) {
        return response;
      }

      // Retry transient upstream failures.
      if ([408, 409, 425, 429, 500, 502, 503, 504, 520, 522, 524].includes(response.status)) {
        lastError = new Error(`HTTP ${response.status}`);
      } else {
        return response;
      }
    } catch (error) {
      lastError = error;
    }

    if (i < attempts - 1) {
      const delay = initialDelayMs * 2 ** i;
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  if (lastError instanceof Error) {
    throw lastError;
  }
  throw new Error("fetchWithRetry exhausted without response");
}

async function callSupabaseRest(params: {
  env: Env;
  method: "GET" | "POST" | "PATCH";
  pathWithQuery: string;
  body?: unknown;
  prefer?: string;
}): Promise<{ ok: boolean; status: number; data: unknown }> {
  const url = `${params.env.SUPABASE_URL}${SUPABASE_REST_BASE_PATH}/${params.pathWithQuery}`;
  const response = await fetch(url, {
    method: params.method,
    headers: {
      authorization: `Bearer ${params.env.SUPABASE_SERVICE_ROLE_KEY}`,
      apikey: params.env.SUPABASE_SERVICE_ROLE_KEY,
      "content-type": "application/json",
      ...(params.prefer ? { Prefer: params.prefer } : {}),
    },
    body: params.body !== undefined ? JSON.stringify(params.body) : undefined,
  });

  const text = await response.text();
  let data: unknown = text;
  try {
    data = text.length > 0 ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  return { ok: response.ok, status: response.status, data };
}

async function createIngestionRun(
  env: Env,
  sourceUrl: string,
): Promise<string | null> {
  const response = await callSupabaseRest({
    env,
    method: "POST",
    pathWithQuery: "mpf_ingestion_run?select=id",
    body: {
      source_url: sourceUrl,
      status: "running",
    },
    prefer: "return=representation",
  });

  if (!response.ok) {
    return null;
  }

  if (Array.isArray(response.data) && response.data[0]) {
    return (response.data[0] as { id?: string }).id ?? null;
  }

  if (response.data && typeof response.data === "object") {
    return (response.data as { id?: string }).id ?? null;
  }

  return null;
}

async function updateIngestionRun(
  env: Env,
  runId: string,
  payload: Record<string, unknown>,
): Promise<void> {
  await callSupabaseRest({
    env,
    method: "PATCH",
    pathWithQuery: `mpf_ingestion_run?id=eq.${encodeURIComponent(runId)}`,
    body: payload,
  });
}

async function getLatestRunHash(
  env: Env,
  sourceUrl: string,
): Promise<string | null> {
  const response = await callSupabaseRest({
    env,
    method: "GET",
    pathWithQuery:
      `mpf_ingestion_run?select=payload_hash` +
      `&source_url=eq.${encodeURIComponent(sourceUrl)}` +
      `&status=eq.success&order=created_at.desc&limit=1`,
  });

  if (!response.ok || !Array.isArray(response.data)) {
    return null;
  }
  const first = response.data[0] as { payload_hash?: string } | undefined;
  return first?.payload_hash ?? null;
}

function maxDate(input: (string | null | undefined)[]): string | null {
  let latest: string | null = null;
  for (const value of input) {
    if (!value) {
      continue;
    }
    if (!latest || value > latest) {
      latest = value;
    }
  }
  return latest;
}

async function upsertTrusteeFundsAndPrices(params: {
  env: Env;
  trustee: Trustee;
  schemeCode: string;
  sourceUrl: string;
  fundRows: TrusteeFundUpsertRow[];
  priceSeedRows: TrusteePriceSeedRow[];
}): Promise<IngestionOutcome> {
  const dedupFundMap = new Map<string, TrusteeFundUpsertRow>();
  for (const row of params.fundRows) {
    dedupFundMap.set(`${row.scheme_code}::${row.fund_code}`, row);
  }
  const dedupFunds = [...dedupFundMap.values()];

  if (dedupFunds.length === 0) {
    return {
      rowsRead: 0,
      rowsUpserted: 0,
      latestDataDate: null,
      payloadHash: await hashText(
        JSON.stringify({
          source: params.sourceUrl,
          trustee: params.trustee,
          schemeCode: params.schemeCode,
          funds: 0,
          prices: 0,
        }),
      ),
      payload: { funds: 0, prices: 0 },
    };
  }

  const fundUpsert = await callSupabaseRest({
    env: params.env,
    method: "POST",
    pathWithQuery:
      "mpf_trustee_fund?on_conflict=trustee,scheme_code,fund_code",
    body: dedupFunds,
    prefer: "resolution=merge-duplicates,return=minimal",
  });
  if (!fundUpsert.ok) {
    throw new Error(
      `Fund upsert failed (${fundUpsert.status}): ${JSON.stringify(fundUpsert.data)}`,
    );
  }

  const fundLookup = await callSupabaseRest({
    env: params.env,
    method: "GET",
    pathWithQuery:
      "mpf_trustee_fund?select=id,scheme_code,fund_code" +
      `&trustee=eq.${params.trustee}` +
      `&scheme_code=eq.${encodeURIComponent(params.schemeCode)}` +
      "&limit=5000",
  });
  if (!fundLookup.ok || !Array.isArray(fundLookup.data)) {
    throw new Error(
      `Fund lookup failed (${fundLookup.status}): ${JSON.stringify(fundLookup.data)}`,
    );
  }

  const fundIdMap = new Map<string, string>();
  for (const row of fundLookup.data as {
    id: string;
    scheme_code: string;
    fund_code: string;
  }[]) {
    fundIdMap.set(`${row.scheme_code}::${row.fund_code}`, row.id);
  }

  const dedupPriceMap = new Map<
    string,
    {
      fund_id: string;
      price_date: string;
      bid_price: number | null;
      offer_price: number | null;
      currency_code: string;
      source: string;
    }
  >();
  for (const seed of params.priceSeedRows) {
    const fundId = fundIdMap.get(`${seed.schemeCode}::${seed.fundCode}`);
    if (!fundId) {
      continue;
    }
    dedupPriceMap.set(`${fundId}::${seed.priceDate}`, {
      fund_id: fundId,
      price_date: seed.priceDate,
      bid_price: seed.bidPrice,
      offer_price: seed.offerPrice,
      currency_code: seed.currencyCode,
      source: seed.source,
    });
  }
  const dedupPrices = [...dedupPriceMap.values()];

  if (dedupPrices.length > 0) {
    const priceUpsert = await callSupabaseRest({
      env: params.env,
      method: "POST",
      pathWithQuery:
        "mpf_trustee_fund_price?on_conflict=fund_id,price_date",
      body: dedupPrices,
      prefer: "resolution=merge-duplicates,return=minimal",
    });
    if (!priceUpsert.ok) {
      throw new Error(
        `Price upsert failed (${priceUpsert.status}): ${JSON.stringify(priceUpsert.data)}`,
      );
    }
  }

  const latestDataDate = maxDate(dedupPrices.map((row) => row.price_date));
  const payloadHash = await hashText(
    JSON.stringify({
      source: params.sourceUrl,
      trustee: params.trustee,
      schemeCode: params.schemeCode,
      funds: dedupFunds.length,
      prices: dedupPrices.length,
      latestDataDate,
    }),
  );

  return {
    rowsRead: params.priceSeedRows.length,
    rowsUpserted: dedupPrices.length,
    latestDataDate,
    payloadHash,
    payload: {
      fundsUpserted: dedupFunds.length,
      pricesUpserted: dedupPrices.length,
      latestDataDate,
    },
  };
}

async function withIngestionRun(params: {
  env: Env;
  job: string;
  sourceUrl: string;
  execute: () => Promise<IngestionOutcome>;
}): Promise<JobResult> {
  const runId = await createIngestionRun(params.env, params.sourceUrl);
  try {
    const outcome = await params.execute();
    if (runId) {
      await updateIngestionRun(params.env, runId, {
        status: "success",
        payload_hash: outcome.payloadHash,
        rows_read: outcome.rowsRead,
        rows_upserted: outcome.rowsUpserted,
        latest_data_date: outcome.latestDataDate,
        completed_at: new Date().toISOString(),
      });
    }
    return {
      job: params.job,
      ok: true,
      skipped: outcome.skipped ?? false,
      rowsRead: outcome.rowsRead,
      rowsUpserted: outcome.rowsUpserted,
      latestDataDate: outcome.latestDataDate,
      payload: outcome.payload,
    };
  } catch (error) {
    if (runId) {
      await updateIngestionRun(params.env, runId, {
        status: "failed",
        error_message:
          error instanceof Error ? error.message : "Unknown ingestion error",
        completed_at: new Date().toISOString(),
      });
    }
    return {
      job: params.job,
      ok: false,
      detail: error instanceof Error ? error.message : "Unknown error",
    };
  }
}

async function runHsbcLikeIngest(params: {
  env: Env;
  enabled: boolean;
  job: string;
  trustee: Trustee;
  schemeCode: string;
  sourceUrl: string;
}): Promise<JobResult> {
  if (!params.enabled) {
    return { job: params.job, ok: true, skipped: true };
  }

  return withIngestionRun({
    env: params.env,
    job: params.job,
    sourceUrl: params.sourceUrl,
    execute: async () => {
      const response = await fetchWithRetry(
        params.sourceUrl,
        { headers: HSBC_API_HEADERS },
        { attempts: 3, initialDelayMs: 500 },
      );
      if (!response.ok) {
        throw new Error(
          `Failed to fetch ${params.job} source (${response.status} ${response.statusText})`,
        );
      }

      const jsonBody = (await response.json()) as {
        data?: {
          schemeInfos?: {
            schemeCode: string;
            schemeIdentifier: string;
            fundInfos: {
              fundCode: string;
              fundIdentifier: string;
              fundNames: { languageCode: string; value: string }[];
              riskLevelValue?: string;
              disFundIndicator?: boolean;
              disRelatedFundCode?: string;
            }[];
            fundPriceInfos: {
              fundCode: string;
              fundPrices: {
                priceDate: string;
                fundBuyPrice?: { amount?: string; fundCurrencyCode?: string };
                fundSellPrice?: { amount?: string; fundCurrencyCode?: string };
              }[];
            }[];
          }[];
        };
      };

      const scheme = jsonBody.data?.schemeInfos?.[0];
      if (!scheme) {
        throw new Error(`${params.job} did not return scheme info.`);
      }

      const fundRows: TrusteeFundUpsertRow[] = scheme.fundInfos.map((fund) => ({
        trustee: params.trustee,
        scheme_code: scheme.schemeCode,
        scheme_identifier: scheme.schemeIdentifier,
        fund_code: fund.fundCode,
        fund_identifier: fund.fundIdentifier,
        fund_name_en:
          sanitizeOfficialFundName(
            fund.fundNames.find((name) => name.languageCode === "en_US")?.value,
          ) ?? fund.fundCode,
        fund_name_zh_hk: sanitizeOfficialFundName(
          fund.fundNames.find((name) => name.languageCode === "zh_HK")?.value,
        ),
        risk_level: fund.riskLevelValue ?? null,
        dis_fund_indicator: Boolean(fund.disFundIndicator),
        dis_related_fund_code: fund.disRelatedFundCode ?? null,
      }));

      const fundCodeSet = new Set(fundRows.map((row) => row.fund_code));
      const priceSeedRows: TrusteePriceSeedRow[] = scheme.fundPriceInfos.flatMap((item) => {
        if (!fundCodeSet.has(item.fundCode)) {
          return [];
        }
        const latest = item.fundPrices[0];
        if (!latest?.priceDate) {
          return [];
        }
        return [
          {
            schemeCode: scheme.schemeCode,
            fundCode: item.fundCode,
            priceDate: parseHsbcPriceDate(latest.priceDate),
            bidPrice: parseNav(latest.fundBuyPrice?.amount),
            offerPrice: parseNav(latest.fundSellPrice?.amount),
            currencyCode:
              latest.fundSellPrice?.fundCurrencyCode ??
              latest.fundBuyPrice?.fundCurrencyCode ??
              "HKD",
            source: `${params.trustee}_api_latest`,
          },
        ];
      });

      return upsertTrusteeFundsAndPrices({
        env: params.env,
        trustee: params.trustee,
        schemeCode: scheme.schemeCode,
        sourceUrl: params.sourceUrl,
        fundRows,
        priceSeedRows,
      });
    },
  });
}

async function runManulifeIngest(env: Env): Promise<JobResult> {
  if (!isEnabled(env.ENABLE_MANULIFE_ETL, true)) {
    return { job: "manulife.daily", ok: true, skipped: true };
  }

  return withIngestionRun({
    env,
    job: "manulife.daily",
    sourceUrl: MANULIFE_FUNDS_LIST_API_URL,
    execute: async () => {
      const response = await fetchWithRetry(
        MANULIFE_FUNDS_LIST_API_URL,
        {
          headers: {
            "user-agent":
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            accept: "application/json,text/plain,*/*",
            "accept-language": "en-HK,en;q=0.9,zh-HK;q=0.8",
          },
        },
        { attempts: 5, initialDelayMs: 700 },
      );
      if (!response.ok) {
        throw new Error(
          `Failed to fetch Manulife funds list (${response.status} ${response.statusText})`,
        );
      }

      const funds = (await response.json()) as {
        fundId: string;
        fundName?: string;
        fundNameEn?: string;
        displayName?: string;
        riskRating?: string;
        currency?: string;
        nav?: { asOfDate?: string; price?: string };
        productsId?: string[];
        products?: { id?: number; name?: string }[];
      }[];

      const schemeCode = "MGS";
      const fundRows: TrusteeFundUpsertRow[] = funds
        .filter((fund) => Boolean(fund.fundId))
        .map((fund) => {
          const product = fund.products?.[0];
          return {
            trustee: "manulife",
            scheme_code: schemeCode,
            scheme_identifier:
              product?.id !== undefined
                ? String(product.id)
                : (fund.productsId?.[0] ?? null),
            fund_code: fund.fundId,
            fund_identifier: fund.fundId,
            fund_name_en:
              sanitizeOfficialFundName(
                fund.fundNameEn ?? fund.fundName ?? fund.displayName,
              ) ?? fund.fundId,
            risk_level: fund.riskRating ?? null,
          };
        });

      const priceSeedRows: TrusteePriceSeedRow[] = funds.flatMap((fund) => {
        const asOfDate = fund.nav?.asOfDate;
        const price = parseNav(fund.nav?.price);
        if (!asOfDate || price === null) {
          return [];
        }
        return [
          {
            schemeCode,
            fundCode: fund.fundId,
            priceDate: asOfDate,
            bidPrice: price,
            offerPrice: null,
            currencyCode: fund.currency ?? "HKD",
            source: "manulife_api_latest",
          },
        ];
      });

      return upsertTrusteeFundsAndPrices({
        env,
        trustee: "manulife",
        schemeCode,
        sourceUrl: MANULIFE_FUNDS_LIST_API_URL,
        fundRows,
        priceSeedRows,
      });
    },
  });
}

async function runAiaIngest(env: Env): Promise<JobResult> {
  if (!isEnabled(env.ENABLE_AIA_ETL, true)) {
    return { job: "aia.daily", ok: true, skipped: true };
  }

  return withIngestionRun({
    env,
    job: "aia.daily",
    sourceUrl: MPFA_MPP_LIST_EN_URL,
    execute: async () => {
      const body = new URLSearchParams({ trustees: MPFA_AIA_TRUSTEE_ID }).toString();
      const [enResponse, zhHkResponse] = await Promise.all([
        fetchWithRetry(MPFA_MPP_LIST_EN_URL, {
          method: "POST",
          headers: { "content-type": "application/x-www-form-urlencoded" },
          body,
        }),
        fetchWithRetry(MPFA_MPP_LIST_ZH_HK_URL, {
          method: "POST",
          headers: { "content-type": "application/x-www-form-urlencoded" },
          body,
        }),
      ]);

      if (!enResponse.ok || !zhHkResponse.ok) {
        throw new Error(
          `AIA source fetch failed (EN ${enResponse.status}, ZH ${zhHkResponse.status})`,
        );
      }

      const [enHtml, zhHkHtml] = await Promise.all([enResponse.text(), zhHkResponse.text()]);
      const enRows = parseMpfaMppFundRows(enHtml);
      const zhHkRows = parseMpfaMppFundRows(zhHkHtml);
      const zhMap = new Map(zhHkRows.map((row) => [row.cfId, row]));

      const fundRows: TrusteeFundUpsertRow[] = enRows.map((row) => ({
        trustee: "aia",
        scheme_code: AIA_SCHEME_CODE,
        scheme_identifier: row.schemeName ?? "AIA MPF - Prime Value Choice",
        fund_code: row.cfId,
        fund_identifier: row.cfId,
        fund_name_en: row.fundName ?? `AIA Fund ${row.cfId}`,
        fund_name_zh_hk: zhMap.get(row.cfId)?.fundName ?? null,
      }));

      const outcome = await upsertTrusteeFundsAndPrices({
        env,
        trustee: "aia",
        schemeCode: AIA_SCHEME_CODE,
        sourceUrl: MPFA_MPP_LIST_EN_URL,
        fundRows,
        priceSeedRows: [],
      });

      return {
        ...outcome,
        rowsRead: fundRows.length,
        rowsUpserted: 0,
        latestDataDate: null,
        payload: {
          mode: "metadata_only",
          source: "mpfa_mpp_list",
          fundsUpserted: fundRows.length,
          pricesUpserted: 0,
        },
      };
    },
  });
}

async function loadSunLifeDailySnapshots(
  browserBinding: Fetcher,
): Promise<{
  snapshots: {
    schemeCode: string;
    schemeNameEn: string;
    schemeNameZhHk: string | null;
    valuationDateIso: string;
    rows: {
      fundCode: string;
      fundNameEn: string | null;
      fundNameZhHk: string | null;
      nav: number | null;
      valuationDateIso: string | null;
    }[];
  }[];
}> {
  const attempts = 4;
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    let browser: Awaited<ReturnType<typeof puppeteer.launch>> | null = null;
    try {
      browser = await puppeteer.launch(browserBinding);
      const page = await browser.newPage();
      await page.setViewport({ width: 1366, height: 768, deviceScaleFactor: 1 });
      await page.setUserAgent(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
      );
      await page.setExtraHTTPHeaders({
        "accept-language": "en-HK,en;q=0.9,zh-HK;q=0.8",
        "cache-control": "no-cache",
        pragma: "no-cache",
      });
      await page.evaluateOnNewDocument(() => {
        Object.defineProperty(navigator, "webdriver", { get: () => undefined });
        Object.defineProperty(navigator, "languages", { get: () => ["en-HK", "en", "zh-HK"] });
        Object.defineProperty(navigator, "platform", { get: () => "MacIntel" });
      });

      await page.goto(SUNLIFE_PAGE_URL, { waitUntil: "domcontentloaded", timeout: 45_000 });
      await page.waitForSelector("body", { timeout: 30_000 });
      await page.mouse.move(240, 180);
      await scheduler.wait(1200);

      const data = await page.evaluate(async () => {
      type SchemeRow = {
        LINE_OF_BUSINESS?: string;
        LINE_OF_BUSINESS_EN?: string;
        LINE_OF_BUSINESS_ZH?: string;
      };

      const post = async (payload: unknown) => {
        const response = await fetch("/webservice/getmpforsofunddata", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(payload),
          credentials: "include",
        });
        if (!response.ok) {
          throw new Error(`Sun Life API ${response.status} ${response.statusText}`);
        }
        return (await response.json()) as unknown;
      };

      const toIso = (value: string) => {
        const [dd, mm, yyyy] = value.split("/");
        if (!dd || !mm || !yyyy) {
          return null;
        }
        return `${yyyy.padStart(4, "0")}-${mm.padStart(2, "0")}-${dd.padStart(2, "0")}`;
      };

      const schemes = (await post({
        domainAndUserName: "hkportal",
        sqlKey: "MPF_SCHEME",
      })) as SchemeRow[];

      const snapshots: {
        schemeCode: string;
        schemeNameEn: string;
        schemeNameZhHk: string | null;
        valuationDateIso: string;
        rows: {
          fundCode: string;
          fundNameEn: string | null;
          fundNameZhHk: string | null;
          nav: number | null;
          valuationDateIso: string | null;
        }[];
      }[] = [];

      for (const scheme of schemes ?? []) {
        const schemeCode = scheme.LINE_OF_BUSINESS;
        if (!schemeCode) {
          continue;
        }

        const valuationResult = (await post({
          domainAndUserName: "hkportal",
          sqlKey: "MPF_LAST_VALUATION_DT",
          sqlParams: [schemeCode],
        })) as { LAST_VALUATION_DT?: string }[];
        const valuationRaw = valuationResult?.[0]?.LAST_VALUATION_DT;
        const valuationDateIso = valuationRaw ? toIso(valuationRaw) : null;
        if (!valuationDateIso) {
          continue;
        }

        const dailyRows = (await post({
          domainAndUserName: "hkportal",
          sqlKey: "MPF_DAILYPRICE",
          sqlParams: [valuationDateIso, schemeCode],
        })) as {
          FUND_CD?: string;
          FUND_DESCRIPTION_EN?: string;
          FUND_DESCRIPTION_ZH?: string;
          NAV?: number | string | null;
          VALUATION_DT?: string;
        }[];

        snapshots.push({
          schemeCode,
          schemeNameEn: scheme.LINE_OF_BUSINESS_EN ?? schemeCode,
          schemeNameZhHk: scheme.LINE_OF_BUSINESS_ZH ?? null,
          valuationDateIso,
          rows: (dailyRows ?? []).map((row) => ({
            fundCode: row.FUND_CD ?? "",
            fundNameEn: row.FUND_DESCRIPTION_EN ?? null,
            fundNameZhHk: row.FUND_DESCRIPTION_ZH ?? null,
            nav:
              typeof row.NAV === "number"
                ? row.NAV
                : typeof row.NAV === "string"
                  ? Number.parseFloat(row.NAV.replace(/,/g, ""))
                  : null,
            valuationDateIso: row.VALUATION_DT ? toIso(row.VALUATION_DT) : null,
          })),
        });
      }

        return { snapshots };
      });

      return data;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt < attempts) {
        const lower = 1200 * attempt;
        const upper = 2400 * attempt;
        const delayMs = lower + Math.floor(Math.random() * (upper - lower + 1));
        await scheduler.wait(delayMs);
      }
    } finally {
      if (browser) {
        await browser.close();
      }
    }
  }

  throw new Error(
    `Sun Life browser ingest failed after ${attempts} attempts: ${lastError?.message ?? "unknown error"}`,
  );
}

async function runSunLifeBrowserIngest(env: Env): Promise<JobResult> {
  if (!isEnabled(env.ENABLE_SUNLIFE_BROWSER_ETL, true)) {
    return { job: "sunlife.browser.daily", ok: true, skipped: true };
  }
  if (!env.MYBROWSER) {
    return {
      job: "sunlife.browser.daily",
      ok: false,
      detail: "Missing MYBROWSER browser binding.",
    };
  }

  return withIngestionRun({
    env,
    job: "sunlife.browser.daily",
    sourceUrl: SUNLIFE_SOURCE_URL,
    execute: async () => {
      const loaded = await loadSunLifeDailySnapshots(env.MYBROWSER as Fetcher);

      const fundRows: TrusteeFundUpsertRow[] = [];
      const priceSeedRows: TrusteePriceSeedRow[] = [];

      for (const snapshot of (loaded?.snapshots ?? [])) {
        for (const row of snapshot.rows) {
          if (!row.fundCode) {
            continue;
          }
          fundRows.push({
            trustee: "sunlife",
            scheme_code: snapshot.schemeCode,
            scheme_identifier: snapshot.schemeNameEn,
            fund_code: row.fundCode,
            fund_identifier: row.fundCode,
            fund_name_en: sanitizeOfficialFundName(row.fundNameEn) ?? row.fundCode,
            fund_name_zh_hk: sanitizeOfficialFundName(row.fundNameZhHk),
          });

          const nav = parseNav(row.nav);
          const date = row.valuationDateIso ?? snapshot.valuationDateIso;
          if (!date || nav === null) {
            continue;
          }
          priceSeedRows.push({
            schemeCode: snapshot.schemeCode,
            fundCode: row.fundCode,
            priceDate: date,
            bidPrice: nav,
            offerPrice: null,
            currencyCode: "HKD",
            source: "sunlife_browser_daily",
          });
        }
      }

      // Sun Life has multiple scheme codes; upsert one scheme at a time.
      const byScheme = new Map<string, { funds: TrusteeFundUpsertRow[]; prices: TrusteePriceSeedRow[] }>();
      for (const row of fundRows) {
        const bucket = byScheme.get(row.scheme_code) ?? { funds: [], prices: [] };
        bucket.funds.push(row);
        byScheme.set(row.scheme_code, bucket);
      }
      for (const row of priceSeedRows) {
        const bucket = byScheme.get(row.schemeCode) ?? { funds: [], prices: [] };
        bucket.prices.push(row);
        byScheme.set(row.schemeCode, bucket);
      }

      let totalRead = 0;
      let totalUpserted = 0;
      const latestDates: string[] = [];
      const payloads: unknown[] = [];

      for (const [schemeCode, bucket] of byScheme.entries()) {
        const outcome = await upsertTrusteeFundsAndPrices({
          env,
          trustee: "sunlife",
          schemeCode,
          sourceUrl: SUNLIFE_SOURCE_URL,
          fundRows: bucket.funds,
          priceSeedRows: bucket.prices,
        });
        totalRead += outcome.rowsRead;
        totalUpserted += outcome.rowsUpserted;
        if (outcome.latestDataDate) {
          latestDates.push(outcome.latestDataDate);
        }
        payloads.push({ schemeCode, ...outcome.payload });
      }

      return {
        rowsRead: totalRead,
        rowsUpserted: totalUpserted,
        latestDataDate: maxDate(latestDates),
        payloadHash: await hashText(JSON.stringify(payloads)),
        payload: payloads,
      };
    },
  });
}

async function runAllJobs(
  env: Env,
  trigger: TriggerSource,
): Promise<OrchestratorResult> {
  assertRequiredEnv(env);
  const startedAt = new Date().toISOString();

  const jobs: JobResult[] = [];
  jobs.push(
    await runHsbcLikeIngest({
      env,
      enabled: isEnabled(env.ENABLE_HSBC_ETL, true),
      job: "hsbc.daily",
      trustee: "hsbc",
      schemeCode: "HB",
      sourceUrl: HSBC_FUNDS_API_URL,
    }),
  );
  jobs.push(
    await runHsbcLikeIngest({
      env,
      enabled: isEnabled(env.ENABLE_HANGSENG_ETL, true),
      job: "hangseng.daily",
      trustee: "hangseng",
      schemeCode: "HS",
      sourceUrl: HANG_SENG_FUNDS_API_URL,
    }),
  );
  jobs.push(await runManulifeIngest(env));
  jobs.push(await runAiaIngest(env));
  jobs.push(await runSunLifeBrowserIngest(env));

  const finishedAt = new Date().toISOString();
  return {
    trigger,
    startedAt,
    finishedAt,
    ok: jobs.every((job) => job.ok),
    jobs,
  };
}

function isAuthorizedManualRequest(request: Request, env: Env): boolean {
  if (!env.MANUAL_TRIGGER_TOKEN) {
    return false;
  }
  const authHeader = request.headers.get("authorization");
  if (!authHeader?.startsWith("Bearer ")) {
    return false;
  }
  return authHeader.slice("Bearer ".length) === env.MANUAL_TRIGGER_TOKEN;
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/health") {
      return json({ ok: true, service: "mpf-etl-orchestrator" }, { status: 200 });
    }

    if (request.method === "POST" && url.pathname === "/run") {
      if (!isAuthorizedManualRequest(request, env)) {
        return json({ ok: false, error: "Unauthorized." }, { status: 401 });
      }
      const runPromise = runAllJobs(env, "manual");
      ctx.waitUntil(runPromise);
      const result = await runPromise;
      return json(result, { status: result.ok ? 200 : 500 });
    }

    return json({ ok: false, error: "Not Found" }, { status: 404 });
  },

  async scheduled(event, env, ctx): Promise<void> {
    const runPromise = runAllJobs(env, "scheduled").then((result) => {
      console.log(
        JSON.stringify(
          {
            message: "MPF ETL scheduled run completed.",
            schedule: event.cron,
            result,
          },
          null,
          2,
        ),
      );
    });
    ctx.waitUntil(runPromise);
  },
} satisfies ExportedHandler<Env>;
