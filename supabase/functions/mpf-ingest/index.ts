import { createClient } from "npm:@supabase/supabase-js@2";

const MPFA_DATASET_URL =
  "https://www.mpfa.org.hk/en/-/media/files/information-centre/research-and-statistics/other-reports/statistics/net_asset_values_of_approved_constituent_funds02_en.csv";

const categoryColumnMap = {
  equity: "Equity fund (HK$million)",
  mixed_assets: "Mixed assets fund (HK$million)",
  bond: "Bond fund (HK$million)",
  guaranteed: "Guaranteed fund (HK$million)",
  money_market_mpf_conservative:
    "Money market fund-MPF conservative fund (HK$million)",
  money_market_other:
    "Money market fund-other than MPF conservative fund (HK$million)",
} as const;

type Category = keyof typeof categoryColumnMap;

function parseCsvLine(line: string): string[] {
  const fields: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"' && next === '"') {
      current += '"';
      i++;
      continue;
    }

    if (char === '"') {
      inQuotes = !inQuotes;
      continue;
    }

    if (char === "," && !inQuotes) {
      fields.push(current.trim());
      current = "";
      continue;
    }

    current += char;
  }

  fields.push(current.trim());
  return fields;
}

function parseCsv(content: string): string[][] {
  return content
    .replaceAll("\r\n", "\n")
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map(parseCsvLine);
}

function parseMpfDate(value: string): string {
  const [dd, mm, yyyy] = value.split("-");
  if (!dd || !mm || !yyyy) {
    throw new Error(`Invalid MPFA date value: ${value}`);
  }
  return `${yyyy}-${mm.padStart(2, "0")}-${dd.padStart(2, "0")}`;
}

function parseNumeric(value: string): number {
  const parsed = Number.parseFloat(value.replaceAll(",", ""));
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid numeric value: ${value}`);
  }
  return parsed;
}

async function hashText(text: string): Promise<string> {
  const bytes = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

Deno.serve(async (req) => {
  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

    if (!supabaseUrl || !serviceRoleKey) {
      return new Response(
        JSON.stringify({
          ok: false,
          error: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.",
        }),
        { status: 500, headers: { "Content-Type": "application/json" } },
      );
    }

    const body =
      req.method === "POST" ? await req.json().catch(() => ({})) : {};
    const sourceUrl =
      typeof body?.sourceUrl === "string" && body.sourceUrl.length > 0
        ? body.sourceUrl
        : MPFA_DATASET_URL;

    const supabase = createClient(supabaseUrl, serviceRoleKey);

    const runInsert = await supabase
      .from("mpf_ingestion_run")
      .insert({
        source_url: sourceUrl,
        status: "running",
      })
      .select("id")
      .single();

    if (runInsert.error || !runInsert.data) {
      throw new Error(runInsert.error?.message ?? "Failed to create run.");
    }

    const runId = runInsert.data.id as string;

    try {
      const response = await fetch(sourceUrl);
      if (!response.ok) {
        throw new Error(
          `Failed to fetch MPFA dataset (${response.status} ${response.statusText})`,
        );
      }

      const csv = await response.text();
      const payloadHash = await hashText(csv);
      const lines = parseCsv(csv);

      if (lines.length <= 1) {
        throw new Error("MPFA dataset returned no data rows.");
      }

      const latestHashResult = await supabase
        .from("mpf_ingestion_run")
        .select("payload_hash")
        .eq("status", "success")
        .not("payload_hash", "is", null)
        .order("created_at", { ascending: false })
        .limit(1)
        .maybeSingle();

      if (latestHashResult.error) {
        throw new Error(latestHashResult.error.message);
      }

      if (latestHashResult.data?.payload_hash === payloadHash) {
        await supabase
          .from("mpf_ingestion_run")
          .update({
            status: "success",
            payload_hash: payloadHash,
            rows_read: lines.length - 1,
            rows_upserted: 0,
            completed_at: new Date().toISOString(),
          })
          .eq("id", runId);

        return new Response(
          JSON.stringify({
            ok: true,
            skipped: true,
            reason: "No data change (same CSV hash).",
            rowsRead: lines.length - 1,
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }

      const header = lines[0] ?? [];
      const headerIndex = new Map(header.map((name, index) => [name, index]));
      const asAtIndex = headerIndex.get("As at");
      if (asAtIndex === undefined) {
        throw new Error('MPFA dataset does not include "As at" column.');
      }

      const upsertRows: {
        as_of_date: string;
        category: Category;
        net_asset_value_million: number;
        source: string;
      }[] = [];

      for (const line of lines.slice(1)) {
        const dateValue = line[asAtIndex];
        if (!dateValue) {
          continue;
        }
        const asOfDate = parseMpfDate(dateValue);

        for (const [category, columnName] of Object.entries(categoryColumnMap) as [
          Category,
          string,
        ][]) {
          const columnIndex = headerIndex.get(columnName);
          if (columnIndex === undefined) {
            throw new Error(`Missing column in MPFA dataset: ${columnName}`);
          }

          upsertRows.push({
            as_of_date: asOfDate,
            category,
            net_asset_value_million: parseNumeric(line[columnIndex] ?? "0"),
            source: "mpfa_official_dataset",
          });
        }
      }

      const upsertResult = await supabase
        .from("mpf_category_nav")
        .upsert(upsertRows, { onConflict: "as_of_date,category" });

      if (upsertResult.error) {
        throw new Error(upsertResult.error.message);
      }

      const latestDataDate = upsertRows.reduce(
        (latest, row) => (row.as_of_date > latest ? row.as_of_date : latest),
        upsertRows[0]?.as_of_date ?? null,
      );

      await supabase
        .from("mpf_ingestion_run")
        .update({
          status: "success",
          payload_hash: payloadHash,
          rows_read: lines.length - 1,
          rows_upserted: upsertRows.length,
          latest_data_date: latestDataDate,
          completed_at: new Date().toISOString(),
        })
        .eq("id", runId);

      return new Response(
        JSON.stringify({
          ok: true,
          skipped: false,
          rowsRead: lines.length - 1,
          rowsUpserted: upsertRows.length,
          latestDataDate,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    } catch (error) {
      await supabase
        .from("mpf_ingestion_run")
        .update({
          status: "failed",
          error_message:
            error instanceof Error ? error.message : "Unknown ingestion error",
          completed_at: new Date().toISOString(),
        })
        .eq("id", runId);
      throw error;
    }
  } catch (error) {
    return new Response(
      JSON.stringify({
        ok: false,
        error: error instanceof Error ? error.message : "Unknown error",
      }),
      { status: 500, headers: { "Content-Type": "application/json" } },
    );
  }
});
