import { createHash } from "node:crypto";

import type { TRPCRouterRecord } from "@trpc/server";
import { and, asc, desc, eq, gte, inArray, lte, sql } from "@acme/db";
import {
  MpfCategoryNav,
  MpfContribution,
  MpfIngestionRun,
  MpfTrusteeFund,
  MpfTrusteeFundPrice,
} from "@acme/db/schema";
import { z } from "zod/v4";

import { protectedProcedure, publicProcedure } from "../trpc";

const MPFA_DATASET_URL =
  "https://www.mpfa.org.hk/en/-/media/files/information-centre/research-and-statistics/other-reports/statistics/net_asset_values_of_approved_constituent_funds02_en.csv";
const HSBC_FUNDS_API_URL =
  "https://rbwm-api.hsbc.com.hk/wpb-gpbw-mmw-hk-hbap-pa-p-wpp-mpf-market-data-prod-proxy/v1/funds?schemeCodes=HB&includes=fundPrice";
const HSBC_DOWNLOAD_API_BASE =
  "https://rbwm-api.hsbc.com.hk/wpb-gpbw-mmw-hk-hbap-pa-p-wpp-mpf-market-data-prod-proxy/v1/download-funds";
const HANG_SENG_FUNDS_API_URL =
  "https://rbwm-api.hsbc.com.hk/wpb-gpbw-mmw-hk-hase-pa-p-wpp-mpf-market-data-prod-proxy/v1/funds?schemeCodes=HS&includes=fundPrice";
const HANG_SENG_DOWNLOAD_API_BASE =
  "https://rbwm-api.hsbc.com.hk/wpb-gpbw-mmw-hk-hase-pa-p-wpp-mpf-market-data-prod-proxy/v1/download-funds";
const MANULIFE_FUNDS_LIST_API_URL =
  "https://www.manulife.com.hk/bin/funds/fundslist?productLine=mpf&overrideLocale=en_HK";
const MANULIFE_FUND_HISTORY_API_BASE =
  "https://www.manulife.com.hk/bin/funds/fundhistory";
const MPFA_MPP_LIST_EN_URL = "https://mfp.mpfa.org.hk/eng/mpp_list.jsp";
const MPFA_MPP_LIST_ZH_HK_URL = "https://mfp.mpfa.org.hk/tch/mpp_list.jsp";
const MPFA_SUNLIFE_TRUSTEE_ID = "17";
const HSBC_API_HEADERS = {
  "x-hsbc-channel-id": "WEB",
  client_id: "5eca677638ab454086052a18da4e2cb0",
  client_secret: "d35073Cf96B64b1E9CE25f4E07746300",
} as const;

const categoryValues = [
  "equity",
  "mixed_assets",
  "bond",
  "guaranteed",
  "money_market_mpf_conservative",
  "money_market_other",
] as const;

type MpfFundCategoryValue = (typeof categoryValues)[number];

const categorySchema = z.enum(categoryValues);

const categoryColumnMap: Record<MpfFundCategoryValue, string> = {
  equity: "Equity fund (HK$million)",
  mixed_assets: "Mixed assets fund (HK$million)",
  bond: "Bond fund (HK$million)",
  guaranteed: "Guaranteed fund (HK$million)",
  money_market_mpf_conservative:
    "Money market fund-MPF conservative fund (HK$million)",
  money_market_other:
    "Money market fund-other than MPF conservative fund (HK$million)",
};

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
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map(parseCsvLine);
}

function stripHtmlTags(input: string): string {
  return input
    .replace(/<[^>]*>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

function parseMpfaFundRows(html: string): {
  cfId: string;
  schemeName: string | null;
  fundName: string | null;
}[] {
  const rows: { cfId: string; schemeName: string | null; fundName: string | null }[] = [];
  const rowRegex = /<tr class="wr"[\s\S]*?<\/tr>/g;
  const cfIdRegex = /name="sortlist_checkbox"[\s\S]*?value="(\d+)"/;
  const schemeRegex = /class="txt">([\s\S]*?)<\/td>/;
  const fundRegex = /class="table">([\s\S]*?)<\/td>/;
  const rowBlocks = html.match(rowRegex) ?? [];

  for (const row of rowBlocks) {
    const cfId = cfIdRegex.exec(row)?.[1];
    const schemeRaw = schemeRegex.exec(row)?.[1];
    const fundRaw = fundRegex.exec(row)?.[1];
    if (!cfId) {
      continue;
    }

    const schemeName = sanitizeOfficialFundName(stripHtmlTags(schemeRaw ?? ""));
    const fundName = sanitizeOfficialFundName(stripHtmlTags(fundRaw ?? ""));
    rows.push({ cfId, schemeName, fundName });
  }

  return rows;
}

function addDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

function dateToIso(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseHsbcPriceDate(value: string): string {
  return value.slice(0, 10);
}

function parseHsbcHistoryCsv(csvText: string): {
  fundName: string;
  prices: { priceDate: string; bidPrice: number | null; offerPrice: number | null }[];
} {
  const rows = parseCsv(csvText);
  const datesRow = rows[0] ?? [];
  const valueHeaderRow = rows[1] ?? [];
  const valuesRow = rows[2] ?? [];

  if (datesRow.length < 3 || valueHeaderRow.length < 3 || valuesRow.length < 3) {
    return { fundName: "", prices: [] };
  }

  const fundName = valuesRow[0] ?? "";
  const prices: {
    priceDate: string;
    bidPrice: number | null;
    offerPrice: number | null;
  }[] = [];

  for (let i = 1; i < datesRow.length - 1; i += 2) {
    const dateValue = datesRow[i];
    const bidLabel = valueHeaderRow[i];
    const offerLabel = valueHeaderRow[i + 1];
    const bidRaw = valuesRow[i];
    const offerRaw = valuesRow[i + 1];

    if (!dateValue || !bidLabel || !offerLabel) {
      continue;
    }

    if (
      bidLabel.toUpperCase() !== "BID" ||
      offerLabel.toUpperCase() !== "OFFER" ||
      datesRow[i] !== datesRow[i + 1]
    ) {
      continue;
    }

    const bidPrice = bidRaw ? Number.parseFloat(bidRaw) : null;
    const offerPrice = offerRaw ? Number.parseFloat(offerRaw) : null;

    prices.push({
      priceDate: dateValue,
      bidPrice: Number.isFinite(bidPrice ?? NaN) ? bidPrice : null,
      offerPrice: Number.isFinite(offerPrice ?? NaN) ? offerPrice : null,
    });
  }

  return { fundName, prices };
}

function maxIsoDate(input: (string | null | undefined)[]): string | null {
  let max: string | null = null;
  for (const value of input) {
    if (!value) {
      continue;
    }
    if (!max || value > max) {
      max = value;
    }
  }
  return max;
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

function parseMpfDate(value: string): string {
  const [dd, mm, yyyy] = value.split("-");
  if (!dd || !mm || !yyyy) {
    throw new Error(`Invalid MPFA date value: ${value}`);
  }
  return `${yyyy}-${mm.padStart(2, "0")}-${dd.padStart(2, "0")}`;
}

function parseNumeric(value: string): number {
  const parsed = Number.parseFloat(value.replace(/,/g, ""));
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid numeric value: ${value}`);
  }
  return parsed;
}

function toIsoDate(value: string | Date): string {
  return typeof value === "string" ? value : value.toISOString().slice(0, 10);
}

function buildContributionBuckets(
  contributions: { contributionDate: string | Date; amount: number }[],
) {
  const byDate = new Map<string, number>();
  for (const item of contributions) {
    const date = toIsoDate(item.contributionDate);
    byDate.set(date, (byDate.get(date) ?? 0) + item.amount);
  }
  return byDate;
}

function normalizeWeights(
  input: { category: MpfFundCategoryValue; weight: number }[],
): { category: MpfFundCategoryValue; weight: number }[] {
  const sum = input.reduce((acc, item) => acc + item.weight, 0);
  if (sum <= 0) {
    throw new Error("Weight sum must be greater than zero.");
  }

  const normalizedFactor = Math.abs(sum - 100) < 0.0001 ? 100 : sum;
  return input.map((item) => ({
    category: item.category,
    weight: item.weight / normalizedFactor,
  }));
}

function monthAdd(date: Date): Date {
  const next = new Date(date);
  next.setMonth(next.getMonth() + 1);
  return next;
}

function createSyntheticMonthlyContributions(params: {
  startDate: string;
  endDate: string;
  amount: number;
}) {
  if (params.amount <= 0) {
    return [] as { contributionDate: string; amount: number }[];
  }

  const rows: { contributionDate: string; amount: number }[] = [];
  let cursor = new Date(params.startDate);
  const end = new Date(params.endDate);

  while (cursor <= end) {
    rows.push({
      contributionDate: cursor.toISOString().slice(0, 10),
      amount: params.amount,
    });
    cursor = monthAdd(cursor);
  }

  return rows;
}

function computeBacktestSeries(params: {
  navRows: {
    asOfDate: string | Date;
    category: MpfFundCategoryValue;
    netAssetValueMillion: number;
  }[];
  weights: { category: MpfFundCategoryValue; weight: number }[];
  contributions: { contributionDate: string | Date; amount: number }[];
  initialInvestment: number;
}) {
  const byDate = new Map<string, Map<MpfFundCategoryValue, number>>();
  for (const row of params.navRows) {
    const date = toIsoDate(row.asOfDate);
    const byCategory = byDate.get(date) ?? new Map<MpfFundCategoryValue, number>();
    byCategory.set(row.category, row.netAssetValueMillion);
    byDate.set(date, byCategory);
  }

  const dates = [...byDate.keys()].sort();
  if (dates.length < 2) {
    return {
      timeline: [] as {
        asOfDate: string;
        periodReturn: number;
        contribution: number;
        value: number;
      }[],
      finalValue: params.initialInvestment,
    };
  }

  const contributionByDate = buildContributionBuckets(params.contributions);

  let currentValue = params.initialInvestment;
  const firstDate = dates[0];
  if (!firstDate) {
    return { timeline: [], finalValue: currentValue };
  }
  currentValue += contributionByDate.get(firstDate) ?? 0;

  const timeline = [
    {
      asOfDate: firstDate,
      periodReturn: 0,
      contribution: contributionByDate.get(firstDate) ?? 0,
      value: currentValue,
    },
  ];

  for (let i = 1; i < dates.length; i++) {
    const previousDate = dates[i - 1];
    const currentDate = dates[i];
    if (!previousDate || !currentDate) {
      continue;
    }
    const prev = byDate.get(previousDate);
    const curr = byDate.get(currentDate);

    if (!prev || !curr) {
      continue;
    }

    let periodReturn = 0;

    for (const weight of params.weights) {
      const prevValue = prev.get(weight.category);
      const currValue = curr.get(weight.category);
      if (!prevValue || !currValue || prevValue <= 0) {
        continue;
      }
      periodReturn += weight.weight * (currValue / prevValue - 1);
    }

    currentValue *= 1 + periodReturn;

    let periodContribution = 0;
    for (const [contributionDate, amount] of contributionByDate.entries()) {
      if (contributionDate > previousDate && contributionDate <= currentDate) {
        periodContribution += amount;
      }
    }

    currentValue += periodContribution;

    timeline.push({
      asOfDate: currentDate,
      periodReturn,
      contribution: periodContribution,
      value: currentValue,
    });
  }

  return { timeline, finalValue: currentValue };
}

function xnpv(rate: number, cashflows: { date: Date; amount: number }[]) {
  const firstDate = cashflows[0]?.date;
  if (!firstDate) {
    return 0;
  }

  return cashflows.reduce((acc, flow) => {
    const years =
      (flow.date.getTime() - firstDate.getTime()) / (365 * 24 * 60 * 60 * 1000);
    return acc + flow.amount / (1 + rate) ** years;
  }, 0);
}

function computeXirr(cashflows: { date: Date; amount: number }[]) {
  const hasPositive = cashflows.some((item) => item.amount > 0);
  const hasNegative = cashflows.some((item) => item.amount < 0);

  if (!hasPositive || !hasNegative) {
    return null;
  }

  let rate = 0.1;
  for (let i = 0; i < 60; i++) {
    const f = xnpv(rate, cashflows);
    const derivative = (xnpv(rate + 1e-7, cashflows) - f) / 1e-7;

    if (!Number.isFinite(derivative) || Math.abs(derivative) < 1e-10) {
      break;
    }

    const next = rate - f / derivative;
    if (!Number.isFinite(next) || next <= -0.9999) {
      break;
    }

    if (Math.abs(next - rate) < 1e-8) {
      return next;
    }
    rate = next;
  }

  let low = -0.9999;
  let high = 10;
  let fLow = xnpv(low, cashflows);
  let fHigh = xnpv(high, cashflows);

  if (Math.sign(fLow) === Math.sign(fHigh)) {
    return null;
  }

  for (let i = 0; i < 200; i++) {
    const mid = (low + high) / 2;
    const fMid = xnpv(mid, cashflows);

    if (Math.abs(fMid) < 1e-8) {
      return mid;
    }

    if (Math.sign(fMid) === Math.sign(fLow)) {
      low = mid;
      fLow = fMid;
    } else {
      high = mid;
      fHigh = fMid;
    }

    if (Math.abs(high - low) < 1e-8 || Math.abs(fHigh - fLow) < 1e-8) {
      return mid;
    }
  }

  return null;
}

export const mpfRouter = {
  ingestOfficialDataset: protectedProcedure
    .input(
      z
        .object({
          sourceUrl: z.url().optional(),
        })
        .optional(),
    )
    .mutation(async ({ ctx, input }) => {
      const sourceUrl = input?.sourceUrl ?? MPFA_DATASET_URL;
      const run = await ctx.db
        .insert(MpfIngestionRun)
        .values({
          sourceUrl,
          status: "running",
        })
        .returning({ id: MpfIngestionRun.id });

      const runId = run[0]?.id;
      if (!runId) {
        throw new Error("Failed to create ingestion run.");
      }

      try {
        const response = await fetch(sourceUrl);
        if (!response.ok) {
          throw new Error(
            `Failed to fetch MPFA dataset (${response.status} ${response.statusText})`,
          );
        }

        const csv = await response.text();
      const csvHash = createHash("sha256").update(csv).digest("hex");
        const lines = parseCsv(csv);

        if (lines.length <= 1) {
          throw new Error("MPFA dataset returned no data rows.");
        }

        const header = lines[0] ?? [];
        const headerIndex = new Map(header.map((name, index) => [name, index]));

        const values = lines.slice(1).flatMap((line) => {
          const asAtIndex = headerIndex.get("As at");
          if (asAtIndex === undefined) {
            throw new Error('MPFA dataset does not include "As at" column.');
          }
          const dateValue = line[asAtIndex];
          if (!dateValue) {
            return [];
          }
          const asOfDate = parseMpfDate(dateValue);

          return categoryValues.map((category) => {
            const columnName = categoryColumnMap[category];
            const columnIndex = headerIndex.get(columnName);
            if (columnIndex === undefined) {
              throw new Error(`Missing column in MPFA dataset: ${columnName}`);
            }

            const numericValue = line[columnIndex];
            return {
              asOfDate,
              category,
              netAssetValueMillion: parseNumeric(numericValue ?? "0"),
              source: "mpfa_official_dataset",
            };
          });
        });

        if (values.length === 0) {
          throw new Error("No category NAV values were parsed from MPFA dataset.");
        }

        await ctx.db
          .insert(MpfCategoryNav)
          .values(values)
          .onConflictDoUpdate({
            target: [MpfCategoryNav.asOfDate, MpfCategoryNav.category],
            set: {
              netAssetValueMillion: sql`excluded.net_asset_value_million`,
              source: sql`excluded.source`,
              updatedAt: sql`now()`,
            },
          });

        const firstValue = values[0];
        if (!firstValue) {
          throw new Error("No MPFA values available after parsing.");
        }
        const latestDataDate = values.reduce(
          (latest, row) => (row.asOfDate > latest ? row.asOfDate : latest),
          firstValue.asOfDate,
        );

        await ctx.db
          .update(MpfIngestionRun)
          .set({
            status: "success",
            payloadHash: csvHash,
            rowsRead: lines.length - 1,
            rowsUpserted: values.length,
            latestDataDate,
            completedAt: new Date(),
          })
          .where(eq(MpfIngestionRun.id, runId));

        return {
          sourceUrl,
          rowsRead: lines.length - 1,
          rowsUpserted: values.length,
          latestDataDate,
        };
      } catch (error) {
        await ctx.db
          .update(MpfIngestionRun)
          .set({
            status: "failed",
            errorMessage:
              error instanceof Error ? error.message : "Unknown ingestion error",
            completedAt: new Date(),
          })
          .where(eq(MpfIngestionRun.id, runId));
        throw error;
      }
    }),

  latestSeries: publicProcedure
    .input(
      z
        .object({
          categories: z.array(categorySchema).min(1).optional(),
          limit: z.number().int().min(1).max(120).default(40),
        })
        .optional(),
    )
    .query(async ({ ctx, input }) => {
      const categories = input?.categories ?? categoryValues;
      const limit = input?.limit ?? 40;

      const rows = await ctx.db.query.MpfCategoryNav.findMany({
        where: inArray(MpfCategoryNav.category, [...categories]),
        orderBy: [asc(MpfCategoryNav.asOfDate)],
      });

      const grouped = new Map<string, Partial<Record<MpfFundCategoryValue, number>>>();
      for (const row of rows) {
        const date = toIsoDate(row.asOfDate);
        const current = grouped.get(date) ?? {};
        current[row.category] = row.netAssetValueMillion;
        grouped.set(date, current);
      }

      return [...grouped.entries()]
        .map(([asOfDate, values]) => ({ asOfDate, values }))
        .slice(-limit);
    }),

  compare: publicProcedure
    .input(
      z.object({
        categories: z.array(categorySchema).min(1).optional(),
        fromDate: z.string().date().optional(),
        toDate: z.string().date().optional(),
      }),
    )
    .query(async ({ ctx, input }) => {
      const categories = input.categories ?? categoryValues;
      const whereClauses = [inArray(MpfCategoryNav.category, [...categories])];

      if (input.fromDate) {
        whereClauses.push(gte(MpfCategoryNav.asOfDate, input.fromDate));
      }
      if (input.toDate) {
        whereClauses.push(lte(MpfCategoryNav.asOfDate, input.toDate));
      }

      const rows = await ctx.db.query.MpfCategoryNav.findMany({
        where: and(...whereClauses),
        orderBy: [asc(MpfCategoryNav.asOfDate)],
      });

      const firstByCategory = new Map<MpfFundCategoryValue, number>();
      const grouped = new Map<
        string,
        Partial<Record<MpfFundCategoryValue, { nav: number; cumulativeReturn: number }>>
      >();

      for (const row of rows) {
        const date = toIsoDate(row.asOfDate);
        if (!firstByCategory.has(row.category)) {
          firstByCategory.set(row.category, row.netAssetValueMillion);
        }
        const base = firstByCategory.get(row.category) ?? row.netAssetValueMillion;
        const cumulativeReturn =
          base > 0 ? row.netAssetValueMillion / base - 1 : 0;

        const current = grouped.get(date) ?? {};
        current[row.category] = {
          nav: row.netAssetValueMillion,
          cumulativeReturn,
        };
        grouped.set(date, current);
      }

      return {
        categories,
        series: [...grouped.entries()].map(([asOfDate, values]) => ({
          asOfDate,
          values,
        })),
      };
    }),

  addContribution: protectedProcedure
    .input(
      z.object({
        contributionDate: z.string().date(),
        amount: z.number().positive(),
        category: categorySchema.nullable().optional(),
        note: z.string().max(500).optional(),
      }),
    )
    .mutation(async ({ ctx, input }) => {
      const inserted = await ctx.db
        .insert(MpfContribution)
        .values({
          userId: ctx.session.user.id,
          contributionDate: input.contributionDate,
          amount: input.amount,
          category: input.category ?? null,
          note: input.note,
        })
        .returning({
          id: MpfContribution.id,
          contributionDate: MpfContribution.contributionDate,
          amount: MpfContribution.amount,
          category: MpfContribution.category,
          note: MpfContribution.note,
        });

      return inserted[0] ?? null;
    }),

  listContributions: protectedProcedure
    .input(
      z
        .object({
          fromDate: z.string().date().optional(),
          toDate: z.string().date().optional(),
        })
        .optional(),
    )
    .query(async ({ ctx, input }) => {
      const whereClauses = [eq(MpfContribution.userId, ctx.session.user.id)];
      if (input?.fromDate) {
        whereClauses.push(gte(MpfContribution.contributionDate, input.fromDate));
      }
      if (input?.toDate) {
        whereClauses.push(lte(MpfContribution.contributionDate, input.toDate));
      }

      return ctx.db.query.MpfContribution.findMany({
        where: and(...whereClauses),
        orderBy: [asc(MpfContribution.contributionDate)],
      });
    }),

  backtest: protectedProcedure
    .input(
      z.object({
        startDate: z.string().date(),
        endDate: z.string().date(),
        weights: z
          .array(
            z.object({
              category: categorySchema,
              weight: z.number().positive(),
            }),
          )
          .min(1),
        initialInvestment: z.number().nonnegative().default(0),
        monthlyContribution: z.number().nonnegative().default(0),
        useSavedContributions: z.boolean().default(true),
      }),
    )
    .query(async ({ ctx, input }) => {
      if (input.startDate >= input.endDate) {
        throw new Error("startDate must be earlier than endDate.");
      }

      const normalizedWeights = normalizeWeights(input.weights);
      const categories = normalizedWeights.map((item) => item.category);

      const navRows = await ctx.db.query.MpfCategoryNav.findMany({
        where: and(
          inArray(MpfCategoryNav.category, categories),
          gte(MpfCategoryNav.asOfDate, input.startDate),
          lte(MpfCategoryNav.asOfDate, input.endDate),
        ),
        orderBy: [asc(MpfCategoryNav.asOfDate)],
      });

      const savedContributions = input.useSavedContributions
        ? await ctx.db.query.MpfContribution.findMany({
            where: and(
              eq(MpfContribution.userId, ctx.session.user.id),
              gte(MpfContribution.contributionDate, input.startDate),
              lte(MpfContribution.contributionDate, input.endDate),
            ),
            columns: {
              contributionDate: true,
              amount: true,
            },
            orderBy: [asc(MpfContribution.contributionDate)],
          })
        : [];

      const synthetic = createSyntheticMonthlyContributions({
        startDate: input.startDate,
        endDate: input.endDate,
        amount: input.monthlyContribution,
      });

      const mergedContributions = [...savedContributions, ...synthetic];

      const result = computeBacktestSeries({
        navRows,
        weights: normalizedWeights,
        contributions: mergedContributions,
        initialInvestment: input.initialInvestment,
      });

      const totalContributed =
        input.initialInvestment +
        mergedContributions.reduce((acc, item) => acc + item.amount, 0);

      const cashflows = [
        ...mergedContributions.map((item) => ({
          date: new Date(toIsoDate(item.contributionDate)),
          amount: -item.amount,
        })),
        { date: new Date(input.startDate), amount: -input.initialInvestment },
      ];

      const finalPoint = result.timeline[result.timeline.length - 1];
      if (finalPoint) {
        cashflows.push({
          date: new Date(finalPoint.asOfDate),
          amount: finalPoint.value,
        });
      }

      const xirr = computeXirr(cashflows);

      return {
        assumptions: {
          source: "mpfa_official_dataset",
          frequency: "dataset_frequency",
        },
        summary: {
          points: result.timeline.length,
          totalContributed,
          finalValue: result.finalValue,
          totalReturn:
            totalContributed > 0 ? result.finalValue / totalContributed - 1 : 0,
          xirr,
        },
        timeline: result.timeline,
      };
    }),

  ingestHsbcFunds: protectedProcedure
    .input(
      z
        .object({
          fromDate: z.string().date().optional(),
          toDate: z.string().date().optional(),
          fundCodes: z.array(z.string().min(1)).optional(),
          language: z.enum(["en_US", "zh_HK", "zh_CN"]).default("en_US"),
        })
        .optional(),
    )
    .mutation(async ({ ctx, input }) => {
      const toDate = input?.toDate ?? dateToIso(new Date());
      const fromDate =
        input?.fromDate ?? dateToIso(addDays(new Date(toDate), -365));
      const language = input?.language ?? "en_US";

      if (fromDate > toDate) {
        throw new Error("fromDate must be earlier than or equal to toDate.");
      }

      const run = await ctx.db
        .insert(MpfIngestionRun)
        .values({
          sourceUrl: HSBC_FUNDS_API_URL,
          status: "running",
        })
        .returning({ id: MpfIngestionRun.id });

      const runId = run[0]?.id;
      if (!runId) {
        throw new Error("Failed to create HSBC ingestion run.");
      }

      try {
        const fundsResponse = await fetch(HSBC_FUNDS_API_URL, {
          headers: HSBC_API_HEADERS,
        });

        if (!fundsResponse.ok) {
          throw new Error(
            `Failed to fetch HSBC funds API (${fundsResponse.status} ${fundsResponse.statusText})`,
          );
        }

        const fundsJson = (await fundsResponse.json()) as {
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

        const scheme = fundsJson.data?.schemeInfos?.[0];
        if (!scheme) {
          throw new Error("HSBC funds API did not return scheme data.");
        }

        const selectedFundCodes = input?.fundCodes?.length
          ? new Set(input.fundCodes.map((code) => code.toUpperCase()))
          : null;

        const selectedFunds = scheme.fundInfos.filter((fund) =>
          selectedFundCodes
            ? selectedFundCodes.has(fund.fundCode.toUpperCase())
            : true,
        );

        if (selectedFunds.length === 0) {
          throw new Error("No HSBC funds matched the requested fund codes.");
        }

        await ctx.db
          .insert(MpfTrusteeFund)
          .values(
            selectedFunds.map((fund) => ({
              trustee: "hsbc" as const,
              schemeCode: scheme.schemeCode,
              schemeIdentifier: scheme.schemeIdentifier,
              fundCode: fund.fundCode,
              fundIdentifier: fund.fundIdentifier,
              fundNameEn:
                sanitizeOfficialFundName(
                  fund.fundNames.find((name) => name.languageCode === "en_US")
                    ?.value,
                ) ?? fund.fundCode,
              fundNameZhHk: sanitizeOfficialFundName(
                fund.fundNames.find((name) => name.languageCode === "zh_HK")
                  ?.value,
              ),
              riskLevel: fund.riskLevelValue,
              disFundIndicator: Boolean(fund.disFundIndicator),
              disRelatedFundCode: fund.disRelatedFundCode,
            })),
          )
          .onConflictDoUpdate({
            target: [
              MpfTrusteeFund.trustee,
              MpfTrusteeFund.schemeCode,
              MpfTrusteeFund.fundCode,
            ],
            set: {
              fundIdentifier: sql`excluded.fund_identifier`,
              fundNameEn: sql`excluded.fund_name_en`,
              fundNameZhHk: sql`excluded.fund_name_zh_hk`,
              riskLevel: sql`excluded.risk_level`,
              disFundIndicator: sql`excluded.dis_fund_indicator`,
              disRelatedFundCode: sql`excluded.dis_related_fund_code`,
              updatedAt: sql`now()`,
            },
          });

        const persistedFunds = await ctx.db
          .select({
            id: MpfTrusteeFund.id,
            fundCode: MpfTrusteeFund.fundCode,
          })
          .from(MpfTrusteeFund)
          .where(
            and(
            eq(MpfTrusteeFund.trustee, "hsbc"),
            eq(MpfTrusteeFund.schemeCode, scheme.schemeCode),
            inArray(
              MpfTrusteeFund.fundCode,
              selectedFunds.map((fund) => fund.fundCode),
            ),
            ),
          );

        const fundIdByCode = new Map(
          persistedFunds.map((fund) => [fund.fundCode, fund.id]),
        );

        let pricesInserted = 0;
        let rowsRead = 0;

        const latestSnapshots = scheme.fundPriceInfos
          .filter((item) => fundIdByCode.has(item.fundCode))
          .flatMap((item) => {
            const fundId = fundIdByCode.get(item.fundCode);
            const latest = item.fundPrices[0];
            if (!fundId || !latest) {
              return [];
            }
            return [
              {
                fundId,
                priceDate: parseHsbcPriceDate(latest.priceDate),
                bidPrice: latest.fundBuyPrice?.amount
                  ? Number.parseFloat(latest.fundBuyPrice.amount)
                  : null,
                offerPrice: latest.fundSellPrice?.amount
                  ? Number.parseFloat(latest.fundSellPrice.amount)
                  : null,
                currencyCode:
                  latest.fundSellPrice?.fundCurrencyCode ??
                  latest.fundBuyPrice?.fundCurrencyCode ??
                  "HKD",
                source: "hsbc_api_latest",
              },
            ];
          });

        if (latestSnapshots.length > 0) {
          rowsRead += latestSnapshots.length;
          await ctx.db
            .insert(MpfTrusteeFundPrice)
            .values(latestSnapshots)
            .onConflictDoUpdate({
              target: [MpfTrusteeFundPrice.fundId, MpfTrusteeFundPrice.priceDate],
              set: {
                bidPrice: sql`excluded.bid_price`,
                offerPrice: sql`excluded.offer_price`,
                currencyCode: sql`excluded.currency_code`,
                source: sql`excluded.source`,
                updatedAt: sql`now()`,
              },
            });
          pricesInserted += latestSnapshots.length;
        }

        for (const fund of selectedFunds) {
          const fundId = fundIdByCode.get(fund.fundCode);
          if (!fundId) {
            continue;
          }

          const historyUrl = `${HSBC_DOWNLOAD_API_BASE}?schemeCodes=HB&fundPricePeriodFrom=${fromDate}&fundPricePeriodTo=${toDate}&fundCode=${encodeURIComponent(fund.fundCode)}&language=${language}`;

          const historyResponse = await fetch(historyUrl, {
            headers: HSBC_API_HEADERS,
          });

          if (!historyResponse.ok) {
            throw new Error(
              `Failed to fetch HSBC history for ${fund.fundCode} (${historyResponse.status})`,
            );
          }

          const historyCsv = await historyResponse.text();
          const parsed = parseHsbcHistoryCsv(historyCsv);
          rowsRead += parsed.prices.length;

          if (parsed.prices.length === 0) {
            await delay(250);
            continue;
          }

          await ctx.db
            .insert(MpfTrusteeFundPrice)
            .values(
              parsed.prices.map((price) => ({
                fundId,
                priceDate: price.priceDate,
                bidPrice: price.bidPrice,
                offerPrice: price.offerPrice,
                currencyCode: "HKD",
                source: "hsbc_api_history",
              })),
            )
            .onConflictDoUpdate({
              target: [MpfTrusteeFundPrice.fundId, MpfTrusteeFundPrice.priceDate],
              set: {
                bidPrice: sql`excluded.bid_price`,
                offerPrice: sql`excluded.offer_price`,
                currencyCode: sql`excluded.currency_code`,
                source: sql`excluded.source`,
                updatedAt: sql`now()`,
              },
            });

          pricesInserted += parsed.prices.length;
          await delay(250);
        }

        const payloadHash = createHash("sha256")
          .update(`${scheme.schemeCode}:${fromDate}:${toDate}:${selectedFunds.length}`)
          .digest("hex");

        await ctx.db
          .update(MpfIngestionRun)
          .set({
            status: "success",
            payloadHash,
            rowsRead,
            rowsUpserted: pricesInserted,
            latestDataDate: toDate,
            completedAt: new Date(),
          })
          .where(eq(MpfIngestionRun.id, runId));

        return {
          trustee: "hsbc",
          schemeCode: scheme.schemeCode,
          fundCount: selectedFunds.length,
          rowsRead,
          rowsUpserted: pricesInserted,
          fromDate,
          toDate,
        };
      } catch (error) {
        await ctx.db
          .update(MpfIngestionRun)
          .set({
            status: "failed",
            errorMessage:
              error instanceof Error ? error.message : "Unknown HSBC ingestion error",
            completedAt: new Date(),
          })
          .where(eq(MpfIngestionRun.id, runId));
        throw error;
      }
    }),

  listHsbcFunds: publicProcedure
    .input(
      z
        .object({
          search: z.string().min(1).optional(),
          limit: z.number().int().min(1).max(200).default(100),
        })
        .optional(),
    )
    .query(async ({ ctx, input }) => {
      const rows = await ctx.db
        .select()
        .from(MpfTrusteeFund)
        .where(
          and(
          eq(MpfTrusteeFund.trustee, "hsbc"),
          eq(MpfTrusteeFund.schemeCode, "HB"),
          ),
        )
        .orderBy(asc(MpfTrusteeFund.fundCode));

      const keyword = input?.search?.toLowerCase();
      const filtered = keyword
        ? rows.filter((row) => {
            const haystack =
              `${row.fundCode} ${row.fundNameEn} ${row.fundNameZhHk ?? ""}`.toLowerCase();
            return haystack.includes(keyword);
          })
        : rows;

      return filtered.slice(0, input?.limit ?? 100);
    }),

  hsbcFundPriceSeries: publicProcedure
    .input(
      z.object({
        fundCode: z.string().min(1),
        fromDate: z.string().date().optional(),
        toDate: z.string().date().optional(),
      }),
    )
    .query(async ({ ctx, input }) => {
      const [fund] = await ctx.db
        .select()
        .from(MpfTrusteeFund)
        .where(
          and(
          eq(MpfTrusteeFund.trustee, "hsbc"),
          eq(MpfTrusteeFund.schemeCode, "HB"),
          eq(MpfTrusteeFund.fundCode, input.fundCode.toUpperCase()),
          ),
        )
        .limit(1);

      if (!fund) {
        throw new Error(`HSBC fund not found: ${input.fundCode}`);
      }

      const whereClauses = [eq(MpfTrusteeFundPrice.fundId, fund.id)];
      if (input.fromDate) {
        whereClauses.push(gte(MpfTrusteeFundPrice.priceDate, input.fromDate));
      }
      if (input.toDate) {
        whereClauses.push(lte(MpfTrusteeFundPrice.priceDate, input.toDate));
      }

      const prices = await ctx.db
        .select()
        .from(MpfTrusteeFundPrice)
        .where(and(...whereClauses))
        .orderBy(asc(MpfTrusteeFundPrice.priceDate));

      return {
        fund: {
          fundCode: fund.fundCode,
          fundNameEn: fund.fundNameEn,
          riskLevel: fund.riskLevel,
        },
        prices: prices.map((row) => ({
          priceDate: toIsoDate(row.priceDate),
          bidPrice: row.bidPrice,
          offerPrice: row.offerPrice,
          currencyCode: row.currencyCode,
          source: row.source,
        })),
      };
    }),

  ingestHangSengFunds: protectedProcedure
    .input(
      z
        .object({
          fromDate: z.string().date().optional(),
          toDate: z.string().date().optional(),
          fundCodes: z.array(z.string().min(1)).optional(),
          language: z.enum(["en_US", "zh_HK", "zh_CN"]).default("en_US"),
        })
        .optional(),
    )
    .mutation(async ({ ctx, input }) => {
      const toDate = input?.toDate ?? dateToIso(new Date());
      const fromDate =
        input?.fromDate ?? dateToIso(addDays(new Date(toDate), -365));
      const language = input?.language ?? "en_US";

      if (fromDate > toDate) {
        throw new Error("fromDate must be earlier than or equal to toDate.");
      }

      const run = await ctx.db
        .insert(MpfIngestionRun)
        .values({
          sourceUrl: HANG_SENG_FUNDS_API_URL,
          status: "running",
        })
        .returning({ id: MpfIngestionRun.id });

      const runId = run[0]?.id;
      if (!runId) {
        throw new Error("Failed to create Hang Seng ingestion run.");
      }

      try {
        const fundsResponse = await fetch(HANG_SENG_FUNDS_API_URL, {
          headers: HSBC_API_HEADERS,
        });

        if (!fundsResponse.ok) {
          throw new Error(
            `Failed to fetch Hang Seng funds API (${fundsResponse.status} ${fundsResponse.statusText})`,
          );
        }

        const fundsJson = (await fundsResponse.json()) as {
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

        const scheme = fundsJson.data?.schemeInfos?.[0];
        if (!scheme) {
          throw new Error("Hang Seng funds API did not return scheme data.");
        }

        const selectedFundCodes = input?.fundCodes?.length
          ? new Set(input.fundCodes.map((code) => code.toUpperCase()))
          : null;

        const selectedFunds = scheme.fundInfos.filter((fund) =>
          selectedFundCodes
            ? selectedFundCodes.has(fund.fundCode.toUpperCase())
            : true,
        );

        if (selectedFunds.length === 0) {
          throw new Error("No Hang Seng funds matched the requested fund codes.");
        }

        await ctx.db
          .insert(MpfTrusteeFund)
          .values(
            selectedFunds.map((fund) => ({
              trustee: "hangseng" as const,
              schemeCode: scheme.schemeCode,
              schemeIdentifier: scheme.schemeIdentifier,
              fundCode: fund.fundCode,
              fundIdentifier: fund.fundIdentifier,
              fundNameEn:
                sanitizeOfficialFundName(
                  fund.fundNames.find((name) => name.languageCode === "en_US")
                    ?.value,
                ) ?? fund.fundCode,
              fundNameZhHk: sanitizeOfficialFundName(
                fund.fundNames.find((name) => name.languageCode === "zh_HK")
                  ?.value,
              ),
              riskLevel: fund.riskLevelValue,
              disFundIndicator: Boolean(fund.disFundIndicator),
              disRelatedFundCode: fund.disRelatedFundCode,
            })),
          )
          .onConflictDoUpdate({
            target: [
              MpfTrusteeFund.trustee,
              MpfTrusteeFund.schemeCode,
              MpfTrusteeFund.fundCode,
            ],
            set: {
              fundIdentifier: sql`excluded.fund_identifier`,
              fundNameEn: sql`excluded.fund_name_en`,
              fundNameZhHk: sql`excluded.fund_name_zh_hk`,
              riskLevel: sql`excluded.risk_level`,
              disFundIndicator: sql`excluded.dis_fund_indicator`,
              disRelatedFundCode: sql`excluded.dis_related_fund_code`,
              updatedAt: sql`now()`,
            },
          });

        const persistedFunds = await ctx.db
          .select({
            id: MpfTrusteeFund.id,
            fundCode: MpfTrusteeFund.fundCode,
          })
          .from(MpfTrusteeFund)
          .where(
            and(
              eq(MpfTrusteeFund.trustee, "hangseng"),
              eq(MpfTrusteeFund.schemeCode, scheme.schemeCode),
              inArray(
                MpfTrusteeFund.fundCode,
                selectedFunds.map((fund) => fund.fundCode),
              ),
            ),
          );

        const fundIdByCode = new Map(
          persistedFunds.map((fund) => [fund.fundCode, fund.id]),
        );

        let pricesInserted = 0;
        let rowsRead = 0;

        const latestSnapshots = scheme.fundPriceInfos
          .filter((item) => fundIdByCode.has(item.fundCode))
          .flatMap((item) => {
            const fundId = fundIdByCode.get(item.fundCode);
            const latest = item.fundPrices[0];
            if (!fundId || !latest) {
              return [];
            }
            return [
              {
                fundId,
                priceDate: parseHsbcPriceDate(latest.priceDate),
                bidPrice: latest.fundBuyPrice?.amount
                  ? Number.parseFloat(latest.fundBuyPrice.amount)
                  : null,
                offerPrice: latest.fundSellPrice?.amount
                  ? Number.parseFloat(latest.fundSellPrice.amount)
                  : null,
                currencyCode:
                  latest.fundSellPrice?.fundCurrencyCode ??
                  latest.fundBuyPrice?.fundCurrencyCode ??
                  "HKD",
                source: "hangseng_api_latest",
              },
            ];
          });

        if (latestSnapshots.length > 0) {
          rowsRead += latestSnapshots.length;
          await ctx.db
            .insert(MpfTrusteeFundPrice)
            .values(latestSnapshots)
            .onConflictDoUpdate({
              target: [MpfTrusteeFundPrice.fundId, MpfTrusteeFundPrice.priceDate],
              set: {
                bidPrice: sql`excluded.bid_price`,
                offerPrice: sql`excluded.offer_price`,
                currencyCode: sql`excluded.currency_code`,
                source: sql`excluded.source`,
                updatedAt: sql`now()`,
              },
            });
          pricesInserted += latestSnapshots.length;
        }

        for (const fund of selectedFunds) {
          const fundId = fundIdByCode.get(fund.fundCode);
          if (!fundId) {
            continue;
          }

          const historyUrl = `${HANG_SENG_DOWNLOAD_API_BASE}?schemeCodes=HS&fundPricePeriodFrom=${fromDate}&fundPricePeriodTo=${toDate}&fundCode=${encodeURIComponent(fund.fundCode)}&language=${language}`;

          const historyResponse = await fetch(historyUrl, {
            headers: HSBC_API_HEADERS,
          });

          if (!historyResponse.ok) {
            throw new Error(
              `Failed to fetch Hang Seng history for ${fund.fundCode} (${historyResponse.status})`,
            );
          }

          const historyCsv = await historyResponse.text();
          const parsed = parseHsbcHistoryCsv(historyCsv);
          rowsRead += parsed.prices.length;

          if (parsed.prices.length === 0) {
            await delay(250);
            continue;
          }

          await ctx.db
            .insert(MpfTrusteeFundPrice)
            .values(
              parsed.prices.map((price) => ({
                fundId,
                priceDate: price.priceDate,
                bidPrice: price.bidPrice,
                offerPrice: price.offerPrice,
                currencyCode: "HKD",
                source: "hangseng_api_history",
              })),
            )
            .onConflictDoUpdate({
              target: [MpfTrusteeFundPrice.fundId, MpfTrusteeFundPrice.priceDate],
              set: {
                bidPrice: sql`excluded.bid_price`,
                offerPrice: sql`excluded.offer_price`,
                currencyCode: sql`excluded.currency_code`,
                source: sql`excluded.source`,
                updatedAt: sql`now()`,
              },
            });

          pricesInserted += parsed.prices.length;
          await delay(250);
        }

        const payloadHash = createHash("sha256")
          .update(`${scheme.schemeCode}:${fromDate}:${toDate}:${selectedFunds.length}`)
          .digest("hex");

        await ctx.db
          .update(MpfIngestionRun)
          .set({
            status: "success",
            payloadHash,
            rowsRead,
            rowsUpserted: pricesInserted,
            latestDataDate: toDate,
            completedAt: new Date(),
          })
          .where(eq(MpfIngestionRun.id, runId));

        return {
          trustee: "hangseng",
          schemeCode: scheme.schemeCode,
          fundCount: selectedFunds.length,
          rowsRead,
          rowsUpserted: pricesInserted,
          fromDate,
          toDate,
        };
      } catch (error) {
        await ctx.db
          .update(MpfIngestionRun)
          .set({
            status: "failed",
            errorMessage:
              error instanceof Error
                ? error.message
                : "Unknown Hang Seng ingestion error",
            completedAt: new Date(),
          })
          .where(eq(MpfIngestionRun.id, runId));
        throw error;
      }
    }),

  listHangSengFunds: publicProcedure
    .input(
      z
        .object({
          search: z.string().min(1).optional(),
          limit: z.number().int().min(1).max(200).default(100),
        })
        .optional(),
    )
    .query(async ({ ctx, input }) => {
      const rows = await ctx.db
        .select()
        .from(MpfTrusteeFund)
        .where(
          and(
            eq(MpfTrusteeFund.trustee, "hangseng"),
            eq(MpfTrusteeFund.schemeCode, "HS"),
          ),
        )
        .orderBy(asc(MpfTrusteeFund.fundCode));

      const keyword = input?.search?.toLowerCase();
      const filtered = keyword
        ? rows.filter((row) => {
            const haystack =
              `${row.fundCode} ${row.fundNameEn} ${row.fundNameZhHk ?? ""}`.toLowerCase();
            return haystack.includes(keyword);
          })
        : rows;

      return filtered.slice(0, input?.limit ?? 100);
    }),

  hangSengFundPriceSeries: publicProcedure
    .input(
      z.object({
        fundCode: z.string().min(1),
        fromDate: z.string().date().optional(),
        toDate: z.string().date().optional(),
      }),
    )
    .query(async ({ ctx, input }) => {
      const [fund] = await ctx.db
        .select()
        .from(MpfTrusteeFund)
        .where(
          and(
            eq(MpfTrusteeFund.trustee, "hangseng"),
            eq(MpfTrusteeFund.schemeCode, "HS"),
            eq(MpfTrusteeFund.fundCode, input.fundCode.toUpperCase()),
          ),
        )
        .limit(1);

      if (!fund) {
        throw new Error(`Hang Seng fund not found: ${input.fundCode}`);
      }

      const whereClauses = [eq(MpfTrusteeFundPrice.fundId, fund.id)];
      if (input.fromDate) {
        whereClauses.push(gte(MpfTrusteeFundPrice.priceDate, input.fromDate));
      }
      if (input.toDate) {
        whereClauses.push(lte(MpfTrusteeFundPrice.priceDate, input.toDate));
      }

      const prices = await ctx.db
        .select()
        .from(MpfTrusteeFundPrice)
        .where(and(...whereClauses))
        .orderBy(asc(MpfTrusteeFundPrice.priceDate));

      return {
        fund: {
          fundCode: fund.fundCode,
          fundNameEn: fund.fundNameEn,
          riskLevel: fund.riskLevel,
        },
        prices: prices.map((row) => ({
          priceDate: toIsoDate(row.priceDate),
          bidPrice: row.bidPrice,
          offerPrice: row.offerPrice,
          currencyCode: row.currencyCode,
          source: row.source,
        })),
      };
    }),

  ingestManulifeFunds: protectedProcedure
    .input(
      z
        .object({
          fromDate: z.string().date().optional(),
          toDate: z.string().date().optional(),
          fundIds: z.array(z.string().min(1)).optional(),
          overrideLocale: z.string().min(2).default("en_HK"),
        })
        .optional(),
    )
    .mutation(async ({ ctx, input }) => {
      const toDate = input?.toDate ?? dateToIso(new Date());
      const fromDate = input?.fromDate;
      const overrideLocale = input?.overrideLocale ?? "en_HK";
      const schemeCode = "MGS";

      if (fromDate && fromDate > toDate) {
        throw new Error("fromDate must be earlier than or equal to toDate.");
      }

      const run = await ctx.db
        .insert(MpfIngestionRun)
        .values({
          sourceUrl: MANULIFE_FUNDS_LIST_API_URL,
          status: "running",
        })
        .returning({ id: MpfIngestionRun.id });

      const runId = run[0]?.id;
      if (!runId) {
        throw new Error("Failed to create Manulife ingestion run.");
      }

      try {
        const listResponse = await fetch(MANULIFE_FUNDS_LIST_API_URL);
        if (!listResponse.ok) {
          throw new Error(
            `Failed to fetch Manulife funds list (${listResponse.status} ${listResponse.statusText})`,
          );
        }

        const fundsList = (await listResponse.json()) as {
          fundId: string;
          fundName?: string;
          fundNameEn?: string;
          displayName?: string;
          riskRating?: string;
          currency?: string;
          nav?: {
            asOfDate?: string;
            price?: string;
          };
          productsId?: string[];
          products?: { id?: number; name?: string }[];
        }[];

        const selectedFundIds = input?.fundIds?.length
          ? new Set(input.fundIds.map((id) => id.toUpperCase()))
          : null;

        const selectedFunds = fundsList.filter((fund) =>
          selectedFundIds ? selectedFundIds.has(fund.fundId.toUpperCase()) : true,
        );

        if (selectedFunds.length === 0) {
          throw new Error("No Manulife funds matched the requested fund IDs.");
        }

        await ctx.db
          .insert(MpfTrusteeFund)
          .values(
            selectedFunds.map((fund) => {
              const product = fund.products?.[0];
              const displayName = sanitizeOfficialFundName(
                fund.fundNameEn ?? fund.fundName ?? fund.displayName,
              );
              return {
                trustee: "manulife" as const,
                schemeCode,
                schemeIdentifier:
                  product?.id !== undefined
                    ? String(product.id)
                    : (fund.productsId?.[0] ?? null),
                fundCode: fund.fundId,
                fundIdentifier: fund.fundId,
                fundNameEn: displayName ?? fund.fundId,
                riskLevel: fund.riskRating,
              };
            }),
          )
          .onConflictDoUpdate({
            target: [
              MpfTrusteeFund.trustee,
              MpfTrusteeFund.schemeCode,
              MpfTrusteeFund.fundCode,
            ],
            set: {
              schemeIdentifier: sql`excluded.scheme_identifier`,
              fundIdentifier: sql`excluded.fund_identifier`,
              fundNameEn: sql`excluded.fund_name_en`,
              riskLevel: sql`excluded.risk_level`,
              updatedAt: sql`now()`,
            },
          });

        const persistedFunds = await ctx.db
          .select({
            id: MpfTrusteeFund.id,
            fundCode: MpfTrusteeFund.fundCode,
          })
          .from(MpfTrusteeFund)
          .where(
            and(
              eq(MpfTrusteeFund.trustee, "manulife"),
              eq(MpfTrusteeFund.schemeCode, schemeCode),
              inArray(
                MpfTrusteeFund.fundCode,
                selectedFunds.map((fund) => fund.fundId),
              ),
            ),
          );

        const fundIdByCode = new Map(
          persistedFunds.map((fund) => [fund.fundCode, fund.id]),
        );

        let rowsRead = 0;
        let rowsUpserted = 0;
        const loadedDates: string[] = [];

        const latestSnapshots = selectedFunds.flatMap((fund) => {
          const persistedFundId = fundIdByCode.get(fund.fundId);
          const priceDate = fund.nav?.asOfDate;
          const bidPrice = fund.nav?.price ? Number.parseFloat(fund.nav.price) : null;
          if (!persistedFundId || !priceDate) {
            return [];
          }

          const normalizedBidPrice =
            bidPrice !== null && Number.isFinite(bidPrice) ? bidPrice : null;
          if (normalizedBidPrice === null) {
            return [];
          }

          loadedDates.push(priceDate);
          return [
            {
              fundId: persistedFundId,
              priceDate,
              bidPrice: normalizedBidPrice,
              offerPrice: null,
              currencyCode: fund.currency ?? "HKD",
              source: "manulife_api_latest",
            },
          ];
        });

        if (latestSnapshots.length > 0) {
          rowsRead += latestSnapshots.length;
          await ctx.db
            .insert(MpfTrusteeFundPrice)
            .values(latestSnapshots)
            .onConflictDoUpdate({
              target: [MpfTrusteeFundPrice.fundId, MpfTrusteeFundPrice.priceDate],
              set: {
                bidPrice: sql`excluded.bid_price`,
                offerPrice: sql`excluded.offer_price`,
                currencyCode: sql`excluded.currency_code`,
                source: sql`excluded.source`,
                updatedAt: sql`now()`,
              },
            });
          rowsUpserted += latestSnapshots.length;
        }

        for (const fund of selectedFunds) {
          const persistedFundId = fundIdByCode.get(fund.fundId);
          if (!persistedFundId) {
            continue;
          }

          const historyUrl = `${MANULIFE_FUND_HISTORY_API_BASE}?id=${encodeURIComponent(fund.fundId)}&productLine=mpf&overrideLocale=${encodeURIComponent(overrideLocale)}`;
          const historyResponse = await fetch(historyUrl);
          if (!historyResponse.ok) {
            throw new Error(
              `Failed to fetch Manulife history for ${fund.fundId} (${historyResponse.status})`,
            );
          }

          const historyJson = (await historyResponse.json()) as {
            priceHistory?: {
              asOfDate: string;
              price?: number;
              offerPrice?: number;
            }[];
          };

          const historyRows = (historyJson.priceHistory ?? [])
            .filter((row) => {
              if (!row.asOfDate) {
                return false;
              }
              if (fromDate && row.asOfDate < fromDate) {
                return false;
              }
              return row.asOfDate <= toDate;
            })
            .map((row) => {
              loadedDates.push(row.asOfDate);
              return {
                fundId: persistedFundId,
                priceDate: row.asOfDate,
                bidPrice:
                  typeof row.price === "number" && Number.isFinite(row.price)
                    ? row.price
                    : null,
                offerPrice:
                  typeof row.offerPrice === "number" &&
                  Number.isFinite(row.offerPrice)
                    ? row.offerPrice
                    : null,
                currencyCode: fund.currency ?? "HKD",
                source: "manulife_api_history",
              };
            });

          rowsRead += historyRows.length;
          if (historyRows.length === 0) {
            await delay(250);
            continue;
          }

          await ctx.db
            .insert(MpfTrusteeFundPrice)
            .values(historyRows)
            .onConflictDoUpdate({
              target: [MpfTrusteeFundPrice.fundId, MpfTrusteeFundPrice.priceDate],
              set: {
                bidPrice: sql`excluded.bid_price`,
                offerPrice: sql`excluded.offer_price`,
                currencyCode: sql`excluded.currency_code`,
                source: sql`excluded.source`,
                updatedAt: sql`now()`,
              },
            });

          rowsUpserted += historyRows.length;
          await delay(250);
        }

        const latestDataDate = maxIsoDate(loadedDates);
        const payloadHash = createHash("sha256")
          .update(`manulife:${overrideLocale}:${fromDate ?? "all"}:${toDate}:${selectedFunds.length}`)
          .digest("hex");

        await ctx.db
          .update(MpfIngestionRun)
          .set({
            status: "success",
            payloadHash,
            rowsRead,
            rowsUpserted,
            latestDataDate,
            completedAt: new Date(),
          })
          .where(eq(MpfIngestionRun.id, runId));

        return {
          trustee: "manulife",
          schemeCode,
          fundCount: selectedFunds.length,
          rowsRead,
          rowsUpserted,
          fromDate: fromDate ?? null,
          toDate,
          latestDataDate,
        };
      } catch (error) {
        await ctx.db
          .update(MpfIngestionRun)
          .set({
            status: "failed",
            errorMessage:
              error instanceof Error
                ? error.message
                : "Unknown Manulife ingestion error",
            completedAt: new Date(),
          })
          .where(eq(MpfIngestionRun.id, runId));
        throw error;
      }
    }),

  listManulifeFunds: publicProcedure
    .input(
      z
        .object({
          search: z.string().min(1).optional(),
          limit: z.number().int().min(1).max(300).default(200),
        })
        .optional(),
    )
    .query(async ({ ctx, input }) => {
      const rows = await ctx.db
        .select()
        .from(MpfTrusteeFund)
        .where(
          and(
            eq(MpfTrusteeFund.trustee, "manulife"),
            eq(MpfTrusteeFund.schemeCode, "MGS"),
          ),
        )
        .orderBy(asc(MpfTrusteeFund.fundCode));

      const keyword = input?.search?.toLowerCase();
      const filtered = keyword
        ? rows.filter((row) =>
            `${row.fundCode} ${row.fundNameEn}`.toLowerCase().includes(keyword),
          )
        : rows;

      return filtered.slice(0, input?.limit ?? 200);
    }),

  manulifeFundPriceSeries: publicProcedure
    .input(
      z.object({
        fundId: z.string().min(1),
        fromDate: z.string().date().optional(),
        toDate: z.string().date().optional(),
      }),
    )
    .query(async ({ ctx, input }) => {
      const [fund] = await ctx.db
        .select()
        .from(MpfTrusteeFund)
        .where(
          and(
            eq(MpfTrusteeFund.trustee, "manulife"),
            eq(MpfTrusteeFund.schemeCode, "MGS"),
            eq(MpfTrusteeFund.fundCode, input.fundId.toUpperCase()),
          ),
        )
        .limit(1);

      if (!fund) {
        throw new Error(`Manulife fund not found: ${input.fundId}`);
      }

      const whereClauses = [eq(MpfTrusteeFundPrice.fundId, fund.id)];
      if (input.fromDate) {
        whereClauses.push(gte(MpfTrusteeFundPrice.priceDate, input.fromDate));
      }
      if (input.toDate) {
        whereClauses.push(lte(MpfTrusteeFundPrice.priceDate, input.toDate));
      }

      const prices = await ctx.db
        .select()
        .from(MpfTrusteeFundPrice)
        .where(and(...whereClauses))
        .orderBy(asc(MpfTrusteeFundPrice.priceDate));

      return {
        fund: {
          fundId: fund.fundCode,
          fundNameEn: fund.fundNameEn,
          riskLevel: fund.riskLevel,
        },
        prices: prices.map((row) => ({
          priceDate: toIsoDate(row.priceDate),
          bidPrice: row.bidPrice,
          offerPrice: row.offerPrice,
          currencyCode: row.currencyCode,
          source: row.source,
        })),
      };
    }),

  ingestSunLifeFunds: protectedProcedure
    .input(
      z
        .object({
          fundIds: z.array(z.string().min(1)).optional(),
        })
        .optional(),
    )
    .mutation(async ({ ctx, input }) => {
      const run = await ctx.db
        .insert(MpfIngestionRun)
        .values({
          sourceUrl: MPFA_MPP_LIST_EN_URL,
          status: "running",
        })
        .returning({ id: MpfIngestionRun.id });

      const runId = run[0]?.id;
      if (!runId) {
        throw new Error("Failed to create Sun Life ingestion run.");
      }

      try {
        const formBody = new URLSearchParams({ trustees: MPFA_SUNLIFE_TRUSTEE_ID });
        const [enResponse, zhHkResponse] = await Promise.all([
          fetch(MPFA_MPP_LIST_EN_URL, {
            method: "POST",
            body: formBody,
            headers: { "content-type": "application/x-www-form-urlencoded" },
          }),
          fetch(MPFA_MPP_LIST_ZH_HK_URL, {
            method: "POST",
            body: formBody,
            headers: { "content-type": "application/x-www-form-urlencoded" },
          }),
        ]);

        if (!enResponse.ok) {
          throw new Error(
            `Failed to fetch MPFA Sun Life EN list (${enResponse.status} ${enResponse.statusText})`,
          );
        }
        if (!zhHkResponse.ok) {
          throw new Error(
            `Failed to fetch MPFA Sun Life ZH-HK list (${zhHkResponse.status} ${zhHkResponse.statusText})`,
          );
        }

        const [enHtml, zhHkHtml] = await Promise.all([
          enResponse.text(),
          zhHkResponse.text(),
        ]);

        const enRows = parseMpfaFundRows(enHtml);
        const zhHkRows = parseMpfaFundRows(zhHkHtml);
        const zhHkByCfId = new Map(zhHkRows.map((row) => [row.cfId, row]));

        const selectedIds = input?.fundIds?.length
          ? new Set(input.fundIds.map((value) => value.trim()))
          : null;
        const selectedRows = enRows.filter((row) =>
          selectedIds ? selectedIds.has(row.cfId) : true,
        );

        if (selectedRows.length === 0) {
          throw new Error("No Sun Life funds matched the requested fund IDs.");
        }

        const values = selectedRows.map((row) => {
          const zhHkRow = zhHkByCfId.get(row.cfId);
          return {
            trustee: "sunlife" as const,
            schemeCode: "SLR",
            schemeIdentifier: row.schemeName ?? "Sun Life Rainbow MPF Scheme",
            fundCode: row.cfId,
            fundIdentifier: row.cfId,
            fundNameEn: row.fundName ?? `Sun Life Fund ${row.cfId}`,
            fundNameZhHk: zhHkRow?.fundName ?? null,
          };
        });

        await ctx.db.insert(MpfTrusteeFund).values(values).onConflictDoUpdate({
          target: [
            MpfTrusteeFund.trustee,
            MpfTrusteeFund.schemeCode,
            MpfTrusteeFund.fundCode,
          ],
          set: {
            schemeIdentifier: sql`excluded.scheme_identifier`,
            fundIdentifier: sql`excluded.fund_identifier`,
            fundNameEn: sql`excluded.fund_name_en`,
            fundNameZhHk: sql`excluded.fund_name_zh_hk`,
            updatedAt: sql`now()`,
          },
        });

        const payloadHash = createHash("sha256")
          .update(`sunlife:${selectedRows.length}`)
          .digest("hex");

        await ctx.db
          .update(MpfIngestionRun)
          .set({
            status: "success",
            payloadHash,
            rowsRead: selectedRows.length,
            rowsUpserted: selectedRows.length,
            completedAt: new Date(),
          })
          .where(eq(MpfIngestionRun.id, runId));

        return {
          trustee: "sunlife",
          schemeCode: "SLR",
          fundCount: selectedRows.length,
          rowsRead: selectedRows.length,
          rowsUpserted: selectedRows.length,
          note: "MPFA source provides official fund list; historical daily NAV endpoint is not publicly available from Sun Life site due anti-bot protection.",
        };
      } catch (error) {
        await ctx.db
          .update(MpfIngestionRun)
          .set({
            status: "failed",
            errorMessage:
              error instanceof Error ? error.message : "Unknown Sun Life ingestion error",
            completedAt: new Date(),
          })
          .where(eq(MpfIngestionRun.id, runId));
        throw error;
      }
    }),

  listSunLifeFunds: publicProcedure
    .input(
      z
        .object({
          search: z.string().min(1).optional(),
          limit: z.number().int().min(1).max(300).default(200),
        })
        .optional(),
    )
    .query(async ({ ctx, input }) => {
      const rows = await ctx.db
        .select()
        .from(MpfTrusteeFund)
        .where(
          and(eq(MpfTrusteeFund.trustee, "sunlife"), eq(MpfTrusteeFund.schemeCode, "SLR")),
        )
        .orderBy(asc(MpfTrusteeFund.fundCode));

      const keyword = input?.search?.toLowerCase();
      const filtered = keyword
        ? rows.filter((row) =>
            `${row.fundCode} ${row.fundNameEn} ${row.fundNameZhHk ?? ""}`
              .toLowerCase()
              .includes(keyword),
          )
        : rows;

      return filtered.slice(0, input?.limit ?? 200);
    }),

  sunLifeFundPriceSeries: publicProcedure
    .input(
      z.object({
        fundId: z.string().min(1),
        fromDate: z.string().date().optional(),
        toDate: z.string().date().optional(),
      }),
    )
    .query(async ({ ctx, input }) => {
      const [fund] = await ctx.db
        .select()
        .from(MpfTrusteeFund)
        .where(
          and(
            eq(MpfTrusteeFund.trustee, "sunlife"),
            eq(MpfTrusteeFund.schemeCode, "SLR"),
            eq(MpfTrusteeFund.fundCode, input.fundId),
          ),
        )
        .limit(1);

      if (!fund) {
        throw new Error(`Sun Life fund not found: ${input.fundId}`);
      }

      const whereClauses = [eq(MpfTrusteeFundPrice.fundId, fund.id)];
      if (input.fromDate) {
        whereClauses.push(gte(MpfTrusteeFundPrice.priceDate, input.fromDate));
      }
      if (input.toDate) {
        whereClauses.push(lte(MpfTrusteeFundPrice.priceDate, input.toDate));
      }

      const prices = await ctx.db
        .select()
        .from(MpfTrusteeFundPrice)
        .where(and(...whereClauses))
        .orderBy(asc(MpfTrusteeFundPrice.priceDate));

      return {
        fund: {
          fundId: fund.fundCode,
          fundNameEn: fund.fundNameEn,
          fundNameZhHk: fund.fundNameZhHk,
          riskLevel: fund.riskLevel,
        },
        prices: prices.map((row) => ({
          priceDate: toIsoDate(row.priceDate),
          bidPrice: row.bidPrice,
          offerPrice: row.offerPrice,
          currencyCode: row.currencyCode,
          source: row.source,
        })),
      };
    }),

  latestIngestionRuns: protectedProcedure
    .input(z.object({ limit: z.number().int().min(1).max(50).default(20) }))
    .query(({ ctx, input }) => {
      return ctx.db.query.MpfIngestionRun.findMany({
        orderBy: [desc(MpfIngestionRun.createdAt)],
        limit: input.limit,
      });
    }),
} satisfies TRPCRouterRecord;
