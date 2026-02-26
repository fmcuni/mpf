import { sql } from "drizzle-orm";
import { pgEnum, pgTable, primaryKey, uniqueIndex } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod/v4";

import { user } from "./auth-schema";

export const Post = pgTable("post", (t) => ({
  id: t.uuid().notNull().primaryKey().defaultRandom(),
  title: t.varchar({ length: 256 }).notNull(),
  content: t.text().notNull(),
  createdAt: t.timestamp().defaultNow().notNull(),
  updatedAt: t
    .timestamp({ mode: "date", withTimezone: true })
    .$onUpdateFn(() => sql`now()`),
}));

export const CreatePostSchema = createInsertSchema(Post, {
  title: z.string().max(256),
  content: z.string().max(256),
}).omit({
  id: true,
  createdAt: true,
  updatedAt: true,
});

export const MpfFundCategory = pgEnum("mpf_fund_category", [
  "equity",
  "mixed_assets",
  "bond",
  "guaranteed",
  "money_market_mpf_conservative",
  "money_market_other",
]);

export const MpfIngestionStatus = pgEnum("mpf_ingestion_status", [
  "running",
  "success",
  "failed",
]);

export const MpfTrustee = pgEnum("mpf_trustee", [
  "aia",
  "hsbc",
  "manulife",
  "hangseng",
  "sunlife",
]);

export const MpfCategoryNav = pgTable(
  "mpf_category_nav",
  (t) => ({
    asOfDate: t.date().notNull(),
    category: MpfFundCategory().notNull(),
    netAssetValueMillion: t.numeric({ mode: "number" }).notNull(),
    source: t.varchar({ length: 64 }).notNull().default("mpfa_official_dataset"),
    createdAt: t.timestamp().defaultNow().notNull(),
    updatedAt: t
      .timestamp({ mode: "date", withTimezone: true })
      .$onUpdateFn(() => sql`now()`),
  }),
  (table) => [primaryKey({ columns: [table.asOfDate, table.category] })],
);

export const MpfIngestionRun = pgTable("mpf_ingestion_run", (t) => ({
  id: t.uuid().notNull().primaryKey().defaultRandom(),
  sourceUrl: t.text().notNull(),
  status: MpfIngestionStatus().notNull().default("running"),
  payloadHash: t.varchar({ length: 64 }),
  rowsRead: t.integer().notNull().default(0),
  rowsUpserted: t.integer().notNull().default(0),
  latestDataDate: t.date(),
  errorMessage: t.text(),
  createdAt: t.timestamp().defaultNow().notNull(),
  completedAt: t.timestamp({ mode: "date", withTimezone: true }),
}));

export const MpfTrusteeFund = pgTable(
  "mpf_trustee_fund",
  (t) => ({
    id: t.uuid().notNull().primaryKey().defaultRandom(),
    trustee: MpfTrustee().notNull().default("hsbc"),
    schemeCode: t.varchar({ length: 32 }).notNull(),
    schemeIdentifier: t.varchar({ length: 64 }),
    fundCode: t.varchar({ length: 32 }).notNull(),
    fundIdentifier: t.varchar({ length: 64 }),
    fundNameEn: t.varchar({ length: 256 }).notNull(),
    fundNameZhHk: t.varchar({ length: 256 }),
    riskLevel: t.varchar({ length: 16 }),
    disFundIndicator: t.boolean().notNull().default(false),
    disRelatedFundCode: t.varchar({ length: 32 }),
    createdAt: t.timestamp().defaultNow().notNull(),
    updatedAt: t
      .timestamp({ mode: "date", withTimezone: true })
      .$onUpdateFn(() => sql`now()`),
  }),
  (table) => [
    uniqueIndex("mpf_trustee_fund_unique").on(
      table.trustee,
      table.schemeCode,
      table.fundCode,
    ),
  ],
);

export const MpfTrusteeFundPrice = pgTable(
  "mpf_trustee_fund_price",
  (t) => ({
    fundId: t
      .uuid()
      .notNull()
      .references(() => MpfTrusteeFund.id, { onDelete: "cascade" }),
    priceDate: t.date().notNull(),
    bidPrice: t.numeric({ mode: "number" }),
    offerPrice: t.numeric({ mode: "number" }),
    currencyCode: t.varchar({ length: 8 }).notNull().default("HKD"),
    source: t.varchar({ length: 64 }).notNull().default("hsbc_api"),
    createdAt: t.timestamp().defaultNow().notNull(),
    updatedAt: t
      .timestamp({ mode: "date", withTimezone: true })
      .$onUpdateFn(() => sql`now()`),
  }),
  (table) => [primaryKey({ columns: [table.fundId, table.priceDate] })],
);

export const MpfContribution = pgTable("mpf_contribution", (t) => ({
  id: t.uuid().notNull().primaryKey().defaultRandom(),
  userId: t
    .text()
    .notNull()
    .references(() => user.id, { onDelete: "cascade" }),
  contributionDate: t.date().notNull(),
  amount: t.numeric({ mode: "number" }).notNull(),
  category: MpfFundCategory(),
  note: t.varchar({ length: 500 }),
  createdAt: t.timestamp().defaultNow().notNull(),
  updatedAt: t
    .timestamp({ mode: "date", withTimezone: true })
    .$onUpdateFn(() => sql`now()`),
}));

export const CreateMpfContributionSchema = createInsertSchema(MpfContribution, {
  amount: z.number().positive(),
  note: z.string().max(500).optional(),
}).omit({
  id: true,
  userId: true,
  createdAt: true,
  updatedAt: true,
});

export * from "./auth-schema";
