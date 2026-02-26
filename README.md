# mpf

Web app for Hong Kong MPF analytics.

## Goals

- Fetch and store the latest Hong Kong MPF fund data.
- Review and compare MPF fund performance.
- Support backtesting for selected funds/portfolios.
- Save each user's MPF contribution records.
- Calculate accurate personal XIRR based on contribution cashflows.

## Core Features (Planned)

- Automated MPF fund data ingestion pipeline.
- Fund performance dashboard (return, drawdown, volatility, ranking).
- Backtesting engine with configurable rebalance and period.
- User contribution ledger (amount, date, scheme/fund allocation).
- XIRR calculator using real contribution history.

## Tech Stack

This project starts from the `create-t3-turbo` template and uses a pnpm monorepo setup.

## Development Rules

- Use `pnpm` only.
- Do not add production dependencies without explicit approval.
- After every change, run `lint`, `test`, and `build` as far as possible, then report results.
