# RECONNAISSANCE – dbt jaffle_shop

---

## 1. Repository Overview

It exists mainly to showcase dbt features, and this version is version 3.0.0, built to require **dbt >= 1.5.0**. The tech stack is:

- **dbt Core** (adaptor-agnostic    written to work on Postgres, BigQuery, Fabric, etc., notice the multi-dispatch macro)
- **dbt MetricFlow**    semantic models and metrics are defined on top of the mart tables, which is the modern dbt way of doing BI-layer logic
- **dbt-utils**    for surrogate key generation (`generate_surrogate_key`) and expression tests
- **dbt-audit-helper**    pinned as a dependency (used for comparing model outputs during refactors)
- **Seeds** as the data source    raw CSVs committed to the repo, loaded into a `raw` schema

The high-level directory layout looks like this:

```
jaffle-shop/
├── seeds/
│   └── jaffle-data/          # Six raw CSV files    the "source" data
├── models/
│   ├── staging/              # Six staging views    one per source table
│   └── marts/                # Eight final tables + metricflow_time_spine
├── macros/
│   ├── cents_to_dollars.sql  # Multi-adapter currency macro
│   └── generate_schema_name.sql
├── analyses/                 # (Empty/placeholder)
├── data-tests/               # Custom singular tests
├── dbt_project.yml
└── packages.yml
```

Short and clean. No intermediate layer (no intermediate/), so everything goes straight from staging → marts.

---

## 2. Primary Data Ingestion Path

The data enters through **CSV seeds** committed directly to the repo. There's no real database source here    it's all `seeds/jaffle-data/*.csv`. 

The staging layer then reads from those raw tables using `{{ source('ecom', 'raw_*') }}` references, defined in `models/staging/__sources.yml`. That's the handshake point between seeds and models.

**Key source/seed tables and their first-touch models:**

| Seed CSV | Source Reference | First-touch Staging Model |
|---|---|---|
| `seeds/jaffle-data/raw_customers.csv` | `source('ecom', 'raw_customers')` | [`models/staging/stg_customers.sql`](models/staging/stg_customers.sql) |
| `seeds/jaffle-data/raw_orders.csv` | `source('ecom', 'raw_orders')` | [`models/staging/stg_orders.sql`](models/staging/stg_orders.sql) |
| `seeds/jaffle-data/raw_items.csv` | `source('ecom', 'raw_items')` | [`models/staging/stg_order_items.sql`](models/staging/stg_order_items.sql) |
| `seeds/jaffle-data/raw_products.csv` | `source('ecom', 'raw_products')` | [`models/staging/stg_products.sql`](models/staging/stg_products.sql) |
| `seeds/jaffle-data/raw_supplies.csv` | `source('ecom', 'raw_supplies')` | [`models/staging/stg_supplies.sql`](models/staging/stg_supplies.sql) |
| `seeds/jaffle-data/raw_stores.csv` | `source('ecom', 'raw_stores')` | [`models/staging/stg_locations.sql`](models/staging/stg_locations.sql) |

---

## 3. Critical Output Datasets

The project produces six mart tables (plus a `metricflow_time_spine` helper). These are all materialized as **tables**.

Here are the five that actually matter:

1. **`models/marts/orders.sql`** The order fact table. One row per order. Contains order totals, food/drink booleans, supply costs, item counts, and crucially a `customer_order_number` window function that marks each customer's first order vs. repeat orders. Also has MetricFlow semantic model + 6 metrics defined. This is the workhorse.

2. **`models/marts/order_items.sql`** The order-item grain table. It's actually an intermediate-ish model that `orders.sql` depends on    it joins raw order items with products and supplies to compute per-item pricing and cost. Sits between staging and the final order mart.

3. **`models/marts/customers.sql`** Customer dimension / 360 view. Rolls up all order history per customer: lifetime spend, first/last order timestamp, order count, and a `customer_type` field ('new' vs 'returning'). MetricFlow metrics for LTV and AOV are defined here.

4. **`models/marts/locations.sql`**    Store/location dimension. Simple pass-through from `stg_locations`. Low complexity but needed as a join key in the orders semantic model.

5. **`models/marts/products.sql`**    Product dimension. Also a simple pass-through from `stg_products`. Needed for the order_items join chain.

---

## 4. Blast Radius of the Most Critical Module

The most critical model is, **`models/marts/order_items.sql`**.

Here's why. It's not the highest-profile model    `orders` and `customers` get the spotlight    but `order_items` is the **load-bearing middle layer** that everything else depends on for financial accuracy. Specifically:

- `orders.sql` depends on `order_items` to compute `order_cost`, `order_items_subtotal`, `count_food_items`, and `count_drink_items`. The `is_food_order` / `is_drink_order` booleans that drive the MetricFlow metrics come from this model.
- `customers.sql` depends on `orders.sql`, which depends on `order_items`. So any schema change in `order_items` cascades up two levels.

**What would break if `order_items` failed or changed schema?**

| Downstream Model | What Breaks |
|---|---|
| `orders.sql` | Order cost summaries become NULL or wrong; food/drink classification fails |
| `customers.sql` | Lifetime spend is computed from `orders.subtotal`, which traces back here |
| `orders.yml` metrics (MetricFlow) | `order_total`, `food_orders`, `drink_orders`, `new_customer_orders` all break |
| `customers.yml` metrics | `lifetime_spend_pretax`, `average_order_value` become incorrect |
| Data quality tests | The `orders.yml` test `order_items_subtotal = subtotal` would fail, alerting you |

---

## 5. Business Logic: Concentrated vs Distributed

**Where logic is concentrated:**

- **`models/staging/stg_orders.sql`**    This is where the `cents_to_dollars` macro is called three times (subtotal, tax_paid, order_total), and where timestamps are truncated to day-level using `{{ dbt.date_trunc() }}`. Every downstream dollar amount traces back to this conversion. If the macro had a bug, every financial figure in the project would be off.

- **`models/staging/stg_supplies.sql`**    Uses `dbt_utils.generate_surrogate_key(['id', 'sku'])` to build a composite surrogate key. This is the one place where key generation logic lives; if the surrogate key strategy changes, all supply-cost rollups in `order_items` would be affected.

- **`models/staging/stg_products.sql`**    This is where food/drink classification happens: `coalesce(type = 'jaffle', false) as is_food_item`. The entire food vs drink split in the analytics layer is decided by a single `coalesce()` in staging. That's a thin but load-bearing line of code.

- **`models/marts/orders.sql`**    Business logic is dense here: window functions for `customer_order_number`, conditional aggregations for food/drink item counts, joins between orders and the order_items summary. This is where order-level analytics get assembled.

- **`models/marts/customers.sql`**    The `customer_type` CASE expression (`'new'` vs `'returning'`) lives here. Also rolls up multiple financial aggregations from the orders mart.

- **`macros/cents_to_dollars.sql`**    Centralized currency conversion with four adapter dispatches (default, postgres, bigquery, fabric). Any rounding or casting change here affects every dollar figure in the project.

**Where logic is thin (pass-through / renaming only):**

- **`models/staging/stg_customers.sql`**    Just renames `id` to `customer_id`. That's literally it.
- **`models/staging/stg_order_items.sql`**    Rename + cast. Minimal logic.
- **`models/marts/locations.sql`**    Pure pass-through from `stg_locations`. No joins, no logic.
- **`models/marts/products.sql`**    Same story. `select * from stg_products`. Just a table materialization of the staging view.
- **`models/marts/supplies.sql`**    Also pass-through.

---

## 6. Git Velocity Map (Last 90 Days)

To inspect git history, I ran:
```bash
git log --since="90 days ago" --pretty=format:"%h %ad %s" --date=short --name-only
git log --pretty=format:"%h %ad %s" --date=short -n 30
```

**What the history shows:**

- **1 commit in the last 90 days** (Jan 21, 2026): "Update package versions"    touched `.github/workflows/codeowners-check.yml` and `.pre-commit-config.yaml`.
- Looking further back, only a handful of commits appear in the full log. Two notable ones: updating `packages.yml` to pin `audit-helper`, and adding the multi-dispatch `cents_to_dollars` macro (PR #43).

**"Hot" files/directories from recent history:**
- `.github/workflows/`    CI/tooling maintenance
- `.pre-commit-config.yaml`    Linting/hook updates
- `macros/cents_to_dollars.sql`    Was modified to add multi-adapter dispatch
- `packages.yml`    Dependency pinning

This repo is essentially **stable and in maintenance mode**. The core model logic (staging, marts) has not been touched recently. All recent activity is tooling/CI housekeeping. 

---

## 7. Difficulty & Confusion Analysis

**What was hardest to figure out manually:**

1. **The `order_items` dependency chain**    At first glance, I expected `orders.sql` to pull from `stg_orders` and do its own item joins. But `orders.sql` actually refs `order_items` (the mart), not `stg_order_items` (staging). That's an inter-mart dependency, which is unusual.

2. **Where food/drink classification lives**    I spent time looking for it in the mart models. It's actually in `stg_products.sql` as a simple `coalesce(type = 'jaffle', false)` line. Very easy to overlook; there's no obvious signal that staging is doing business classification, not just renaming.

3. **The seed-gating flag**    The `load_source_data` variable in `dbt_project.yml` means seeds are *disabled by default*. If I were setting this up fresh and just ran `dbt build`, I'd get empty tables and wonder where the data went.

**Where I got lost or had to guess:**

- The `analyses/` directory is referenced in `dbt_project.yml` but is completely empty    nothing to analyze there. It's a "framework placeholder."
- `data-tests/` is referenced but I couldn't immediately find custom test files in it without digging further. Probably empty or near-empty.

**How this informs Cartographer priorities:**

- **Visualize the DAG first**    The inter-mart dependency (`orders` → `order_items` → `stg_*`) is the biggest point. A DAG diagram would instantly clarify this.
- **Surface classification logic**    Flag staging models that contain business-decision logic (not just renaming). `stg_products.sql` line 26 is the kind of thing that should jump out visually.
- **Show blast radius overlays**    Which models are downstream of each node. The `order_items` blast radius is the most critical thing to automate surfacing.
- **Highlight the currency macro**    Since `cents_to_dollars` is called across three staging models and all financial metrics trace back to it, it deserves a "critical macro" callout in any documentation.

---

## 8. Assumptions & Open Questions

**Assumptions I had to make:**

- I assumed the `analyses/` and `data-tests/` directories are intentionally sparse (demo project), not accidentally empty (broken setup).
- I couldn't run `dbt docs generate` to get a rendered DAG, so the lineage analysis above was done by hand tracing `ref()` calls. My blast radius map might miss edge cases if there are additional models I didn't open.

**Questions I'd ask the team if they were available:**

1. Is `data-tests/` actually empty, or are there singular tests I should know about? Custom data tests are often where the interesting edge-case logic lives.
2. Why does `orders.sql` reference `order_items` (the mart) instead of `stg_order_items` (staging)? Is this intentional?
3. The `load_source_data` seed flag defaults to `false`    what's the intended workflow for a new developer setting this project up? Is there a `dbt seed --vars '{"load_source_data": true}'` command documented somewhere beyond the README?
