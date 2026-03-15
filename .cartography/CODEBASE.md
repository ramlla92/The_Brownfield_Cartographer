# CODEBASE.md — jaffle-shop
> Generated: 2026-03-15T02:34:13.293883+00:00
> Use this as system context for AI agents to provide architectural grounding.

---

## 1. Executive Summary
- **Inventory**: 37 modules, 30 data transformations.
- **Topology**: 0 domains detected.
- **Data Lineage**: 19 datasets (0 sources, 7 sinks).

## 2. Critical Architectural Hubs (High PageRank)
1. `.pre-commit-config.yaml` — None
1. `dbt_project.yml` — None
1. `package-lock.yml` — None
1. `packages.yml` — None
1. `Taskfile.yml` — None


## 3. High-Impact Blast Radius
No significant blast radius detected.

## 4. Primary Data Entities
- `stg_orders` (4 connections)
- `stg_products` (4 connections)
- `stg_supplies` (4 connections)
- `stg_customers` (3 connections)
- `stg_locations` (3 connections)


## 5. Domain Architecture Map
Domain clustering not yet performed.

## 6. Technical Debt & Drift
- **Circular Dependencies**: 0 SCCs detected.
- **Documentation Drift**: 0 modules out of sync.