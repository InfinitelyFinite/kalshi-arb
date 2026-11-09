# Data Quality & Reliability Report

- **Generated At (UTC):** `2026-11-09T16:54:29.540126+00:00`
- **Target Dataset:** `research_snapshots`
- **Overall Reliability Label:** **`UNRELIABLE`**

## Executive Summary

| Metric | Value |
| :--- | :--- |
| Overall Assessment | **`unreliable`** |
| Time Window | `2026-11-09T13:57:05.210245` to `2026-11-09T15:55:01.048071` (1.97h) |
| Total Snapshots Analyzed | `104440` |
| Coverage Completeness | `100.0%` (expected ~119) |
| Significant Gaps (> 300.0s) | `1` |
| Missing Exchange Timestamps | `0.04%` (40) |
| Stale Quotes (> 60s) | `0.0%` (0) |
| Crossed Books (bid > ask) | `522` |

## Order Book Depth & VWAP Fillability

| Contract Order Size | Fillable Snapshots % |
| :--- | :--- |
| Size 1 Pct | `100.0%` |
| Size 10 Pct | `100.0%` |
| Size 50 Pct | `100.0%` |

- **Empty Books:** `0.0%` (0)
- **One-Sided Books:** `0.0%` (0)

## Cross-Platform Quote Skew

- Paired Snapshots Evaluated: `52200`
- Local Observation Skew (Median / P95): `0.0 ms` / `0.0 ms`
- Exchange Quote Skew (Median / P95): `0.0 ms` / `0.0 ms`

## Data Gaps Detected

| Gap Start (UTC) | Gap End (UTC) | Duration |
| :--- | :--- | :--- |
| `2026-11-09T15:10:59.257021` | `2026-11-09T15:51:56.516525` | `2457.3s` |
