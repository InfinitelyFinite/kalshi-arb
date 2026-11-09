# Empirical Market Study: Arbitrage Detection, Persistence, and Executability Analysis

> **Generated:** 2026-11-09T18:29:47.199951+00:00 | **Dataset:** `research_snapshots`

## Executive Summary
- **Study Duration:** 1.23 wall-clock hours across 20 verified pairs.
- **Total Valid Pair-Hours:** 17.25 pair-hours.
- **Usable Synchronized Market Data:** 69.7%.
- **Overall Executable Opportunities / Hour:** 19395.122 opportunities / hour.

---

## 1. Opportunity Frequency per Pair-Hour (Q1)
Frequency is computed strictly against **valid pair observation hours**, avoiding downtime or missing quote distortions:

| Market Pair ID | Valid Pair-Hours | Raw Opps | Fee-Adjusted Opps | Executable Opps | Executable / Pair-Hour |
|:---|---:|---:|---:|---:|---:|
| `dem-nom-2028-alexandria-ocasio-cortez` | 1.23h | 2244 | 1985 | 1985 | **1611.201** |
| `dem-nom-2028-andy-beshear` | 1.23h | 1896 | 1384 | 1384 | **1123.377** |
| `dem-nom-2028-cory-booker` | 1.23h | 1944 | 1593 | 1593 | **1293.019** |
| `dem-nom-2028-gavin-newsom` | 1.23h | 2172 | 1906 | 1906 | **1547.078** |
| `dem-nom-2028-gretchen-whitmer` | 1.23h | 2158 | 1877 | 1877 | **1523.539** |
| `dem-nom-2028-jon-ossoff` | 1.23h | 2112 | 1673 | 1673 | **1357.955** |
| `dem-nom-2028-josh-shapiro` | 1.23h | 2224 | 1888 | 1888 | **1532.468** |
| `dem-nom-2028-kamala-harris` | 1.23h | 2460 | 2367 | 2367 | **1921.266** |
| `dem-nom-2028-mark-kelly` | 1.23h | 1910 | 1560 | 1560 | **1266.234** |
| `dem-nom-2028-michelle-obama` | 1.23h | 1843 | 1298 | 1298 | **1053.571** |
| `dem-nom-2028-pete-buttigieg` | 1.23h | 1802 | 1314 | 1314 | **1066.558** |
| `dem-nom-2028-rahm-emanuel` | 1.23h | 2064 | 1760 | 1760 | **1428.571** |
| `dem-nom-2028-raphael-warnock` | 1.23h | 1995 | 1598 | 1598 | **1297.078** |
| `dem-nom-2028-wes-moore` | 1.23h | 2024 | 1653 | 1653 | **1341.721** |

---

## 2. Opportunity Funnel & Drop-Off Reasons (Q2)
Stage-by-stage classification progression across the research period:

| Funnel Stage | Count | Transition Conversion % |
|:---|---:|---:|
| 1 Total Pair Evaluations | 73,080 | — |
| 2 Raw Pricing Opportunities | 28,848 | — |
| 3 Fee Adjusted Opportunities | 23,856 | — |
| 4 Executable Candidate Observations | 23,856 | — |
| 5 Distinct Executable Episodes | 23,856 | — |

### Primary Rejection & Drop-Off Reasons
| Reject Reason Code | Filtered Count | Description / Mechanism |
|:---|---:|:---|
| `NO_RAW_EDGE` | 44,232 | Top-of-book or depth failure at this threshold |
| `BELOW_MIN_EDGE_AFTER_COSTS` | 4,992 | Top-of-book or depth failure at this threshold |

---

## 3. Distribution of Net Edges (Q3)
- **Sample Size:** 23856 executable candidate observations
- **Mean Net Edge:** 15.37¢ | **Median:** 11.50¢ | **Std Dev:** 11.93¢
- **Min Net Edge:** 2.10¢ | **Max Net Edge:** 60.90¢

| Percentile | Net Edge (¢) |
|:---|---:|
| P25 | 6.00¢ |
| P50 | 11.50¢ |
| P75 | 22.20¢ |
| P90 | 33.30¢ |
| P95 | 40.60¢ |

---

## 4. Opportunity Persistence & Duration (Q4)
- **Distinct Episodes Tracked:** 23856
- **Mean Persistence Duration:** 0.08s
- **Median Duration:** 0.00s

### Survival Across Latency & Persistence Thresholds
| Threshold | Surviving Episodes | Survival Rate % |
|:---|---:|---:|
| Persistence ≥ 0 seconds | 23856 | 100.0% |
| Persistence ≥ 5 seconds | 5 | 0.0% |
| Persistence ≥ 10 seconds | 5 | 0.0% |
| Persistence ≥ 30 seconds | 5 | 0.0% |

---

## 5. Order Book Depth & Fillability (Q5)
- **Average Resting YES Depth:** 50.0 contracts
- **Average Resting NO Depth:** 50.0 contracts

| Order Size $q$ (Contracts) | Fillability Rate % | Successful Fill Count |
|:---|---:|---:|
| 1 contracts | 100.0% | 36,540 |
| 10 contracts | 100.0% | 36,540 |
| 50 contracts | 100.0% | 36,540 |

---

## 6. Data Synchronization & Quality (Q7)
- **Usable Synchronized Pair Time:** **69.7%**
- **Median Quote Skew:** 0.0 ms (P95: 0.0 ms)
- **Median Quote Age:** 0.0 s (P95: 0.0 s)
- **Missing Exchange Timestamp %:** 0.0%

---
*Empirical analysis report compiled by `scripts/analyze_study.py` for Step 13 Senior Seminar.*