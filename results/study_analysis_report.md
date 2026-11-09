# Empirical Market Study: Arbitrage Detection, Persistence, and Executability Analysis

> **Generated:** 2026-11-09T16:54:06.686454+00:00 | **Dataset:** `research_snapshots`

## Executive Summary
- **Study Duration:** 1.23 wall-clock hours across 20 verified pairs.
- **Total Valid Pair-Hours:** 24.64 pair-hours.
- **Usable Synchronized Market Data:** 99.9%.
- **Overall Executable Opportunities / Hour:** 28695.122 opportunities / hour.

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
| `rep-nom-2028-donald-trump-jr` | 1.23h | 2308 | 2043 | 2043 | **1658.279** |
| `rep-nom-2028-jd-vance` | 1.23h | 2361 | 2010 | 2010 | **1631.494** |
| `rep-nom-2028-marco-rubio` | 1.23h | 2159 | 1674 | 1674 | **1358.766** |
| `rep-nom-2028-ron-desantis` | 1.23h | 2078 | 1674 | 1674 | **1358.766** |
| `rep-nom-2028-ted-cruz` | 1.23h | 2349 | 2098 | 2098 | **1702.922** |
| `rep-nom-2028-tucker-carlson` | 1.23h | 2275 | 1940 | 1940 | **1574.675** |

---

## 2. Opportunity Funnel & Drop-Off Reasons (Q2)
Stage-by-stage classification progression across the research period:

| Funnel Stage | Count | Transition Conversion % |
|:---|---:|---:|
| 1 Total Pair Evaluations | 104,400 | — |
| 2 Raw Pricing Opportunities | 42,378 | — |
| 3 Fee Adjusted Opportunities | 35,295 | — |
| 4 Executable Candidate Observations | 35,295 | — |
| 5 Distinct Executable Episodes | 35,295 | — |

### Primary Rejection & Drop-Off Reasons
| Reject Reason Code | Filtered Count | Description / Mechanism |
|:---|---:|:---|
| `NO_RAW_EDGE` | 62,022 | Top-of-book or depth failure at this threshold |
| `BELOW_MIN_EDGE_AFTER_COSTS` | 7,083 | Top-of-book or depth failure at this threshold |

---

## 3. Distribution of Net Edges (Q3)
- **Sample Size:** 35295 executable candidate observations
- **Mean Net Edge:** 15.22¢ | **Median:** 11.60¢ | **Std Dev:** 11.71¢
- **Min Net Edge:** 2.10¢ | **Max Net Edge:** 68.20¢

| Percentile | Net Edge (¢) |
|:---|---:|
| P25 | 6.10¢ |
| P50 | 11.60¢ |
| P75 | 21.50¢ |
| P90 | 32.50¢ |
| P95 | 39.70¢ |

---

## 4. Opportunity Persistence & Duration (Q4)
- **Distinct Episodes Tracked:** 35295
- **Mean Persistence Duration:** 0.06s
- **Median Duration:** 0.00s

### Survival Across Latency & Persistence Thresholds
| Threshold | Surviving Episodes | Survival Rate % |
|:---|---:|---:|
| Persistence ≥ 0 seconds | 35295 | 100.0% |
| Persistence ≥ 5 seconds | 4 | 0.0% |
| Persistence ≥ 10 seconds | 4 | 0.0% |
| Persistence ≥ 30 seconds | 3 | 0.0% |

---

## 5. Order Book Depth & Fillability (Q5)
- **Average Resting YES Depth:** 50.0 contracts
- **Average Resting NO Depth:** 50.0 contracts

| Order Size $q$ (Contracts) | Fillability Rate % | Successful Fill Count |
|:---|---:|---:|
| 1 contracts | 100.0% | 52,200 |
| 10 contracts | 100.0% | 52,200 |
| 50 contracts | 100.0% | 52,200 |

---

## 6. Data Synchronization & Quality (Q7)
- **Usable Synchronized Pair Time:** **99.9%**
- **Median Quote Skew:** 0.0 ms (P95: 0.0 ms)
- **Median Quote Age:** 0.0 s (P95: 0.0 s)
- **Missing Exchange Timestamp %:** 0.0%

---
*Empirical analysis report compiled by `scripts/analyze_study.py` for Step 13 Senior Seminar.*