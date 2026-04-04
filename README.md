# 连板天梯

A desktop app for tracking A-share limit-up (涨停) stocks, visualizing multi-day consecutive limit-up ladders, sector strength, and chip concentration — built with PyQt6 + AKShare + SQLite.

![Python](https://img.shields.io/badge/python-3.11%2B-blue) ![PyQt6](https://img.shields.io/badge/PyQt6-6.x-green) ![License](https://img.shields.io/badge/license-MIT-lightgrey)

---

## Features

### Tab 1 — 连板天梯 (Limit-Up Ladder)
- **Date columns**: each column is one trading day; scroll left/right across the date range
- **Band grouping**: stocks grouped by consecutive limit-up day count (1板, 2板, 3板 …)
- **Promotion rate**: percentage of stocks that advanced to the next board the following day
- **断板 (broken limit)**: stocks that were limit-up yesterday but not today, shown at the bottom of each column with their actual price change %
- **Stock cards** show:
  - Price capsule (expands to fill row) + actual day-change % + board badge
  - Name + code pill on the same row
  - Concept/sector pills (top 3 by global heat, color-coded)
  - Float market cap + PE + 90% chip concentration (集中度)

### Tab 2 — 板块统计 (Sector Stats)
- Sector × date grid: limit-up count, limit-down count, main-fund net inflow
- Leader stock per sector per day

### Left Filter Panel
| Filter | Description |
|--------|-------------|
| 连板数 | Min consecutive days (toggle buttons) |
| 市场板块 | 主板 / 创业板 / 科创板 / 北交所 |
| 流通市值 | Float-cap range (亿元) |
| 股价 | Price range (元) |
| 市盈率 PE | Include loss stocks / PE range |
| 筹码集中度 | 6 color dots: All · ≤3% · ≤8% · ≤15% · ≤20% · ≤30% |
| 概念板块 | Type-ahead search, multi-select |
| 概念口径 | EastMoney / THS / 全部 |

### Data Fetching
- **涨停池**: `stock_zt_pool_em` (EastMoney) — includes 涨跌幅, 封单量, 首次封停时间
- **跌停池**: `stock_dt_pool_em`
- **PE**: `stock_a_lg_t_valuation_em` batch fetch
- **行业/概念**: EastMoney concept (em) + THS concept (ths, via py_mini_racer)
- **筹码集中度**: `stock_cyq_em` — 90% chip concentration, direct from EastMoney JS engine (requires external network)
- **断板涨跌幅**: `stock_zh_a_daily` (Tencent) — actual close price vs previous ZT price
- All data cached in SQLite (`~/.limit_ladder.db`) with configurable TTL

---

## Requirements

```
python >= 3.11
PyQt6
akshare
pandas
py_mini_racer   # for THS concept and chip concentration (stock_cyq_em)
```

Install:
```bash
pip install -r requirements.txt
```

---

## Usage

```bash
python app.py
```

Click **刷新数据** to fetch the latest data. On first run it will populate the last ~7 trading days.

> **Note on chip concentration (筹码集中度)**:  
> `stock_cyq_em` calls EastMoney's K-line API which is blocked on some corporate networks.  
> Switch to an external network before clicking 刷新数据 to fetch chip data for all historical stocks.  
> Data is cached per stock with a 48-hour TTL so subsequent runs are fast.

---

## File Structure

| File | Description |
|------|-------------|
| `app.py` | PyQt6 UI — main window, tabs, stock cards, filters, background workers |
| `fetcher.py` | AKShare data fetch layer — all API calls, caching logic |
| `store.py` | SQLite storage layer — schema, upserts, queries |
| `requirements.txt` | Python dependencies |

---

## Database Schema

SQLite at `~/.limit_ladder.db`:

| Table | Key columns |
|-------|-------------|
| `zt_records` | date, code, name, price, consecutive_days, float_cap, total_cap, first_limit_time, seal_amount, pe, pct_change |
| `dt_records` | date, code, name, price, float_cap, total_cap |
| `stock_concepts` | code, concept_name, source (em/ths) |
| `sector_daily_stats` | date, sector_name, zt_count, dt_count, net_buy_main, leader info |
| `duanban_records` | date, code, pct_change |
| `cyq_records` | date, code, concentration_90 |
| `fetch_log` | key, fetched_at, status |
