"""
AKShare 数据获取层
"""
import json
import math
import threading
import time
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# 公司网络存在 MITM SSL 代理，Python 的 certifi 无法验证其企业根证书，
# 导致所有 HTTPS 请求被代理重置（RemoteDisconnected）。
# 对本地行情工具禁用 SSL 验证是可接受的取舍。
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import requests as _req
_orig_session_request = _req.Session.request
def _session_request_no_verify(self, method, url, **kwargs):
    kwargs.setdefault("verify", False)
    return _orig_session_request(self, method, url, **kwargs)
_req.Session.request = _session_request_no_verify

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta

_DATES_CACHE_FILE = Path.home() / ".limit_ladder_dates.json"


def _call_with_timeout(fn, timeout: int = 20):
    """在守护线程中调用 fn()，超时则抛 TimeoutError（守护线程继续但不阻塞主流程）"""
    result: list = [None]
    exc: list = [None]

    def _run():
        try:
            result[0] = fn()
        except Exception as e:
            exc[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if exc[0] is not None:
        raise exc[0]
    if t.is_alive():
        raise TimeoutError(f"API call timed out after {timeout}s")
    return result[0]

from store import (
    is_fetched, mark_fetched,
    upsert_zt_records, upsert_dt_records, upsert_zt_pe,
    upsert_stock_concepts, upsert_sector_stats,
    upsert_duanban_records, upsert_cyq_records,
    get_conn,
)


# ─────────────────────────── 辅助 ───────────────────────────

def _safe_float(v, default=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _normalize_cap(v):
    """市值字段：AKShare 有时返回元、有时返回亿元，统一转为亿元"""
    f = _safe_float(v)
    if f is None:
        return None
    # 超过 1e8 认为是元，转亿
    return f / 1e8 if f > 1e8 else f


def _normalize_seal(v):
    """封板资金：统一转为万元"""
    f = _safe_float(v)
    if f is None:
        return None
    return f / 1e4 if f > 1e4 else f


# ─────────────────────────── 交易日历 ───────────────────────────

def get_trading_dates(months: int = 6) -> list[str]:
    """返回过去 months 个月的交易日列表，降序（最新在前）；结果缓存30分钟到文件"""
    # 方案一：交易日历文件缓存（30分钟），避免每次启动都调 Sina API
    try:
        if _DATES_CACHE_FILE.exists():
            data = json.loads(_DATES_CACHE_FILE.read_text(encoding="utf-8"))
            if data.get("months") == months:
                cached_at = datetime.fromisoformat(data["cached_at"])
                if datetime.now() - cached_at < timedelta(minutes=30):
                    return data["dates"]
    except Exception:
        pass

    today = datetime.now().date()
    cutoff = (today - timedelta(days=months * 31))

    try:
        df = ak.tool_trade_date_hist_sina()
        if df is not None and not df.empty:
            col = df.columns[0]
            # 日期列取前10字符统一解析（兼容 "2024-01-02" 和 "1991-01-02T00:00:00.000Z"）
            raw = df[col].astype(str).str[:10]
            dates = pd.to_datetime(raw, format="%Y-%m-%d", errors="coerce").dropna()
            dates = dates[(dates.dt.date >= cutoff) & (dates.dt.date <= today)]
            result = sorted([d.strftime("%Y%m%d") for d in dates], reverse=True)
            if result:
                try:
                    _DATES_CACHE_FILE.write_text(
                        json.dumps({"months": months, "cached_at": datetime.now().isoformat(), "dates": result}),
                        encoding="utf-8",
                    )
                except Exception:
                    pass
                return result
    except Exception as e:
        print(f"get_trading_dates API error: {e}")

    # 降级：生成最近 months 个月的所有工作日（周一~周五）
    result = []
    cur = today
    while cur >= cutoff:
        if cur.weekday() < 5:  # 0=周一 … 4=周五
            result.append(cur.strftime("%Y%m%d"))
        cur -= timedelta(days=1)

    # 写入文件缓存
    try:
        _DATES_CACHE_FILE.write_text(
            json.dumps({"months": months, "cached_at": datetime.now().isoformat(), "dates": result}),
            encoding="utf-8",
        )
    except Exception:
        pass
    return result  # 已降序


# ─────────────────────────── 涨停池 ───────────────────────────

_ZT_POOL_MAX_DAYS = 8  # stock_zt_pool_em 实测仅近 ~7 个交易日可用，超出则挂起


def fetch_and_store_zt(date_str: str, force: bool = False, max_age_hours: float = 12) -> int:
    """获取并存储某日涨停数据，返回记录数"""
    key = f"zt_{date_str}"
    if not force and is_fetched(key, max_age_hours=max_age_hours):
        return -1

    # 超过 _ZT_POOL_MAX_DAYS 天的日期 zt_pool API 不可靠，直接标 empty，留给日K重建
    cutoff = (datetime.now() - timedelta(days=_ZT_POOL_MAX_DAYS)).strftime("%Y%m%d")
    if date_str < cutoff:
        mark_fetched(key, "empty")
        return 0

    try:
        df = _call_with_timeout(lambda: ak.stock_zt_pool_em(date=date_str), timeout=20)
        if df is None or df.empty:
            mark_fetched(key, "empty")
            return 0
        rows = []
        concept_pairs: list[tuple[str, str]] = []
        for _, r in df.iterrows():
            code = str(r.get("代码", "")).strip().zfill(6)
            if len(code) != 6:
                continue
            rows.append({
                "date": date_str,
                "code": code,
                "name": str(r.get("名称", "")),
                "price": _safe_float(r.get("最新价")),
                "pct_change": _safe_float(r.get("涨跌幅")),
                "consecutive_days": int(r.get("连板数", 1) or 1),
                "float_cap": _normalize_cap(r.get("流通市值")),
                "total_cap": _normalize_cap(r.get("总市值")),
                "first_limit_time": str(r.get("首次封板时间", "")),
                "last_limit_time": str(r.get("最后封板时间", "")),
                "seal_amount": _normalize_seal(r.get("封板资金")),
            })
            # 顺手写入行业信息（所属行业字段由 stock_zt_pool_em 提供）
            industry = str(r.get("所属行业", "")).strip()
            if industry and industry not in ("nan", "None", ""):
                concept_pairs.append((code, industry))
        upsert_zt_records(rows)
        if concept_pairs:
            upsert_stock_concepts(concept_pairs, source='em')
        # 同步拉取当日 PE，按当日价格比例换算后存储
        if rows:
            pe_price_map = fetch_pe_map(codes=[r["code"] for r in rows])
            if pe_price_map:
                date_pe = {r["code"]: _scale_pe(r["code"], r.get("price"), pe_price_map)
                           for r in rows}
                date_pe = {k: v for k, v in date_pe.items() if v is not None}
                if date_pe:
                    upsert_zt_pe(date_str, date_pe)
        mark_fetched(key)
        return len(rows)
    except Exception as e:
        print(f"fetch_zt {date_str} error: {e}")
        # 标记为 empty，让 OHLC 重建作为兜底
        mark_fetched(key, "empty")
        return 0


# ─────────────────────────── 跌停池 ───────────────────────────

def fetch_and_store_dt(date_str: str, force: bool = False, max_age_hours: float = 12) -> int:
    """获取并存储某日跌停数据，返回记录数"""
    key = f"dt_{date_str}"
    if not force and is_fetched(key, max_age_hours=max_age_hours):
        return -1
    cutoff = (datetime.now() - timedelta(days=_ZT_POOL_MAX_DAYS)).strftime("%Y%m%d")
    if date_str < cutoff:
        mark_fetched(key, "empty")
        return 0
    try:
        df = _call_with_timeout(lambda: ak.stock_zt_pool_dtgc_em(date=date_str), timeout=20)
        if df is None or df.empty:
            mark_fetched(key, "empty")
            return 0
        rows = []
        for _, r in df.iterrows():
            code = str(r.get("代码", "")).strip().zfill(6)
            if len(code) != 6:
                continue
            rows.append({
                "date": date_str,
                "code": code,
                "name": str(r.get("名称", "")),
                "price": _safe_float(r.get("最新价")),
                "float_cap": _normalize_cap(r.get("流通市值")),
                "total_cap": _normalize_cap(r.get("总市值")),
            })
        upsert_dt_records(rows)
        mark_fetched(key)
        return len(rows)
    except Exception as e:
        print(f"fetch_dt {date_str} error: {e}")
        mark_fetched(key, "empty")
        return 0


# ─────────────────────────── 概念数据（同花顺）───────────────────────────

def _get_ths_headers() -> dict | None:
    """生成同花顺请求所需的 Cookie/UA 头，失败则返回 None"""
    try:
        import py_mini_racer
        from akshare.datasets import get_ths_js
        with open(get_ths_js("ths.js"), encoding="utf-8") as f:
            js_content = f.read()
        js_code = py_mini_racer.MiniRacer()
        js_code.eval(js_content)
        v_code = js_code.call("v")
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Cookie": f"v={v_code}",
            "Referer": "https://q.10jqka.com.cn",
        }
    except Exception as e:
        print(f"_get_ths_headers error: {e}")
        return None


def _fetch_ths_concept_page(concept_code: str, page: int, headers: dict) -> list[str]:
    """
    从同花顺概念板块详情页获取第 page 页的股票代码列表。
    URL: https://q.10jqka.com.cn/gn/detail/code/{concept_code}/page/{page}/
    返回 [code, ...] 或 [] (出错/最后一页)
    """
    import requests as _req
    from bs4 import BeautifulSoup as _BS
    url = f"https://q.10jqka.com.cn/gn/detail/code/{concept_code}/page/{page}/"
    if page == 1:
        url = f"https://q.10jqka.com.cn/gn/detail/code/{concept_code}/"
    r = _req.get(url, headers=headers, timeout=15)
    soup = _BS(r.text, "lxml")
    tables = soup.find_all("table")
    if not tables:
        return []
    codes = []
    for row in tables[0].find_all("tr")[1:]:
        tds = row.find_all("td")
        if len(tds) >= 2:
            code = tds[1].text.strip()
            if len(code) == 6 and code.isdigit():
                codes.append(code)
    return codes


def fetch_and_store_ths_concepts(
    target_codes: set[str],
    progress_cb=None,
    stop_flag=None,
    max_pages: int = 3,
) -> int:
    """
    从同花顺 375 个概念板块反查股票所属概念，仅保留 target_codes 中的股票。
    按涨跌幅降序，今日涨停股必在第1页，历史涨停股覆盖前 max_pages×10 只。
    返回新增的 (code, concept) 对数量。
    """
    key = "ths_concepts_built"
    if is_fetched(key, max_age_hours=168):
        return 0

    headers = _get_ths_headers()
    if not headers:
        return 0

    try:
        df = ak.stock_board_concept_name_ths()
        concepts = list(zip(df["name"].tolist(), df["code"].tolist()))
    except Exception as e:
        print(f"fetch concept list error: {e}")
        return 0

    batch: list[tuple[str, str]] = []
    total = len(concepts)
    total_pairs = 0

    for i, (concept_name, concept_code) in enumerate(concepts):
        if stop_flag and stop_flag():
            break
        if progress_cb:
            progress_cb(i + 1, total, concept_name)

        for page in range(1, max_pages + 1):
            try:
                codes = _fetch_ths_concept_page(concept_code, page, headers)
            except Exception:
                break
            for code in codes:
                if code in target_codes:
                    batch.append((code, concept_name))
            if len(codes) < 10:  # 最后一页
                break
            time.sleep(0.25)

        # 每 20 个概念写一次库（防止中途退出丢失进度）
        if batch and (i % 20 == 19 or i == total - 1):
            upsert_stock_concepts(batch, source='ths')
            total_pairs += len(batch)
            batch.clear()
        time.sleep(0.15)

    if batch:
        upsert_stock_concepts(batch, source='ths')
        total_pairs += len(batch)

    mark_fetched(key)
    return total_pairs


def fetch_and_store_concepts(
    target_codes: set[str],
    progress_cb=None,
    stop_flag=None,
) -> int:
    """已废弃（东方财富接口不可用），转发到 THS 版本"""
    return fetch_and_store_ths_concepts(target_codes, progress_cb, stop_flag)

    if pairs:
        upsert_stock_concepts(pairs)
    mark_fetched(key)
    return len(pairs)


# ─────────────────────────── 行业补充（个股详情回填）───────────────────────────

def fetch_and_store_missing_industry(codes: list[str], progress_cb=None, stop_flag=None) -> int:
    """
    对于没有行业数据的股票，逐个调用 stock_individual_info_em 补充行业。
    返回成功补充的数量。
    """
    pairs: list[tuple[str, str]] = []
    for i, code in enumerate(codes):
        if stop_flag and stop_flag():
            break
        if progress_cb:
            progress_cb(i + 1, len(codes), code)
        try:
            df = _call_with_timeout(
                lambda c=code: ak.stock_individual_info_em(symbol=c), timeout=10
            )
            if df is None or df.empty:
                continue
            # 找行业行
            industry_row = df[df["item"] == "行业"]
            if industry_row.empty:
                continue
            industry = str(industry_row.iloc[0]["value"]).strip()
            if industry and industry not in ("nan", "None", ""):
                pairs.append((code, industry))
        except Exception as e:
            pass
        time.sleep(0.1)

    if pairs:
        upsert_stock_concepts(pairs, source='em')
    return len(pairs)


# ─────────────────────────── 板块统计 ───────────────────────────

def build_sector_stats_for_date(date_str: str, net_buy_map: dict | None = None):
    """
    根据 zt/dt 记录和概念数据计算板块统计并存储。
    net_buy_map: {sector_name: 万元净买入} 可选，若提供则写入。
    """
    with get_conn() as c:
        zt_rows = c.execute(
            """SELECT z.code, z.name, z.consecutive_days, z.first_limit_time,
                      sc.concept_name
               FROM zt_records z
               LEFT JOIN stock_concepts sc ON sc.code = z.code
               WHERE z.date = ?""",
            (date_str,),
        ).fetchall()

        dt_rows = c.execute(
            """SELECT z.code, z.name, z.float_cap, sc.concept_name
               FROM dt_records z
               LEFT JOIN stock_concepts sc ON sc.code = z.code
               WHERE z.date = ?""",
            (date_str,),
        ).fetchall()

    zt_by_sector: dict[str, int] = {}
    leaders: dict[str, tuple] = {}  # sector → (days, ftime, code, name)

    for row in zt_rows:
        concept = row["concept_name"]
        if not concept:
            continue
        zt_by_sector[concept] = zt_by_sector.get(concept, 0) + 1
        days = row["consecutive_days"] or 1
        ftime = row["first_limit_time"] or "235959"
        cur = leaders.get(concept)
        if cur is None or days > cur[0] or (days == cur[0] and ftime < cur[1]):
            leaders[concept] = (days, ftime, row["code"], row["name"])

    dt_by_sector: dict[str, int] = {}
    dt_leaders: dict[str, tuple] = {}  # sector → (float_cap, code, name)
    for row in dt_rows:
        concept = row["concept_name"]
        if not concept:
            continue
        dt_by_sector[concept] = dt_by_sector.get(concept, 0) + 1
        cap = row["float_cap"] or 0
        cur = dt_leaders.get(concept)
        if cur is None or cap > cur[0]:
            dt_leaders[concept] = (cap, row["code"], row["name"])

    all_sectors = set(zt_by_sector) | set(dt_by_sector)
    stats_rows = []
    for sector in all_sectors:
        ldr = leaders.get(sector)            # 优先取涨停龙头
        if ldr is None:
            dt_ldr = dt_leaders.get(sector)  # 无涨停时取跌停市值最大股
            if dt_ldr:
                ldr = (None, None, dt_ldr[1], dt_ldr[2])
        stats_rows.append({
            "date": date_str,
            "sector_name": sector,
            "zt_count": zt_by_sector.get(sector, 0),
            "dt_count": dt_by_sector.get(sector, 0),
            "net_buy_main": (net_buy_map or {}).get(sector),
            "leader_code": ldr[2] if ldr else None,
            "leader_name": ldr[3] if ldr else None,
            "leader_consecutive_days": ldr[0] if ldr else None,
            "leader_first_limit_time": ldr[1] if ldr else None,
        })
    if stats_rows:
        upsert_sector_stats(stats_rows)


def fetch_pe_map(codes: list[str] | None = None,
                 progress_cb=None) -> dict[str, tuple[float, float]]:
    """
    用腾讯财经 HTTP 接口批量获取动态 PE 和当前价格。
    返回 {code: (pe, current_price)}，供调用方做历史日期价格比例换算。
    """
    import requests
    if not codes:
        return {}

    def _market_prefix(code: str) -> str:
        if code.startswith("6") or code.startswith("5"):
            return f"sh{code}"
        elif code.startswith("8") or code.startswith("4"):
            return f"bj{code}"
        else:
            return f"sz{code}"

    result: dict[str, tuple[float, float]] = {}
    batch_size = 50
    total = len(codes)
    for batch_start in range(0, total, batch_size):
        batch = codes[batch_start: batch_start + batch_size]
        if progress_cb:
            progress_cb(batch_start + len(batch), total, batch[-1])
        symbols = ",".join(_market_prefix(c) for c in batch)
        try:
            resp = requests.get(
                f"http://qt.gtimg.cn/q={symbols}",
                timeout=10,
                headers={"Referer": "http://gu.qq.com/"},
            )
            for line in resp.text.splitlines():
                if "=" not in line:
                    continue
                _, val = line.split("=", 1)
                val = val.strip().strip('";')
                parts = val.split("~")
                if len(parts) < 40:
                    continue
                code = parts[2].zfill(6)
                pe = _safe_float(parts[39])     # 动态市盈率
                price = _safe_float(parts[3])   # 当前价格
                if pe is not None and pe != 0 and price and price > 0:
                    result[code] = (pe, price)
        except Exception as e:
            print(f"[PE] batch error: {e}")
        time.sleep(0.1)
    return result


def _scale_pe(code: str, date_price: float | None,
              pe_price_map: dict[str, tuple[float, float]]) -> float | None:
    """用价格比例将当前 PE 换算为历史日期的 PE。"""
    if code not in pe_price_map or not date_price or date_price <= 0:
        return None
    current_pe, current_price = pe_price_map[code]
    if current_price <= 0:
        return None
    # historical_PE ≈ date_price × (current_PE / current_price)
    return date_price * current_pe / current_price


def fetch_today_sector_net_buy() -> dict[str, float]:
    """
    获取今日板块主力净买入，返回 {sector_name: 元}。
    同时查询东财「行业资金流」和「概念资金流」，确保与涨停池「所属行业」命名对应。
    接口返回值单位为元，直接存储。
    """
    result: dict[str, float] = {}
    for sector_type in ["行业资金流", "概念资金流"]:
        try:
            df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type=sector_type)
            if df is None or df.empty:
                continue
            for _, row in df.iterrows():
                name = str(row.get("名称", "")).strip()
                val = _safe_float(str(row.get("今日主力净流入-净额", "")))
                if name and val is not None and name not in result:
                    result[name] = val
        except Exception as e:
            print(f"fetch_today_sector_net_buy [{sector_type}] error: {e}")
    return result


# ─────────────────────────── 全量获取入口 ───────────────────────────

def fetch_range(
    dates: list[str],
    progress_cb=None,
    stop_flag=None,
    today: str | None = None,
) -> dict[str, int]:
    """
    批量获取指定日期范围的 zt/dt 数据（方案二：zt+dt 并行，跨日期并行）。
    today: 今日日期字符串，今日数据使用5分钟缓存，历史数据使用12小时缓存。
    progress_cb(i, total, date_str, type_str)
    返回 {date: zt_count}
    """
    result: dict[str, int] = {}
    if not dates:
        return result

    lock = threading.Lock()
    completed = [0]

    def _fetch_one(date_str: str) -> tuple[str, int]:
        if stop_flag and stop_flag():
            return date_str, 0
        # 方案一：今日用5分钟缓存，历史用12小时缓存
        max_age = 5 / 60 if date_str == today else 12
        cnt_zt = fetch_and_store_zt(date_str, max_age_hours=max_age)
        if stop_flag and stop_flag():
            return date_str, max(cnt_zt, 0)
        fetch_and_store_dt(date_str, max_age_hours=max_age)
        return date_str, max(cnt_zt, 0)

    # 最多3个日期并行，避免触发东方财富限流
    with ThreadPoolExecutor(max_workers=min(len(dates), 3)) as exe:
        future_map = {exe.submit(_fetch_one, d): d for d in dates}
        for fut in as_completed(future_map):
            d, cnt = fut.result()
            with lock:
                result[d] = cnt
                completed[0] += 1
            if progress_cb:
                progress_cb(completed[0], len(dates), d, "zt+dt")

    return result


# ─────────────────────── 历史涨停（日K反推）───────────────────────

def _market_prefix(code: str) -> str:
    """返回腾讯行情接口所需市场前缀（sh/sz/bj）"""
    if code.startswith(("43", "83", "87", "92")):  # 北交所（含新代码段 92xxxx）
        return "bj"
    if code.startswith(("6", "9")):                 # 上交所
        return "sh"
    return "sz"                                      # 深交所


def _get_limit_pct(code: str, name: str) -> float:
    """根据代码和名称判断涨停幅度"""
    name_up = (name or "").upper()
    if "ST" in name_up:
        return 0.05
    if code.startswith(("688", "689")):
        return 0.20
    if code.startswith(("300", "301")):
        return 0.20
    if code.startswith("8"):  # 北交所
        return 0.30
    return 0.10


def _calc_zt_for_stock(
    code: str,
    name: str,
    float_cap,
    total_cap,
    start: str,
    end: str,
    needed_dates: frozenset,
) -> list[dict]:
    """从腾讯日K数据计算某只股票在指定日期集合内的涨停记录（不依赖 py_mini_racer）"""
    try:
        sym = f"{_market_prefix(code)}{code}"
        url = (
            f"https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
            f"?_var=x&param={sym},day,,,600,&cb=&r=0.1"
        )
        resp = _req.get(url, timeout=15, verify=False)
        text = resp.text
        if text.startswith("x="):
            text = text[2:]
        raw = json.loads(text)
        rows = raw.get("data", {}).get(sym, {}).get("day", [])
        if not rows:
            return []

        limit_pct = _get_limit_pct(code, name)
        records: list[dict] = []
        consecutive = 0
        prev_close = None

        for r in rows:
            try:
                date_str = str(r[0]).replace("-", "")[:8]
                close = float(r[2])
                high = float(r[3])
                low = float(r[4])
            except (IndexError, ValueError):
                prev_close = None
                consecutive = 0
                continue

            if prev_close is not None and date_str <= end:
                limit_price = round(prev_close * (1 + limit_pct), 2)
                is_zt = abs(close - limit_price) < 0.005 and abs(high - close) < 0.005
                if is_zt:
                    consecutive += 1
                    if date_str in needed_dates:
                        pct = round((close - prev_close) / prev_close * 100, 2)
                        records.append({
                            "date": date_str,
                            "code": code,
                            "name": name,
                            "price": close,
                            "pct_change": pct,
                            "consecutive_days": consecutive,
                            "float_cap": float_cap,
                            "total_cap": total_cap,
                            "first_limit_time": None,
                            "last_limit_time": None,
                            "seal_amount": None,
                        })
                else:
                    consecutive = 0

            prev_close = close

        return records
    except Exception:
        return []


def fetch_historical_zt_all(progress_cb=None, stop_flag=None) -> int:
    """
    用日K数据为历史空白日期（stock_zt_pool_em 不覆盖的）重建涨停记录。
    返回填充的交易日数量。
    """
    if is_fetched("historical_zt_built", max_age_hours=168):  # 一周有效
        return 0

    # 找出 status='empty' 的历史日期
    with get_conn() as c:
        rows = c.execute(
            "SELECT key FROM fetch_log WHERE status='empty' AND key LIKE 'zt_%'"
        ).fetchall()
    needed_dates = frozenset(r[0][3:] for r in rows)  # 去掉 'zt_' 前缀

    if not needed_dates:
        mark_fetched("historical_zt_built")
        return 0

    if progress_cb:
        progress_cb(f"历史数据：需填充 {len(needed_dates)} 个交易日，获取股票列表中...")

    # 一次性获取全市场实时行情，拿到市值
    cap_map: dict[str, tuple] = {}
    try:
        spot_df = ak.stock_zh_a_spot_em()
        for _, r in spot_df.iterrows():
            code = str(r.get("代码", "")).strip().zfill(6)
            if len(code) != 6:
                continue
            cap_map[code] = (
                _normalize_cap(r.get("流通市值")),
                _normalize_cap(r.get("总市值")),
                str(r.get("名称", "")),
            )
    except Exception as e:
        print(f"fetch spot error: {e}")

    # 获取全量代码列表
    try:
        code_df = ak.stock_info_a_code_name()
        stock_list: list[tuple] = []
        for _, r in code_df.iterrows():
            code = str(r.iloc[0]).strip().zfill(6)
            name = str(r.iloc[1]) if len(r) > 1 else cap_map.get(code, (None, None, ""))[2]
            fc, tc, _ = cap_map.get(code, (None, None, ""))
            stock_list.append((code, name, fc, tc))
    except Exception as e:
        print(f"fetch code list error: {e}")
        return 0

    dates_sorted = sorted(needed_dates)
    start_date, end_date = dates_sorted[0], dates_sorted[-1]
    total = len(stock_list)

    if progress_cb:
        progress_cb(f"从日K重建历史涨停（{total} 只股票，请耐心等待...）")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    lock = threading.Lock()
    buf: list[dict] = []
    done_count = [0]

    def _process(args):
        if stop_flag and stop_flag():
            return []
        code, name, fc, tc = args
        return _calc_zt_for_stock(code, name, fc, tc, start_date, end_date, needed_dates)

    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(_process, args): args[0] for args in stock_list}
        for fut in as_completed(futures):
            if stop_flag and stop_flag():
                break
            try:
                records = fut.result(timeout=30)
            except Exception:
                records = []

            done_count[0] += 1
            with lock:
                buf.extend(records)
                if len(buf) >= 1000:
                    upsert_zt_records(buf)
                    buf.clear()

            if progress_cb and done_count[0] % 100 == 0:
                progress_cb(
                    f"历史涨停重建 ({done_count[0]}/{total})  "
                    f"已找到记录: {len(buf)}"
                )

    with lock:
        if buf:
            upsert_zt_records(buf)

    # 将 empty 状态改为 ok_ohlc，避免下次重复处理
    with get_conn() as c:
        c.executemany(
            "UPDATE fetch_log SET status='ok_ohlc' WHERE key=?",
            [(f"zt_{d}",) for d in needed_dates],
        )
    mark_fetched("historical_zt_built")

    if progress_cb:
        progress_cb(f"历史数据重建完成，覆盖 {len(needed_dates)} 个交易日")
    return len(needed_dates)


# ─────────────────────────── 断板涨跌幅 ───────────────────────────

def fetch_and_store_duanban_pct(
    date_str: str,
    code_prev_price: dict[str, float],
    max_age_hours: float = 24,
    stop_flag=None,
) -> int:
    """
    获取并存储指定日期各断板股票的当日实际涨跌幅。
    code_prev_price: {code: 前一交易日收盘价（即ZT价格）}
    使用 stock_zh_a_daily 拉取当日收盘价，计算 pct_change。
    返回成功写入的记录数；仅在有数据写入时才缓存成功标记。
    """
    key = f"duanban_pct_{date_str}"
    if is_fetched(key, max_age_hours=max_age_hours):
        # 缓存有效，但若记录数少于预期股票数则视为部分拉取，继续补全
        with get_conn() as c:
            cnt = c.execute(
                "SELECT COUNT(*) FROM duanban_records WHERE date=?", (date_str,)
            ).fetchone()[0]
        if cnt >= len(code_prev_price) or not code_prev_price:
            return -1
        # 记录不全（部分拉取），落穿继续补全

    if not code_prev_price:
        mark_fetched(key)
        return 0

    rows: list[dict] = []

    for code, prev_close in code_prev_price.items():
        if stop_flag and stop_flag():
            break
        if not prev_close or prev_close <= 0:
            continue
        try:
            prefix = _market_prefix(code)
            today_close = None
            if prefix == "bj":
                # 北交所：stock_zh_a_daily 不支持，改用腾讯 proxy kline
                sym = f"bj{code}"
                url = (f"https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
                       f"?_var=x&param={sym},day,,,5,&cb=&r=0.1")
                resp = _req.get(url, timeout=12, verify=False)
                text = resp.text[2:] if resp.text.startswith("x=") else resp.text
                krows = json.loads(text).get("data", {}).get(sym, {}).get("day", [])
                for r in krows:
                    if str(r[0]).replace("-", "")[:8] == date_str:
                        today_close = _safe_float(r[2])
                        break
            else:
                df = _call_with_timeout(
                    lambda c=code, p=prefix: ak.stock_zh_a_daily(
                        symbol=f"{p}{c}",
                        start_date=date_str,
                        end_date=date_str,
                        adjust="",
                    ),
                    timeout=12,
                )
                if df is not None and not df.empty:
                    today_close = _safe_float(df.iloc[-1]["close"])
            if today_close and today_close > 0:
                pct = round((today_close - prev_close) / prev_close * 100, 2)
                rows.append({"date": date_str, "code": code, "pct_change": pct})
        except Exception as e:
            print(f"fetch_duanban_pct {code} {date_str}: {e}")
        time.sleep(0.05)

    if rows:
        upsert_duanban_records(rows)
        mark_fetched(key)
    return len(rows)


# ─────────────────────────── 筹码集中度 ───────────────────────────

def _compute_cyq_concentration(
    df: pd.DataFrame, pct: float = 0.9, factor: int = 150, window: int = 120
) -> float | None:
    """
    复现东方财富 CYQCalculator 算法，计算 90% 筹码集中度。
    df 需含列: open, high, low, close, turnover（换手率小数形式，如 0.02 = 2%）
    返回 (pr1-pr0)/(pr1+pr0)，越小越集中；失败返回 None。
    """
    if df is None or len(df) == 0:
        return None
    required = {"open", "high", "low", "close", "turnover"}
    if not required.issubset(df.columns):
        return None

    kdata = df.tail(window).reset_index(drop=True)
    maxprice = float(kdata["high"].max())
    minprice = float(kdata["low"].min())
    if maxprice <= minprice:
        return None

    accuracy = max(0.01, (maxprice - minprice) / (factor - 1))
    xdata = [0.0] * factor

    for _, row in kdata.iterrows():
        o = float(row["open"])
        c = float(row["close"])
        h = float(row["high"])
        lo = float(row["low"])
        avg = (o + c + h + lo) / 4.0
        tr = float(row["turnover"]) if not pd.isna(row["turnover"]) else 0.0
        turnover_rate = min(1.0, tr)

        H = int((h - minprice) / accuracy)
        L = math.ceil((lo - minprice) / accuracy)
        G_x = float(factor - 1) if h == lo else 2.0 / (h - lo)
        G_y = int((avg - minprice) / accuracy)

        decay = 1.0 - turnover_rate
        for n in range(factor):
            xdata[n] *= decay

        if h == lo:
            if 0 <= G_y < factor:
                xdata[G_y] += G_x * turnover_rate / 2.0
        else:
            for j in range(max(0, L), min(H + 1, factor)):
                curprice = minprice + accuracy * j
                if curprice <= avg:
                    denom = avg - lo
                    if abs(denom) < 1e-8:
                        xdata[j] += G_x * turnover_rate
                    else:
                        xdata[j] += (curprice - lo) / denom * G_x * turnover_rate
                else:
                    denom = h - avg
                    if abs(denom) < 1e-8:
                        xdata[j] += G_x * turnover_rate
                    else:
                        xdata[j] += (h - curprice) / denom * G_x * turnover_rate

    total_chips = sum(xdata)
    if total_chips <= 0:
        return None

    ps0 = (1 - pct) / 2
    ps1 = (1 + pct) / 2

    def get_cost_by_chip(target: float) -> float:
        s = 0.0
        for i, x in enumerate(xdata):
            if s + x > target:
                return minprice + i * accuracy
            s += x
        return maxprice

    pr0 = get_cost_by_chip(total_chips * ps0)
    pr1 = get_cost_by_chip(total_chips * ps1)
    if (pr0 + pr1) == 0:
        return 0.0
    return round((pr1 - pr0) / (pr0 + pr1), 4)


def _fetch_kline_with_turnover(code: str, n_days: int = 500) -> "pd.DataFrame | None":
    """
    用腾讯 proxy.finance.qq.com 日K接口获取 OHLCV + 官方换手率（字段[7]，单位%）。
    返回含 date(YYYYMMDD)/open/high/low/close/turnover 列的 DataFrame（升序），
    失败返回 None。
    turnover = 字段[7] / 100（官方换手率，已还原为小数）。
    """
    import requests as _r

    sym = f"{_market_prefix(code)}{code}"
    url = (
        f"https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
        f"?_var=x&param={sym},day,,,{n_days},&cb=&r=0.1"
    )
    resp = _r.get(url, timeout=10, verify=False)
    text = resp.text
    # 去掉 JSONP 包装 "x="
    if text.startswith("x="):
        text = text[2:]
    raw = json.loads(text)
    records = raw.get("data", {}).get(sym, {}).get("day", [])
    if not records:
        return None

    rows = []
    for r in records:
        try:
            date_str = str(r[0]).replace("-", "")[:8]
            # 字段[7] 是官方换手率（%），除以100得小数；字段数<8时回退0
            turnover_pct = float(r[7]) if len(r) > 7 and r[7] not in ("", None) else 0.0
            rows.append({
                "date":     date_str,
                "open":     float(r[1]),
                "close":    float(r[2]),
                "high":     float(r[3]),
                "low":      float(r[4]),
                "turnover": min(1.0, turnover_pct / 100.0),
            })
        except (IndexError, ValueError):
            continue
    return pd.DataFrame(rows) if rows else None


def fetch_and_store_cyq(
    codes: list[str],
    max_age_hours: float = 48,
    stop_flag=None,
) -> int:
    """
    用腾讯日 K JSON + CYQCalculator 为每个涨停日期独立计算筹码集中度。
    对每只股票取最近500日K线，对其在 zt_records 中每次出现的日期，
    各自截取该日往前120日的窗口数据单独计算，确保历史日期的集中度
    反映当时的筹码分布，而非用同一个值代替所有日期。
    """
    from store import get_conn
    CYQ_WINDOW = 120
    total = 0
    for code in codes:
        if stop_flag and stop_flag():
            break
        key = f"cyq_code_{code}"
        if is_fetched(key, max_age_hours=max_age_hours):
            continue
        try:
            df = _fetch_kline_with_turnover(code, n_days=500)
            if df is None or df.empty:
                continue

            # 建立日期→位置映射，便于按 zt_date 截窗口
            date_list = list(df["date"])
            date_to_pos = {d: i for i, d in enumerate(date_list)}

            with get_conn() as conn:
                zt_dates = [r[0] for r in conn.execute(
                    "SELECT DISTINCT date FROM zt_records WHERE code=?", (code,)
                ).fetchall()]
            if not zt_dates:
                continue

            ohlct_cols = ["open", "close", "high", "low", "turnover"]
            rows_to_insert = []
            for zt_date in zt_dates:
                # 找到 zt_date 对应的 kline 位置（或最近较早的一天）
                pos = date_to_pos.get(zt_date)
                if pos is None:
                    earlier = [i for i, d in enumerate(date_list) if d <= zt_date]
                    if not earlier:
                        continue
                    pos = earlier[-1]

                start = max(0, pos - CYQ_WINDOW + 1)
                window_df = df.iloc[start: pos + 1][ohlct_cols]

                if len(window_df) < 10:
                    continue
                c = _compute_cyq_concentration(window_df)
                if c is not None:
                    rows_to_insert.append({
                        "date": zt_date, "code": code,
                        "concentration_90": float(c),
                    })

            if rows_to_insert:
                upsert_cyq_records(rows_to_insert)
                mark_fetched(key)
                total += len(rows_to_insert)
        except Exception as e:
            print(f"fetch_cyq {code}: {e}")
        time.sleep(0.1)
    return total
