"""
SQLite 存储层
"""
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

DB_PATH = Path.home() / ".limit_ladder.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS zt_records (
            date             TEXT NOT NULL,
            code             TEXT NOT NULL,
            name             TEXT NOT NULL,
            price            REAL,
            consecutive_days INTEGER DEFAULT 1,
            float_cap        REAL,
            total_cap        REAL,
            first_limit_time TEXT,
            last_limit_time  TEXT,
            seal_amount      REAL,
            pe               REAL,
            PRIMARY KEY (date, code)
        );

        CREATE TABLE IF NOT EXISTS dt_records (
            date      TEXT NOT NULL,
            code      TEXT NOT NULL,
            name      TEXT NOT NULL,
            price     REAL,
            float_cap REAL,
            total_cap REAL,
            PRIMARY KEY (date, code)
        );

        CREATE TABLE IF NOT EXISTS stock_concepts (
            code         TEXT NOT NULL,
            concept_name TEXT NOT NULL,
            source       TEXT DEFAULT 'em',
            PRIMARY KEY (code, concept_name)
        );

        CREATE TABLE IF NOT EXISTS sector_daily_stats (
            date                    TEXT NOT NULL,
            sector_name             TEXT NOT NULL,
            zt_count                INTEGER DEFAULT 0,
            dt_count                INTEGER DEFAULT 0,
            net_buy_main            REAL,
            leader_code             TEXT,
            leader_name             TEXT,
            leader_consecutive_days INTEGER,
            leader_first_limit_time TEXT,
            PRIMARY KEY (date, sector_name)
        );

        CREATE TABLE IF NOT EXISTS fetch_log (
            key        TEXT PRIMARY KEY,
            fetched_at TEXT,
            status     TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_zt_date ON zt_records(date);
        """)
        # 兼容旧版数据库：pe 列可能不存在
        try:
            c.execute("ALTER TABLE zt_records ADD COLUMN pe REAL")
        except Exception:
            pass  # 列已存在，忽略
        # 兼容旧版数据库：source 列可能不存在
        try:
            c.execute("ALTER TABLE stock_concepts ADD COLUMN source TEXT DEFAULT 'em'")
        except Exception:
            pass  # 列已存在，忽略
        c.executescript("""
        CREATE INDEX IF NOT EXISTS idx_zt_code ON zt_records(code);
        CREATE INDEX IF NOT EXISTS idx_dt_date ON dt_records(date);
        CREATE INDEX IF NOT EXISTS idx_sc_code ON stock_concepts(code);
        CREATE INDEX IF NOT EXISTS idx_sc_name ON stock_concepts(concept_name);
        CREATE INDEX IF NOT EXISTS idx_ss_date ON sector_daily_stats(date);
        """)


# ─────────────────────────── fetch log ───────────────────────────

def is_fetched(key: str, max_age_hours: float = 12) -> bool:
    with get_conn() as c:
        row = c.execute(
            "SELECT fetched_at, status FROM fetch_log WHERE key=?", (key,)
        ).fetchone()
    if row is None or row["status"] != "ok":
        return False
    try:
        t = datetime.fromisoformat(row["fetched_at"])
        return datetime.now() - t < timedelta(hours=max_age_hours)
    except Exception:
        return False


def mark_fetched(key: str, status: str = "ok"):
    with get_conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO fetch_log VALUES (?,?,?)",
            (key, datetime.now().isoformat(), status),
        )


# ─────────────────────────── upserts ───────────────────────────

def upsert_zt_records(rows: list[dict]):
    if not rows:
        return
    with get_conn() as c:
        c.executemany(
            """INSERT OR REPLACE INTO zt_records
               (date, code, name, price, consecutive_days, float_cap, total_cap,
                first_limit_time, last_limit_time, seal_amount, pe)
               VALUES
               (:date,:code,:name,:price,:consecutive_days,:float_cap,:total_cap,
                :first_limit_time,:last_limit_time,:seal_amount,:pe)""",
            [{**r, "pe": r.get("pe")} for r in rows],
        )


def upsert_zt_pe(date: str, pe_map: dict[str, float]):
    """批量更新某日涨停记录的 PE 值"""
    if not pe_map:
        return
    with get_conn() as c:
        c.executemany(
            "UPDATE zt_records SET pe=? WHERE date=? AND code=?",
            [(pe, date, code) for code, pe in pe_map.items()],
        )


def upsert_dt_records(rows: list[dict]):
    if not rows:
        return
    with get_conn() as c:
        c.executemany(
            """INSERT OR REPLACE INTO dt_records
               (date, code, name, price, float_cap, total_cap)
               VALUES (:date,:code,:name,:price,:float_cap,:total_cap)""",
            rows,
        )


def upsert_stock_concepts(pairs: list[tuple], source: str = 'em'):
    """pairs = [(code, concept_name), ...]"""
    if not pairs:
        return
    with get_conn() as c:
        c.executemany(
            "INSERT OR IGNORE INTO stock_concepts (code, concept_name, source) VALUES (?,?,?)",
            [(code, name, source) for code, name in pairs],
        )


def upsert_sector_stats(rows: list[dict]):
    if not rows:
        return
    with get_conn() as c:
        c.executemany(
            """INSERT INTO sector_daily_stats
               (date, sector_name, zt_count, dt_count, net_buy_main,
                leader_code, leader_name, leader_consecutive_days,
                leader_first_limit_time)
               VALUES
               (:date,:sector_name,:zt_count,:dt_count,:net_buy_main,
                :leader_code,:leader_name,:leader_consecutive_days,
                :leader_first_limit_time)
               ON CONFLICT(date, sector_name) DO UPDATE SET
                 zt_count=excluded.zt_count,
                 dt_count=excluded.dt_count,
                 net_buy_main=COALESCE(excluded.net_buy_main, net_buy_main),
                 leader_code=excluded.leader_code,
                 leader_name=excluded.leader_name,
                 leader_consecutive_days=excluded.leader_consecutive_days,
                 leader_first_limit_time=excluded.leader_first_limit_time""",
            rows,
        )


# ─────────────────────────── queries ───────────────────────────

def get_zt_for_dates(dates: list[str]) -> dict[str, list[dict]]:
    """返回 {date: [row_dict, ...]} 已按 consecutive_days DESC 排序"""
    if not dates:
        return {}
    ph = ",".join("?" * len(dates))
    with get_conn() as c:
        rows = c.execute(
            f"""SELECT date, code, name, price, consecutive_days,
                       float_cap, total_cap, first_limit_time, seal_amount, pe
                FROM zt_records
                WHERE date IN ({ph})
                ORDER BY date DESC, consecutive_days DESC""",
            dates,
        ).fetchall()
    result: dict[str, list[dict]] = {}
    for row in rows:
        result.setdefault(row["date"], []).append(dict(row))
    return result


def get_dt_for_dates(dates: list[str]) -> dict[str, list[dict]]:
    if not dates:
        return {}
    ph = ",".join("?" * len(dates))
    with get_conn() as c:
        rows = c.execute(
            f"""SELECT date, code, name, price, float_cap, total_cap
                FROM dt_records
                WHERE date IN ({ph})
                ORDER BY date DESC""",
            dates,
        ).fetchall()
    result: dict[str, list[dict]] = {}
    for row in rows:
        result.setdefault(row["date"], []).append(dict(row))
    return result


def get_stock_concepts(codes: list[str], source: str | None = None) -> dict[str, list[str]]:
    """返回 {code: [concept_name, ...]}；source='em'/'ths' 可过滤来源"""
    if not codes:
        return {}
    ph = ",".join("?" * len(codes))
    params: list = list(codes)
    src_clause = ""
    if source is not None:
        src_clause = " AND source=?"
        params.append(source)
    with get_conn() as c:
        rows = c.execute(
            f"SELECT code, concept_name FROM stock_concepts WHERE code IN ({ph}){src_clause}",
            params,
        ).fetchall()
    result: dict[str, list[str]] = {}
    for row in rows:
        result.setdefault(row["code"], []).append(row["concept_name"])
    return result


def get_primary_concept_for_stock(code: str, date: str, all_concepts: list[str]) -> str:
    """主要概念 = 当日zt_pool中该概念下涨停股最多的那个"""
    if not all_concepts:
        return "N/A"
    ph = ",".join("?" * len(all_concepts))
    with get_conn() as c:
        row = c.execute(
            f"""SELECT sc.concept_name, COUNT(*) AS cnt
                FROM stock_concepts sc
                JOIN zt_records zt ON zt.code = sc.code AND zt.date = ?
                WHERE sc.concept_name IN ({ph})
                GROUP BY sc.concept_name
                ORDER BY cnt DESC
                LIMIT 1""",
            [date] + all_concepts,
        ).fetchone()
    if row:
        return row["concept_name"]
    return all_concepts[0]


def compute_primary_concepts_batch(date: str, codes: list[str]) -> dict[str, str]:
    """批量计算某日各股的主要概念，返回 {code: primary_concept}"""
    if not codes:
        return {}
    ph = ",".join("?" * len(codes))
    with get_conn() as c:
        # 先获取这批股票在当日zt_pool中各概念的涨停数
        rows = c.execute(
            f"""SELECT sc.code, sc.concept_name,
                       (SELECT COUNT(*) FROM stock_concepts sc2
                        JOIN zt_records zt ON zt.code = sc2.code AND zt.date = ?
                        WHERE sc2.concept_name = sc.concept_name) AS heat
                FROM stock_concepts sc
                WHERE sc.code IN ({ph})
                ORDER BY sc.code, heat DESC""",
            [date] + codes,
        ).fetchall()

    result: dict[str, str] = {}
    for row in rows:
        if row["code"] not in result:
            result[row["code"]] = row["concept_name"]
    return result


def get_concept_global_heat(source: str | None = None) -> dict[str, int]:
    """返回 {concept_name: 历史涨停股中拥有该概念的不同股票数量}。
    用于为每只股票固定主标签（全局热度最高的概念），确保同股跨日标签一致。"""
    src_clause = "" if source is None else " AND sc.source=?"
    params = [] if source is None else [source]
    with get_conn() as c:
        rows = c.execute(
            f"""SELECT sc.concept_name, COUNT(DISTINCT sc.code) AS cnt
               FROM stock_concepts sc
               WHERE sc.code IN (SELECT DISTINCT code FROM zt_records){src_clause}
               GROUP BY sc.concept_name""",
            params,
        ).fetchall()
    return {r["concept_name"]: r["cnt"] for r in rows}


def get_all_concept_names(source: str | None = None) -> list[str]:
    src_clause = "" if source is None else " WHERE source=?"
    params = [] if source is None else [source]
    with get_conn() as c:
        rows = c.execute(
            f"SELECT DISTINCT concept_name FROM stock_concepts{src_clause} ORDER BY concept_name",
            params,
        ).fetchall()
    return [r[0] for r in rows]


def get_zt_by_sector_for_date(date: str) -> dict[str, list[dict]]:
    """返回 {sector_name: [stock_dicts]} 涨停个股，用于 tooltip"""
    with get_conn() as c:
        rows = c.execute(
            """SELECT sc.concept_name, z.code, z.name, z.consecutive_days
               FROM zt_records z
               JOIN stock_concepts sc ON sc.code = z.code
               WHERE z.date = ?
               ORDER BY z.consecutive_days DESC""",
            (date,),
        ).fetchall()
    result: dict[str, list[dict]] = {}
    for r in rows:
        result.setdefault(r["concept_name"], []).append(dict(r))
    return result


def get_dt_by_sector_for_date(date: str) -> dict[str, list[dict]]:
    """返回 {sector_name: [stock_dicts]} 跌停个股，用于 tooltip"""
    with get_conn() as c:
        rows = c.execute(
            """SELECT sc.concept_name, z.code, z.name
               FROM dt_records z
               JOIN stock_concepts sc ON sc.code = z.code
               WHERE z.date = ?""",
            (date,),
        ).fetchall()
    result: dict[str, list[dict]] = {}
    for r in rows:
        result.setdefault(r["concept_name"], []).append(dict(r))
    return result


def get_sector_stats_for_date(date: str) -> list[dict]:
    with get_conn() as c:
        rows = c.execute(
            """SELECT * FROM sector_daily_stats
               WHERE date=? ORDER BY zt_count DESC""",
            (date,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_codes_missing_industry() -> list[str]:
    """返回在 zt_records 中但没有 stock_concepts 记录的股票代码列表"""
    with get_conn() as c:
        rows = c.execute(
            """SELECT DISTINCT z.code FROM zt_records z
               WHERE NOT EXISTS (
                   SELECT 1 FROM stock_concepts sc WHERE sc.code = z.code
               )"""
        ).fetchall()
    return [r[0] for r in rows]


def get_zt_dates_missing_industry(days: int = 8) -> list[str]:
    """返回最近 N 天内有涨停记录但包含缺少行业数据股票的日期列表"""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    with get_conn() as c:
        rows = c.execute(
            """SELECT DISTINCT z.date FROM zt_records z
               WHERE z.date >= ?
               AND NOT EXISTS (
                   SELECT 1 FROM stock_concepts sc WHERE sc.code = z.code
               )
               ORDER BY z.date""",
            (cutoff,),
        ).fetchall()
    return [r[0] for r in rows]


def clear_zt_fetch_log_recent(days: int = 8):
    """清除最近 N 天的涨停池 fetch_log，使 FetchWorker 强制重新拉取（含行业字段）"""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    with get_conn() as c:
        c.execute(
            "DELETE FROM fetch_log WHERE key LIKE 'zt_%' AND SUBSTR(key, 4) >= ?",
            (cutoff,),
        )


def get_dates_with_zt_data() -> list[str]:
    """返回已有涨停数据的日期列表，降序"""
    with get_conn() as c:
        rows = c.execute(
            "SELECT DISTINCT date FROM zt_records ORDER BY date DESC"
        ).fetchall()
    return [r[0] for r in rows]


def get_dates_missing_pe(days: int = 14) -> list[str]:
    """返回最近 N 天内有涨停记录但存在 pe IS NULL 的日期列表（升序）"""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    with get_conn() as c:
        rows = c.execute(
            """SELECT DISTINCT date FROM zt_records
               WHERE date >= ? AND pe IS NULL
               ORDER BY date""",
            (cutoff,),
        ).fetchall()
    return [r[0] for r in rows]


def get_codes_for_date(date: str) -> list[str]:
    """返回某日涨停记录的代码列表"""
    with get_conn() as c:
        rows = c.execute(
            "SELECT code FROM zt_records WHERE date=?", (date,)
        ).fetchall()
    return [r[0] for r in rows]


def has_concept_data() -> bool:
    with get_conn() as c:
        row = c.execute("SELECT COUNT(*) as n FROM stock_concepts").fetchone()
    return (row["n"] if row else 0) > 0
