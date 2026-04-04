"""
连板天梯 & 板块日统计
PyQt6 桌面应用
"""

import sys
import json
from datetime import datetime
from pathlib import Path

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QGroupBox, QLabel, QSpinBox, QDoubleSpinBox, QPushButton,
    QTabWidget, QScrollArea, QFrame, QSplitter, QStatusBar,
    QLineEdit, QCompleter, QTableWidget, QTableWidgetItem,
    QHeaderView, QSizePolicy, QAbstractScrollArea, QProgressBar,
    QDialog, QCalendarWidget, QDialogButtonBox,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QStringListModel, QTimer, QDate
from PyQt6.QtGui import QColor, QFont, QBrush, QTextCharFormat

import store
import fetcher

# ──────────────────────────────── 颜色常量 ────────────────────────────────

BG          = "#FFFFFF"
SURFACE     = "#F7F7F7"
TEXT        = "#1A1A1A"
MUTED       = "#8E8E93"
ACCENT      = "#007AFF"
BORDER      = "#E5E5E7"
SEL         = "#EBEBEB"
ROW_SEP     = "#F0F0F0"
RED         = "#FF3B30"
RED_DIM     = "#FDECEA"
GREEN       = "#34C759"
GREEN_DIM   = "#E8F8ED"
ORANGE      = "#FF9500"

# 连板数 → (背景色, 文字色)
DAYS_COLOR: dict[int, tuple[str, str]] = {
    1:  ("#F0F0F0", "#555555"),
    2:  ("#FFF3E0", "#BF360C"),
    3:  ("#FFE0B2", "#BF360C"),
    4:  ("#FFCC80", "#8D3800"),
    5:  ("#FF9800", "#FFFFFF"),
    6:  ("#FF6D00", "#FFFFFF"),
    7:  ("#DD2C00", "#FFFFFF"),
    8:  ("#B71C1C", "#FFFFFF"),
    9:  ("#880E4F", "#FFFFFF"),
    10: ("#4A148C", "#FFFFFF"),
}

# 概念胶囊颜色调色板（bg, fg），按概念名稳定哈希分配
PILL_COLORS: list[tuple[str, str]] = [
    ("#DBEAFE", "#1E40AF"),  # blue
    ("#EDE9FE", "#5B21B6"),  # violet
    ("#D1FAE5", "#065F46"),  # emerald
    ("#FEF3C7", "#78350F"),  # amber
    ("#FFE4E6", "#881337"),  # rose
    ("#CCFBF1", "#134E4A"),  # teal
    ("#E0F2FE", "#0C4A6E"),  # sky
    ("#F3E8FF", "#581C87"),  # purple
    ("#FFF7ED", "#7C2D12"),  # orange
    ("#F0FDF4", "#14532D"),  # green
    ("#FDF4FF", "#701A75"),  # fuchsia
    ("#FFFBEB", "#78350F"),  # yellow
    ("#FCE7F3", "#831843"),  # pink
    ("#F0F9FF", "#0C4A6E"),  # sky-light
    ("#F7FEE7", "#3F6212"),  # lime
    ("#FEF2F2", "#991B1B"),  # red-light
]


def concept_pill_color(name: str) -> tuple[str, str]:
    """将概念名稳定映射到胶囊颜色（跨日期/卡片一致）"""
    idx = sum(ord(c) for c in name) % len(PILL_COLORS)
    return PILL_COLORS[idx]


# 连板天梯布局常量
CARD_H = 100      # 每张股票卡片的固定高度（px，增高以容纳多行胶囊）
CARD_SPACING = 6  # 卡片间距
BAND_PAD = 8      # 每个板数分组的顶部内边距

def days_color(n: int) -> tuple[str, str]:
    if n <= 0:
        n = 1
    return DAYS_COLOR.get(n, DAYS_COLOR[10])


CONFIG_PATH = Path.home() / ".limit_ladder_config.json"

# ──────────────────────────────── 后台线程 ────────────────────────────────

class FetchWorker(QThread):
    """获取缺失日期的涨停/跌停数据；若 dates 为 None 则自行计算缺失日期"""
    progress = pyqtSignal(str)
    done = pyqtSignal(list, int)   # (all_dates, new_records_count)

    def __init__(self, dates: list[str] | None = None, months: int = 6,
                 force_dates: list[str] | None = None, parent=None):
        super().__init__(parent)
        self._dates = dates   # None = 由线程自行获取交易日历
        self._months = months
        self._force_dates = set(force_dates or [])
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        try:
            if self._dates is None:
                self.progress.emit("正在获取交易日历...")
                all_dates = fetcher.get_trading_dates(months=self._months)
                if not all_dates:
                    self.progress.emit("获取交易日历失败，请检查网络")
                    self.done.emit([], 0)
                    return
                cached = set(store.get_dates_with_zt_data())
                today = all_dates[0] if all_dates else None
                # 方案一：今日数据5分钟内已拉取则跳过，超过5分钟则重拉
                need = [d for d in all_dates
                        if d not in cached
                        or d in self._force_dates
                        or (d == today and not store.is_fetched(f"zt_{d}", max_age_hours=5 / 60))]
            else:
                all_dates = None  # 由调用方提供，done 后由主线程查 DB
                need = self._dates
                today = None

            def cb(i, total, date, typ):
                self.progress.emit(f"正在获取 {date} {typ} ({i}/{total})")

            # 方案二：fetch_range 内部并行拉取 zt+dt
            counts = fetcher.fetch_range(need, progress_cb=cb, stop_flag=lambda: self._stop, today=today)
            new_total = sum(v for v in counts.values() if v > 0)

            # 补填历史日期缺失的 PE（按日期价格比例换算，各日期独立截面）
            if not self._stop:
                dates_missing_pe = store.get_dates_missing_pe(days=300)
                for i, d in enumerate(dates_missing_pe):
                    if self._stop:
                        break
                    rows = store.get_zt_for_dates([d]).get(d, [])
                    if not rows:
                        continue
                    codes = [r["code"] for r in rows]
                    self.progress.emit(f"补填 PE {i+1}/{len(dates_missing_pe)}  {d}")
                    pe_price_map = fetcher.fetch_pe_map(codes=codes)
                    if pe_price_map:
                        date_pe = {r["code"]: fetcher._scale_pe(r["code"], r.get("price"), pe_price_map)
                                   for r in rows}
                        date_pe = {k: v for k, v in date_pe.items() if v is not None}
                        if date_pe:
                            store.upsert_zt_pe(d, date_pe)

            # 补填断板日涨跌幅（最近30天）
            if not self._stop:
                all_zt_dates = sorted(store.get_dates_with_zt_data())  # 升序
                recent = all_zt_dates[-30:] if len(all_zt_dates) > 30 else all_zt_dates
                for i in range(1, len(recent)):
                    if self._stop:
                        break
                    prev_d = recent[i - 1]
                    curr_d = recent[i]
                    prev_zt = store.get_zt_for_dates([prev_d]).get(prev_d, [])
                    curr_codes = set(store.get_codes_for_date(curr_d))
                    # 断板股 → {code: 前日ZT收盘价}
                    code_prev_price = {
                        s["code"]: s["price"]
                        for s in prev_zt
                        if s["code"] not in curr_codes and s.get("price")
                    }
                    if not code_prev_price:
                        continue
                    self.progress.emit(f"断板涨跌幅 {curr_d}  {len(code_prev_price)}只")
                    fetcher.fetch_and_store_duanban_pct(
                        curr_d, code_prev_price, stop_flag=lambda: self._stop
                    )

            # 补填筹码集中度（全量历史个股，本地近似算法）
            if not self._stop:
                all_codes_cyq = store.get_all_zt_codes()
                self.progress.emit(f"筹码集中度 共{len(all_codes_cyq)}只")
                fetcher.fetch_and_store_cyq(all_codes_cyq, stop_flag=lambda: self._stop)

            # 完成后从 DB 取完整日期列表
            result = store.get_dates_with_zt_data()
            self.done.emit(result, new_total)
        except Exception as e:
            import traceback
            self.progress.emit(f"数据拉取异常: {e}")
            traceback.print_exc()
            self.done.emit(store.get_dates_with_zt_data(), 0)


class ConceptWorker(QThread):
    """从同花顺抓取概念板块成员，建立股票→概念映射"""
    progress = pyqtSignal(str)
    done = pyqtSignal()

    def __init__(self, codes: set[str], parent=None):
        super().__init__(parent)
        self._codes = codes
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        def cb(i, total, name):
            self.progress.emit(f"THS概念({i}/{total}): {name}")

        fetcher.fetch_and_store_ths_concepts(
            self._codes, progress_cb=cb, stop_flag=lambda: self._stop
        )
        self.done.emit()


class IndustryFillWorker(QThread):
    """对缺少行业数据的历史股票，逐个调用个股详情 API 补充"""
    progress = pyqtSignal(str)
    done = pyqtSignal()

    def __init__(self, codes: list[str], parent=None):
        super().__init__(parent)
        self._codes = codes
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        def cb(i, total, code):
            self.progress.emit(f"补充行业({i}/{total}): {code}")
        fetcher.fetch_and_store_missing_industry(
            self._codes, progress_cb=cb, stop_flag=lambda: self._stop
        )
        self.done.emit()


class HistoricalFetchWorker(QThread):
    """用日K数据重建历史涨停记录"""
    progress = pyqtSignal(str)
    done = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        try:
            fetcher.fetch_historical_zt_all(
                progress_cb=self.progress.emit,
                stop_flag=lambda: self._stop,
            )
        except Exception as e:
            import traceback
            self.progress.emit(f"历史数据重建异常: {e}")
            traceback.print_exc()
        self.done.emit()


class SectorStatsWorker(QThread):
    progress = pyqtSignal(str)
    done = pyqtSignal()

    def __init__(self, dates: list[str], parent=None):
        super().__init__(parent)
        self._dates = dates

    def run(self):
        net_buy = fetcher.fetch_today_sector_net_buy()
        for d in self._dates:
            self.progress.emit(f"计算板块统计: {d}")
            fetcher.build_sector_stats_for_date(d, net_buy if d == self._dates[0] else None)
        self.done.emit()


# ──────────────────────────────── 股票卡片 ────────────────────────────────

CARD_W = 148


def _limit_pct_display(code: str, name: str) -> str:
    """根据代码和名称返回涨停幅度字符串，如 '+10%'"""
    name_up = (name or "").upper()
    if "ST" in name_up:
        pct = 5
    elif code.startswith(("688", "689")):
        pct = 20
    elif code.startswith(("300", "301")):
        pct = 20
    elif code.startswith("8"):
        pct = 30
    else:
        pct = 10
    return f"+{pct}%"


class StockCard(QFrame):
    """天梯中的单张股票卡片"""

    def __init__(self, data: dict, concept_heat: dict,
                 on_concept_click=None, on_days_click=None,
                 on_stock_click=None, parent=None):
        super().__init__(parent)
        self._data = data
        self.setFixedWidth(CARD_W)
        self.setFixedHeight(CARD_H)
        self.setCursor(Qt.CursorShape.PointingHandCursor)

        days = data.get("consecutive_days", 1) or 1
        bg, fg = days_color(days)

        self.setStyleSheet(f"""
            QFrame {{
                background: {BG};
                border: 1px solid {BORDER};
                border-radius: 6px;
            }}
            QFrame:hover {{
                border: 1px solid {ACCENT};
            }}
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(2)

        # 第一行：价格 + 涨跌幅 + 连板徽章
        row1 = QHBoxLayout()
        row1.setSpacing(4)

        price = data.get("price")
        price_str = f"¥{price:.2f}" if price else "—"
        price_lbl = QLabel(price_str)
        price_lbl.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        price_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        price_lbl.setStyleSheet(f"color:{TEXT}; font-size:10px;")

        pct_val = data.get("pct_change")
        if pct_val is not None:
            pct_str = f"{pct_val:+.2f}%"
            pct_color = RED if pct_val > 0 else GREEN if pct_val < 0 else MUTED
        else:
            pct_str = _limit_pct_display(data.get("code", ""), data.get("name", ""))
            pct_color = RED
        pct_lbl = QLabel(pct_str)
        pct_lbl.setStyleSheet(f"color:{pct_color}; font-size:10px; font-weight:bold;")

        badge = QPushButton(f" {days}板 ")
        badge.setFlat(True)
        badge.setCursor(Qt.CursorShape.PointingHandCursor)
        badge.setToolTip(f"点击筛选 {days} 板")
        badge.setStyleSheet(
            f"QPushButton {{ background:{bg}; color:{fg}; font-size:11px; font-weight:bold;"
            f" border-radius:3px; padding:1px 4px; border:none; }}"
            f"QPushButton:hover {{ border: 1px solid {fg}; }}"
        )
        if on_days_click:
            badge.clicked.connect(lambda _=False, d=days: on_days_click(d))

        row1.addWidget(price_lbl)
        row1.addWidget(pct_lbl)
        row1.addWidget(badge)

        # 第二行：名称 + 代码胶囊（"平安银行 000001"）
        name = data.get("name", "")
        code = data.get("code", "")
        name_lbl = QLabel(
            f"<span style='font-weight:bold; font-size:12px; color:{TEXT};'>{name}</span>"
            f"&nbsp;&nbsp;"
            f"<span style='font-size:10px; color:{MUTED};'>{code}</span>"
        )
        name_lbl.setStyleSheet(
            f"background:{SURFACE}; border:1px solid {BORDER}; border-radius:8px;"
            f" padding:2px 7px;"
        )
        name_lbl.setWordWrap(False)

        # 第三行：概念胶囊（前3个，按全局热度排序，颜色跨卡片一致，可点击筛选）
        all_concepts = data.get("all_concepts", [])
        top3 = sorted(all_concepts, key=lambda c: concept_heat.get(c, 0), reverse=True)[:3]
        pills_row = QHBoxLayout()
        pills_row.setSpacing(3)
        pills_row.setContentsMargins(0, 0, 0, 0)
        for concept in top3:
            pill_bg, pill_fg = concept_pill_color(concept)
            short = concept if len(concept) <= 5 else concept[:4] + "…"
            pill = QPushButton(short)
            pill.setFlat(True)
            pill.setToolTip(f"{concept}\n（点击添加到板块筛选）")
            pill.setCursor(Qt.CursorShape.PointingHandCursor)
            pill.setStyleSheet(
                f"QPushButton {{ background:{pill_bg}; color:{pill_fg}; font-size:9px;"
                f" border-radius:7px; padding:2px 5px; border:none; }}"
                f"QPushButton:hover {{ border: 1px solid {pill_fg}; }}"
            )
            if on_concept_click:
                pill.clicked.connect(lambda _=False, c=concept: on_concept_click(c))
            pills_row.addWidget(pill)
        pills_row.addStretch()

        # 第四行：市值 + PE + 集中度
        cap = data.get("float_cap")
        pe = data.get("pe")
        cyq = data.get("concentration_90")
        cap_str = f"{cap:.1f}亿" if cap else "—"
        if pe is None:
            pe_str = "PE:—"
            pe_color = MUTED
        elif pe < 0:
            pe_str = f"PE:{pe:.0f}"
            pe_color = "#f87171"   # 红色：亏损
        else:
            pe_str = f"PE:{pe:.0f}"
            pe_color = MUTED
        # 集中度：越小越集中，颜色越深（深蓝→浅蓝→灰）与滤镜彩点配色一致
        if cyq is not None:
            cyq_pct = cyq * 100
            cyq_str = f"集:{cyq_pct:.1f}%"
            if cyq_pct <= 3:
                cyq_color = "#1E3A8A"
            elif cyq_pct <= 8:
                cyq_color = "#1E40AF"
            elif cyq_pct <= 15:
                cyq_color = "#2563EB"
            elif cyq_pct <= 20:
                cyq_color = "#3B82F6"
            elif cyq_pct <= 30:
                cyq_color = "#93C5FD"
            else:
                cyq_color = MUTED
        else:
            cyq_str = ""
            cyq_color = MUTED

        row4 = QHBoxLayout()
        row4.setSpacing(0)
        cap_lbl = QLabel(cap_str)
        cap_lbl.setStyleSheet(f"color:{MUTED}; font-size:10px;")
        pe_lbl = QLabel(pe_str)
        pe_lbl.setStyleSheet(f"color:{pe_color}; font-size:10px;")
        row4.addWidget(cap_lbl)
        row4.addStretch()
        row4.addWidget(pe_lbl)
        if cyq_str:
            row4.addStretch()
            cyq_lbl = QLabel(cyq_str)
            cyq_lbl.setStyleSheet(f"color:{cyq_color}; font-size:10px; font-weight:bold;")
            row4.addWidget(cyq_lbl)
        row4.addStretch()

        layout.addLayout(row1)
        layout.addWidget(name_lbl)
        layout.addLayout(pills_row)
        layout.addLayout(row4)

        # hover tooltip 显示全部概念
        concepts = data.get("all_concepts", [])
        if concepts:
            self.setToolTip("全部概念：\n" + "\n".join(concepts))

        self._on_stock_click = on_stock_click

    def mousePressEvent(self, event):
        if self._on_stock_click:
            code = self._data.get("code", "")
            name = self._data.get("name", "")
            self._on_stock_click(code, name)


# ──────────────────────────────── 日期列 ────────────────────────────────

COL_W = CARD_W


HEADER_H = 48  # 日期头（日期行 + 数量行）总高度

BAND_HEADER_H = 24   # 板数分组标题行高度
MAX_BAND = 7         # ≥7板合并到同一分组显示
SEP_H = 1            # 分隔线高度

class DayColumn(QWidget):
    """
    每日一列，按 band_heights 中各连板数分组对齐。
    不同日期同板数的股票保持相同 Y 起始位置。
    """
    def __init__(self, date_str: str, stocks: list[dict],
                 band_heights: dict[int, int],
                 promotion_rates: dict[int, int] | None = None,
                 concept_heat: dict | None = None,
                 on_concept_click=None,
                 on_days_click=None,
                 on_stock_click=None,
                 duanban: list[dict] | None = None,
                 parent=None):
        """
        band_heights: {consecutive_days: band_height_px}，所有列共用同一份。
        promotion_rates: {consecutive_days: rate_pct}，当日各板数的晋级率。
        concept_heat: {concept_name: global_count}，用于胶囊颜色和排序。
        on_concept_click: 点击胶囊时的回调 fn(concept_name)。
        """
        super().__init__(parent)
        self.setFixedWidth(COL_W)
        if promotion_rates is None:
            promotion_rates = {}
        if concept_heat is None:
            concept_heat = {}

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        # ── 日期头 ──
        d = datetime.strptime(date_str, "%Y%m%d")
        week = ["一", "二", "三", "四", "五", "六", "日"][d.weekday()]
        header = QLabel(f"{d.strftime('%m-%d')}  周{week}")
        header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        header.setFixedHeight(28)
        header.setStyleSheet(
            f"color:{TEXT}; font-weight:bold; font-size:12px;"
            f" background:{SURFACE}; border-radius:4px;"
        )
        count_lbl = QLabel(f"{len(stocks)} 只")
        count_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        count_lbl.setFixedHeight(18)
        count_lbl.setStyleSheet(f"color:{MUTED}; font-size:10px;")

        outer.addWidget(header)
        outer.addWidget(count_lbl)

        # 按板数分组（≥MAX_BAND 的合并到 MAX_BAND 组）
        stocks_by_board: dict[int, list[dict]] = {}
        for s in stocks:
            b = min(MAX_BAND, max(1, s.get("consecutive_days") or 1))
            stocks_by_board.setdefault(b, []).append(s)

        # ── 各板数分组（高→低）：若本列无股票则跳过空 band，断板区直接跟在 header 后 ──
        for boards in sorted(band_heights.keys(), reverse=True) if stocks else []:
            bh = band_heights[boards]
            if bh <= 0:
                continue

            # 分隔线（无背景色）
            sep = QFrame()
            sep.setFrameShape(QFrame.Shape.HLine)
            sep.setFixedHeight(SEP_H)
            sep.setStyleSheet(f"background:{BORDER}; border:none;")
            outer.addWidget(sep)

            # 板数标题行（晋级率改为游戏生命条样式）
            cnt_here = len(stocks_by_board.get(boards, []))
            rate = promotion_rates.get(boards)

            band_hdr_w = QWidget()
            band_hdr_w.setFixedHeight(BAND_HEADER_H)
            band_hdr_w.setStyleSheet(f"background:{SURFACE};")
            bhl = QHBoxLayout(band_hdr_w)
            bhl.setContentsMargins(8, 0, 8, 0)
            bhl.setSpacing(5)

            board_label = f"{boards}板+" if boards == MAX_BAND else f"{boards}板"
            info_lbl = QLabel(f"{board_label}  {cnt_here}只")
            info_lbl.setStyleSheet(
                f"color:{MUTED}; font-size:10px; background:transparent;"
            )
            bhl.addWidget(info_lbl)
            bhl.addStretch()

            if rate is not None:
                r = max(0, min(100, rate))
                if r >= 60:
                    bar_color = GREEN
                elif r >= 40:
                    bar_color = ORANGE
                elif r >= 20:
                    bar_color = "#D97706"
                else:
                    bar_color = RED

                hp_bar = QProgressBar()
                hp_bar.setRange(0, 100)
                hp_bar.setValue(r)
                hp_bar.setTextVisible(False)
                hp_bar.setFixedHeight(7)
                hp_bar.setFixedWidth(52)
                hp_bar.setStyleSheet(f"""
                    QProgressBar {{
                        background: {BORDER};
                        border-radius: 3px;
                        border: none;
                    }}
                    QProgressBar::chunk {{
                        background: {bar_color};
                        border-radius: 3px;
                        min-width: 7px;
                    }}
                """)
                pct_lbl = QLabel(f"{r}%")
                pct_lbl.setStyleSheet(
                    f"color:{bar_color}; font-size:9px;"
                    f" font-weight:bold; background:transparent;"
                )
                bhl.addWidget(hp_bar)
                bhl.addWidget(pct_lbl)

            outer.addWidget(band_hdr_w)

            # 股票卡片区
            band = QWidget()
            band.setFixedHeight(bh)
            bl = QVBoxLayout(band)
            bl.setContentsMargins(0, BAND_PAD, 0, 0)
            bl.setSpacing(CARD_SPACING)

            for s in stocks_by_board.get(boards, []):
                card = StockCard(s, concept_heat,
                                 on_concept_click=on_concept_click,
                                 on_days_click=on_days_click,
                                 on_stock_click=on_stock_click)
                bl.addWidget(card)

            bl.addStretch()
            outer.addWidget(band)

        # ── 断板区（昨日涨停、今日不在池中） ──
        if duanban:
            db_sep = QFrame()
            db_sep.setFrameShape(QFrame.Shape.HLine)
            db_sep.setFixedHeight(SEP_H)
            db_sep.setStyleSheet(f"background:{BORDER}; border:none;")
            outer.addWidget(db_sep)

            db_hdr = QLabel(f"  断板 {len(duanban)}只")
            db_hdr.setFixedHeight(BAND_HEADER_H)
            db_hdr.setStyleSheet(
                f"background:{SURFACE}; color:{MUTED}; font-size:10px;"
            )
            outer.addWidget(db_hdr)

            for s in duanban:
                days = s.get("consecutive_days", 1) or 1
                bg, fg = days_color(days)
                code = s.get("code", "")
                name = s.get("name", "")
                row_w = QWidget()
                row_w.setFixedHeight(26)
                row_w.setStyleSheet("QWidget { background: transparent; } QWidget:hover { background: #F3F4F6; }")
                if on_stock_click:
                    row_w.setCursor(Qt.CursorShape.PointingHandCursor)
                    row_w.mousePressEvent = (
                        lambda e, c=code, n=name: on_stock_click(c, n)
                    )
                rl = QHBoxLayout(row_w)
                rl.setContentsMargins(8, 0, 8, 0)
                rl.setSpacing(6)

                name_lbl = QLabel(name)
                name_lbl.setStyleSheet(f"color:{MUTED}; font-size:11px;")

                db_pct = s.get("duanban_pct")
                if db_pct is not None:
                    db_pct_str = f"{db_pct:+.2f}%"
                    db_pct_color = RED if db_pct > 0 else GREEN if db_pct < 0 else MUTED
                else:
                    db_pct_str = "—"
                    db_pct_color = MUTED
                db_pct_lbl = QLabel(db_pct_str)
                db_pct_lbl.setStyleSheet(f"color:{db_pct_color}; font-size:9px; font-weight:bold;")

                days_lbl = QLabel(f"<s>{days}板</s>")
                days_lbl.setTextFormat(Qt.TextFormat.RichText)
                days_lbl.setStyleSheet(
                    f"background:#EBEBEB; color:{MUTED}; font-size:9px; font-weight:bold;"
                    f" border-radius:2px; padding:1px 4px;"
                )

                rl.addWidget(name_lbl, 1)
                rl.addWidget(db_pct_lbl)
                rl.addWidget(days_lbl)
                outer.addWidget(row_w)

        outer.addStretch()


# ──────────────────────────────── 板块筛选器 (chip UI) ────────────────────────────────

class SectorFilterWidget(QWidget):
    changed = pyqtSignal(list)  # 当前已选板块列表

    MAX_SECTORS = 5

    def __init__(self, parent=None):
        super().__init__(parent)
        self._selected: list[str] = []
        self._all_concepts: list[str] = []
        self._mode: str = 'and'   # 'and'=交集  'or'=并集

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        self._edit = QLineEdit()
        self._edit.setPlaceholderText("搜索概念板块...")
        self._edit.setStyleSheet(
            f"QLineEdit {{ border:1px solid {BORDER}; border-radius:4px;"
            f" padding:4px 8px; background:{BG}; color:{TEXT}; font-size:12px; }}"
        )
        self._completer = QCompleter([])
        self._completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self._completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self._edit.setCompleter(self._completer)
        self._edit.returnPressed.connect(self._add_current)

        layout.addWidget(self._edit)

        # ── 交集 / 并集 toggle（不放在此 layout，由外部 header row 持有）──
        self._toggle_widget = self._make_toggle_widget()

        self._chips_widget = QWidget()
        self._chips_layout = QVBoxLayout(self._chips_widget)
        self._chips_layout.setContentsMargins(0, 0, 0, 0)
        self._chips_layout.setSpacing(3)
        layout.addWidget(self._chips_widget)

        clear_btn = QPushButton("清空板块筛选")
        clear_btn.setStyleSheet(
            f"QPushButton {{ color:{MUTED}; font-size:11px; border:none;"
            f" background:transparent; text-align:left; }}"
            f"QPushButton:hover {{ color:{RED}; }}"
        )
        clear_btn.clicked.connect(self.clear_all)
        layout.addWidget(clear_btn)

    def set_concepts(self, concepts: list[str]):
        self._all_concepts = concepts
        model = QStringListModel(concepts)
        self._completer.setModel(model)

    def _add_current(self):
        text = self._edit.text().strip()
        if not text:
            return
        # 允许前缀模糊匹配
        matched = next(
            (c for c in self._all_concepts if text.lower() in c.lower()), None
        )
        if matched and matched not in self._selected:
            if len(self._selected) < self.MAX_SECTORS:
                self._selected.append(matched)
                self._rebuild_chips()
                self.changed.emit(list(self._selected))
        self._edit.clear()

    def _rebuild_chips(self):
        while self._chips_layout.count():
            item = self._chips_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        for sector in self._selected:
            chip = QFrame()
            chip.setStyleSheet(
                f"QFrame {{ background:{ACCENT}; border-radius:4px; }}"
            )
            row = QHBoxLayout(chip)
            row.setContentsMargins(6, 2, 4, 2)
            row.setSpacing(4)
            lbl = QLabel(sector)
            lbl.setStyleSheet("color:white; font-size:11px;")
            lbl.setWordWrap(True)
            del_btn = QPushButton("×")
            del_btn.setFixedSize(16, 16)
            del_btn.setStyleSheet(
                "QPushButton { color:white; border:none; font-size:12px;"
                " background:transparent; }"
                "QPushButton:hover { color:#FFD0CC; }"
            )
            _s = sector

            def make_remover(s):
                def _remove():
                    self._selected.remove(s)
                    self._rebuild_chips()
                    self.changed.emit(list(self._selected))
                return _remove

            del_btn.clicked.connect(make_remover(_s))
            row.addWidget(lbl, 1)
            row.addWidget(del_btn)
            self._chips_layout.addWidget(chip)

    def add_concept(self, concept: str):
        """从外部（如点击胶囊）直接添加一个概念到筛选"""
        if concept not in self._all_concepts:
            return
        if concept not in self._selected and len(self._selected) < self.MAX_SECTORS:
            self._selected.append(concept)
            self._rebuild_chips()
            self.changed.emit(list(self._selected))

    def clear_all(self):
        self._selected.clear()
        self._rebuild_chips()
        self.changed.emit([])

    def _make_toggle_widget(self) -> QWidget:
        """构建 iOS 风格的分段切换控件（∩ / ∪），由外部放入 header row"""
        container = QFrame()
        container.setFixedHeight(22)
        container.setStyleSheet(
            "QFrame { background: #E5E7EB; border-radius: 5px; }"
        )
        cl = QHBoxLayout(container)
        cl.setContentsMargins(2, 2, 2, 2)
        cl.setSpacing(2)

        self._and_btn = QPushButton("∩")
        self._and_btn.setFixedHeight(18)
        self._and_btn.setToolTip("交集：个股同时属于所有选中概念")
        self._and_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._and_btn.clicked.connect(lambda: self._set_mode('and'))

        self._or_btn = QPushButton("∪")
        self._or_btn.setFixedHeight(18)
        self._or_btn.setToolTip("并集：个股属于任意一个选中概念")
        self._or_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._or_btn.clicked.connect(lambda: self._set_mode('or'))

        cl.addWidget(self._and_btn)
        cl.addWidget(self._or_btn)

        self._refresh_toggle_style()
        return container

    def _set_mode(self, mode: str):
        if self._mode == mode:
            return
        self._mode = mode
        self._refresh_toggle_style()
        if self._selected:
            self.changed.emit(list(self._selected))

    def _refresh_toggle_style(self):
        _active = (
            "QPushButton { background: white; color: #111827; border: none;"
            " border-radius: 4px; font-size: 12px; font-weight: bold;"
            " padding: 0px 7px; }"
        )
        _inactive = (
            f"QPushButton {{ background: transparent; color: {MUTED}; border: none;"
            f" border-radius: 4px; font-size: 12px; padding: 0px 7px; }}"
            f"QPushButton:hover {{ color: {TEXT}; }}"
        )
        self._and_btn.setStyleSheet(_active if self._mode == 'and' else _inactive)
        self._or_btn.setStyleSheet(_active  if self._mode == 'or'  else _inactive)

    def get_mode(self) -> str:
        return self._mode

    def get_selected(self) -> list[str]:
        return list(self._selected)


# ──────────────────────────────── 连板数按钮筛选器 ────────────────────────────────

class DaysFilterWidget(QWidget):
    """8个切换按钮：1~7板 + 7板+，多选，空选=全部"""
    changed = pyqtSignal()

    _DAYS   = [1, 2, 3, 4, 5, 6, 7, -1]
    _LABELS = ["1板", "2板", "3板", "4板", "5板", "6板", "7板", "7板+"]

    def __init__(self, parent=None):
        super().__init__(parent)
        self._selected: set[int] = set()

        grid = QGridLayout(self)
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setSpacing(4)

        self._btns: dict[int, QPushButton] = {}
        for i, (day, label) in enumerate(zip(self._DAYS, self._LABELS)):
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setFixedHeight(26)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            self._btns[day] = btn
            btn.clicked.connect(lambda _=False, d=day: self._toggle(d))
            grid.addWidget(btn, i // 4, i % 4)

        self._refresh_styles()

    def _toggle(self, day: int):
        if day in self._selected:
            self._selected.discard(day)
        else:
            self._selected.add(day)
        self._refresh_styles()
        self.changed.emit()

    def _refresh_styles(self):
        for day, btn in self._btns.items():
            if day in self._selected:
                d = day if day > 0 else 10
                bg, fg = days_color(d)
                btn.setStyleSheet(
                    f"QPushButton {{ background:{bg}; color:{fg}; border:none;"
                    f" border-radius:4px; font-size:11px; font-weight:bold; }}"
                )
            else:
                btn.setStyleSheet(
                    f"QPushButton {{ background:{SURFACE}; color:{MUTED};"
                    f" border:1px solid {BORDER}; border-radius:4px; font-size:11px; }}"
                    f"QPushButton:hover {{ background:{SEL}; color:{TEXT}; }}"
                )

    def select_only(self, days: int):
        """单选指定板数（卡片徽章点击时使用）"""
        self._selected.clear()
        key = days if days <= 7 else -1
        self._selected.add(key)
        self._refresh_styles()
        self.changed.emit()

    def get_selected(self) -> set[int]:
        return set(self._selected)

    def matches(self, days: int) -> bool:
        if not self._selected:
            return True
        if days in self._selected:
            return True
        if -1 in self._selected and days >= 7:
            return True
        return False

    def clear(self):
        self._selected.clear()
        self._refresh_styles()
        self.changed.emit()


# ──────────────────────────────── 连板天梯 Tab ────────────────────────────────

class LadderTab(QWidget):
    status_msg = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)

        # 完整数据缓存 {date: [stock_dict]}
        self._full_data: dict[str, list[dict]] = {}
        self._current_dates: list[str] = []
        self._all_dates: list[str] = []    # 所有有数据的日期
        self._col_widgets: list[DayColumn] = []
        self._page_offset: int = 0           # 当前显示从第几天（_all_dates 索引）开始
        self._loaded_count: int = 0          # 当前已渲染的日期数量（随滚动增加）
        self._loading_more: bool = False     # 防止并发追加
        self._band_heights: dict[int, int] = {}  # 当前各板的行高，用于追加时比对
        self._concept_heat: dict[str, int] = {}  # 全局概念热度，用于胶囊颜色/排序
        self._stock_code_filter: str | None = None
        self._PAGE_SIZE = 10
        self._APPEND_SIZE = 5

        self._setup_ui()

    def _setup_ui(self):
        main_split = QSplitter(Qt.Orientation.Horizontal, self)
        main_split.setHandleWidth(1)
        main_split.setStyleSheet("QSplitter::handle { background: #E5E5E7; }")

        # ── 左侧筛选面板（可垂直滚动）──
        left = QWidget()
        left.setFixedWidth(220)
        left.setStyleSheet(f"background:{SURFACE};")
        _left_outer = QVBoxLayout(left)
        _left_outer.setContentsMargins(0, 0, 0, 0)
        _left_outer.setSpacing(0)

        _left_scroll = QScrollArea()
        _left_scroll.setWidgetResizable(True)
        _left_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        _left_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        _left_scroll.setStyleSheet(
            f"QScrollArea {{ border:none; background:{SURFACE}; }}"
        )
        _left_inner = QWidget()
        _left_inner.setStyleSheet(f"background:{SURFACE};")
        lv = QVBoxLayout(_left_inner)
        lv.setContentsMargins(12, 12, 12, 12)
        lv.setSpacing(10)

        title = QLabel("筛选条件")
        title.setStyleSheet(f"color:{TEXT}; font-weight:bold; font-size:14px;")
        lv.addWidget(title)

        # 连板数（按钮切换）
        lv.addWidget(self._section("连板数"))
        self._days_filter = DaysFilterWidget()
        self._days_filter.changed.connect(self._apply_filters)
        lv.addWidget(self._days_filter)

        # 市场板块
        lv.addWidget(self._section("市场板块"))
        _mkt_style = (
            f"QPushButton {{ background:{SURFACE}; color:{MUTED};"
            f" border:1px solid {BORDER}; border-radius:4px; font-size:11px; }}"
            f"QPushButton:checked {{ background:{ACCENT}; color:white;"
            f" border:1px solid {ACCENT}; font-weight:bold; }}"
        )
        self._market_filter: set[str] = set()
        self._market_btns: dict[str, QPushButton] = {}
        mkt_row1 = QHBoxLayout()
        mkt_row1.setSpacing(4)
        mkt_row2 = QHBoxLayout()
        mkt_row2.setSpacing(4)
        for label, row in [("主板", mkt_row1), ("创业板", mkt_row1),
                            ("科创板", mkt_row2), ("北交所", mkt_row2)]:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setFixedHeight(26)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.setStyleSheet(_mkt_style)
            btn.clicked.connect(self._on_market_filter_changed)
            self._market_btns[label] = btn
            row.addWidget(btn)
        lv.addLayout(mkt_row1)
        lv.addLayout(mkt_row2)

        # 流通市值
        lv.addWidget(self._section("流通市值（亿元）"))
        row_cap = QHBoxLayout()
        row_cap.setSpacing(6)
        self._min_cap = self._dbl_spinbox(0, 0, 99999)
        self._max_cap = self._dbl_spinbox(99999, 0, 99999)
        row_cap.addWidget(self._min_cap)
        row_cap.addWidget(self._range_dash())
        row_cap.addWidget(self._max_cap)
        lv.addLayout(row_cap)

        # 股价
        lv.addWidget(self._section("股价（元）"))
        row_price = QHBoxLayout()
        row_price.setSpacing(6)
        self._min_price = self._dbl_spinbox(0, 0, 9999)
        self._max_price = self._dbl_spinbox(9999, 0, 9999)
        row_price.addWidget(self._min_price)
        row_price.addWidget(self._range_dash())
        row_price.addWidget(self._max_price)
        lv.addLayout(row_price)

        # 市盈率 PE
        lv.addWidget(self._section("市盈率 PE"))
        self._pe_loss_btn = QPushButton("亏损（PE < 0）")
        self._pe_loss_btn.setCheckable(True)
        self._pe_loss_btn.setFixedHeight(26)
        self._pe_loss_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._pe_loss_btn.setStyleSheet(
            f"QPushButton {{ background:{SURFACE}; color:{MUTED};"
            f" border:1px solid {BORDER}; border-radius:4px; font-size:11px; }}"
            f"QPushButton:checked {{ background:#FFE4E6; color:#881337;"
            f" border:1px solid #881337; font-weight:bold; }}"
        )
        self._pe_loss_btn.clicked.connect(self._apply_filters)
        lv.addWidget(self._pe_loss_btn)

        row_pe = QHBoxLayout()
        row_pe.setSpacing(6)
        self._min_pe = self._dbl_spinbox(0, 0, 9999)
        self._max_pe = self._dbl_spinbox(9999, 0, 9999)
        row_pe.addWidget(self._min_pe)
        row_pe.addWidget(self._range_dash())
        row_pe.addWidget(self._max_pe)
        lv.addLayout(row_pe)

        # 90% 筹码集中度 — 彩点横向（6档，蓝色深→浅）
        lv.addWidget(self._section("筹码集中度"))
        self._cyq_threshold: int | None = None
        self._cyq_btns: dict = {}
        _cyq_opts = [
            (None, "#B0B0B0", "不限"),
            (3,    "#1E3A8A", "≤3%"),
            (8,    "#1E40AF", "≤8%"),
            (15,   "#2563EB", "≤15%"),
            (20,   "#3B82F6", "≤20%"),
            (30,   "#93C5FD", "≤30%"),
        ]
        cyq_dot_row = QHBoxLayout()
        cyq_dot_row.setSpacing(4)
        cyq_dot_row.setContentsMargins(0, 0, 0, 0)
        for _t, _dc, _tip in _cyq_opts:
            btn = QPushButton("●")
            btn.setFixedSize(24, 24)
            btn.setToolTip(_tip)
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn._threshold = _t
            btn._dot_color = _dc
            btn.clicked.connect(lambda _, t=_t: self._on_cyq_filter_click(t))
            self._cyq_btns[_t] = btn
            cyq_dot_row.addWidget(btn)
        cyq_dot_row.addStretch()
        lv.addLayout(cyq_dot_row)
        self._update_cyq_row_styles()

        # 概念口径切换
        lv.addWidget(self._section("概念口径"))
        self._concept_source = 'all'
        src_row = QHBoxLayout()
        src_row.setSpacing(4)
        _src_style = (
            f"QPushButton {{ background:{SURFACE}; color:{MUTED};"
            f" border:1px solid {BORDER}; border-radius:4px; font-size:11px; }}"
            f"QPushButton:checked {{ background:{ACCENT}; color:white;"
            f" border:1px solid {ACCENT}; font-weight:bold; }}"
        )
        self._src_all_btn = QPushButton("全部")
        self._src_em_btn  = QPushButton("东财行业")
        self._src_ths_btn = QPushButton("同花顺")
        for _b in (self._src_all_btn, self._src_em_btn, self._src_ths_btn):
            _b.setCheckable(True)
            _b.setFixedHeight(26)
            _b.setCursor(Qt.CursorShape.PointingHandCursor)
            _b.setStyleSheet(_src_style)
            src_row.addWidget(_b)
        self._src_all_btn.setChecked(True)
        self._src_all_btn.clicked.connect(lambda: self._set_concept_source('all'))
        self._src_em_btn.clicked.connect(lambda: self._set_concept_source('em'))
        self._src_ths_btn.clicked.connect(lambda: self._set_concept_source('ths'))
        lv.addLayout(src_row)

        # 选定个股（点击卡片）
        lv.addWidget(self._section("选定个股"))
        self._stock_chip = QFrame()
        self._stock_chip.setStyleSheet(
            f"QFrame {{ background:{ACCENT}; border-radius:4px; }}"
        )
        _sc_row = QHBoxLayout(self._stock_chip)
        _sc_row.setContentsMargins(6, 2, 4, 2)
        _sc_row.setSpacing(4)
        self._stock_chip_lbl = QLabel("—")
        self._stock_chip_lbl.setStyleSheet("color:white; font-size:11px;")
        _sc_clear = QPushButton("×")
        _sc_clear.setFixedSize(16, 16)
        _sc_clear.setStyleSheet(
            "QPushButton { color:white; border:none; font-size:13px; background:transparent; }"
            "QPushButton:hover { color:#FFD0CC; }"
        )
        _sc_clear.clicked.connect(self._clear_stock_filter)
        _sc_row.addWidget(self._stock_chip_lbl, 1)
        _sc_row.addWidget(_sc_clear)
        self._stock_chip.hide()
        lv.addWidget(self._stock_chip)
        self._stock_code_filter: str | None = None

        # 板块筛选（section 标题与交集/并集 toggle 同行）
        self._sector_filter = SectorFilterWidget()
        self._sector_filter.changed.connect(self._apply_filters)
        sec_concept_row = QHBoxLayout()
        sec_concept_row.setContentsMargins(0, 0, 0, 0)
        sec_concept_row.setSpacing(6)
        sec_concept_row.addWidget(self._section("概念板块（最多5个）"), 1)
        sec_concept_row.addWidget(self._sector_filter._toggle_widget)
        lv.addLayout(sec_concept_row)
        lv.addWidget(self._sector_filter)

        # 按钮组
        btn_row = QHBoxLayout()
        apply_btn = QPushButton("应用筛选")
        apply_btn.setStyleSheet(self._btn_style(ACCENT, "white"))
        apply_btn.clicked.connect(self._apply_filters)
        reset_btn = QPushButton("重置")
        reset_btn.setStyleSheet(self._btn_style(SEL, TEXT))
        reset_btn.clicked.connect(self._reset_filters)
        btn_row.addWidget(apply_btn)
        btn_row.addWidget(reset_btn)
        lv.addLayout(btn_row)
        lv.addStretch()

        # ── 系统信息 ──
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet(f"color:{BORDER};")
        lv.addWidget(sep)

        self._sys_info_lbl = QLabel()
        self._sys_info_lbl.setWordWrap(True)
        self._sys_info_lbl.setStyleSheet(
            f"color:{MUTED}; font-size:10px; padding:4px 2px 6px 2px;"
        )
        self._sys_info_lbl.setAlignment(Qt.AlignmentFlag.AlignLeft)
        lv.addWidget(self._sys_info_lbl)
        self._refresh_sys_info()

        _left_scroll.setWidget(_left_inner)
        _left_outer.addWidget(_left_scroll)

        # ── 右侧天梯区域 ──
        right = QWidget()
        rv = QVBoxLayout(right)
        rv.setContentsMargins(0, 0, 0, 0)
        rv.setSpacing(0)

        # 日期导航（仅保留日期范围标签，向右滚动到底自动加载更早数据）
        nav = QHBoxLayout()
        nav.setContentsMargins(8, 6, 8, 6)
        self._date_btn = QPushButton("—")
        self._date_btn.setFlat(True)
        self._date_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._date_btn.setToolTip("点击选择日期")
        self._date_btn.setStyleSheet(
            f"QPushButton {{ color:{MUTED}; font-size:11px; background:transparent; border:none; }}"
            f"QPushButton:hover {{ color:{ACCENT}; text-decoration:underline; }}"
        )
        self._date_btn.clicked.connect(self._show_date_picker)
        nav.addStretch()
        nav.addWidget(self._date_btn)
        nav.addStretch()

        nav_frame = QFrame()
        nav_frame.setStyleSheet(f"background:{SURFACE}; border-bottom:1px solid {BORDER};")
        nav_frame.setLayout(nav)
        rv.addWidget(nav_frame)

        # 横向滚动天梯
        self._ladder_scroll = QScrollArea()
        self._ladder_scroll.setWidgetResizable(True)
        self._ladder_scroll.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded
        )
        self._ladder_scroll.setVerticalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAsNeeded
        )
        self._ladder_scroll.setStyleSheet(
            f"QScrollArea {{ border:none; background:{BG}; }}"
        )

        self._ladder_inner = QWidget()
        self._ladder_inner.setStyleSheet(f"background:{BG};")
        self._ladder_hbox = QHBoxLayout(self._ladder_inner)
        self._ladder_hbox.setContentsMargins(8, 8, 8, 8)
        self._ladder_hbox.setSpacing(6)
        self._ladder_hbox.addStretch()
        self._ladder_scroll.setWidget(self._ladder_inner)
        rv.addWidget(self._ladder_scroll, 1)
        # 滚动到右端时自动追加更早的日期列
        self._ladder_scroll.horizontalScrollBar().valueChanged.connect(self._on_hscroll)

        main_split.addWidget(left)
        main_split.addWidget(right)
        main_split.setSizes([220, 9999])

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.addWidget(main_split)

        # 连接筛选 spinbox
        for w in (self._min_cap, self._max_cap, self._min_price, self._max_price,
                  self._min_pe, self._max_pe):
            w.valueChanged.connect(self._apply_filters)

    # ── 辅助 builder ──

    def _section(self, text: str) -> QLabel:
        lbl = QLabel(text)
        lbl.setStyleSheet(f"color:{MUTED}; font-size:11px; font-weight:bold;")
        return lbl

    def _spinbox(self, val, mn, mx) -> QSpinBox:
        sb = QSpinBox()
        sb.setRange(mn, mx)
        sb.setValue(val)
        sb.setStyleSheet(f"""
            QSpinBox {{
                border: 1px solid {BORDER}; border-radius: 4px;
                padding: 2px 6px; background: {BG}; color: {TEXT}; font-size: 12px;
            }}
            QSpinBox::up-button {{ width: 0; height: 0; border: none; }}
            QSpinBox::down-button {{ width: 0; height: 0; border: none; }}
        """)
        return sb

    def _dbl_spinbox(self, val, mn, mx) -> QDoubleSpinBox:
        sb = QDoubleSpinBox()
        sb.setRange(mn, mx)
        sb.setValue(val)
        sb.setDecimals(1)
        sb.setStyleSheet(f"""
            QDoubleSpinBox {{
                border: 1px solid {BORDER}; border-radius: 4px;
                padding: 2px 6px; background: {BG}; color: {TEXT}; font-size: 12px;
            }}
            QDoubleSpinBox::up-button {{ width: 0; height: 0; border: none; }}
            QDoubleSpinBox::down-button {{ width: 0; height: 0; border: none; }}
        """)
        return sb

    def _range_dash(self) -> QLabel:
        lbl = QLabel("—")
        lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl.setStyleSheet(f"color:{MUTED}; font-size:12px; background:transparent;")
        lbl.setFixedWidth(16)
        return lbl

    def _btn_style(self, bg: str, fg: str) -> str:
        return (
            f"QPushButton {{ background:{bg}; color:{fg}; border:none;"
            f" border-radius:5px; padding:5px 10px; font-size:12px; }}"
            f"QPushButton:hover {{ opacity:0.85; }}"
        )

    # ── 数据加载 ──

    def load_data(self, all_dates: list[str]):
        self._all_dates = all_dates
        self._page_offset = 0
        self._loaded_count = self._PAGE_SIZE
        self._refresh_page()

    def _current_page_dates(self) -> list[str]:
        # all_dates 已按 DESC 排列（最新在前），offset=0 显示最新一页
        start = self._page_offset
        end = min(len(self._all_dates), start + self._loaded_count)
        return self._all_dates[start:end]  # 保持降序：最新在左

    def _get_prev_date(self, date_str: str) -> str | None:
        """返回 date_str 在交易日历中的前一交易日"""
        try:
            idx = self._all_dates.index(date_str)
            if idx + 1 < len(self._all_dates):
                return self._all_dates[idx + 1]
        except ValueError:
            pass
        return None

    def _refresh_page(self):
        dates = self._current_page_dates()
        self._current_dates = dates
        if dates:
            self._date_btn.setText(self._fmt_date_range(dates))
        else:
            self._date_btn.setText("无数据")

        # 同时拉取每日的前一交易日数据（用于断板计算）
        self._prev_date_map: dict[str, str | None] = {}
        extra_prev: list[str] = []
        for d in dates:
            pd = self._get_prev_date(d)
            self._prev_date_map[d] = pd
            if pd and pd not in dates:
                extra_prev.append(pd)

        fetch_dates = list(dates) + extra_prev
        raw = store.get_zt_for_dates(fetch_dates)

        # 加载断板涨跌幅（断板发生日 → {code: pct_change}）
        self._duanban_pct_map = store.get_duanban_pct_for_dates(fetch_dates)

        # 加载筹码集中度（{date: {code: concentration_90}}）
        self._cyq_map = store.get_cyq_for_dates(fetch_dates)

        # 附加概念信息
        all_codes = {s["code"] for stocks in raw.values() for s in stocks}
        _src = None if self._concept_source == 'all' else self._concept_source
        concept_map = store.get_stock_concepts(list(all_codes), source=_src)
        # 全局热度：历史涨停股中各概念的股票数量，确保同一只股票跨日标签一致
        concept_heat = store.get_concept_global_heat(source=_src)
        self._concept_heat = concept_heat

        cyq_map = getattr(self, "_cyq_map", {})
        for date, stocks in raw.items():
            date_cyq = cyq_map.get(date, {})
            for s in stocks:
                code = s["code"]
                concepts = concept_map.get(code, [])
                s["all_concepts"] = concepts
                if concepts:
                    s["primary_concept"] = max(concepts, key=lambda c: concept_heat.get(c, 0))
                else:
                    s["primary_concept"] = "N/A"
                cyq_val = date_cyq.get(code)
                if cyq_val is not None:
                    s["concentration_90"] = cyq_val

        self._full_data = raw
        self._apply_filters()

    def _on_cyq_filter_click(self, threshold):
        self._cyq_threshold = threshold
        self._update_cyq_row_styles()
        self._apply_filters()

    def _update_cyq_row_styles(self):
        for t, btn in self._cyq_btns.items():
            dc = btn._dot_color
            active = (t == self._cyq_threshold)
            if active:
                btn.setStyleSheet(
                    f"QPushButton {{ background:transparent; color:{dc};"
                    f" border:2px solid {dc}; border-radius:12px;"
                    f" font-size:13px; padding:0; }}"
                )
            else:
                btn.setStyleSheet(
                    f"QPushButton {{ background:transparent; color:{dc};"
                    f" border:2px solid transparent; border-radius:12px;"
                    f" font-size:13px; padding:0; }}"
                    f"QPushButton:hover {{ border:2px solid {dc}; }}"
                )

    def _on_market_filter_changed(self):
        self._market_filter = {
            label for label, btn in self._market_btns.items() if btn.isChecked()
        }
        self._apply_filters()

    @staticmethod
    def _code_market(code: str) -> str:
        if code.startswith(("688", "689")):
            return "科创板"
        if code.startswith(("300", "301")):
            return "创业板"
        if code.startswith(("8", "4", "92")):
            return "北交所"
        return "主板"

    def _apply_filters(self, *_):
        min_cap = self._min_cap.value()
        max_cap = self._max_cap.value()
        min_p = self._min_price.value()
        max_p = self._max_price.value()
        include_loss = self._pe_loss_btn.isChecked()
        min_pe = self._min_pe.value()
        max_pe = self._max_pe.value()
        positive_pe_filter_active = not (min_pe == 0 and max_pe == 9999)
        pe_filter_active = include_loss or positive_pe_filter_active
        cyq_threshold = self._cyq_threshold
        cyq_filter_active = cyq_threshold is not None
        sectors = self._sector_filter.get_selected()
        sector_mode = self._sector_filter.get_mode()   # 'and' | 'or'

        def _passes(s: dict) -> bool:
            days = s.get("consecutive_days", 1) or 1
            cap = s.get("float_cap") or 0
            price = s.get("price") or 0
            if self._stock_code_filter and s.get("code") != self._stock_code_filter:
                return False
            if self._market_filter and self._code_market(s.get("code", "")) not in self._market_filter:
                return False
            if not self._days_filter.matches(days):
                return False
            if not (min_cap <= cap <= max_cap):
                return False
            if not (min_p <= price <= max_p):
                return False
            if pe_filter_active:
                pe = s.get("pe")
                if pe is None:
                    return False
                if pe < 0:
                    if not include_loss:
                        return False
                else:
                    if not positive_pe_filter_active:
                        return False
                    if not (min_pe <= pe <= max_pe):
                        return False
            if cyq_filter_active:
                cyq = s.get("concentration_90")
                if cyq is None:
                    return False
                if cyq * 100 > cyq_threshold:
                    return False
            if sectors:
                sc = set(s.get("all_concepts", []))
                if sector_mode == 'and':
                    if not sc.issuperset(sectors):   # 交集：必须含全部选中概念
                        return False
                else:
                    if not sc.intersection(sectors): # 并集：含任意一个即可
                        return False
            return True

        # 先计算每日筛选结果
        filtered_by_date: dict[str, list[dict]] = {}
        for date in self._current_dates:
            filtered_by_date[date] = [s for s in self._full_data.get(date, []) if _passes(s)]

        # 计算各板数 band 高度（所有列共用，保证同板数同高）
        band_max: dict[int, int] = {}
        for stocks in filtered_by_date.values():
            by_board: dict[int, int] = {}
            for s in stocks:
                b = min(MAX_BAND, max(1, s.get("consecutive_days") or 1))
                by_board[b] = by_board.get(b, 0) + 1
            for b, cnt in by_board.items():
                band_max[b] = max(band_max.get(b, 0), cnt)

        band_heights: dict[int, int] = {
            b: cnt * (CARD_H + CARD_SPACING) + BAND_PAD
            for b, cnt in band_max.items()
            if cnt > 0
        }
        self._band_heights = band_heights

        # 计算各日期各板数的晋级率
        # self._current_dates 降序（最新在前），dates[i+1] 是 dates[i] 的前一交易日
        by_board_count: dict[str, dict[int, int]] = {}
        for date in self._current_dates:
            bbc: dict[int, int] = {}
            for s in filtered_by_date.get(date, []):
                b = min(MAX_BAND, max(1, s.get("consecutive_days") or 1))
                bbc[b] = bbc.get(b, 0) + 1
            by_board_count[date] = bbc

        promotion_rates_by_date: dict[str, dict[int, int]] = {}
        for i, date in enumerate(self._current_dates):
            rates: dict[int, int] = {}
            if i + 1 < len(self._current_dates):
                prev_date = self._current_dates[i + 1]
                prev_bbc = by_board_count.get(prev_date, {})
                curr_bbc = by_board_count.get(date, {})
                for b, cnt in curr_bbc.items():
                    if b > 1:
                        prev_cnt = prev_bbc.get(b - 1, 0)
                        if prev_cnt > 0:
                            rates[b] = round(cnt / prev_cnt * 100)
            promotion_rates_by_date[date] = rates

        # 计算各日断板（昨日在涨停池、今日不在），同步应用筛选条件
        duanban_by_date: dict[str, list[dict]] = {}
        duanban_pct_map = getattr(self, "_duanban_pct_map", {})
        for date in self._current_dates:
            prev_d = getattr(self, "_prev_date_map", {}).get(date)
            if prev_d and prev_d in self._full_data:
                today_codes = {s["code"] for s in self._full_data.get(date, [])}
                pct_for_date = duanban_pct_map.get(date, {})
                duanban_by_date[date] = [
                    {**s, "duanban_pct": pct_for_date.get(s["code"])}
                    for s in self._full_data[prev_d]
                    if s["code"] not in today_codes and _passes(s)
                ]
            else:
                duanban_by_date[date] = []

        # 重建列
        self._clear_columns()
        for i, date in enumerate(self._current_dates):
            # 周分隔线：当前日期与前一列（更新的日期）跨周时插入
            if i > 0 and self._is_week_boundary(self._current_dates[i - 1], date):
                sep = self._make_week_sep()
                self._ladder_hbox.insertWidget(self._ladder_hbox.count() - 1, sep)
                self._col_widgets.append(sep)
            col = DayColumn(
                date, filtered_by_date[date], band_heights,
                promotion_rates=promotion_rates_by_date.get(date),
                concept_heat=self._concept_heat,
                on_concept_click=self._sector_filter.add_concept,
                on_days_click=self._on_days_filter_click,
                on_stock_click=self._on_stock_filter_click,
                duanban=duanban_by_date.get(date),
            )
            self._ladder_hbox.insertWidget(self._ladder_hbox.count() - 1, col)
            self._col_widgets.append(col)

    def _clear_columns(self):
        for w in self._col_widgets:
            self._ladder_hbox.removeWidget(w)
            w.deleteLater()
        self._col_widgets.clear()

    @staticmethod
    def _is_week_boundary(newer: str, older: str) -> bool:
        """判断相邻两个交易日是否跨越自然周（ISO week）"""
        from datetime import date as _date
        d1 = _date(int(newer[:4]), int(newer[4:6]), int(newer[6:]))
        d2 = _date(int(older[:4]), int(older[4:6]), int(older[6:]))
        return d1.isocalendar()[1] != d2.isocalendar()[1]

    @staticmethod
    def _make_week_sep() -> QFrame:
        """生成周分隔竖线"""
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.VLine)
        sep.setFixedWidth(1)
        sep.setStyleSheet("QFrame { color: #D1D5DB; }")
        return sep

    def _show_date_picker(self):
        dlg = _DatePickerDialog(self._all_dates, self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            chosen = dlg.selected_date()  # "YYYYMMDD"
            if chosen and chosen in self._all_dates:
                self._page_offset = self._all_dates.index(chosen)
                self._loaded_count = self._PAGE_SIZE
                self._refresh_page()

    def _refresh_sys_info(self):
        """读取 DB 元信息并更新侧边栏系统信息标签"""
        import os
        from pathlib import Path
        try:
            db_path = Path.home() / ".limit_ladder.db"
            size_mb = os.path.getsize(db_path) / 1024 / 1024
            with store.get_conn() as c:
                zt_cnt   = c.execute("SELECT COUNT(*) FROM zt_records").fetchone()[0]
                cyq_cnt  = c.execute("SELECT COUNT(*) FROM cyq_records").fetchone()[0]
                sc_cnt   = c.execute("SELECT COUNT(*) FROM stock_concepts").fetchone()[0]
                db_cnt   = c.execute("SELECT COUNT(*) FROM duanban_records").fetchone()[0]
                date_cnt = c.execute("SELECT COUNT(DISTINCT date) FROM zt_records").fetchone()[0]
            text = (
                f"数据库  {size_mb:.1f} MB\n"
                f"涨停记录  {zt_cnt:,} 条  /{date_cnt} 日\n"
                f"筹码集中度  {cyq_cnt:,} 条\n"
                f"概念映射  {sc_cnt:,} 条\n"
                f"断板记录  {db_cnt:,} 条"
            )
        except Exception:
            text = "系统信息读取失败"
        self._sys_info_lbl.setText(text)

    def _reset_filters(self):
        self._clear_stock_filter()
        self._days_filter.clear()
        self._market_filter = set()
        for btn in self._market_btns.values():
            btn.setChecked(False)
        self._min_cap.setValue(0)
        self._max_cap.setValue(99999)
        self._min_price.setValue(0)
        self._max_price.setValue(9999)
        self._pe_loss_btn.setChecked(False)
        self._min_pe.setValue(0)
        self._max_pe.setValue(9999)
        self._sector_filter.clear_all()

    def _fmt_date_range(self, dates: list[str]) -> str:
        if not dates:
            return "无数据"
        newest, oldest = dates[0], dates[-1]
        return (f"{newest[:4]}-{newest[4:6]}-{newest[6:]}  ~  "
                f"{oldest[:4]}-{oldest[4:6]}-{oldest[6:]}")

    def _on_hscroll(self, value: int):
        """横向滚动条接近右端时，自动追加更早的日期列"""
        bar = self._ladder_scroll.horizontalScrollBar()
        if bar.maximum() > 0 and value >= bar.maximum() - 300 and not self._loading_more:
            self._append_more()

    def _append_more(self):
        """追加下一批更早的日期列（不清空已有列，直接在末尾插入）"""
        end = self._page_offset + self._loaded_count
        if end >= len(self._all_dates):
            return
        self._loading_more = True

        new_dates = self._all_dates[end: end + self._APPEND_SIZE]

        # 拉取新日期的数据
        extra_prev = []
        for d in new_dates:
            pd = self._get_prev_date(d)
            if pd and pd not in self._full_data and pd not in new_dates:
                extra_prev.append(pd)
        fetch_dates = new_dates + extra_prev
        new_raw = store.get_zt_for_dates(fetch_dates)
        new_duanban = store.get_duanban_pct_for_dates(fetch_dates)
        new_cyq = store.get_cyq_for_dates(fetch_dates)

        # 附加概念
        new_codes = {s["code"] for stocks in new_raw.values() for s in stocks}
        _src = None if self._concept_source == 'all' else self._concept_source
        new_concepts = store.get_stock_concepts(list(new_codes), source=_src)
        for date, stocks in new_raw.items():
            date_cyq = new_cyq.get(date, {})
            for s in stocks:
                code = s["code"]
                concepts = new_concepts.get(code, [])
                s["all_concepts"] = concepts
                s["primary_concept"] = (
                    max(concepts, key=lambda c: self._concept_heat.get(c, 0))
                    if concepts else "N/A"
                )
                cyq_val = date_cyq.get(code)
                if cyq_val is not None:
                    s["concentration_90"] = cyq_val

        # 合并进全量缓存
        self._full_data.update(new_raw)
        self._duanban_pct_map.update(new_duanban)
        self._cyq_map = {**getattr(self, "_cyq_map", {}), **new_cyq}
        for d in new_dates:
            self._prev_date_map[d] = self._get_prev_date(d)

        self._loaded_count += len(new_dates)
        self._current_dates = self._all_dates[
            self._page_offset: self._page_offset + self._loaded_count
        ]

        # 检查追加后 band_heights 是否需要扩展
        new_band_max: dict[int, int] = dict(self._band_heights)
        for d in new_dates:
            by_board: dict[int, int] = {}
            for s in new_raw.get(d, []):
                b = min(MAX_BAND, max(1, s.get("consecutive_days") or 1))
                by_board[b] = by_board.get(b, 0) + 1
            for b, cnt in by_board.items():
                new_band_max[b] = max(new_band_max.get(b, 0), cnt)
        new_band_heights = {
            b: cnt * (CARD_H + CARD_SPACING) + BAND_PAD
            for b, cnt in new_band_max.items() if cnt > 0
        }

        if new_band_heights != self._band_heights:
            # band 高度有变化，整体重绘
            self._apply_filters()
        else:
            # band 高度不变，只追加新列
            cyq_map = getattr(self, "_cyq_map", {})
            duanban_pct_map = getattr(self, "_duanban_pct_map", {})
            # 已渲染的最后一列日期（用于判断首条是否需要分隔线）
            last_rendered = self._current_dates[-(len(new_dates) + 1)] if len(self._current_dates) > len(new_dates) else None
            for i, date in enumerate(new_dates):
                # 判断与前一列（已渲染最后一列或本批上一列）是否跨周
                prev_col_date = new_dates[i - 1] if i > 0 else last_rendered
                if prev_col_date and self._is_week_boundary(prev_col_date, date):
                    sep = self._make_week_sep()
                    self._ladder_hbox.insertWidget(self._ladder_hbox.count() - 1, sep)
                    self._col_widgets.append(sep)
                prev_d = self._prev_date_map.get(date)
                today_codes = {s["code"] for s in self._full_data.get(date, [])}
                duanban = []
                if prev_d and prev_d in self._full_data:
                    pct_for_date = duanban_pct_map.get(date, {})
                    duanban = [
                        {**s, "duanban_pct": pct_for_date.get(s["code"])}
                        for s in self._full_data[prev_d]
                        if s["code"] not in today_codes
                    ]
                col = DayColumn(
                    date, new_raw.get(date, []), new_band_heights,
                    promotion_rates={},
                    concept_heat=self._concept_heat,
                    on_concept_click=self._sector_filter.add_concept,
                    on_days_click=self._on_days_filter_click,
                    on_stock_click=self._on_stock_filter_click,
                    duanban=duanban,
                )
                self._ladder_hbox.insertWidget(self._ladder_hbox.count() - 1, col)
                self._col_widgets.append(col)
            self._date_btn.setText(self._fmt_date_range(self._current_dates))

        self._loading_more = False

    def _on_stock_filter_click(self, code: str, name: str):
        """点击个股卡片：已选则取消，未选则设为筛选条件"""
        if self._stock_code_filter == code:
            self._clear_stock_filter()
            return
        self._stock_code_filter = code
        self._stock_chip_lbl.setText(f"{name}  {code}")
        self._stock_chip.show()
        self._ensure_duanban_in_window(code)
        self._apply_filters()

    def _ensure_duanban_in_window(self, code: str):
        """若该股断板日期（ZT日次交易日）在 _all_dates 中但超出已加载窗口，则扩展窗口。
        主要针对通过日期选择器跳转到历史段、断板日落在窗口左侧（更新方向）的场景。"""
        if not self._all_dates:
            return
        zt_dates = store.get_zt_dates_for_code(code)
        if not zt_dates:
            return
        zt_set = set(zt_dates)
        all_idx = {d: i for i, d in enumerate(self._all_dates)}

        # 找出断板日期（ZT日在 _all_dates 中的次日，且本身不在ZT池）
        # 若该日期的 index < page_offset，说明它在当前窗口之外（更新方向）
        min_new_idx = self._page_offset
        for zt_date in zt_dates:
            idx = all_idx.get(zt_date)
            if idx is None or idx == 0:
                continue
            next_date = self._all_dates[idx - 1]   # 下一交易日（更新）
            if next_date not in zt_set:             # 非ZT → 断板日
                duanban_idx = idx - 1
                if duanban_idx < self._page_offset:
                    min_new_idx = min(min_new_idx, duanban_idx)

        if min_new_idx >= self._page_offset:
            return  # 窗口已覆盖所有断板日

        new_dates = self._all_dates[min_new_idx : self._page_offset]

        # 计算额外需要的 prev 日期（用于断板行计算）
        extra_prev = []
        for d in new_dates:
            pd = self._get_prev_date(d)
            if pd and pd not in self._full_data and pd not in new_dates:
                extra_prev.append(pd)

        fetch = [d for d in (list(new_dates) + extra_prev) if d not in self._full_data]
        if fetch:
            new_raw = store.get_zt_for_dates(fetch)
            new_duanban = store.get_duanban_pct_for_dates(fetch)
            new_cyq = store.get_cyq_for_dates(fetch)

            new_codes = {s["code"] for ss in new_raw.values() for s in ss}
            _src = None if self._concept_source == 'all' else self._concept_source
            new_concepts = store.get_stock_concepts(list(new_codes), source=_src)
            for date, stocks in new_raw.items():
                date_cyq = new_cyq.get(date, {})
                for s in stocks:
                    c = s["code"]
                    concepts = new_concepts.get(c, [])
                    s["all_concepts"] = concepts
                    s["primary_concept"] = (
                        max(concepts, key=lambda cc: self._concept_heat.get(cc, 0))
                        if concepts else "N/A"
                    )
                    cyq_val = date_cyq.get(c)
                    if cyq_val is not None:
                        s["concentration_90"] = cyq_val

            self._full_data.update(new_raw)
            self._duanban_pct_map.update(new_duanban)
            self._cyq_map = {**getattr(self, "_cyq_map", {}), **new_cyq}

        for d in new_dates:
            if d not in self._prev_date_map:
                self._prev_date_map[d] = self._get_prev_date(d)

        # 扩展窗口（向更新方向：减小 page_offset，增加 loaded_count）
        n_added = self._page_offset - min_new_idx
        self._page_offset = min_new_idx
        self._loaded_count += n_added
        self._current_dates = self._all_dates[
            self._page_offset : self._page_offset + self._loaded_count
        ]

    def _clear_stock_filter(self):
        self._stock_code_filter = None
        self._stock_chip.hide()
        self._apply_filters()

    def _on_days_filter_click(self, days: int):
        """点击卡片板数徽章时，单选该板数（>=7板 归入 7板+）"""
        self._days_filter.select_only(days)

    def _set_concept_source(self, source: str):
        self._concept_source = source
        self._src_all_btn.setChecked(source == 'all')
        self._src_em_btn.setChecked(source == 'em')
        self._src_ths_btn.setChecked(source == 'ths')
        _src = None if source == 'all' else source
        concepts = store.get_all_concept_names(source=_src)
        self._sector_filter.set_concepts(concepts)
        self._refresh_page()

    def set_concepts(self, concepts: list[str]):
        self._sector_filter.set_concepts(concepts)


# ──────────────────────────────── 板块日统计 Tab ────────────────────────────────

class _NumericItem(QTableWidgetItem):
    """支持数值排序的 QTableWidgetItem"""
    def __init__(self, display: str, sort_val: float):
        super().__init__(display)
        self._val = sort_val

    def __lt__(self, other):
        if isinstance(other, _NumericItem):
            return self._val < other._val
        return super().__lt__(other)


class _DatePickerDialog(QDialog):
    """弹出式日历，让用户选择一个交易日并跳转。非交易日和未来日期显示为灰色。"""

    def __init__(self, all_dates: list[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("选择日期")
        self.setModal(True)
        self._all_dates = all_dates          # 降序，最新在前
        self._date_set = set(all_dates)      # 快速查询
        self._chosen: str | None = None

        # 灰色格式（无数据日期）：文字灰色
        self._gray_fmt = QTextCharFormat()
        self._gray_fmt.setForeground(QBrush(QColor("#C8C8C8")))

        # 有数据日期：蓝色背景框出，形成可视区间块
        self._normal_fmt = QTextCharFormat()
        self._normal_fmt.setBackground(QBrush(QColor("#DBEAFE")))   # 淡蓝底
        self._normal_fmt.setForeground(QBrush(QColor("#1E40AF")))   # 深蓝字

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        self._cal = QCalendarWidget()
        self._cal.setGridVisible(True)
        self._cal.setVerticalHeaderFormat(QCalendarWidget.VerticalHeaderFormat.NoVerticalHeader)

        # 日期格字体 9pt，导航栏"年月"标题 11pt（大2号）
        cal_font = self._cal.font()
        cal_font.setPointSize(9)
        self._cal.setFont(cal_font)
        self._cal.setStyleSheet(
            "QCalendarWidget QToolButton#qt_calendar_monthbutton,"
            "QCalendarWidget QToolButton#qt_calendar_yearbutton {"
            "  font-size: 11pt; font-weight: bold;"
            "}"
        )

        if all_dates:
            newest = all_dates[0]
            oldest = all_dates[-1]
            oldest_qdate = QDate(int(oldest[:4]), int(oldest[4:6]), int(oldest[6:]))
            newest_qdate = QDate(int(newest[:4]), int(newest[4:6]), int(newest[6:]))
            self._cal.setMinimumDate(oldest_qdate)
            self._cal.setMaximumDate(newest_qdate)
            self._cal.setSelectedDate(newest_qdate)
            # 一次性把全范围内所有日期都灰掉，再亮显有数据的日期
            # 这样无论用户切换到哪个月，格式都是正确的
            self._apply_all_formats(oldest_qdate, newest_qdate)

        self._cal.clicked.connect(self._on_date_clicked)
        layout.addWidget(self._cal)

        btns = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        btns.accepted.connect(self._on_accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _apply_all_formats(self, oldest_qdate: QDate, newest_qdate: QDate):
        """一次性灰掉整个范围内所有日期，再亮显 date_set 中的有数据日期。"""
        # 覆盖前后各14天，确保日历边缘溢出格也被格式化
        d = oldest_qdate.addDays(-14)
        end = newest_qdate.addDays(14)
        while d <= end:
            date_str = d.toString("yyyyMMdd")
            fmt = self._normal_fmt if date_str in self._date_set else self._gray_fmt
            self._cal.setDateTextFormat(d, fmt)
            d = d.addDays(1)

    def _on_date_clicked(self, qdate: QDate):
        self._chosen = qdate.toString("yyyyMMdd")

    def _on_accept(self):
        qdate = self._cal.selectedDate()
        chosen = qdate.toString("yyyyMMdd")
        if chosen in self._date_set:
            self._chosen = chosen
        else:
            # 取 <= chosen 的最近交易日
            candidates = [d for d in self._all_dates if d <= chosen]
            self._chosen = candidates[0] if candidates else (self._all_dates[0] if self._all_dates else None)
        self.accept()

    def selected_date(self) -> str | None:
        return self._chosen


class SectorTab(QWidget):
    """板块日统计 Tab：板块为行、日期为列，横向对齐同一板块。"""

    WEEK_SIZE = 5
    TOP_N = 15       # 最多显示前 N 个板块

    def __init__(self, parent=None):
        super().__init__(parent)
        self._all_dates: list[str] = []
        self._page_offset: int = 0
        self._setup_ui()

    def _setup_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 8, 12, 12)
        root.setSpacing(6)

        # ── 导航栏 ──
        nav = QHBoxLayout()
        self._prev_btn = QPushButton("◀  更新")
        self._next_btn = QPushButton("更早  ▶")
        for btn in (self._prev_btn, self._next_btn):
            btn.setStyleSheet(
                f"QPushButton {{ background:{SEL}; color:{TEXT}; border:none;"
                f" border-radius:5px; padding:5px 14px; font-size:12px; }}"
            )
            btn.setFixedHeight(28)
        self._prev_btn.clicked.connect(self._page_next)
        self._next_btn.clicked.connect(self._page_prev)
        self._date_btn = QPushButton("—")
        self._date_btn.setFlat(True)
        self._date_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._date_btn.setToolTip("点击选择日期")
        self._date_btn.setStyleSheet(
            f"QPushButton {{ color:{MUTED}; font-size:11px; background:transparent; border:none; }}"
            f"QPushButton:hover {{ color:{ACCENT}; text-decoration:underline; }}"
        )
        self._date_btn.clicked.connect(self._show_date_picker)
        nav.addWidget(self._prev_btn)
        nav.addStretch()
        nav.addWidget(self._date_btn)
        nav.addStretch()
        nav.addWidget(self._next_btn)
        nav_frame = QFrame()
        nav_frame.setStyleSheet(f"background:{SURFACE}; border-bottom:1px solid {BORDER};")
        nav_frame.setLayout(nav)
        root.addWidget(nav_frame)

        # ── 两张跨日期表格 ──
        tbl_style = f"""
            QTableWidget {{
                border: none; background: {BG};
                alternate-background-color: {SURFACE};
                color: {TEXT}; font-size: 12px;
                gridline-color: {BORDER};
            }}
            QTableWidget::item {{ padding: 4px 8px; }}
            QHeaderView::section {{
                background: {SURFACE}; color: {MUTED}; font-size: 11px;
                font-weight: bold; border: none;
                border-right: 1px solid {BORDER};
                border-bottom: 1px solid {BORDER}; padding: 4px 8px;
            }}
        """

        def make_frame(title: str, color: str) -> tuple[QFrame, QTableWidget]:
            frame = QFrame()
            frame.setStyleSheet(
                f"QFrame {{ background:{BG}; border:1px solid {BORDER}; border-radius:8px; }}"
            )
            fv = QVBoxLayout(frame)
            fv.setContentsMargins(0, 0, 0, 0)
            fv.setSpacing(0)
            hdr = QLabel(f"  {title}")
            hdr.setFixedHeight(32)
            hdr.setStyleSheet(
                f"color:{color}; font-weight:bold; font-size:13px;"
                f" background:{SURFACE}; border-bottom:1px solid {BORDER};"
                f" border-radius:8px 8px 0 0;"
            )
            fv.addWidget(hdr)
            tbl = QTableWidget()
            tbl.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
            tbl.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
            tbl.setAlternatingRowColors(True)
            tbl.setShowGrid(True)
            tbl.verticalHeader().setVisible(False)
            tbl.setStyleSheet(tbl_style)
            fv.addWidget(tbl)
            return frame, tbl

        self._zt_frame, self._zt_table = make_frame("涨停板块热度", RED)
        self._dt_frame, self._dt_table = make_frame("跌停板块热度", GREEN)

        content = QVBoxLayout()
        content.setSpacing(12)
        content.addWidget(self._zt_frame, 1)
        content.addWidget(self._dt_frame, 1)

        scroll_inner = QWidget()
        scroll_inner.setLayout(content)
        scroll = QScrollArea()
        scroll.setWidget(scroll_inner)
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet(f"QScrollArea {{ border:none; background:{BG}; }}")
        root.addWidget(scroll, 1)

    # ── 数据加载 ──

    def load_data(self, all_dates: list[str]):
        self._all_dates = all_dates
        self._page_offset = 0
        self._rebuild()

    def _current_page_dates(self) -> list[str]:
        start = self._page_offset
        end = min(len(self._all_dates), start + self.WEEK_SIZE)
        # all_dates 降序，直接返回（最新在左）
        return self._all_dates[start:end]

    def _rebuild(self):
        dates = self._current_page_dates()   # 升序：最早在左
        if not dates:
            self._date_btn.setText("无数据")
            return

        # dates[0] 最新，dates[-1] 最旧
        self._date_btn.setText(
            f"{dates[0][:4]}-{dates[0][4:6]}-{dates[0][6:]} ~ "
            f"{dates[-1][:4]}-{dates[-1][4:6]}-{dates[-1][6:]}"
        )

        # 收集所有日期的板块数据及个股列表（用于 tooltip）
        day_data: dict[str, dict[str, dict]] = {}
        zt_stocks: dict[str, dict[str, list]] = {}
        dt_stocks: dict[str, dict[str, list]] = {}
        for d in dates:
            rows = store.get_sector_stats_for_date(d)
            day_data[d] = {r["sector_name"]: r for r in rows}
            zt_stocks[d] = store.get_zt_by_sector_for_date(d)
            dt_stocks[d] = store.get_dt_by_sector_for_date(d)

        self._fill_table(self._zt_table, dates, day_data, "zt", zt_stocks)
        self._fill_table(self._dt_table, dates, day_data, "dt", dt_stocks)

    @staticmethod
    def _fmt_net(val) -> str:
        """将万元净买入格式化为亿元字符串"""
        if val is None:
            return "—"
        sign = "+" if val >= 0 else ""
        return f"{sign}{val/10000:.1f}亿"

    def _fill_table(self, tbl: QTableWidget, dates: list[str],
                    day_data: dict[str, dict[str, dict]], mode: str,
                    stocks_by_date: dict[str, dict[str, list]] | None = None):
        cnt_key = "zt_count" if mode == "zt" else "dt_count"
        color = RED if mode == "zt" else GREEN

        # 收集出现过的板块并按总热度排序
        sector_totals: dict[str, int] = {}
        for d in dates:
            for sector, r in day_data[d].items():
                cnt = r.get(cnt_key, 0) or 0
                if cnt > 0:
                    sector_totals[sector] = sector_totals.get(sector, 0) + cnt

        top_sectors = sorted(sector_totals, key=sector_totals.get, reverse=True)[:self.TOP_N]
        if not top_sectors:
            tbl.setRowCount(0)
            tbl.setColumnCount(0)
            return

        # 列：板块 | [date1计数, date1净流入] × N | 龙头(最新)
        n_dates = len(dates)
        total_cols = 1 + n_dates * 2 + 1
        tbl.setSortingEnabled(False)
        tbl.setColumnCount(total_cols)

        headers = ["板块"]
        for d in dates:
            dt = datetime.strptime(d, "%Y%m%d")
            week = ["一", "二", "三", "四", "五", "六", "日"][dt.weekday()]
            label = f"{dt.strftime('%m-%d')} 周{week}"
            headers += [label, "净流入"]
        headers.append("龙头(最新)")
        tbl.setHorizontalHeaderLabels(headers)

        hdr = tbl.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for i in range(n_dates):
            cnt_col = 1 + i * 2
            net_col = 2 + i * 2
            hdr.setSectionResizeMode(cnt_col, QHeaderView.ResizeMode.ResizeToContents)
            hdr.setSectionResizeMode(net_col, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(total_cols - 1, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setMinimumSectionSize(44)

        tbl.setRowCount(len(top_sectors))
        tbl.verticalHeader().setDefaultSectionSize(28)

        for row_i, sector in enumerate(top_sectors):
            # 板块名
            name_item = QTableWidgetItem(sector)
            name_item.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
            tbl.setItem(row_i, 0, name_item)

            latest_leader = ""
            for col_i, d in enumerate(dates):
                r = day_data[d].get(sector)
                cnt = (r.get(cnt_key, 0) or 0) if r else 0
                net = r.get("net_buy_main") if r else None

                # 计数列（支持数值排序，点击列头可排序）
                cnt_col = 1 + col_i * 2
                cnt_text = str(cnt) if cnt > 0 else "—"
                cnt_item = _NumericItem(cnt_text, cnt)
                cnt_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                if cnt > 0:
                    cnt_item.setForeground(QBrush(QColor(color)))
                    f = cnt_item.font()
                    f.setBold(True)
                    cnt_item.setFont(f)
                    # tooltip：列出该板块当日具体个股
                    if stocks_by_date:
                        sector_stocks = (stocks_by_date.get(d) or {}).get(sector, [])
                        if sector_stocks:
                            if mode == "zt":
                                tip = "\n".join(
                                    f"{s['name']}({s['code']})  {s.get('consecutive_days', 1)}板"
                                    for s in sector_stocks
                                )
                            else:
                                tip = "\n".join(
                                    f"{s['name']}({s['code']})"
                                    for s in sector_stocks
                                )
                            cnt_item.setToolTip(tip)
                    # 只取最新日的龙头（首次遇到有数据的日期，即 dates[0]）
                    if not latest_leader:
                        ldr = (r.get("leader_name") or "") if r else ""
                        ldr_days = (r.get("leader_consecutive_days") or 0) if r else 0
                        if ldr:
                            # zt模式显示板数；dt模式仅显示名称（市值龙头无连板概念）
                            if mode == "zt" and ldr_days:
                                latest_leader = f"{ldr} {ldr_days}板"
                            else:
                                latest_leader = ldr
                else:
                    cnt_item.setForeground(QBrush(QColor(BORDER)))
                tbl.setItem(row_i, cnt_col, cnt_item)

                # 净流入列
                net_col = 2 + col_i * 2
                net_str = self._fmt_net(net)
                net_val = net if net is not None else 0.0
                net_item = _NumericItem(net_str, net_val)
                net_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                if net is not None:
                    net_item.setForeground(QBrush(QColor(RED if net >= 0 else GREEN)))
                else:
                    net_item.setForeground(QBrush(QColor(BORDER)))
                tbl.setItem(row_i, net_col, net_item)

            # 龙头列
            ldr_item = QTableWidgetItem(latest_leader)
            ldr_item.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
            tbl.setItem(row_i, total_cols - 1, ldr_item)

        tbl.setSortingEnabled(True)

    def _page_prev(self):
        new_off = self._page_offset + self.WEEK_SIZE
        if new_off < len(self._all_dates):
            self._page_offset = new_off
            self._rebuild()

    def _page_next(self):
        new_off = max(0, self._page_offset - self.WEEK_SIZE)
        self._page_offset = new_off
        self._rebuild()

    def _show_date_picker(self):
        dlg = _DatePickerDialog(self._all_dates, self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            chosen = dlg.selected_date()  # "YYYYMMDD"
            if chosen and chosen in self._all_dates:
                self._page_offset = self._all_dates.index(chosen)
                self._rebuild()


# ──────────────────────────────── 因子分析 Tab ────────────────────────────────

class FactorTab(QWidget):
    """因子分析 Tab：分析指定5天连板个股各因子与连板率的关系"""

    DAYS = 5

    def __init__(self, parent=None):
        super().__init__(parent)
        self._all_dates: list[str] = []
        self._page_offset: int = 0
        self._setup_ui()

    # ── UI ──

    def _setup_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 8, 12, 12)
        root.setSpacing(6)

        # 导航栏（与板块日统计相同风格）
        nav = QHBoxLayout()
        self._prev_btn = QPushButton("◀  更新")
        self._next_btn = QPushButton("更早  ▶")
        for btn in (self._prev_btn, self._next_btn):
            btn.setStyleSheet(
                f"QPushButton {{ background:{SEL}; color:{TEXT}; border:none;"
                f" border-radius:5px; padding:5px 14px; font-size:12px; }}"
            )
            btn.setFixedHeight(28)
        self._prev_btn.clicked.connect(self._page_next)
        self._next_btn.clicked.connect(self._page_prev)
        self._date_btn = QPushButton("—")
        self._date_btn.setFlat(True)
        self._date_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self._date_btn.setToolTip("点击选择日期")
        self._date_btn.setStyleSheet(
            f"QPushButton {{ color:{MUTED}; font-size:11px; background:transparent; border:none; }}"
            f"QPushButton:hover {{ color:{ACCENT}; text-decoration:underline; }}"
        )
        self._date_btn.clicked.connect(self._show_date_picker)
        nav.addWidget(self._prev_btn)
        nav.addStretch()
        nav.addWidget(self._date_btn)
        nav.addStretch()
        nav.addWidget(self._next_btn)
        nav_frame = QFrame()
        nav_frame.setStyleSheet(f"background:{SURFACE}; border-bottom:1px solid {BORDER};")
        nav_frame.setLayout(nav)
        root.addWidget(nav_frame)

        self._scroll = QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._scroll.setStyleSheet(f"QScrollArea {{ border:none; background:{BG}; }}")
        root.addWidget(self._scroll, 1)

    # ── 导航 ──

    def _page_prev(self):
        new_off = self._page_offset + self.DAYS
        if new_off < len(self._all_dates):
            self._page_offset = new_off
            self._rebuild()

    def _page_next(self):
        new_off = max(0, self._page_offset - self.DAYS)
        self._page_offset = new_off
        self._rebuild()

    def _show_date_picker(self):
        dlg = _DatePickerDialog(self._all_dates, self)
        if dlg.exec() == QDialog.DialogCode.Accepted:
            chosen = dlg.selected_date()
            if chosen and chosen in self._all_dates:
                self._page_offset = self._all_dates.index(chosen)
                self._rebuild()

    # ── 数据 ──

    def load_data(self, all_dates: list[str]):
        self._all_dates = all_dates
        self._page_offset = 0
        self._rebuild()

    def _rebuild(self):
        # 每次重建时替换 scroll 内的 widget（旧的会被 Qt 自动销毁）
        content = QWidget()
        content.setStyleSheet(f"background:{BG};")
        grid = QGridLayout(content)
        grid.setSpacing(12)
        grid.setContentsMargins(0, 4, 0, 16)
        grid.setColumnStretch(0, 1)
        grid.setColumnStretch(1, 1)

        if not self._all_dates:
            self._date_btn.setText("—")
            self._scroll.setWidget(content)
            return

        # 当前页日期（all_dates 降序，page_offset 起的 DAYS 天）
        start = self._page_offset
        end   = min(len(self._all_dates), start + self.DAYS)
        analysis_dates = self._all_dates[start:end]

        # 更新导航标签
        self._date_btn.setText(f"{analysis_dates[-1]} ~ {analysis_dates[0]}")
        self._prev_btn.setEnabled(self._page_offset > 0)
        self._next_btn.setEnabled(end < len(self._all_dates))

        # 最新日晋级判断：需要 page_offset 前一格（更新的那天）
        extra_recent: list[str] = []
        if self._page_offset > 0:
            extra_recent = [self._all_dates[self._page_offset - 1]]

        fetch_dates = list(analysis_dates) + extra_recent
        zt_by_date  = store.get_zt_for_dates(fetch_dates)
        cyq_by_date = store.get_cyq_for_dates(fetch_dates)

        all_codes: set[str] = set()
        for d in analysis_dates:
            for row in zt_by_date.get(d, []):
                all_codes.add(row["code"])

        concept_map = store.get_stock_concepts(list(all_codes))

        # 构建 (date, code) 记录，analysis_dates 是 DESC
        # "下一交易日（更新）" = analysis_dates[i-1]（i>0），最新日用 extra_recent
        records: list[dict] = []
        for i, d in enumerate(analysis_dates):
            if i == 0:
                if extra_recent:
                    next_codes = {r["code"] for r in zt_by_date.get(extra_recent[0], [])}
                    has_next = True
                else:
                    next_codes = set()
                    has_next = False
            else:
                next_codes = {r["code"] for r in zt_by_date.get(analysis_dates[i - 1], [])}
                has_next = True

            for row in zt_by_date.get(d, []):
                code = row["code"]
                if code.startswith(("43", "83", "87", "92")):
                    market = "北交所"
                elif code.startswith(("300", "301")):
                    market = "创业板"
                elif code.startswith("688"):
                    market = "科创板"
                else:
                    market = "主板"

                records.append({
                    "date":          d,
                    "code":          code,
                    "market":        market,
                    "float_cap":     row.get("float_cap"),
                    "price":         row.get("price"),
                    "pe":            row.get("pe"),
                    "consecutive":   row.get("consecutive_days", 1),
                    "concentration": cyq_by_date.get(d, {}).get(code),
                    "concepts":      concept_map.get(code, []),
                    "promoted":      (code in next_codes) if has_next else None,
                    "has_next":      has_next,
                })

        known_date_set = {r["date"] for r in records if r["has_next"]}
        known = [r for r in records if r["has_next"]]

        if not known:
            lbl = QLabel("暂无足够数据（至少需要 2 个交易日数据）")
            lbl.setStyleSheet(f"color:{MUTED}; font-size:13px; padding:20px;")
            grid.addWidget(lbl, 0, 0, 1, 3)
            grid.setRowStretch(1, 1)
            self._scroll.setWidget(content)
            return

        # 信息头
        info = QLabel(f"共 {len(known)} 条有效记录  ·  连板率 = 次日继续涨停的概率")
        info.setStyleSheet(f"color:{MUTED}; font-size:11px; padding:2px 0 6px 0;")
        grid.addWidget(info, 0, 0, 1, 2)

        # col_dates: 列顺序用时间升序（左旧右新），analysis_dates 是 DESC 需要反转
        col_dates = list(reversed(analysis_dates))
        kw = dict(col_dates=col_dates, known_date_set=known_date_set)

        # 构建各 section（row builder 传全量 records，section 内部按日期拆分）
        s_market  = self._build_section("市场板块",          self._rows_market(records), **kw)
        s_consec  = self._build_section("连板数",             self._rows_consecutive(records), **kw)
        s_price   = self._build_section("股价（元）",         self._rows_price(records), **kw)
        s_fcap    = self._build_section("流通市值",           self._rows_float_cap(records), **kw)
        s_pe      = self._build_section("市盈率 PE",          self._rows_pe(records), **kw)
        s_cyq     = self._build_section("筹码集中度 90%",     self._rows_concentration(records), **kw)
        s_concept = self._build_section("概念板块（Top 15）", self._rows_concept(records), **kw)

        # 2 列网格排列（AlignTop 确保各格子顶部对齐，不被拉伸）
        AT = Qt.AlignmentFlag.AlignTop
        grid.addWidget(s_market,  1, 0, AT)
        grid.addWidget(s_consec,  1, 1, AT)
        grid.addWidget(s_price,   2, 0, AT)
        grid.addWidget(s_fcap,    2, 1, AT)
        grid.addWidget(s_pe,      3, 0, AT)
        grid.addWidget(s_cyq,     3, 1, AT)
        grid.addWidget(s_concept, 4, 0, 1, 2)   # 概念独占一整行
        grid.setRowStretch(5, 1)

        self._scroll.setWidget(content)

    # ── 辅助：统计 ──

    @staticmethod
    def _rate(grp: list[dict]) -> tuple[int, int, float]:
        """(total, promoted_count, rate_pct)"""
        total = len(grp)
        promoted = sum(1 for r in grp if r["promoted"])
        return total, promoted, (promoted / total * 100) if total else 0.0

    @staticmethod
    def _pearson(records: list[dict], key: str) -> float | None:
        vals = [(r[key], 1 if r["promoted"] else 0)
                for r in records if r[key] is not None]
        if len(vals) < 5:
            return None
        n = len(vals)
        xs = [v[0] for v in vals]
        ys = [v[1] for v in vals]
        mx, my = sum(xs) / n, sum(ys) / n
        num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        den = (sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys)) ** 0.5
        return num / den if den else None

    # ── 辅助：各因子分组（返回 [(label, [records])]）──

    def _rows_market(self, records: list[dict]) -> list[tuple]:
        groups: dict[str, list] = {}
        for r in records:
            groups.setdefault(r["market"], []).append(r)
        return [(m, groups[m]) for m in ["主板", "创业板", "科创板", "北交所"] if m in groups]

    def _rows_consecutive(self, records: list[dict]) -> list[tuple]:
        groups: dict[int, list] = {}
        for r in records:
            groups.setdefault(r["consecutive"], []).append(r)
        return [(f"{d}板", groups[d]) for d in sorted(groups)]

    def _rows_float_cap(self, records: list[dict]) -> list[tuple]:
        bins = [
            ("< 20亿",    lambda v: v < 20),
            ("20~50亿",   lambda v: 20 <= v < 50),
            ("50~100亿",  lambda v: 50 <= v < 100),
            ("100~300亿", lambda v: 100 <= v < 300),
            ("300~500亿", lambda v: 300 <= v < 500),
            ("> 500亿",   lambda v: v >= 500),
        ]
        return self._bin_rows(records, "float_cap", bins)

    def _rows_price(self, records: list[dict]) -> list[tuple]:
        bins = [
            ("< 10元",    lambda v: v < 10),
            ("10~20元",   lambda v: 10 <= v < 20),
            ("20~50元",   lambda v: 20 <= v < 50),
            ("50~100元",  lambda v: 50 <= v < 100),
            ("> 100元",   lambda v: v >= 100),
        ]
        return self._bin_rows(records, "price", bins)

    def _rows_pe(self, records: list[dict]) -> list[tuple]:
        bins = [
            ("亏损(PE<0)", lambda v: v < 0),
            ("0~30",       lambda v: 0 <= v < 30),
            ("30~60",      lambda v: 30 <= v < 60),
            ("60~100",     lambda v: 60 <= v < 100),
            ("100~200",    lambda v: 100 <= v < 200),
            ("> 200",      lambda v: v >= 200),
        ]
        rows = self._bin_rows(records, "pe", bins)
        no_pe = [r for r in records if r["pe"] is None]
        if no_pe:
            rows.append(("无PE数据", no_pe))
        return rows

    def _rows_concentration(self, records: list[dict]) -> list[tuple]:
        # concentration_90 存储为小数：0.08 = 8%
        bins = [
            ("≤ 3%",   lambda v: v <= 0.03),
            ("3~8%",   lambda v: 0.03 < v <= 0.08),
            ("8~15%",  lambda v: 0.08 < v <= 0.15),
            ("15~20%", lambda v: 0.15 < v <= 0.20),
            ("20~30%", lambda v: 0.20 < v <= 0.30),
            ("> 30%",  lambda v: v > 0.30),
        ]
        rows = self._bin_rows(records, "concentration", bins)
        no_cyq = [r for r in records if r["concentration"] is None]
        if no_cyq:
            rows.append(("无数据", no_cyq))
        return rows

    def _rows_concept(self, records: list[dict]) -> list[tuple]:
        bucket: dict[str, list] = {}
        for r in records:
            for c in r["concepts"]:
                bucket.setdefault(c, []).append(r)
        top = sorted(bucket.items(), key=lambda x: len(x[1]), reverse=True)[:15]
        return list(top)

    @staticmethod
    def _bin_rows(records: list[dict], key: str, bins: list[tuple]) -> list[tuple]:
        rows = []
        for label, fn in bins:
            grp = [r for r in records if r[key] is not None and fn(r[key])]
            if grp:
                rows.append((label, grp))
        return rows

    # ── 辅助：构建 section ──

    def _build_section(self, title: str, rows: list[tuple],
                        col_dates: list[str], known_date_set: set[str]) -> QFrame:
        """
        rows: [(label, [records]), ...]
        col_dates: 按时间升序的日期列表（左旧右新）
        known_date_set: 有 has_next=True 数据的日期集合
        列布局: 分组 | D1 | D2 | ... | Dn | 合计
        每格显示 "率%(N)" 若已知，"N只" 若无晋级数据，"—" 若无股票
        """
        frame = QFrame()
        frame.setStyleSheet(
            f"QFrame {{ background:{BG}; border:1px solid {BORDER}; border-radius:8px; }}"
        )
        fv = QVBoxLayout(frame)
        fv.setContentsMargins(0, 0, 0, 0)
        fv.setSpacing(0)

        # 标题行
        hdr_w = QWidget()
        hdr_w.setStyleSheet(
            f"background:{SURFACE}; border-radius:8px 8px 0 0;"
            f" border-bottom:1px solid {BORDER};"
        )
        hdr_h = QHBoxLayout(hdr_w)
        hdr_h.setContentsMargins(12, 8, 12, 8)
        title_lbl = QLabel(title)
        title_lbl.setStyleSheet(
            f"color:{TEXT}; font-weight:bold; font-size:13px;"
            f" background:transparent; border:none;"
        )
        hdr_h.addWidget(title_lbl)
        fv.addWidget(hdr_w)

        if not rows:
            empty = QLabel("  暂无数据")
            empty.setStyleSheet(f"color:{MUTED}; font-size:12px; padding:8px;")
            fv.addWidget(empty)
            return frame

        # 列：分组 + 每日 + 合计
        n_date_cols = len(col_dates)
        total_cols  = 1 + n_date_cols + 1

        def date_hdr(d: str) -> str:
            # YYYYMMDD → MM/DD，已知晋级日加星标
            label = f"{d[4:6]}/{d[6:8]}"
            return label if d in known_date_set else f"{label}*"

        headers = ["分组"] + [date_hdr(d) for d in col_dates] + ["合计"]

        ROW_H = 36
        HDR_H = 30

        def _bar_color(rate: float) -> str:
            if rate >= 60:
                return GREEN
            if rate >= 40:
                return ORANGE
            if rate >= 20:
                return "#D97706"
            return RED

        def _make_bar(rate: float, count: int) -> QWidget:
            """N只  ━━━━━━░░░  60%
            count 标签固定宽，生命条 Expanding 自动填满列宽，无胶囊边框。
            """
            r = max(0, min(100, int(round(rate))))
            color = _bar_color(rate)

            w = QWidget()
            w.setStyleSheet("background:transparent; border:none;")
            h = QHBoxLayout(w)
            h.setContentsMargins(4, 0, 6, 0)
            h.setSpacing(3)

            cnt_lbl = QLabel(f"{count}")
            cnt_lbl.setFixedWidth(22)
            cnt_lbl.setAlignment(
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
            )
            cnt_lbl.setStyleSheet(
                f"color:{MUTED}; font-size:10px;"
                f" background:transparent; border:none;"
            )
            h.addWidget(cnt_lbl)

            hp_bar = QProgressBar()
            hp_bar.setRange(0, 100)
            hp_bar.setValue(r)
            hp_bar.setTextVisible(False)
            hp_bar.setFixedHeight(7)
            hp_bar.setMinimumWidth(20)
            hp_bar.setSizePolicy(
                QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed
            )
            hp_bar.setStyleSheet(f"""
                QProgressBar {{
                    background: {BORDER}; border-radius: 3px; border: none;
                }}
                QProgressBar::chunk {{
                    background: {color}; border-radius: 3px; min-width: 4px;
                }}
            """)
            h.addWidget(hp_bar)

            pct_lbl = QLabel(f"{r}%")
            pct_lbl.setFixedWidth(26)
            pct_lbl.setStyleSheet(
                f"color:{color}; font-size:10px; font-weight:bold;"
                f" background:transparent; border:none;"
            )
            h.addWidget(pct_lbl)
            return w

        def _no_data_label(text: str) -> QLabel:
            lbl = QLabel(text)
            lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lbl.setStyleSheet(f"color:{MUTED}; font-size:11px;")
            return lbl

        def _fill_col(grp: list[dict]) -> tuple[float, QWidget]:
            """返回 (sort_val, cell_widget)"""
            known = [r for r in grp if r["has_next"]]
            total = len(grp)
            if total == 0:
                return -1.0, _no_data_label("—")
            if not known:
                return -1.0, _no_data_label(f"{total}只")
            prm  = sum(1 for r in known if r["promoted"])
            rate = prm / len(known) * 100
            return rate, None   # widget 由调用方决定 dim

        tbl_style = f"""
            QTableWidget {{
                border: none; background: {BG};
                alternate-background-color: {SURFACE};
                color: {TEXT}; font-size: 12px;
                gridline-color: {BORDER};
            }}
            QTableWidget::item {{ padding: 0px; }}
            QHeaderView::section {{
                background: {SURFACE}; color: {MUTED}; font-size: 11px;
                font-weight: bold; border: none;
                border-right: 1px solid {BORDER};
                border-bottom: 1px solid {BORDER}; padding: 3px 8px;
            }}
        """
        tbl = QTableWidget(len(rows), total_cols)
        tbl.setHorizontalHeaderLabels(headers)
        tbl.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        tbl.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        tbl.verticalHeader().setVisible(False)
        tbl.setShowGrid(True)
        tbl.setAlternatingRowColors(True)
        tbl.setStyleSheet(tbl_style)

        hdr = tbl.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        for c in range(1, total_cols):
            hdr.setSectionResizeMode(c, QHeaderView.ResizeMode.Stretch)

        tbl.verticalHeader().setDefaultSectionSize(ROW_H)

        tbl.setSortingEnabled(False)
        for i, (label, all_recs) in enumerate(rows):
            tbl.setItem(i, 0, QTableWidgetItem(str(label)))

            # 每日列（浅色条）
            for j, d in enumerate(col_dates):
                day_recs = [r for r in all_recs if r["date"] == d]
                sort_val, w = _fill_col(day_recs)
                tbl.setItem(i, 1 + j, _NumericItem("", sort_val))
                if w is None:
                    known = [r for r in day_recs if r["has_next"]]
                    prm  = sum(1 for r in known if r["promoted"])
                    rate = prm / len(known) * 100
                    w = _make_bar(rate, len(day_recs))
                    w.setToolTip(f"涨停 {len(day_recs)} 只，晋级 {prm} 只")
                tbl.setCellWidget(i, 1 + j, w)

            # 合计列
            sort_val, w = _fill_col(all_recs)
            tbl.setItem(i, 1 + n_date_cols, _NumericItem("", sort_val))
            if w is None:
                known = [r for r in all_recs if r["has_next"]]
                prm  = sum(1 for r in known if r["promoted"])
                rate = prm / len(known) * 100
                w = _make_bar(rate, len(all_recs))
                w.setToolTip(f"涨停 {len(all_recs)} 只，晋级 {prm} 只")
            tbl.setCellWidget(i, 1 + n_date_cols, w)

        tbl.setSortingEnabled(True)
        tbl.sortByColumn(n_date_cols + 1, Qt.SortOrder.DescendingOrder)
        tbl.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        tbl.setFixedHeight(len(rows) * ROW_H + HDR_H)
        fv.addWidget(tbl)
        return frame


# ──────────────────────────────── 主窗口 ────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("连板天梯")
        self.resize(1440, 900)

        self._fetch_worker: FetchWorker | None = None
        self._concept_worker: ConceptWorker | None = None
        self._sector_worker: SectorStatsWorker | None = None
        self._historical_worker: HistoricalFetchWorker | None = None
        self._industry_fill_worker: IndustryFillWorker | None = None

        store.init_db()
        self._setup_style()
        self._setup_ui()
        # 延迟启动数据加载，让 UI 先渲染
        QTimer.singleShot(300, self._start_initial_load)

    def _setup_style(self):
        self.setStyleSheet(f"""
            QMainWindow, QWidget {{ background: {BG}; color: {TEXT}; font-family: 'PingFang SC', 'Helvetica Neue', Arial, sans-serif; }}
            QTabWidget::pane {{ border: none; }}
            QTabBar::tab {{
                background: {SURFACE};
                color: {MUTED};
                padding: 8px 20px;
                border: none;
                font-size: 13px;
                min-width: 100px;
            }}
            QTabBar::tab:selected {{
                background: {BG};
                color: {TEXT};
                font-weight: bold;
                border-bottom: 2px solid {ACCENT};
            }}
            QTabBar::tab:hover {{ color: {TEXT}; }}
            QScrollBar:horizontal {{
                background: {SURFACE};
                height: 8px;
                border-radius: 4px;
            }}
            QScrollBar::handle:horizontal {{
                background: {BORDER};
                border-radius: 4px;
                min-width: 40px;
            }}
            QScrollBar:vertical {{
                background: {SURFACE};
                width: 8px;
                border-radius: 4px;
            }}
            QScrollBar::handle:vertical {{
                background: {BORDER};
                border-radius: 4px;
                min-height: 40px;
            }}
            QScrollBar::add-line, QScrollBar::sub-line {{ width:0; height:0; }}
        """)

    def _setup_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        cv = QVBoxLayout(central)
        cv.setContentsMargins(0, 0, 0, 0)
        cv.setSpacing(0)

        # 顶栏
        top_bar = QFrame()
        top_bar.setFixedHeight(48)
        top_bar.setStyleSheet(
            f"background:{TEXT}; border-bottom:1px solid #333;"
        )
        tv = QHBoxLayout(top_bar)
        tv.setContentsMargins(16, 0, 16, 0)
        title = QLabel("连板天梯")
        title.setStyleSheet("color:white; font-size:16px; font-weight:bold;")
        self._status_lbl = QLabel("就绪")
        self._status_lbl.setStyleSheet("color:#AAAAAA; font-size:11px;")
        self._status_lbl.setMaximumWidth(500)

        self._progress = QProgressBar()
        self._progress.setRange(0, 0)   # 不确定进度（旋转动画）
        self._progress.setFixedWidth(120)
        self._progress.setFixedHeight(6)
        self._progress.setTextVisible(False)
        self._progress.setStyleSheet("""
            QProgressBar { background:#444; border-radius:3px; }
            QProgressBar::chunk { background:#007AFF; border-radius:3px; }
        """)
        self._progress.hide()

        refresh_btn = QPushButton("刷新数据")
        refresh_btn.setStyleSheet(
            f"QPushButton {{ background:{ACCENT}; color:white; border:none;"
            f" border-radius:5px; padding:5px 14px; font-size:12px; }}"
        )
        refresh_btn.clicked.connect(self._start_full_refresh)
        tv.addWidget(title)
        tv.addStretch()
        tv.addWidget(self._progress)
        tv.addSpacing(8)
        tv.addWidget(self._status_lbl)
        tv.addSpacing(12)
        tv.addWidget(refresh_btn)
        cv.addWidget(top_bar)

        # Tab
        self._tabs = QTabWidget()
        self._ladder_tab = LadderTab()
        self._ladder_tab.status_msg.connect(self._set_status)
        self._sector_tab = SectorTab()
        self._factor_tab = FactorTab()
        self._tabs.addTab(self._ladder_tab, "连板天梯")
        self._tabs.addTab(self._sector_tab, "板块日统计")
        self._tabs.addTab(self._factor_tab, "因子分析")
        cv.addWidget(self._tabs, 1)

    def _set_status(self, msg: str):
        self._status_lbl.setText(msg)
        # 就绪时隐藏进度条，其他状态显示
        is_busy = msg not in ("就绪", "") and not msg.startswith("已加载")
        if is_busy:
            self._progress.show()
        else:
            self._progress.hide()

    # ── 数据加载 ──

    def _start_initial_load(self):
        # 立即把缓存数据渲染到 UI（主线程，纯 DB 读取，不走网络）
        cached_dates = store.get_dates_with_zt_data()
        if cached_dates:
            concepts = store.get_all_concept_names()
            if concepts:
                self._ladder_tab.set_concepts(concepts)
            self._ladder_tab.load_data(cached_dates)
            self._sector_tab.load_data(cached_dates)
            self._factor_tab.load_data(cached_dates)
            self._set_status(f"已显示 {len(cached_dates)} 个缓存日期，后台拉取新数据...")
        else:
            self._set_status("正在后台拉取数据，首次启动需要几分钟...")

        # 如果最近8天内有涨停记录但缺少行业数据，强制重拉那些日期
        dates_need_refetch = store.get_zt_dates_missing_industry(8)
        if dates_need_refetch:
            with store.get_conn() as c:
                for d in dates_need_refetch:
                    c.execute("DELETE FROM fetch_log WHERE key=?", (f"zt_{d}",))

        # 把网络操作全部交给后台线程（dates=None 表示线程自行获取交易日历）
        self._fetch_worker = FetchWorker(
            dates=None, months=9, force_dates=dates_need_refetch
        )
        self._fetch_worker.progress.connect(self._set_status)
        self._fetch_worker.done.connect(self._after_fetch)
        self._fetch_worker.start()

    def _after_fetch(self, all_dates: list[str], new_count: int):
        if not all_dates:
            return

        # 每次 fetch 结束都重载天梯（PE 数据在 fetch 末尾写入，必须刷新）
        concepts = store.get_all_concept_names()
        if concepts:
            self._ladder_tab.set_concepts(concepts)
        self._ladder_tab.load_data(all_dates)

        # 只有实际拉到新数据或首次加载时才重建板块统计
        first_load = not self._sector_tab._all_dates
        if new_count > 0 or first_load:
            self._sector_tab.load_data(all_dates)
            # 行业数据已在涨停池拉取时同步写入 stock_concepts，先算板块统计
            self._start_sector_stats(all_dates[:30])

        self._factor_tab.load_data(all_dates)
        self._set_status(f"已加载 {len(all_dates)} 个交易日数据")
        self._ladder_tab._refresh_sys_info()

        # 若有历史空白日期（status='empty'）尚未重建，启动历史重建
        with store.get_conn() as c:
            empty_count = c.execute(
                "SELECT COUNT(*) FROM fetch_log WHERE status='empty' AND key LIKE 'zt_%'"
            ).fetchone()[0]
        if empty_count > 0 and not store.is_fetched("historical_zt_built", max_age_hours=168):
            self._start_historical_fetch()
            return  # 历史重建完成后会再触发 _after_historical → concept fetch

        # 若 THS 概念映射未建立（首次或一周后过期），后台启动抓取
        if not store.is_fetched("ths_concepts_built", max_age_hours=168):
            self._start_concept_fetch()

    def _start_historical_fetch(self):
        self._set_status("开始后台重建历史涨停数据（约需 2 分钟）...")
        self._historical_worker = HistoricalFetchWorker()
        self._historical_worker.progress.connect(self._set_status)
        self._historical_worker.done.connect(self._after_historical)
        self._historical_worker.start()

    def _after_historical(self):
        all_dates = store.get_dates_with_zt_data()
        self._ladder_tab.load_data(all_dates)
        self._sector_tab.load_data(all_dates)
        self._factor_tab.load_data(all_dates)
        self._set_status(f"历史数据就绪（{len(all_dates)} 个交易日），正在加载概念...")
        if not store.has_concept_data():
            self._start_concept_fetch()
        else:
            self._start_sector_stats(all_dates[:30])

    def _start_concept_fetch(self):
        with store.get_conn() as c:
            codes = {r[0] for r in c.execute(
                "SELECT DISTINCT code FROM zt_records"
            ).fetchall()}
        if not codes:
            return
        self._set_status(f"后台抓取 THS 概念主题（375个板块，约5分钟）...")
        self._concept_worker = ConceptWorker(codes)
        self._concept_worker.progress.connect(self._set_status)
        self._concept_worker.done.connect(self._after_concepts)
        self._concept_worker.start()

    def _after_concepts(self):
        concepts = store.get_all_concept_names()
        self._ladder_tab.set_concepts(concepts)
        all_dates = store.get_dates_with_zt_data()
        self._ladder_tab.load_data(all_dates)  # 重新加载以附加概念
        self._factor_tab.load_data(all_dates)
        self._start_sector_stats(all_dates[:30])
        self._set_status(f"概念数据加载完成，共 {len(concepts)} 个板块")

    def _start_sector_stats(self, dates: list[str]):
        self._sector_worker = SectorStatsWorker(dates)
        self._sector_worker.done.connect(self._after_sector_stats)
        self._sector_worker.start()

    def _after_sector_stats(self):
        all_dates = store.get_dates_with_zt_data()
        self._sector_tab.load_data(all_dates)
        # 检查是否还有历史股票缺少行业数据（只出现在API窗口外的日期）
        missing = store.get_codes_missing_industry()
        if missing:
            self._set_status(f"后台补充 {len(missing)} 只历史股票行业数据...")
            self._industry_fill_worker = IndustryFillWorker(missing)
            self._industry_fill_worker.progress.connect(self._set_status)
            self._industry_fill_worker.done.connect(self._after_industry_fill)
            self._industry_fill_worker.start()
        else:
            self._set_status("就绪")

    def _after_industry_fill(self):
        all_dates = store.get_dates_with_zt_data()
        concepts = store.get_all_concept_names()
        if concepts:
            self._ladder_tab.set_concepts(concepts)
        self._ladder_tab.load_data(all_dates)  # 刷新以显示新概念
        self._factor_tab.load_data(all_dates)
        self._set_status("就绪")

    def _start_full_refresh(self):
        self._set_status("后台增量拉取中（含今日强制更新）...")
        dates_need_refetch = store.get_zt_dates_missing_industry(8)
        if dates_need_refetch:
            with store.get_conn() as c:
                for d in dates_need_refetch:
                    c.execute("DELETE FROM fetch_log WHERE key=?", (f"zt_{d}",))
        self._fetch_worker = FetchWorker(
            dates=None, months=9, force_dates=dates_need_refetch
        )
        self._fetch_worker.progress.connect(self._set_status)
        self._fetch_worker.done.connect(self._after_fetch)
        self._fetch_worker.start()

    def closeEvent(self, event):
        for w in (self._fetch_worker, self._concept_worker,
                  self._sector_worker, self._historical_worker,
                  self._industry_fill_worker):
            if w and w.isRunning():
                w.stop()
                w.wait(2000)
        super().closeEvent(event)


# ──────────────────────────────── 入口 ────────────────────────────────

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("连板天梯")
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
