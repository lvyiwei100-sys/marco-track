import streamlit as st
import pandas as pd
from fredapi import Fred
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
import html
import json
import re
import time
import feedparser
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# 1. 页面级配置与高级 UI 样式 (CSS 注入)
# ==========================================
st.set_page_config(page_title="US Macro Pulse | 美林时钟终端", page_icon="🦅", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #0e1117; color: #e0e6ed; }
    h1, h2, h3 { font-family: 'Helvetica Neue', sans-serif; font-weight: 700 !important; color: #ffffff !important; letter-spacing: -0.5px; }
    .metric-card { background-color: #161b22; border: 1px solid #30363d; border-radius: 12px; padding: 20px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3); transition: transform 0.2s; }
    .metric-card:hover { transform: translateY(-5px); border-color: #58a6ff; }
    .clock-section { background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); border-radius: 16px; padding: 25px; border: 1px solid #38444d; margin-bottom: 25px; }
    @keyframes countdown-urgent-pulse {
        0%, 100% { background-color: rgba(239, 68, 68, 0.22); box-shadow: 0 0 0 1px rgba(248, 113, 113, 0.35), 0 0 14px rgba(239, 68, 68, 0.18); }
        50% { background-color: rgba(239, 68, 68, 0.42); box-shadow: 0 0 0 1px rgba(252, 165, 165, 0.55), 0 0 22px rgba(239, 68, 68, 0.32); }
    }
    .countdown-card {
        background-color: #161b22; border: 1px solid #30363d; border-radius: 12px; padding: 18px 16px;
        min-height: 132px; transition: border-color 0.2s;
    }
    .countdown-card.countdown-urgent {
        border-color: rgba(248, 113, 113, 0.65);
        animation: countdown-urgent-pulse 2.4s ease-in-out infinite;
    }
    .countdown-label { color: #8b949e; font-size: 0.88rem; margin: 0 0 6px 0; }
    .countdown-title { color: #f0f6fc; font-size: 1.05rem; font-weight: 700; margin: 0 0 8px 0; }
    .countdown-time { color: #58a6ff; font-size: 1.65rem; font-weight: 700; letter-spacing: 0.5px; font-variant-numeric: tabular-nums; margin: 0 0 6px 0; }
    .countdown-meta { color: #6e7681; font-size: 0.78rem; margin: 0; line-height: 1.35; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. 初始化 FRED API
# ==========================================
@st.cache_resource
def get_fred_client():
    try:
        api_key = st.secrets["FRED_API_KEY"]
        return Fred(api_key=api_key)
    except Exception as e:
        st.error("⚠️ 未在 secrets.toml 中找到 FRED_API_KEY")
        st.stop()

fred = get_fred_client()

# ==========================================
# 3. 数据全集与单位字典
# ==========================================
FRED_CATEGORIES = {
    "就业 (Employment)": {
        "失业率 (UNRATE)": "UNRATE",
        "非农就业 (PAYEMS)": "PAYEMS",
        "职位空缺 (JTSJOL)": "JTSJOL",
        "初请失业金 (ICSA)": "ICSA",
        "劳动力参与率 (CIVPART)": "CIVPART",
        "就业人口比 (EMRATIO)": "EMRATIO",
        "平均时薪 (CES0500000003)": "CES0500000003",
    },
    "流动性 (Liquidity)": {
        "M2货币供应 (M2SL)": "M2SL",
        "美联储资产负债表 (WALCL)": "WALCL",
        "金融条件指数 (NFCI)": "NFCI",
        "有效联邦基金利率/日 (DFF)": "DFF",
        "隔夜逆回购余额 (RRPONTSYD)": "RRPONTSYD",
    },
    "利率 (Interest Rates)": {
        "联邦基金利率/月 (FEDFUNDS)": "FEDFUNDS",
        "2年期美债 (DGS2)": "DGS2",
        "10年期美债 (DGS10)": "DGS10",
        "10年-2年利差 (T10Y2Y)": "T10Y2Y",
    },
    "通胀 (Inflation)": {
        "CPI总体 (CPIAUCSL)": "CPIAUCSL",
        "核心CPI (CPILFESL)": "CPILFESL",
        "PCE总体 (PCEPI)": "PCEPI",
        "核心PCE (PCEPILFE)": "PCEPILFE",
        "PPI总体 (PPIACO)": "PPIACO",
    },
    "增长 (Growth)": {
        "实际GDP (GDPC1)": "GDPC1",
        "工业生产指数 (INDPRO)": "INDPRO",
        "零售销售 (RSAFS)": "RSAFS",
    },
    "AI代理指标 (AI Proxies)": {
        "半导体工业生产 (IPG3344N)": "IPG3344N",
        "半导体价格PPI (PCU33443344)": "PCU33443344",
    }
}

# 核心：定义哪些指标需要使用原生的同比 (pc1) 接口
PC1_SERIES = ["CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE", "PPIACO"]

# 核心：定义绝对数值 (Value) 的单位后缀
UNIT_MAP = {
    # 利率与比率类
    "UNRATE": "%", "CIVPART": "%", "EMRATIO": "%",
    "FEDFUNDS": "%", "DFF": "%", "DGS2": "%", "DGS10": "%", "T10Y2Y": "%",
    # 就业人数类
    "PAYEMS": " 千人", "JTSJOL": " 千人", "ICSA": " 人",
    # 资金规模类
    "M2SL": " 10亿美元", "RRPONTSYD": " 10亿美元", "WALCL": " 百万美元",
    # 价格与指数类
    "CES0500000003": " $/小时", "INDPRO": "", "IPG3344N": "",
    "GDPC1": " 10亿美元", "RSAFS": " 百万美元", "NFCI": "", "PCU33443344": ""
}

# ==========================================
# 4. 优化数据拉取 (注入 pc1 逻辑)
# ==========================================
@st.cache_data(ttl=86400)
def fetch_data_advanced(series_id, years=6):
    end_date = datetime.today()
    start_date = end_date - relativedelta(years=years)

    req_units = 'pc1' if series_id in PC1_SERIES else 'lin'

    try:
        data = fred.get_series(series_id, observation_start=start_date, observation_end=end_date, units=req_units)
        df = pd.DataFrame({'Date': data.index, 'Value': data.values}).dropna()

        if req_units == 'pc1':
            df['YoY'] = df['Value']
            df['Value_Diff'] = df['Value'].diff(1)
        else:
            if len(df) > 12:
                df['YoY'] = df['Value'].pct_change(12) * 100
                df['Value_Diff'] = df['Value'].diff(1)
            else:
                df['YoY'] = 0
                df['Value_Diff'] = 0

        display_start = end_date - relativedelta(years=5)
        return df[df['Date'] >= display_start]
    except Exception as e:
        return pd.DataFrame()


def warm_core_series_cache():
    """首屏用到的序列并行拉取，命中 cache 后几乎无开销。"""
    core = ("INDPRO", "CPIAUCSL", "CPILFESL", "UNRATE", "FEDFUNDS", "IPG3344N")
    with ThreadPoolExecutor(max_workers=len(core)) as ex:
        futures = [ex.submit(fetch_data_advanced, sid, 6) for sid in core]
        for f in as_completed(futures):
            f.result()


def load_category_parallel(tab_name: str, years: int = 6) -> dict[str, pd.DataFrame]:
    """仅当前分类下的序列并行请求（与 fetch_data_advanced 共用磁盘缓存）。"""
    ids = list(FRED_CATEGORIES[tab_name].values())
    if not ids:
        return {}
    workers = min(10, len(ids))
    out: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        fut_to_sid = {ex.submit(fetch_data_advanced, sid, years): sid for sid in ids}
        for fut in as_completed(fut_to_sid):
            out[fut_to_sid[fut]] = fut.result()
    return out


# ==========================================
# 5. 美林时钟：规则兜底 + 大模型综合判断（可选 OPENAI_API_KEY）
# ==========================================
def _clock_rule_based(g_now: float, g_prev: float, i_now: float, i_prev: float) -> tuple[str, str, str, str]:
    g_up = g_now > g_prev
    i_up = i_now > i_prev
    if g_up and not i_up:
        return "复苏 (Recovery)", "📈 增长 ⬆️ / 通胀增速 ⬇️", "#22c55e", "股票 > 债券 > 大宗"
    if g_up and i_up:
        return "过热 (Overheat)", "🔥 增长 ⬆️ / 通胀增速 ⬆️", "#ef4444", "大宗 > 股票 > 现金"
    if not g_up and i_up:
        return "滞胀 (Stagflation)", "☁️ 增长 ⬇️ / 通胀增速 ⬆️", "#f97316", "现金 > 大宗 > 债券"
    return "衰退 (Reflation)", "🥶 增长 ⬇️ / 通胀增速 ⬇️", "#3b82f6", "债券 > 现金 > 股票"


def _phase_color_fallback(phase: str) -> str:
    p = phase.lower()
    if "复苏" in phase or "recovery" in p:
        return "#22c55e"
    if "过热" in phase or "overheat" in p:
        return "#ef4444"
    if "滞胀" in phase or "stagflation" in p:
        return "#f97316"
    if "衰退" in phase or "reflation" in p:
        return "#3b82f6"
    return "#58a6ff"


def _parse_llm_clock_json(raw: str) -> dict | None:
    t = raw.strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s*```\s*$", "", t)
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        return None


@st.cache_data(ttl=timedelta(days=30))
def _merrill_clock_llm_cached(
    indpro_lvl_now: float,
    indpro_lvl_prev: float,
    cpi_yoy_now: float,
    cpi_yoy_prev: float,
    indpro_yoy_now: float,
    indpro_yoy_prev: float,
    obs_g: str,
    obs_i: str,
) -> str | None:
    """
    调用大模型返回 JSON 字符串；无密钥或失败返回 None（由上层回退规则）。
    缓存键为宏观快照指纹，减少重复计费。
    """
    api_key = None
    for _k in ("OPENAI_API_KEY", "LLM_API_KEY"):
        try:
            _v = st.secrets[_k]
            if _v:
                api_key = str(_v).strip()
                break
        except Exception:
            continue
    if not api_key:
        return None

    try:
        from openai import OpenAI
    except ImportError:
        return None

    base_url = None
    try:
        _bu = st.secrets["OPENAI_BASE_URL"]
        if _bu:
            base_url = str(_bu).strip()
    except Exception:
        pass

    model = "gpt-4o-mini"
    try:
        _m = st.secrets["MERRILL_LLM_MODEL"]
        if _m:
            model = str(_m).strip()
    except Exception:
        pass

    client = OpenAI(api_key=api_key, base_url=base_url) if base_url else OpenAI(api_key=api_key)

    user_block = f"""宏观快照（FRED，近似代表美林时钟两轴）：
- 工业生产 INDPRO：最新水平 {indpro_lvl_now:.4f}，约 3 个月前水平 {indpro_lvl_prev:.4f}；对应同比增速约 {indpro_yoy_now:.2f}% vs 三个月前约 {indpro_yoy_prev:.2f}%。
- CPI 总指数同比增速（YoY%）：最新 {cpi_yoy_now:.2f}%，约三个月前 {cpi_yoy_prev:.2f}%。
- 最近观测日期：INDPRO {obs_g}，CPI {obs_i}。

请勿只做「两个数谁大谁小」的机械四象限；请结合增长边际与通胀动能/拐点的经济含义，判断当前**最接近**美林投资时钟阶段，并给出资产配置倾向（教科书式表述即可，非投资建议）。"""

    sys_prompt = """你是资深全球宏观策略助手，熟悉美林投资时钟四象限（复苏、过热、滞胀、衰退/衰退修复 Reflation）。
你必须基于用户给出的美国宏观代理指标做**综合判断**，允许与简单四象限机械划分不一致（例如：通胀粘性、增长放缓的组合）。
输出**仅**一段合法 JSON，不要 Markdown，不要前后说明。字段：
{"phase":"中文阶段名，须含英文括注，如 复苏 (Recovery)","tagline":"一句话阶段描述（可加 emoji）","assets":"用 > 连接的大类排序，如 股票 > 债券 > 大宗","color_hex":"#RRGGBB","rationale":"2-4 句中文，说明为何如此归类"}

phase 必须从以下四选一（文字须一致或极其接近）：
复苏 (Recovery)、过热 (Overheat)、滞胀 (Stagflation)、衰退 (Reflation)"""

    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0.25,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_block},
            ],
            timeout=60.0,
        )
        return (resp.choices[0].message.content or "").strip() or None
    except Exception:
        return None


def calculate_investment_clock() -> tuple[str, str, str, str, str, bool]:
    """
    返回 (phase, desc, color, assets, rationale_or_note, used_llm)。
    rationale_or_note：使用 LLM 时为模型 rationale；否则为简短说明。
    """
    growth_df = fetch_data_advanced("INDPRO", years=6)
    infl_df = fetch_data_advanced("CPIAUCSL", years=6)

    if growth_df.empty or infl_df.empty or len(growth_df) < 4 or len(infl_df) < 4:
        return "数据不足", "🔧 无法计算时钟阶段", "#808080", "保持现金", "数据不足以判断。", False

    g_now = float(growth_df["Value"].iloc[-1])
    g_prev = float(growth_df["Value"].iloc[-4])
    i_now = float(infl_df["Value"].iloc[-1])
    i_prev = float(infl_df["Value"].iloc[-4])

    obs_g = str(growth_df["Date"].iloc[-1])[:10]
    obs_i = str(infl_df["Date"].iloc[-1])[:10]

    iy_g_now = float(growth_df["YoY"].iloc[-1]) if "YoY" in growth_df.columns and not pd.isna(growth_df["YoY"].iloc[-1]) else float("nan")
    iy_g_prev = float(growth_df["YoY"].iloc[-4]) if "YoY" in growth_df.columns and not pd.isna(growth_df["YoY"].iloc[-4]) else float("nan")
    if pd.isna(iy_g_now):
        iy_g_now = 0.0
    if pd.isna(iy_g_prev):
        iy_g_prev = 0.0

    raw_json = _merrill_clock_llm_cached(
        round(g_now, 4),
        round(g_prev, 4),
        round(i_now, 2),
        round(i_prev, 2),
        round(iy_g_now, 2),
        round(iy_g_prev, 2),
        obs_g,
        obs_i,
    )

    rule_phase, rule_desc, rule_color, rule_assets = _clock_rule_based(g_now, g_prev, i_now, i_prev)

    if raw_json:
        data = _parse_llm_clock_json(raw_json)
        if data and isinstance(data.get("phase"), str) and data["phase"].strip():
            phase = data["phase"].strip()
            tagline = (data.get("tagline") or rule_desc).strip()
            assets = (data.get("assets") or rule_assets).strip()
            rationale = (data.get("rationale") or "").strip()
            chex = (data.get("color_hex") or "").strip()
            if re.match(r"^#[0-9A-Fa-f]{6}$", chex or ""):
                color = chex
            else:
                color = _phase_color_fallback(phase)
            note = f"（规则象限对照：{rule_phase}）"
            full_note = (rationale + " " + note).strip() if rationale else f"大模型判断；{note}"
            return phase, tagline, color, assets, full_note, True
        return (
            rule_phase,
            rule_desc,
            rule_color,
            rule_assets,
            "大模型已响应但 JSON 无法解析，已回退至简化四象限规则。",
            False,
        )

    return rule_phase, rule_desc, rule_color, rule_assets, "未配置 OPENAI_API_KEY 或模型调用失败，已使用 INDPRO×CPI 同比的简化四象限规则。", False

# ==========================================
# 5b. 美联储官员讲话 / 证词（官网 RSS，无额外 API Key）
# ==========================================
_FED_RSS_UA = {
    "User-Agent": "Mozilla/5.0 (compatible; MacroPulse/1.0; educational dashboard; +https://www.federalreserve.gov/feeds/)",
}
# 聚合：全体讲话 + 国会证词（含理事会与地区联储主席等，可用关键词筛选）
_FED_RSS_SPEECHES_ALL = "https://www.federalreserve.gov/feeds/speeches_and_testimony.xml"
# 理事会成员个人订阅源（与官网 feeds 页一致；人事变动时需对照 https://www.federalreserve.gov/feeds/feeds.htm 更新）
_FED_BOARD_RSS_FEEDS = [
    ("Jerome H. Powell", "https://www.federalreserve.gov/feeds/s_t_powell.xml"),
    ("Philip N. Jefferson", "https://www.federalreserve.gov/feeds/s_t_jefferson.xml"),
    ("Michelle W. Bowman", "https://www.federalreserve.gov/feeds/m_w_Bowman.xml"),
    ("Michael S. Barr", "https://www.federalreserve.gov/feeds/s_t_barr.xml"),
    ("Lisa D. Cook", "https://www.federalreserve.gov/feeds/s_t_cook.xml"),
    ("Stephen I. Miran", "https://www.federalreserve.gov/feeds/s_t_miran.xml"),
    ("Christopher J. Waller", "https://www.federalreserve.gov/feeds/s_t_waller.xml"),
]


def _strip_html(text: str) -> str:
    if not text:
        return ""
    t = re.sub(r"<[^>]+>", " ", text)
    return html.unescape(re.sub(r"\s+", " ", t)).strip()


def _entry_ts(entry) -> float:
    tt = entry.get("published_parsed") or entry.get("updated_parsed")
    if tt:
        try:
            return time.mktime(tt)
        except (OverflowError, ValueError):
            pass
    return 0.0


def _parse_feed_entries(parsed) -> list[dict]:
    rows = []
    for e in getattr(parsed, "entries", []) or []:
        link = (e.get("link") or "").strip()
        title = (e.get("title") or "（无标题）").strip()
        if not link:
            continue
        rows.append(
            {
                "title": title,
                "link": link,
                "ts": _entry_ts(e),
                "summary": _strip_html(e.get("summary", ""))[:400],
            }
        )
    return rows


@st.cache_data(ttl=1800)
def fetch_fed_official_speech_feeds() -> tuple[list[dict], str | None]:
    """
    并行拉取理事会成员 RSS，合并去重；若个人源均失败则回退到聚合源。
    返回 (条目列表按时间倒序, 错误说明或 None)。
    """
    merged: dict[str, dict] = {}
    errors: list[str] = []

    def load_one(name: str, url: str) -> tuple[list[dict], str | None]:
        out: list[dict] = []
        try:
            parsed = feedparser.parse(url, request_headers=_FED_RSS_UA)
            if getattr(parsed, "bozo_exception", None) and not parsed.entries:
                return out, f"{name}: 解析异常"
            for row in _parse_feed_entries(parsed):
                row = {**row, "speaker_hint": name}
                out.append(row)
            return out, None
        except Exception as ex:
            return out, f"{name}: {ex!s}"

    with ThreadPoolExecutor(max_workers=min(10, len(_FED_BOARD_RSS_FEEDS))) as ex:
        futs = [ex.submit(load_one, n, u) for n, u in _FED_BOARD_RSS_FEEDS]
        for f in futs:
            batch, err = f.result()
            if err:
                errors.append(err)
            for row in batch:
                if row["link"] not in merged:
                    merged[row["link"]] = row

    rows = sorted(merged.values(), key=lambda r: r["ts"], reverse=True)
    err_note = "; ".join(errors[:3]) if errors else None

    if len(rows) < 3:
        try:
            parsed = feedparser.parse(_FED_RSS_SPEECHES_ALL, request_headers=_FED_RSS_UA)
            fallback = _parse_feed_entries(parsed)
            seen = {r["link"] for r in rows}
            for row in sorted(fallback, key=lambda r: r["ts"], reverse=True):
                if row["link"] not in seen:
                    row["speaker_hint"] = "（聚合源）"
                    rows.append(row)
                    seen.add(row["link"])
            rows.sort(key=lambda r: r["ts"], reverse=True)
        except Exception as ex:
            err_note = (err_note + "; " if err_note else "") + f"聚合源失败: {ex!s}"

    return rows, err_note


# ==========================================
# 5c. 宏观事件倒计时（CPI / 非农 / FOMC，美东发布时刻）
# ==========================================
_ET = ZoneInfo("America/New_York")

# FOMC 声明显示日 + 美东 14:00（来源：federalreserve.gov 日程，遇调整请更新列表）
_FOMC_STATEMENT_LOCAL = [
    (2026, 1, 28), (2026, 3, 18), (2026, 4, 29), (2026, 6, 17), (2026, 7, 29),
    (2026, 9, 16), (2026, 10, 28), (2026, 12, 9),
    (2027, 1, 27), (2027, 3, 17), (2027, 4, 28), (2027, 6, 16), (2027, 7, 28),
    (2027, 9, 15), (2027, 11, 3), (2027, 12, 15),
]


def _add_month(y: int, m: int) -> tuple[int, int]:
    return (y + 1, 1) if m == 12 else (y, m + 1)


def _first_friday_of_month(year: int, month: int) -> date:
    d = date(year, month, 1)
    delta = (4 - d.weekday()) % 7
    return d + timedelta(days=delta)


def _second_tuesday_of_month(year: int, month: int) -> date:
    d = date(year, month, 1)
    delta = (1 - d.weekday()) % 7
    first_tue = d + timedelta(days=delta)
    return first_tue + timedelta(days=7)


def _next_nfp_et(now_utc: datetime) -> datetime | None:
    ref = now_utc.astimezone(_ET)
    y, m = ref.year, ref.month
    for _ in range(28):
        fd = _first_friday_of_month(y, m)
        t = datetime(fd.year, fd.month, fd.day, 8, 30, tzinfo=_ET)
        if t.astimezone(timezone.utc) > now_utc:
            return t
        y, m = _add_month(y, m)
    return None


def _next_cpi_et(now_utc: datetime) -> datetime | None:
    """BLS CPI 多在每月第二个周二 8:30 ET 公布（偶有调整，此为近似）。"""
    y, m = now_utc.astimezone(_ET).year, now_utc.astimezone(_ET).month
    for _ in range(28):
        st_d = _second_tuesday_of_month(y, m)
        t = datetime(st_d.year, st_d.month, st_d.day, 8, 30, tzinfo=_ET)
        if t.astimezone(timezone.utc) > now_utc:
            return t
        y, m = _add_month(y, m)
    return None


def _next_fomc_et(now_utc: datetime) -> datetime | None:
    for y, mo, d in _FOMC_STATEMENT_LOCAL:
        t = datetime(y, mo, d, 14, 0, tzinfo=_ET)
        if t.astimezone(timezone.utc) > now_utc:
            return t
    return None


def _format_countdown_remaining(rem: timedelta) -> str:
    if rem.total_seconds() <= 0:
        return "已到发布窗口"
    total = int(rem.total_seconds())
    days, r = divmod(total, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    if days > 0:
        return f"{days} 天 {h:02d}:{m:02d}:{s:02d}"
    return f"{h:02d}:{m:02d}:{s:02d}"


def _macro_countdown_strip_body():
    now_utc = datetime.now(timezone.utc)
    st.subheader("宏观事件倒计时")
    st.caption("以下倒计时按**本机系统时间**相对美东发布时刻实时计算；发布时间为美国东部 (ET)。")

    events = [
        ("美国 CPI", _next_cpi_et(now_utc), "BLS，通常第二个周二 8:30 ET（近似）"),
        ("非农就业 (NFP)", _next_nfp_et(now_utc), "BLS，当月首个周五 8:30 ET"),
        ("美联储利率决议 (FOMC)", _next_fomc_et(now_utc), "联储声明，当日约 14:00 ET"),
    ]

    c1, c2, c3 = st.columns(3)
    for col, (title, target_et, note) in zip((c1, c2, c3), events):
        with col:
            if target_et is None:
                st.markdown(
                    f'<div class="countdown-card"><p class="countdown-title">{title}</p>'
                    f'<p class="countdown-time">—</p><p class="countdown-meta">日程待更新</p></div>',
                    unsafe_allow_html=True,
                )
                continue
            rem = target_et.astimezone(timezone.utc) - now_utc
            urgent = timedelta(0) < rem < timedelta(hours=24)
            card_cls = "countdown-card countdown-urgent" if urgent else "countdown-card"
            when_et = target_et.strftime("%Y-%m-%d %H:%M")
            tz_abbr = target_et.tzname() or "ET"
            line = _format_countdown_remaining(rem)
            st.markdown(
                f'<div class="{card_cls}">'
                f'<p class="countdown-label">{note}</p>'
                f'<p class="countdown-title">{title}</p>'
                f'<p class="countdown-time">{line}</p>'
                f'<p class="countdown-meta">发布（美东）{when_et} {tz_abbr}</p>'
                f"</div>",
                unsafe_allow_html=True,
            )


# 1s 会高频触发 fragment 重绘；5s 仍足够「实时」且明显减轻压力
_macro_countdown_strip = (
    st.fragment(run_every=timedelta(seconds=5))(_macro_countdown_strip_body)
    if hasattr(st, "fragment")
    else _macro_countdown_strip_body
)

# ==========================================
# 6. UI 主体构建
# ==========================================
cols_head = st.columns([3, 1])
with cols_head[0]:
    st.title("🦅 US Macro Pulse 核心宏观数据终端")
    st.markdown(f"**数据更新时间:** {datetime.now().strftime('%Y-%m-%d')} | **来源:** Federal Reserve Economic Data")
with cols_head[1]:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 刷新缓存数据"):
        st.cache_data.clear()
        st.rerun()

st.markdown("---")

with st.spinner("正在加载首屏数据…"):
    warm_core_series_cache()

phase, desc, color, assets, clock_note, clock_used_llm = calculate_investment_clock()
st.markdown(f"""
<div class="clock-section">
    <div style="display: flex; justify-content: space-between; align-items: center;">
        <div>
            <h3 style="margin:0; color: {color} !important;">当前经济周期阶段：{phase}</h3>
            <p style="margin:5px 0 0 0; font-size: 1.2rem; opacity: 0.8;">{desc}</p>
        </div>
        <div style="text-align: right; background-color: rgba(255,255,255,0.1); padding: 15px; border-radius: 10px;">
            <p style="margin:0; font-size: 0.9rem; opacity: 0.7;">建议超配资产（示意）</p>
            <p style="margin:5px 0 0 0; font-size: 1.3rem; font-weight: bold;">{assets}</p>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)
if clock_used_llm:
    with st.expander("大模型综合判断依据", expanded=True):
        st.markdown(clock_note)
    st.caption("美林阶段由大模型结合 INDPRO 与 CPI 同比等综合给出；与简化四象限不一致时以模型解释为准。非投资建议。")

_macro_countdown_strip()

st.subheader("核心指标动态 (最新数值与近期边际变化)")
top_metrics = {
    "核心CPI (通胀 YoY)": "CPILFESL",
    "失业率 (就业)": "UNRATE",
    "联邦基金利率 (政策)": "FEDFUNDS",
    "工业生产 (增长)": "INDPRO",
    "半导体产出 (AI代理)": "IPG3344N"
}

cols_metrics = st.columns(5)
for i, (name, series_id) in enumerate(top_metrics.items()):
    df = fetch_data_advanced(series_id)
    with cols_metrics[i]:
        if not df.empty:
            latest = df.iloc[-1]
            unit = UNIT_MAP.get(series_id, "")

            # 动态调整显示逻辑和颜色翻转
            if series_id in ["UNRATE", "FEDFUNDS"]:
                val_display = f"{latest['Value']:.2f}{unit}"
                delta_val = f"{latest['Value_Diff']:.2f} pts"
                d_color = "inverse"
            elif series_id in PC1_SERIES:
                val_display = f"{latest['Value']:.2f}%"
                delta_val = f"{latest['Value_Diff']:.2f} pts"
                d_color = "inverse"
            else:
                val_display = f"{latest['Value']:.2f}{unit}"
                delta_val = f"{latest['YoY']:.1f}% YoY"
                d_color = "normal"

            st.markdown(f"""
            <div class="metric-card">
                <p style="color: #8b949e; margin: 0; font-size: 0.9rem;">{name}</p>
                <p style="color: white; margin: 5px 0; font-size: 2.2rem; font-weight: 700;">{val_display}</p>
            </div>
            """, unsafe_allow_html=True)
            st.metric(label="", value="", delta=delta_val, delta_color=d_color)

st.markdown("---")
st.subheader("美联储理事会 · 最新讲话与国会证词")
st.caption(
    "来源：联邦储备委员会官网 RSS（每位理事独立订阅源并行合并；条目过少时自动并入「全体讲话+证词」聚合源）。"
    "缓存约 30 分钟，与 FRED 数据无关、不占用 FRED API 次数。人事变动时请对照 "
    "[联储 RSS 列表](https://www.federalreserve.gov/feeds/feeds.htm) 更新代码中的订阅地址。"
)

_fed_rows, _fed_err = fetch_fed_official_speech_feeds()
if _fed_err:
    st.caption(f"拉取提示：{_fed_err}")

_fc1, _fc2 = st.columns([2, 1])
with _fc2:
    _only_personal = st.checkbox("仅理事个人 RSS", value=True, help="勾选时隐藏聚合源补充的地区联储主席等条目")
    _speech_q = st.text_input("标题关键词筛选", "", placeholder="例：Powell、Inflation、Hearing", key="fed_speech_q")

with _fc1:
    if not _fed_rows:
        st.warning("暂未能拉取联储 RSS，请检查网络或稍后在「刷新缓存数据」后重试。")
    else:
        _filtered_fed: list[dict] = []
        for _row in _fed_rows:
            if _only_personal and _row.get("speaker_hint") == "（聚合源）":
                continue
            if _speech_q and _speech_q.lower() not in _row["title"].lower():
                continue
            _filtered_fed.append(_row)

        def _render_fed_row(_row: dict) -> None:
            _ts = _row["ts"]
            if _ts > 0:
                _dstr = datetime.fromtimestamp(_ts, tz=timezone.utc).strftime("%Y-%m-%d UTC")
            else:
                _dstr = "日期未知"
            st.markdown(
                f"**{_dstr}** · `{_row.get('speaker_hint', '')}`  \n"
                f"[{_row['title']}]({_row['link']})"
            )
            if _row.get("summary"):
                st.caption(_row["summary"][:280] + ("…" if len(_row["summary"]) > 280 else ""))
            st.markdown("")

        if not _filtered_fed:
            st.info("当前筛选条件下没有条目，可放宽关键词或取消「仅理事个人 RSS」。")
        else:
            st.caption("首页展示最新 5 条，更多条目请展开下方区域。")
            for _row in _filtered_fed[:5]:
                _render_fed_row(_row)
            _more = _filtered_fed[5:]
            if _more:
                with st.expander(f"查看更多（另 {len(_more)} 条）", expanded=False):
                    for _row in _more:
                        _render_fed_row(_row)

st.markdown("<br>", unsafe_allow_html=True)

st.subheader("全量指标交互式趋势图 (过去 5 年)")
# 注意：st.tabs 会在每次运行时执行「所有」页签内代码，导致几十次串行 FRED 请求。
# 改为单选分类 + 仅加载当前类，并并行拉取该类下全部序列。
_cat_keys = list(FRED_CATEGORIES.keys())
_selected_cat = st.radio("指标分类", _cat_keys, horizontal=True, key="fred_chart_category")
st.caption("切换分类时仅请求当前组数据；已请求过的序列由缓存复用。")

with st.spinner("正在加载本类图表数据…"):
    _category_dfs = load_category_parallel(_selected_cat)

metrics_dict = FRED_CATEGORIES[_selected_cat]
chart_cols = st.columns(2)

for idx, (metric_name, series_id) in enumerate(metrics_dict.items()):
    df = _category_dfs.get(series_id, pd.DataFrame())
    if not df.empty:
        if series_id in PC1_SERIES or _selected_cat in ["增长 (Growth)", "AI代理指标 (AI Proxies)"]:
            y_col = 'YoY'
            title_suffix = "(同比 YoY %)"
            unit_str = "%"
            chart_type = 'bar'
        else:
            y_col = 'Value'
            title_suffix = f"({UNIT_MAP.get(series_id, '').strip()})" if UNIT_MAP.get(series_id) else ""
            unit_str = UNIT_MAP.get(series_id, "")
            chart_type = 'area'

        fig = go.Figure()

        if chart_type == 'bar':
            fig.add_trace(go.Bar(
                x=df['Date'], y=df[y_col],
                marker_color='#58a6ff',
                name=metric_name,
                hovertemplate=f'%{{x}}<br><b>%{{y:.2f}}{unit_str}</b><extra></extra>'
            ))
        else:
            fig.add_trace(go.Scatter(
                x=df['Date'], y=df[y_col],
                mode='lines',
                line=dict(width=2, color='#58a6ff'),
                fill='tozeroy',
                fillcolor='rgba(88, 166, 255, 0.1)',
                name=metric_name,
                hovertemplate=f'%{{x}}<br><b>%{{y:.2f}}{unit_str}</b><extra></extra>'
            ))

        fig.update_layout(
            title=f"<b>{metric_name} {title_suffix}</b>",
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=10, r=10, t=40, b=10),
            hovermode="x unified",
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='#30363d', zerolinecolor='#30363d')
        )

        chart_cols[idx % 2].plotly_chart(
            fig,
            use_container_width=True,
            config={"displaylogo": False, "modeBarButtonsToRemove": ["lasso2d", "select2d"]},
        )
    else:
        chart_cols[idx % 2].warning(f"{metric_name} 数据获取失败。")
