# -*- coding: utf-8 -*-
"""
Macro Track Reportthinking | 宏观经济终端
完整可运行版：FRED 数据终端 + 动态缓存 + Macro Nowcast + PCA-HMM 投资时钟

运行方式：
1) pip install -r requirements.txt
2) 在项目根目录创建 .streamlit/secrets.toml：
   FRED_API_KEY = "你的 FRED API KEY"
3) streamlit run macro_track_app.py
"""

import html
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import feedparser
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dateutil.relativedelta import relativedelta
from fredapi import Fred

# =====================================================
# 1. 页面配置与 CSS
# =====================================================
st.set_page_config(
    page_title="Macro Track Reportthinking | 宏观经济终端",
    page_icon="🌿",
    layout="wide",
)

st.markdown(
    """
<style>
.stApp {background: linear-gradient(160deg,#f0f7f4 0%,#e8f4f0 45%,#f7f3ee 100%); color:#2d4a3e;}
.block-container {padding-left:1rem!important; padding-right:1rem!important; padding-top:1.2rem!important; max-width:1450px;}
h1,h2,h3,h4 {color:#2d4a3e!important;}
section[data-testid="stSidebar"] {background:#e8f2ee!important; border-right:1px solid #c8e0d8;}
hr {border:none; border-top:1px solid #d0e8df!important; margin:18px 0!important;}
.fresh-card,.metric-card,.countdown-card,.clock-card,.assets-card {
    background:rgba(255,255,255,.78); border:1px solid #d0e8df; border-radius:14px;
    box-shadow:0 2px 10px rgba(60,120,90,.07); padding:16px; margin-bottom:8px;
}
.clock-card {background:linear-gradient(135deg,rgba(255,255,255,.90),rgba(232,244,240,.95)); border:1.5px solid #b8d8ce; border-radius:18px; padding:24px;}
.clock-phase-title {font-size:clamp(1.2rem,4vw,1.8rem); font-weight:800; margin:0;}
.clock-desc {font-size:clamp(.85rem,2.5vw,1rem); color:#5a8a7a; margin:6px 0 0;}
.metric-card {text-align:center; height:100%; transition:transform .2s;}
.metric-card:hover {transform:translateY(-3px)}
.metric-label {color:#6a9e8e; font-size:.76rem; font-weight:700; letter-spacing:.3px; text-transform:uppercase; margin:0 0 4px;}
.metric-name {color:#7aad98; font-size:.75rem; margin:0 0 4px;}
.metric-value {color:#1f3d30; font-size:clamp(1.25rem,4vw,2rem); font-weight:800; margin:0; line-height:1.15;}
.metric-delta {font-size:.76rem; margin:4px 0 0;}
.ml-badge {display:inline-block; background:linear-gradient(135deg,#e8f4f0,#d4ece4); border:1px solid #a8d4c4; border-radius:8px; padding:2px 10px; font-size:.75rem; color:#3a6b5a; font-weight:700;}
.countdown-label {color:#8aad9e; font-size:.72rem; margin:0 0 3px;}
.countdown-title {color:#2d4a3e; font-size:.95rem; font-weight:800; margin:0 0 6px;}
.countdown-time {color:#3a8a6e; font-size:1.25rem; font-weight:800; font-variant-numeric:tabular-nums; margin:0;}
.countdown-meta {color:#8aad9e; font-size:.70rem; margin:2px 0 0;}
@keyframes gentle-pulse {0%,100%{box-shadow:0 2px 8px rgba(229,115,80,.12);border-color:rgba(229,115,80,.5)}50%{box-shadow:0 4px 20px rgba(229,115,80,.28);border-color:rgba(229,115,80,.75)}}
.countdown-card.urgent {animation:gentle-pulse 2.5s ease-in-out infinite;}
.stButton > button {background:linear-gradient(135deg,#3a8a6e,#5aaa8a)!important; color:white!important; border:none!important; border-radius:10px!important; font-weight:700!important;}
.stTextInput input, input, textarea, select {background-color:rgba(255,255,255,.90)!important; color:#2d4a3e!important;}
@media(max-width:640px){.block-container{padding-left:.6rem!important;padding-right:.6rem!important}.metric-value{font-size:1.15rem!important}.countdown-time{font-size:1.05rem!important}}
</style>
""",
    unsafe_allow_html=True,
)

# =====================================================
# 2. FRED API 初始化：不要硬编码 API Key
# =====================================================
@st.cache_resource
def get_fred_client() -> Fred:
    key = ""
    try:
        key = st.secrets.get("FRED_API_KEY", "")
    except Exception:
        key = ""
    if not key:
        st.warning("请在 `.streamlit/secrets.toml` 中配置 `FRED_API_KEY`，否则无法稳定拉取 FRED 数据。")
    return Fred(api_key=key)

fred = get_fred_client()

# =====================================================
# 3. 指标字典与序列元数据
# =====================================================
FRED_CATEGORIES = {
    "增长 (Growth)": {
        "实际GDP (GDPC1)": "GDPC1",
        "GDP环比增速 (A191RL1Q225SBEA)": "A191RL1Q225SBEA",
        "工业生产指数 (INDPRO)": "INDPRO",
        "零售销售 (RSAFS)": "RSAFS",
        "个人消费支出 (PCE)": "PCE",
        "耐用品订单 (DGORDER)": "DGORDER",
        "建筑许可 (PERMIT)": "PERMIT",
        "新屋开工 (HOUST)": "HOUST",
        "领先经济指标 (USSLIND)": "USSLIND",
        "芝加哥联储活动指数 (CFNAI)": "CFNAI",
        "消费者信心指数 (UMCSENT)": "UMCSENT",
    },
    "通胀 (Inflation)": {
        "CPI总体 (CPIAUCSL)": "CPIAUCSL",
        "核心CPI (CPILFESL)": "CPILFESL",
        "CPI住房分项 (CUSR0000SAH1)": "CUSR0000SAH1",
        "PCE总体 (PCEPI)": "PCEPI",
        "核心PCE (PCEPILFE)": "PCEPILFE",
        "PPI总体 (PPIACO)": "PPIACO",
        "PPI最终需求 (PPIFID)": "PPIFID",
        "密歇根通胀预期1Y (MICH)": "MICH",
        "5年盈亏平衡通胀率 (T5YIE)": "T5YIE",
        "10年盈亏平衡通胀率 (T10YIE)": "T10YIE",
    },
    "就业 (Employment)": {
        "失业率 (UNRATE)": "UNRATE",
        "非农就业新增 (PAYEMS)": "PAYEMS",
        "私人非农就业 (USPRIV)": "USPRIV",
        "劳动力参与率 (CIVPART)": "CIVPART",
        "就业人口比 (EMRATIO)": "EMRATIO",
        "平均时薪 (CES0500000003)": "CES0500000003",
        "平均每周工时 (AWHAETP)": "AWHAETP",
        "初请失业金 (ICSA)": "ICSA",
        "续请失业金 (CCSA)": "CCSA",
        "职位空缺 JOLTS (JTSJOL)": "JTSJOL",
    },
    "利率 (Interest Rates)": {
        "联邦基金利率 (FEDFUNDS)": "FEDFUNDS",
        "有效联邦基金利率/日 (DFF)": "DFF",
        "超额准备金利率 (IORB)": "IORB",
        "2年期美债 (DGS2)": "DGS2",
        "10年期美债 (DGS10)": "DGS10",
        "10Y-2Y利差 (T10Y2Y)": "T10Y2Y",
        "10Y-3M利差 (T10Y3M)": "T10Y3M",
        "30年期抵押贷款利率 (MORTGAGE30US)": "MORTGAGE30US",
        "BAA企业债收益率 (BAA)": "BAA",
        "高收益债利差OAS (BAMLH0A0HYM2)": "BAMLH0A0HYM2",
        "投资级债利差OAS (BAMLC0A0CM)": "BAMLC0A0CM",
        "10年期TIPS实际利率 (DFII10)": "DFII10",
    },
    "流动性 (Liquidity)": {
        "M2货币供应 (M2SL)": "M2SL",
        "M1货币供应 (M1SL)": "M1SL",
        "美联储资产负债表 (WALCL)": "WALCL",
        "美联储持有国债 (TREAST)": "TREAST",
        "隔夜逆回购余额 (RRPONTSYD)": "RRPONTSYD",
        "银行准备金余额 (WRESBAL)": "WRESBAL",
        "芝加哥联储金融条件 (NFCI)": "NFCI",
        "圣路易斯金融压力 (STLFSI4)": "STLFSI4",
        "TED利差 (TEDRATE)": "TEDRATE",
        "SOFR隔夜融资利率 (SOFR)": "SOFR",
        "贸易加权美元指数 (DTWEXBGS)": "DTWEXBGS",
    },
    "AI代理指标 (AI Proxies)": {
        "半导体工业生产 (IPG3344N)": "IPG3344N",
        "半导体价格PPI (PCU33443344)": "PCU33443344",
    },
}

SERIES_META = {
    # Growth
    "GDPC1": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "实际GDP 同比 %"},
    "A191RL1Q225SBEA": {"units": "lin", "display": "value", "chart": "bar_abs", "unit_str": "%", "label": "GDP环比增速（季度年化 %）"},
    "INDPRO": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "工业生产 同比 %"},
    "RSAFS": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "零售销售 同比 %"},
    "PCE": {"units": "pc1", "display": "value", "chart": "line_yoy", "unit_str": "%", "label": "个人消费 同比 %"},
    "DGORDER": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "耐用品订单 同比 %"},
    "PERMIT": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "建筑许可 同比 %"},
    "HOUST": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "新屋开工 同比 %"},
    "USSLIND": {"units": "lin", "display": "value", "chart": "line", "unit_str": "", "label": "领先指标（指数）"},
    "CFNAI": {"units": "lin", "display": "value", "chart": "bar_abs", "unit_str": "", "label": "芝加哥联储活动指数"},
    "UMCSENT": {"units": "lin", "display": "value", "chart": "line", "unit_str": "", "label": "密歇根消费者信心"},
    # Inflation
    "CPIAUCSL": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "CPI总体 同比 %"},
    "CPILFESL": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "核心CPI 同比 %"},
    "CUSR0000SAH1": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "CPI住房 同比 %"},
    "PCEPI": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "PCE总体 同比 %"},
    "PCEPILFE": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "核心PCE 同比 %"},
    "PPIACO": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "PPI总体 同比 %"},
    "PPIFID": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "PPI最终需求 同比 %"},
    "MICH": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "密歇根通胀预期 1Y %"},
    "T5YIE": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "5年盈亏平衡通胀率 %"},
    "T10YIE": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "10年盈亏平衡通胀率 %"},
    # Employment
    "UNRATE": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "失业率 %"},
    "PAYEMS": {"units": "lin", "display": "mom_diff", "chart": "bar_abs", "unit_str": " 千人", "label": "非农就业 月增（千人）"},
    "USPRIV": {"units": "lin", "display": "mom_diff", "chart": "bar_abs", "unit_str": " 千人", "label": "私人非农 月增（千人）"},
    "CIVPART": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "劳动力参与率 %"},
    "EMRATIO": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "就业人口比 %"},
    "CES0500000003": {"units": "pc1", "display": "value", "chart": "line_yoy", "unit_str": "%", "label": "平均时薪 同比 %"},
    "AWHAETP": {"units": "lin", "display": "value", "chart": "line", "unit_str": " 小时", "label": "平均每周工时"},
    "ICSA": {"units": "lin", "display": "value", "chart": "line", "unit_str": " 人", "label": "初请失业金（人）"},
    "CCSA": {"units": "lin", "display": "value", "chart": "line", "unit_str": " 人", "label": "续请失业金（人）"},
    "JTSJOL": {"units": "lin", "display": "value", "chart": "line", "unit_str": " 千人", "label": "职位空缺 JOLTS（千人）"},
    # Rates
    "FEDFUNDS": {"units": "lin", "display": "value", "chart": "step", "unit_str": "%", "label": "联邦基金利率 %"},
    "DFF": {"units": "lin", "display": "value", "chart": "step", "unit_str": "%", "label": "有效联邦基金利率 %"},
    "IORB": {"units": "lin", "display": "value", "chart": "step", "unit_str": "%", "label": "准备金利率 IORB %"},
    "DGS2": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "2年期美债 %"},
    "DGS10": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "10年期美债 %"},
    "T10Y2Y": {"units": "lin", "display": "value", "chart": "spread", "unit_str": "%", "label": "10Y-2Y利差 %"},
    "T10Y3M": {"units": "lin", "display": "value", "chart": "spread", "unit_str": "%", "label": "10Y-3M利差 %"},
    "MORTGAGE30US": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "30年抵押贷款利率 %"},
    "BAA": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "BAA企业债收益率 %"},
    "BAMLH0A0HYM2": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "高收益债利差 OAS %"},
    "BAMLC0A0CM": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "投资级债利差 OAS %"},
    "DFII10": {"units": "lin", "display": "value", "chart": "spread", "unit_str": "%", "label": "10年TIPS实际利率 %"},
    # Liquidity
    "M2SL": {"units": "pc1", "display": "value", "chart": "line_yoy", "unit_str": "%", "label": "M2 同比 %"},
    "M1SL": {"units": "pc1", "display": "value", "chart": "line_yoy", "unit_str": "%", "label": "M1 同比 %"},
    "WALCL": {"units": "lin", "display": "value", "chart": "line", "unit_str": " 百万$", "label": "美联储资产负债表（百万$）"},
    "TREAST": {"units": "lin", "display": "value", "chart": "line", "unit_str": " 百万$", "label": "联储持有国债（百万$）"},
    "RRPONTSYD": {"units": "lin", "display": "value", "chart": "line", "unit_str": " 十亿$", "label": "隔夜逆回购余额（十亿$）"},
    "WRESBAL": {"units": "lin", "display": "value", "chart": "line", "unit_str": " 十亿$", "label": "银行准备金（十亿$）"},
    "NFCI": {"units": "lin", "display": "value", "chart": "spread", "unit_str": "", "label": "芝加哥金融条件指数"},
    "STLFSI4": {"units": "lin", "display": "value", "chart": "spread", "unit_str": "", "label": "圣路易斯金融压力指数"},
    "TEDRATE": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "TED利差 %"},
    "SOFR": {"units": "lin", "display": "value", "chart": "line", "unit_str": "%", "label": "SOFR %"},
    "DTWEXBGS": {"units": "lin", "display": "value", "chart": "line", "unit_str": "", "label": "贸易加权美元指数"},
    # AI proxy
    "IPG3344N": {"units": "pc1", "display": "value", "chart": "line_yoy", "unit_str": "%", "label": "半导体工业生产 同比 %"},
    "PCU33443344": {"units": "pc1", "display": "value", "chart": "bar_yoy", "unit_str": "%", "label": "半导体PPI 同比 %"},
}

PC1_SERIES = {sid for sid, m in SERIES_META.items() if m["units"] == "pc1"}
MOM_DIFF_SERIES = {sid for sid, m in SERIES_META.items() if m["display"] == "mom_diff"}
UNIT_MAP = {sid: m["unit_str"] for sid, m in SERIES_META.items()}
CHART_TYPE = {sid: m["chart"] for sid, m in SERIES_META.items()}

# =====================================================
# 4. 数据拉取：动态缓存 + 元数据 + 衍生列
# =====================================================
_ET = ZoneInfo("America/New_York")

SERIES_REFRESH_SECONDS = {
    "DFF": 15 * 60, "DGS2": 15 * 60, "DGS10": 15 * 60,
    "T10Y2Y": 15 * 60, "T10Y3M": 15 * 60, "DFII10": 15 * 60,
    "T5YIE": 15 * 60, "T10YIE": 15 * 60, "SOFR": 15 * 60,
    "BAMLH0A0HYM2": 30 * 60, "BAMLC0A0CM": 30 * 60,
    "NFCI": 30 * 60, "STLFSI4": 30 * 60,
    "RRPONTSYD": 30 * 60, "WRESBAL": 30 * 60,
    "ICSA": 30 * 60, "CCSA": 30 * 60,
    "WALCL": 60 * 60, "TREAST": 60 * 60, "MORTGAGE30US": 60 * 60,
    "CPIAUCSL": 2 * 3600, "CPILFESL": 2 * 3600,
    "PCEPI": 2 * 3600, "PCEPILFE": 2 * 3600,
    "PAYEMS": 2 * 3600, "USPRIV": 2 * 3600, "UNRATE": 2 * 3600,
    "INDPRO": 2 * 3600, "RSAFS": 2 * 3600, "UMCSENT": 2 * 3600,
    "MICH": 2 * 3600, "M2SL": 6 * 3600, "M1SL": 6 * 3600,
    "GDPC1": 12 * 3600, "A191RL1Q225SBEA": 12 * 3600,
}


def _now_et_naive() -> datetime:
    return datetime.now(_ET).replace(tzinfo=None)


def _series_cache_bucket(series_id: str) -> int:
    ttl = SERIES_REFRESH_SECONDS.get(series_id, 6 * 3600)
    return int(time.time() // ttl)


@st.cache_data(ttl=24 * 3600, show_spinner=False)
def _fetch_data_cached(series_id: str, years: int, cache_bucket: int) -> pd.DataFrame:
    meta = SERIES_META.get(series_id, {"units": "lin", "display": "value"})
    req_units = meta.get("units", "lin")
    display = meta.get("display", "value")
    today = _now_et_naive()
    start_date = today - relativedelta(years=years + 3)

    try:
        try:
            info = dict(fred.get_series_info(series_id))
        except Exception:
            info = {}

        data = fred.get_series(series_id, observation_start=start_date, units=req_units)
        if data is None or data.empty:
            return pd.DataFrame()

        df = pd.DataFrame({"Date": pd.to_datetime(data.index), "Value": pd.to_numeric(data.values, errors="coerce")})
        df = df.dropna(subset=["Value"]).sort_values("Date").drop_duplicates("Date", keep="last").reset_index(drop=True)
        if df.empty:
            return pd.DataFrame()

        if display == "mom_diff":
            df["Value_Diff"] = df["Value"].diff(1)
            df["YoY"] = df["Value_Diff"]
            df["Value_3MAvg"] = df["Value_Diff"].rolling(3, min_periods=1).mean()
        elif req_units == "pc1":
            df["YoY"] = df["Value"]
            df["Value_Diff"] = df["Value"].diff(1)
            df["Value_3MAvg"] = df["Value"].rolling(3, min_periods=1).mean()
        else:
            df["YoY"] = df["Value"].pct_change(12) * 100
            df["Value_Diff"] = df["Value"].diff(1)
            df["Value_3MAvg"] = df["Value"].rolling(3, min_periods=1).mean()

        df["Series_ID"] = series_id
        df["Last_Updated"] = str(info.get("last_updated", ""))
        df["Observation_End"] = str(info.get("observation_end", ""))
        df["Frequency"] = str(info.get("frequency_short", info.get("frequency", "")))

        display_start = today - relativedelta(years=years)
        return df[df["Date"] >= display_start].reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def fetch_data_advanced(series_id: str, years: int = 6) -> pd.DataFrame:
    return _fetch_data_cached(series_id, int(years), _series_cache_bucket(series_id))


def warm_core_series_cache() -> None:
    core = tuple(dict.fromkeys([
        "INDPRO", "RSAFS", "CFNAI", "CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE",
        "PAYEMS", "UNRATE", "ICSA", "CCSA", "JTSJOL", "FEDFUNDS", "DGS10", "DGS2",
        "T10Y2Y", "T10Y3M", "T5YIE", "T10YIE", "BAMLH0A0HYM2", "BAMLC0A0CM",
        "NFCI", "STLFSI4", "M2SL", "WALCL", "RRPONTSYD", "WRESBAL", "IPG3344N",
    ]))
    with ThreadPoolExecutor(max_workers=min(12, len(core))) as ex:
        futures = [ex.submit(fetch_data_advanced, sid, 10) for sid in core]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                pass


def load_category_parallel(tab_name: str, years: int = 6) -> dict:
    ids = list(FRED_CATEGORIES[tab_name].values())
    out = {}
    with ThreadPoolExecutor(max_workers=min(12, len(ids))) as ex:
        fut_to_sid = {ex.submit(fetch_data_advanced, sid, years): sid for sid in ids}
        for fut in as_completed(fut_to_sid):
            sid = fut_to_sid[fut]
            try:
                out[sid] = fut.result()
            except Exception:
                out[sid] = pd.DataFrame()
    return out

# =====================================================
# 5. Macro Nowcast + PCA-HMM 投资时钟
# =====================================================
_CLOCK_FEATURES = {
    "INDPRO": {"col": "YoY", "years": 15, "agg": "last", "group": "growth", "sign": 1},
    "RSAFS": {"col": "YoY", "years": 15, "agg": "last", "group": "growth", "sign": 1},
    "CFNAI": {"col": "Value", "years": 15, "agg": "last", "group": "growth", "sign": 1},
    "UMCSENT": {"col": "Value", "years": 15, "agg": "last", "group": "growth", "sign": 1},
    "CPIAUCSL": {"col": "Value", "years": 15, "agg": "last", "group": "inflation", "sign": 1},
    "CPILFESL": {"col": "Value", "years": 15, "agg": "last", "group": "inflation", "sign": 1},
    "PCEPI": {"col": "Value", "years": 15, "agg": "last", "group": "inflation", "sign": 1},
    "PCEPILFE": {"col": "Value", "years": 15, "agg": "last", "group": "inflation", "sign": 1},
    "T10YIE": {"col": "Value", "years": 15, "agg": "last", "group": "inflation", "sign": 1},
    "PAYEMS": {"col": "Value_3MAvg", "years": 15, "agg": "last", "group": "employment", "sign": 1},
    "UNRATE": {"col": "Value", "years": 15, "agg": "last", "group": "employment", "sign": -1},
    "ICSA": {"col": "Value_3MAvg", "years": 15, "agg": "last", "group": "employment", "sign": -1},
    "JTSJOL": {"col": "Value", "years": 15, "agg": "last", "group": "employment", "sign": 1},
    "T10Y3M": {"col": "Value", "years": 15, "agg": "last", "group": "financial", "sign": 1},
    "T10Y2Y": {"col": "Value", "years": 15, "agg": "last", "group": "financial", "sign": 1},
    "BAMLH0A0HYM2": {"col": "Value", "years": 15, "agg": "last", "group": "financial", "sign": -1},
    "NFCI": {"col": "Value", "years": 15, "agg": "last", "group": "financial", "sign": -1},
    "FEDFUNDS": {"col": "Value", "years": 15, "agg": "last", "group": "policy", "sign": -1},
    "M2SL": {"col": "Value", "years": 15, "agg": "last", "group": "liquidity", "sign": 1},
    "WALCL": {"col": "YoY", "years": 15, "agg": "last", "group": "liquidity", "sign": 1},
}


def _to_monthly_series(df: pd.DataFrame, col: str, agg: str = "last") -> pd.Series:
    if df is None or df.empty or col not in df.columns:
        return pd.Series(dtype=float)
    s = df.set_index("Date")[col].copy()
    s.index = pd.to_datetime(s.index)
    s = pd.to_numeric(s, errors="coerce").dropna().sort_index()
    if s.empty:
        return pd.Series(dtype=float)
    if agg == "mean":
        return s.resample("MS").mean()
    if agg == "sum":
        return s.resample("MS").sum()
    return s.resample("MS").last()


def _robust_zscore(df: pd.DataFrame, clip: float = 3.0) -> pd.DataFrame:
    med = df.median(axis=0)
    mad = (df - med).abs().median(axis=0).replace(0, np.nan)
    z = 0.6745 * (df - med) / mad
    return z.replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(-clip, clip)


def _bic_hmm(model, X: np.ndarray) -> float:
    n, p = X.shape
    k = model.n_components
    log_likelihood = model.score(X)  # score 已经是全样本 log-likelihood，不能再乘 n
    if model.covariance_type == "diag":
        cov_params = k * p
    elif model.covariance_type == "full":
        cov_params = k * p * (p + 1) / 2
    else:
        cov_params = k * p
    n_params = (k - 1) + k * (k - 1) + k * p + cov_params
    return -2 * log_likelihood + n_params * np.log(n)


def _composite_profiles(z: pd.DataFrame) -> dict:
    profiles = {}
    for group in ["growth", "inflation", "employment", "financial", "policy", "liquidity"]:
        cols = [sid for sid, cfg in _CLOCK_FEATURES.items() if cfg["group"] == group and sid in z.columns]
        if not cols:
            profiles[group] = 0.0
            continue
        signs = pd.Series({sid: _CLOCK_FEATURES[sid]["sign"] for sid in cols})
        profiles[group] = float((z.iloc[-1][cols] * signs).mean())
    profiles["stress"] = -profiles.get("financial", 0.0)
    profiles["demand"] = float(np.nanmean([profiles.get("growth", 0.0), profiles.get("employment", 0.0)]))
    return profiles


def _sahm_indicator(unrate: pd.Series) -> float:
    u = pd.to_numeric(unrate, errors="coerce").dropna()
    if len(u) < 15:
        return np.nan
    return float(u.rolling(3).mean().iloc[-1] - u.rolling(12).min().iloc[-1])


def _phase_from_composites(prof: dict, latest_raw: pd.Series):
    demand = prof.get("demand", 0.0)
    inflation = prof.get("inflation", 0.0)
    stress = prof.get("stress", 0.0)

    cpi = float(latest_raw.get("CPIAUCSL", np.nan))
    core_cpi = float(latest_raw.get("CPILFESL", np.nan))
    curve_3m = float(latest_raw.get("T10Y3M", np.nan))
    hy_oas = float(latest_raw.get("BAMLH0A0HYM2", np.nan))

    inflation_high = (inflation > 0.25) or (np.nanmax([cpi, core_cpi]) >= 2.7)
    inflation_cooling = (inflation < -0.15) and (np.nanmax([cpi, core_cpi]) < 2.7)
    curve_inverted = np.isfinite(curve_3m) and curve_3m < 0
    credit_stress = (stress > 0.75) or (np.isfinite(hy_oas) and hy_oas > 5.0)
    demand_weak = demand < -0.30
    demand_strong = demand > 0.25

    if demand_weak and credit_stress:
        return "衰退 (Recession)", "🥶 需求转弱 / 信用与金融压力上升", "#6a9fd8", "长久期债券 > 现金 > 防御股"
    if demand_weak and inflation_high:
        return "滞胀 (Stagflation)", "☁️ 增长放缓 / 通胀仍有粘性", "#f2a65a", "现金 > 黄金/大宗 > 短债"
    if demand_strong and inflation_high:
        return "过热 (Overheat)", "🔥 需求偏强 / 通胀压力偏高", "#e07a5f", "大宗商品 > 价值股 > 现金"
    if demand_strong and (inflation_cooling or not inflation_high):
        return "复苏/扩张 (Recovery)", "📈 增长改善 / 通胀相对温和", "#4caf8a", "股票 > 信用债 > 商品"
    if curve_inverted and not demand_strong:
        return "放缓/软着陆 (Slowdown)", "🛬 曲线倒挂 / 增长动能趋缓", "#5abcb0", "高等级债券 > 红利/防御 > 现金"
    return "均衡震荡 (Neutral)", "🌿 增长与通胀信号均衡，等待方向确认", "#3a8a6e", "均衡配置：股债商品分散"


@st.cache_data(ttl=30 * 60, show_spinner=False)
def calculate_ml_investment_clock():
    try:
        from hmmlearn.hmm import GaussianHMM
        from sklearn.decomposition import PCA
    except Exception:
        GaussianHMM = None
        PCA = None

    raw = {}
    sids = list(_CLOCK_FEATURES.keys())
    with ThreadPoolExecutor(max_workers=min(12, len(sids))) as ex:
        fut = {ex.submit(fetch_data_advanced, sid, _CLOCK_FEATURES[sid]["years"]): sid for sid in sids}
        for f in as_completed(fut):
            sid = fut[f]
            try:
                raw[sid] = f.result()
            except Exception:
                raw[sid] = pd.DataFrame()

    series_list = []
    for sid, cfg in _CLOCK_FEATURES.items():
        s = _to_monthly_series(raw.get(sid, pd.DataFrame()), cfg["col"], cfg.get("agg", "last"))
        if not s.empty:
            s.name = sid
            series_list.append(s)

    if len(series_list) < 8:
        return _fallback_rule_clock(raw.get("INDPRO", pd.DataFrame()), raw.get("CPIAUCSL", pd.DataFrame()))

    panel = pd.concat(series_list, axis=1).sort_index()
    panel = panel.loc[panel.index >= (panel.index.max() - pd.DateOffset(years=12))]
    panel = panel.loc[panel.notna().mean(axis=1) >= 0.65].ffill(limit=3)
    panel = panel.loc[:, panel.notna().mean(axis=0) >= 0.75].dropna()

    if len(panel) < 60:
        return _fallback_rule_clock(raw.get("INDPRO", pd.DataFrame()), raw.get("CPIAUCSL", pd.DataFrame()))

    if "UNRATE" in panel.columns:
        panel["UNRATE_MOM"] = panel["UNRATE"].diff(1)
    if "T10Y3M" in panel.columns:
        panel["T10Y3M_3M_CHG"] = panel["T10Y3M"].diff(3)
    if "BAMLH0A0HYM2" in panel.columns:
        panel["HY_OAS_3M_CHG"] = panel["BAMLH0A0HYM2"].diff(3)
    panel = panel.dropna()

    z = _robust_zscore(panel)
    latest_raw = panel.iloc[-1]
    base_cols = [c for c in _CLOCK_FEATURES.keys() if c in z.columns]
    now_prof = _composite_profiles(z[base_cols])
    phase_rule, desc_rule, color_rule, assets_rule = _phase_from_composites(now_prof, latest_raw)
    sahm = _sahm_indicator(panel["UNRATE"]) if "UNRATE" in panel.columns else np.nan

    if GaussianHMM is None or PCA is None or len(panel) < 72:
        note = (
            f"**模型**：规则版 Nowcast Composite（未启用 HMM/PCA）\n\n"
            f"**综合分数**：需求 {now_prof.get('demand', 0):+.2f}｜通胀 {now_prof.get('inflation', 0):+.2f}｜金融压力 {now_prof.get('stress', 0):+.2f}\n\n"
            f"**Sahm近似指标**：{sahm:.2f}（>=0.50 通常提示衰退风险明显抬升）\n\n"
            "_以上为辅助判断，非投资建议。_"
        )
        return phase_rule, desc_rule, color_rule, assets_rule, note, False, 55.0, None

    pca = PCA(n_components=0.90, random_state=42)
    X_pca = pca.fit_transform(z.values)
    if X_pca.shape[1] > 6:
        X_pca = X_pca[:, :6]

    best_model, best_bic, best_n = None, np.inf, None
    for n_states in range(3, 6):
        try:
            model = GaussianHMM(
                n_components=n_states,
                covariance_type="diag",
                n_iter=1000,
                random_state=42,
                tol=1e-4,
                min_covar=1e-3,
            )
            model.fit(X_pca)
            bic = _bic_hmm(model, X_pca)
            if np.isfinite(bic) and bic < best_bic:
                best_model, best_bic, best_n = model, bic, n_states
        except Exception:
            continue

    if best_model is None:
        note = (
            "**模型**：HMM 拟合失败，已回退至规则版 Nowcast Composite\n\n"
            f"**综合分数**：需求 {now_prof.get('demand', 0):+.2f}｜通胀 {now_prof.get('inflation', 0):+.2f}｜金融压力 {now_prof.get('stress', 0):+.2f}\n\n"
            "_以上为辅助判断，非投资建议。_"
        )
        return phase_rule, desc_rule, color_rule, assets_rule, note, False, 50.0, None

    states = best_model.predict(X_pca)
    posteriors = best_model.predict_proba(X_pca)
    current_state = int(states[-1])
    hmm_conf = float(posteriors[-1, current_state]) * 100

    hist = panel.copy()
    hist["state"] = states
    state_labels = {}
    for s in sorted(set(states)):
        mask = hist["state"] == s
        z_state = z.loc[mask, base_cols].mean().to_frame().T
        prof_s = _composite_profiles(z_state)
        raw_s = panel.loc[mask].mean(numeric_only=True)
        state_labels[int(s)] = _phase_from_composites(prof_s, raw_s)

    phase_hmm, desc_hmm, color_hmm, assets_hmm = state_labels.get(current_state, (phase_rule, desc_rule, color_rule, assets_rule))

    use_rule_overlay = False
    overlay_reason = ""
    if (now_prof.get("stress", 0) > 0.90 and now_prof.get("demand", 0) < -0.20) or (np.isfinite(sahm) and sahm >= 0.50):
        if "衰退" not in phase_hmm:
            use_rule_overlay = True
            overlay_reason = "高频就业/信用/金融压力已触发衰退风险纠偏"
    elif "滞胀" in phase_rule and "滞胀" not in phase_hmm and now_prof.get("inflation", 0) > 0.60:
        use_rule_overlay = True
        overlay_reason = "通胀粘性与需求走弱触发滞胀风险纠偏"

    if use_rule_overlay:
        phase, desc, color, assets = phase_rule, desc_rule, color_rule, assets_rule
        confidence = min(88.0, 0.65 * hmm_conf + 25.0)
    else:
        phase, desc, color, assets = phase_hmm, desc_hmm, color_hmm, assets_hmm
        confidence = hmm_conf

    history = hist[["state"]].copy()
    history["phase_name"] = history["state"].map(lambda s: state_labels.get(int(s), ("均衡震荡 (Neutral)", "", "#3a8a6e", ""))[0])
    history["phase_color"] = history["state"].map(lambda s: state_labels.get(int(s), ("", "", "#3a8a6e", ""))[2])

    latest_dates = []
    for sid in ["CPIAUCSL", "PAYEMS", "ICSA", "DGS10", "BAMLH0A0HYM2", "NFCI"]:
        df = raw.get(sid, pd.DataFrame())
        if df is not None and not df.empty:
            latest_dates.append(f"{sid}: {pd.to_datetime(df['Date'].iloc[-1]).strftime('%Y-%m-%d')}")

    note_lines = [
        f"**模型**：Robust Z-score + PCA-HMM，BIC 最优态数 **{best_n}**，当前隐状态 #{current_state}",
        f"**样本截止**：月度面板 {panel.index[-1].strftime('%Y-%m')}；PCA维度 {X_pca.shape[1]}，解释方差 {float(pca.explained_variance_ratio_.sum()) * 100:.1f}%",
        f"**置信度**：{confidence:.1f}%（HMM后验概率 {hmm_conf:.1f}%" + (f"；{overlay_reason}" if overlay_reason else "") + "）",
        f"**Nowcast 综合分数**：需求 {now_prof.get('demand', 0):+.2f}｜增长 {now_prof.get('growth', 0):+.2f}｜就业 {now_prof.get('employment', 0):+.2f}｜通胀 {now_prof.get('inflation', 0):+.2f}｜金融压力 {now_prof.get('stress', 0):+.2f}",
        f"**关键阈值**：CPI {latest_raw.get('CPIAUCSL', np.nan):.2f}%｜核心CPI {latest_raw.get('CPILFESL', np.nan):.2f}%｜10Y-3M {latest_raw.get('T10Y3M', np.nan):.2f}%｜HY OAS {latest_raw.get('BAMLH0A0HYM2', np.nan):.2f}%｜Sahm近似 {sahm:.2f}",
    ]
    if latest_dates:
        note_lines.append("**高频/核心数据最新观测**：" + "；".join(latest_dates))
    note_lines.append("_以上为辅助判断，非投资建议。_")
    return phase, desc, color, assets, "\n\n".join(note_lines), True, confidence, history


def _fallback_rule_clock(growth_df: pd.DataFrame, cpi_df: pd.DataFrame):
    if growth_df.empty or cpi_df.empty or len(growth_df) < 4 or len(cpi_df) < 4:
        return "数据不足", "🔧 无法计算", "#aaaaaa", "保持现金", "数据不足", False, 0.0, None
    g_now = float(growth_df["YoY"].iloc[-1]) if "YoY" in growth_df else 0.0
    i_now = float(cpi_df["Value"].iloc[-1])
    prof = {"demand": (g_now - 1.5) / 2.0, "growth": (g_now - 1.5) / 2.0, "inflation": (i_now - 2.5) / 1.0, "stress": 0.0}
    latest_raw = pd.Series({"CPIAUCSL": i_now, "CPILFESL": i_now, "T10Y3M": 0.5, "BAMLH0A0HYM2": np.nan})
    phase, desc, color, assets = _phase_from_composites(prof, latest_raw)
    note = f"未安装 hmmlearn / scikit-learn，或有效历史数据不足；使用 INDPRO 同比 {g_now:.2f}% × CPI 同比 {i_now:.2f}% 的规则兜底。"
    return phase, desc, color, assets, note, False, 0.0, None

# =====================================================
# 6. 宏观事件倒计时与 Fed RSS
# =====================================================
_FOMC_DATES = [
    (2026, 1, 28), (2026, 3, 18), (2026, 4, 29), (2026, 6, 17),
    (2026, 7, 29), (2026, 9, 16), (2026, 10, 28), (2026, 12, 9),
]


def _add_month(y, m):
    return (y + 1, 1) if m == 12 else (y, m + 1)


def _first_friday(y, m):
    d = date(y, m, 1)
    return d + timedelta(days=(4 - d.weekday()) % 7)


def _second_tuesday(y, m):
    d = date(y, m, 1)
    first_tue = d + timedelta(days=(1 - d.weekday()) % 7)
    return first_tue + timedelta(days=7)


def _next_nfp(now_utc):
    r = now_utc.astimezone(_ET)
    y, m = r.year, r.month
    for _ in range(28):
        fd = _first_friday(y, m)
        t = datetime(fd.year, fd.month, fd.day, 8, 30, tzinfo=_ET)
        if t.astimezone(timezone.utc) > now_utc:
            return t
        y, m = _add_month(y, m)
    return None


def _next_cpi(now_utc):
    r = now_utc.astimezone(_ET)
    y, m = r.year, r.month
    for _ in range(28):
        d2 = _second_tuesday(y, m)
        t = datetime(d2.year, d2.month, d2.day, 8, 30, tzinfo=_ET)
        if t.astimezone(timezone.utc) > now_utc:
            return t
        y, m = _add_month(y, m)
    return None


def _next_fomc(now_utc):
    for y, mo, d in _FOMC_DATES:
        t = datetime(y, mo, d, 14, 0, tzinfo=_ET)
        if t.astimezone(timezone.utc) > now_utc:
            return t
    return None


def _fmt_countdown(rem):
    if rem.total_seconds() <= 0:
        return "已到发布窗口"
    total = int(rem.total_seconds())
    d, r = divmod(total, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    return f"{d}天 {h:02d}:{m:02d}:{s:02d}" if d > 0 else f"{h:02d}:{m:02d}:{s:02d}"


def _macro_countdown_strip_body():
    now_utc = datetime.now(timezone.utc)
    events = [
        ("📊 美国 CPI", _next_cpi(now_utc), "BLS 第二个周二 8:30 ET"),
        ("💼 非农就业 NFP", _next_nfp(now_utc), "BLS 当月首个周五 8:30 ET"),
        ("🏦 FOMC 利率决议", _next_fomc(now_utc), "联储声明约 14:00 ET"),
    ]
    cols = st.columns(3)
    for col, (title, target, note) in zip(cols, events):
        with col:
            if target is None:
                st.markdown(f'<div class="countdown-card"><p class="countdown-title">{title}</p><p class="countdown-time">—</p></div>', unsafe_allow_html=True)
                continue
            rem = target.astimezone(timezone.utc) - now_utc
            urgent = timedelta(0) < rem < timedelta(hours=24)
            cls = "countdown-card urgent" if urgent else "countdown-card"
            st.markdown(
                f'<div class="{cls}"><p class="countdown-label">{note}</p><p class="countdown-title">{title}</p>'
                f'<p class="countdown-time">{_fmt_countdown(rem)}</p><p class="countdown-meta">发布（ET）{target.strftime("%Y-%m-%d %H:%M")}</p></div>',
                unsafe_allow_html=True,
            )


_macro_countdown_strip = st.fragment(run_every=timedelta(seconds=5))(_macro_countdown_strip_body) if hasattr(st, "fragment") else _macro_countdown_strip_body

_FED_RSS_UA = {"User-Agent": "Mozilla/5.0 (compatible; MacroTrack/2.0)"}
_FED_RSS_ALL = "https://www.federalreserve.gov/feeds/speeches_and_testimony.xml"
_FED_BOARD_RSS = [
    ("Jerome H. Powell", "https://www.federalreserve.gov/feeds/s_t_powell.xml"),
    ("Philip N. Jefferson", "https://www.federalreserve.gov/feeds/s_t_jefferson.xml"),
    ("Michelle W. Bowman", "https://www.federalreserve.gov/feeds/m_w_Bowman.xml"),
    ("Lisa D. Cook", "https://www.federalreserve.gov/feeds/s_t_cook.xml"),
    ("Christopher J. Waller", "https://www.federalreserve.gov/feeds/s_t_waller.xml"),
]


def _strip_html(text):
    if not text:
        return ""
    t = re.sub(r"<[^>]+>", " ", text)
    return html.unescape(re.sub(r"\s+", " ", t)).strip()


def _entry_ts(entry):
    tt = entry.get("published_parsed") or entry.get("updated_parsed")
    if tt:
        try:
            return time.mktime(tt)
        except Exception:
            return 0.0
    return 0.0


def _parse_feed_entries(parsed):
    rows = []
    for e in getattr(parsed, "entries", []) or []:
        link = (e.get("link") or "").strip()
        if not link:
            continue
        rows.append({
            "title": (e.get("title") or "（无标题）").strip(),
            "link": link,
            "ts": _entry_ts(e),
            "summary": _strip_html(e.get("summary", ""))[:400],
        })
    return rows


@st.cache_data(ttl=300, show_spinner=False)
def fetch_fed_speech_feeds():
    merged = {}
    errors = []

    def load_one(name, url):
        try:
            parsed = feedparser.parse(url, request_headers=_FED_RSS_UA)
            rows = []
            for row in _parse_feed_entries(parsed):
                row["speaker"] = name
                rows.append(row)
            return rows, None
        except Exception as ex:
            return [], str(ex)

    for name, url in _FED_BOARD_RSS:
        batch, err = load_one(name, url)
        if err:
            errors.append(err)
        for row in batch:
            merged.setdefault(row["link"], row)

    rows = sorted(merged.values(), key=lambda r: r["ts"], reverse=True)
    if len(rows) < 3:
        try:
            parsed = feedparser.parse(_FED_RSS_ALL, request_headers=_FED_RSS_UA)
            seen = {r["link"] for r in rows}
            for row in sorted(_parse_feed_entries(parsed), key=lambda r: r["ts"], reverse=True):
                if row["link"] not in seen:
                    row["speaker"] = "（聚合源）"
                    rows.append(row)
                    seen.add(row["link"])
            rows.sort(key=lambda r: r["ts"], reverse=True)
        except Exception as ex:
            errors.append(f"聚合源失败: {ex}")
    return rows, ("; ".join(errors[:2]) if errors else None)

# =====================================================
# 7. 图表渲染
# =====================================================
FRESH_COLORS = {
    "primary": "#3a8a6e", "secondary": "#6a9fd8", "accent": "#e07a5f", "warm": "#f2a65a",
    "purple": "#9b88c4", "teal": "#5abcb0", "rose": "#d4727a", "olive": "#8aaa5a",
    "palette": ["#3a8a6e", "#6a9fd8", "#e07a5f", "#f2a65a", "#9b88c4", "#5abcb0", "#d4727a", "#8aaa5a"],
}

_BASE_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(255,255,255,0.55)",
    font=dict(family="Arial, sans-serif", color="#2d4a3e", size=11),
    margin=dict(l=10, r=10, t=46, b=10),
    hovermode="x unified",
    hoverlabel=dict(bgcolor="rgba(255,255,255,0.94)", bordercolor="#c8e0d8", font=dict(color="#1f3d30", size=12)),
    xaxis=dict(showgrid=False, linecolor="#d0e8df", tickcolor="#d0e8df", tickfont=dict(size=10, color="#7aad98")),
    yaxis=dict(showgrid=True, gridcolor="rgba(192,220,208,0.45)", zeroline=False, linecolor="#d0e8df", tickfont=dict(size=10, color="#7aad98")),
    legend=dict(bgcolor="rgba(255,255,255,0.75)", bordercolor="#d0e8df", borderwidth=1, font=dict(size=10, color="#2d4a3e")),
)


def _hex_rgba(hex_color, alpha):
    r, g, b = int(hex_color[1:3], 16), int(hex_color[3:5], 16), int(hex_color[5:7], 16)
    return f"rgba({r},{g},{b},{alpha})"


def render_chart(series_id: str, metric_name: str, df: pd.DataFrame, idx: int):
    if df is None or df.empty:
        return go.Figure()

    meta = SERIES_META.get(series_id, {})
    ctype = meta.get("chart", "line")
    label = meta.get("label", metric_name)
    unit = meta.get("unit_str", "")
    color = FRESH_COLORS["palette"][idx % len(FRESH_COLORS["palette"])]
    last_date = pd.to_datetime(df["Date"].iloc[-1]).strftime("%Y-%m-%d")
    full_title = f"{metric_name}｜{label}｜最新 {last_date}"

    if ctype in ["bar_yoy", "bar_abs"]:
        y = df["YoY"] if ctype == "bar_yoy" and "YoY" in df.columns else df["Value"]
        colors = [FRESH_COLORS["primary"] if v >= 0 else FRESH_COLORS["accent"] for v in y.fillna(0)]
        fig = go.Figure(go.Bar(x=df["Date"], y=y, marker_color=colors, marker_line_width=0, name=label, hovertemplate=f"<b>%{{y:.2f}}{unit}</b><extra></extra>"))
        if not y.empty and y.min() < 0:
            fig.add_hline(y=0, line_width=1, line_dash="dot", line_color="rgba(200,80,60,0.45)")
    elif ctype == "line_yoy":
        y = df["YoY"]
        fig = go.Figure(go.Scatter(x=df["Date"], y=y, mode="lines", line=dict(width=2, color=color), fill="tozeroy", fillcolor=_hex_rgba(color, 0.10), name=label, hovertemplate=f"<b>%{{y:.2f}}{unit}</b><extra></extra>"))
    elif ctype == "step":
        fig = go.Figure(go.Scatter(x=df["Date"], y=df["Value"], mode="lines", line=dict(width=2.5, color=color, shape="hv"), fill="tozeroy", fillcolor=_hex_rgba(color, 0.08), name=label, hovertemplate=f"<b>%{{y:.2f}}{unit}</b><extra></extra>"))
    elif ctype == "spread":
        fig = go.Figure()
        fig.add_hline(y=0, line_width=1.2, line_dash="dot", line_color="rgba(200,80,60,0.55)")
        fig.add_trace(go.Scatter(x=df["Date"], y=df["Value"], mode="lines", line=dict(width=2, color=color), fill="tozeroy", fillcolor=_hex_rgba(color, 0.10), name=label, hovertemplate=f"<b>%{{y:.3f}}{unit}</b><extra></extra>"))
    else:
        fig = go.Figure(go.Scatter(x=df["Date"], y=df["Value"], mode="lines", line=dict(width=2, color=color), fill="tozeroy", fillcolor=_hex_rgba(color, 0.09), name=label, hovertemplate=f"<b>%{{y:.2f}}{unit}</b><extra></extra>"))

    layout = dict(**_BASE_LAYOUT)
    layout["title"] = dict(text=full_title, font=dict(size=12, color="#1f3d30"), x=0.01, xanchor="left")
    layout["height"] = 300
    fig.update_layout(**layout)
    return fig


def _latest_metric(series_id: str, years: int = 3):
    df = fetch_data_advanced(series_id, years)
    if df is None or df.empty:
        return None
    meta = SERIES_META.get(series_id, {})
    display_val = df["YoY"].iloc[-1] if meta.get("chart") in ["bar_yoy", "line_yoy"] and "YoY" in df.columns else df["Value"].iloc[-1]
    delta = np.nan
    if len(df) >= 2:
        prev = df["YoY"].iloc[-2] if meta.get("chart") in ["bar_yoy", "line_yoy"] and "YoY" in df.columns else df["Value"].iloc[-2]
        delta = display_val - prev
    return display_val, delta, pd.to_datetime(df["Date"].iloc[-1]), meta.get("unit_str", "")


def _metric_card(title: str, sid: str):
    res = _latest_metric(sid)
    if not res:
        st.markdown(f'<div class="metric-card"><p class="metric-label">{sid}</p><p class="metric-name">{title}</p><p class="metric-value">—</p></div>', unsafe_allow_html=True)
        return
    val, delta, dt, unit = res
    delta_txt = "" if pd.isna(delta) else f"{delta:+.2f}{unit} vs 上期"
    delta_color = "#3a8a6e" if (pd.isna(delta) or delta >= 0) else "#e07a5f"
    st.markdown(
        f'<div class="metric-card"><p class="metric-label">{sid}｜{dt.strftime("%Y-%m-%d")}</p>'
        f'<p class="metric-name">{title}</p><p class="metric-value">{val:.2f}{unit}</p>'
        f'<p class="metric-delta" style="color:{delta_color};">{delta_txt}</p></div>',
        unsafe_allow_html=True,
    )

# =====================================================
# 8. UI 主体
# =====================================================
col_h1, col_h2 = st.columns([4, 1])
with col_h1:
    st.markdown(
        """
        <h1>🌿 Macro Track Reportthinking</h1>
        <p style="color:#6a9e8e; margin-top:-10px; font-size:0.95rem;">
        美联储政策 · 宏观经济 · 投资时钟 &nbsp;|&nbsp; 数据来源：Federal Reserve Economic Data
        </p>
        """,
        unsafe_allow_html=True,
    )
with col_h2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 刷新数据", use_container_width=True):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

st.markdown("---")

@st.cache_data(ttl=6 * 3600, show_spinner=False)
def _check_series_freshness(series_id: str) -> str:
    try:
        info = fred.get_series_info(series_id)
        return str(info.get("observation_end", ""))
    except Exception:
        return ""


def _show_freshness_banner():
    fred_latest = _check_series_freshness("GDPC1")
    if not fred_latest:
        return
    local_df = fetch_data_advanced("GDPC1", years=6)
    if local_df.empty:
        return
    local_latest = pd.to_datetime(local_df["Date"].iloc[-1]).strftime("%Y-%m-%d")
    if local_latest < fred_latest:
        st.info(f"📡 **数据更新提示**：FRED 已发布 GDPC1 至 **{fred_latest}**，当前缓存截止 {local_latest}。点击右上角刷新即可获取最新。", icon="🔔")

_show_freshness_banner()

with st.spinner("正在加载宏观数据…"):
    warm_core_series_cache()
    phase, desc, color, assets, clock_note, used_ml, confidence, history_df = calculate_ml_investment_clock()

st.markdown(
    f"""
<div class="clock-card" style="border-color:{color};">
  <div style="display:flex;justify-content:space-between;gap:16px;align-items:flex-start;flex-wrap:wrap;">
    <div style="flex:1;min-width:260px;">
      <span class="ml-badge">{'PCA-HMM + Nowcast' if used_ml else 'Rule-based Nowcast'}</span>
      <p class="clock-phase-title" style="color:{color};">{phase}</p>
      <p class="clock-desc">{desc}</p>
      <p style="color:#6a9e8e;margin:8px 0 0;font-size:.9rem;">模型置信度：<b>{confidence:.1f}%</b></p>
    </div>
    <div class="assets-card" style="min-width:240px;">
      <p style="margin:0;color:#6a9e8e;font-size:.75rem;font-weight:700;">战术资产倾向</p>
      <p style="margin:5px 0 0;color:#1f3d30;font-size:1.02rem;font-weight:800;">{assets}</p>
    </div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)

with st.expander("🧠 查看周期识别模型细节", expanded=False):
    st.markdown(clock_note)
    if history_df is not None and not history_df.empty:
        h = history_df.tail(84).copy()
        fig_h = go.Figure()
        fig_h.add_trace(go.Scatter(x=h.index, y=h["state"], mode="lines+markers", line=dict(width=2, color=color), marker=dict(size=5), name="Hidden State"))
        fig_h.update_layout(**dict(_BASE_LAYOUT, height=260, title=dict(text="近7年宏观隐状态序列", x=0.01, font=dict(size=12, color="#1f3d30"))))
        st.plotly_chart(fig_h, use_container_width=True)
        st.dataframe(h[["state", "phase_name"]].tail(24), use_container_width=True)

st.subheader("核心宏观仪表盘")
metric_cols = st.columns(6)
core_metrics = [
    ("工业生产同比", "INDPRO"),
    ("CPI同比", "CPIAUCSL"),
    ("核心CPI同比", "CPILFESL"),
    ("失业率", "UNRATE"),
    ("10Y-3M利差", "T10Y3M"),
    ("高收益债利差", "BAMLH0A0HYM2"),
]
for col, (title, sid) in zip(metric_cols, core_metrics):
    with col:
        _metric_card(title, sid)

st.subheader("宏观事件倒计时")
_macro_countdown_strip()

st.markdown("---")

with st.sidebar:
    st.header("⚙️ 参数设置")
    years = st.slider("图表展示年限", min_value=2, max_value=15, value=6, step=1)
    selected_cat = st.radio("指标分类", list(FRED_CATEGORIES.keys()), index=0)
    search_kw = st.text_input("搜索指标 / FRED代码", placeholder="例如 CPI、UNRATE、DGS10")
    show_raw = st.checkbox("显示原始数据表", value=False)
    st.caption("提示：日频/周频序列采用更短缓存桶；月频/季频序列缓存更长，点击刷新可强制清空缓存。")

st.subheader(f"指标图谱：{selected_cat}")
category_items = FRED_CATEGORIES[selected_cat]
if search_kw:
    q = search_kw.lower().strip()
    category_items = {k: v for k, v in category_items.items() if q in k.lower() or q in v.lower()}

if not category_items:
    st.warning("没有匹配的指标。")
else:
    data_map = load_category_parallel(selected_cat, years=years)
    names = list(category_items.items())
    for i in range(0, len(names), 2):
        cols = st.columns(2)
        for j, col in enumerate(cols):
            if i + j >= len(names):
                continue
            metric_name, sid = names[i + j]
            df = data_map.get(sid, pd.DataFrame())
            with col:
                if df is None or df.empty:
                    st.warning(f"{metric_name}（{sid}）暂无数据或取数失败。")
                    continue
                fig = render_chart(sid, metric_name, df, i + j)
                st.plotly_chart(fig, use_container_width=True)
                if show_raw:
                    st.dataframe(df.tail(24), use_container_width=True)

st.markdown("---")
left, right = st.columns([1.2, 1])
with left:
    st.subheader("Fed 官员讲话 / 证词 RSS")
    rows, err = fetch_fed_speech_feeds()
    if err:
        st.caption(f"RSS 部分源读取异常：{err}")
    if not rows:
        st.info("暂未读取到 Fed RSS 内容。")
    else:
        for row in rows[:6]:
            dt = datetime.fromtimestamp(row.get("ts", 0), tz=_ET).strftime("%Y-%m-%d") if row.get("ts") else ""
            st.markdown(
                f"""
<div class="fresh-card">
  <p style="margin:0;color:#6a9e8e;font-size:.75rem;font-weight:700;">{row.get('speaker','')} ｜ {dt}</p>
  <p style="margin:4px 0 5px;font-weight:800;"><a href="{row['link']}" target="_blank">{html.escape(row['title'])}</a></p>
  <p style="margin:0;color:#5a8a7a;font-size:.85rem;">{html.escape(row.get('summary',''))}</p>
</div>
""",
                unsafe_allow_html=True,
            )

with right:
    st.subheader("使用说明")
    st.markdown(
        """
- **数据获取**：所有序列按自身频率设置缓存，日频/周频更及时，月频/季频更稳健。
- **周期识别**：先用宏观 Nowcast Composite 捕捉当前边际变化，再用 PCA-HMM 识别历史隐状态。
- **关键修正**：BIC 采用正确 log-likelihood，避免 HMM 状态数误选；面板对齐采用覆盖率 + 有限前向填充，避免最新月份被滞后指标整体拖掉。
- **解释口径**：周期标签用于宏观状态跟踪和资产配置讨论，不构成投资建议。
"""
    )
