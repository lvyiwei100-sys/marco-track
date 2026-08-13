import streamlit as st
import pandas as pd
import numpy as np
from fredapi import Fred
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta
import html
import re
import time
import traceback
import warnings
import feedparser
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
#  0. 机器学习依赖：模块级自检（只做一次，保留真实报错信息）
# ==========================================
# 说明：原版把 import 放在函数里且用 except ImportError 吞掉，
#       hmmlearn 因 numpy/sklearn 版本不兼容抛 ValueError/RuntimeError 时
#       会被误判为「未安装」，页面永远显示规则模型。
try:
    from hmmlearn.hmm import GaussianHMM
    _HMM_IMPORT_ERR = None
except Exception as _e:               # noqa: BLE001
    GaussianHMM = None
    _HMM_IMPORT_ERR = f"{type(_e).__name__}: {_e}"

try:
    from sklearn.preprocessing import StandardScaler
    _SK_IMPORT_ERR = None
except Exception as _e:               # noqa: BLE001
    StandardScaler = None
    _SK_IMPORT_ERR = f"{type(_e).__name__}: {_e}"


# ==========================================
#  1. 页面配置与小清新风格 CSS
# ==========================================
st.set_page_config(
    page_title="Macro Track Report·thinking | 宏观经济终端",
    page_icon="🌿",
    layout="wide",
)

st.markdown("""
<style>
/* ───── 全局背景与字体 ───── */
html, body, [class*="css"]  {
    font-family: 'Nunito', 'PingFang SC', 'Microsoft YaHei', sans-serif !important;
    color: #2d4a3e;
}
.stApp {
    background: linear-gradient(135deg, #f4faf7 0%, #eaf3ee 50%, #f0f6f2 100%);
}
[data-testid="stHeader"] { background: transparent; }
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1400px; }

/* ───── 章节标题 / Markdown headings：强制深绿色 ───── */
.stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5, .stApp h6,
[data-testid="stMarkdownContainer"] h1,
[data-testid="stMarkdownContainer"] h2,
[data-testid="stMarkdownContainer"] h3,
[data-testid="stMarkdownContainer"] h4,
[data-testid="stMarkdownContainer"] h5,
[data-testid="stMarkdownContainer"] h6 {
    color: #1e6e50 !important;
    font-weight: 800 !important;
    letter-spacing: 0.3px;
}

/* ───── Expander 标题（周期判断依据等） ───── */
[data-testid="stExpander"] summary,
[data-testid="stExpander"] summary p,
details summary,
details summary p {
    color: #1e6e50 !important;
    font-weight: 700 !important;
}

/* ───── 段落 / caption / label / radio / checkbox 文本兜底 ───── */
[data-testid="stMarkdownContainer"] p,
[data-testid="stCaptionContainer"],
.stCaption,
.stRadio label, .stRadio div[role="radiogroup"] label,
.stCheckbox label,
.stTextInput label,
label[data-testid="stWidgetLabel"] p,
label[data-testid="stWidgetLabel"] {
    color: #2d4a3e !important;
}

/* radio 选项文字 */
.stRadio div[role="radiogroup"] label p { color: #2d4a3e !important; }

/* 链接默认色 */
a { color: #1e6e50; }

/* ───── 主标题 ───── */
.fresh-title {
    font-size: 1.75rem; font-weight: 800; color: #1e6e50;
    margin: 0 0 4px 0; letter-spacing: 0.5px;
}
.fresh-subtitle {
    font-size: 0.9rem; color: #5a8070; margin: 0; font-weight: 500;
}

/* ───── 卡片通用 ───── */
.fresh-card {
    background: rgba(255,255,255,0.78);
    border: 1px solid #d4e8df;
    border-radius: 14px;
    padding: 14px 16px;
    box-shadow: 0 2px 8px rgba(58,138,110,0.06);
    transition: all .25s ease;
}
.fresh-card:hover { box-shadow: 0 4px 14px rgba(58,138,110,0.10); transform: translateY(-1px); }

/* ───── 指标卡 ───── */
.metric-card {
    background: rgba(255,255,255,0.82);
    border: 1px solid #d4e8df;
    border-left: 4px solid #3a8a6e;
    border-radius: 12px;
    padding: 14px 16px;
    box-shadow: 0 2px 8px rgba(58,138,110,0.05);
    transition: all .25s ease;
    height: 100%;
}
.metric-card:hover { transform: translateY(-2px); box-shadow: 0 4px 14px rgba(58,138,110,0.12); }
.metric-label { margin: 0; color: #7aad98; font-size: 0.72rem; font-weight: 600; letter-spacing: 0.4px; text-transform: uppercase; }
.metric-name  { margin: 2px 0 4px; color: #2d4a3e; font-size: 0.92rem; font-weight: 700; }
.metric-value { margin: 0; color: #1e6e50; font-size: 1.55rem; font-weight: 800; line-height: 1.15; }
.metric-delta { margin: 4px 0 0; font-size: 0.82rem; font-weight: 700; }

/* ───── 倒计时卡片 ───── */
.countdown-card {
    background: rgba(255,255,255,0.82);
    border: 1px solid #d4e8df;
    border-radius: 12px;
    padding: 12px 16px;
    box-shadow: 0 2px 8px rgba(58,138,110,0.05);
    transition: all .25s ease;
}
.countdown-card.urgent {
    border-color: #e07a5f;
    background: linear-gradient(135deg, rgba(255,245,242,0.92), rgba(255,235,228,0.85));
    animation: pulse 2.2s ease-in-out infinite;
}
@keyframes pulse {
    0%,100% { box-shadow: 0 2px 8px rgba(224,122,95,0.18); }
    50%     { box-shadow: 0 4px 16px rgba(224,122,95,0.32); }
}
.cd-note  { color: #7aad98; font-size: 0.72rem; font-weight: 600; margin: 0 0 2px 0; }
.cd-title { color: #2d4a3e; font-size: 0.95rem; font-weight: 700; margin: 0 0 4px 0; }
.cd-value { color: #1e6e50; font-size: 1.4rem; font-weight: 800; margin: 0; letter-spacing: 0.5px; }
.cd-when  { color: #9abfb0; font-size: 0.7rem; margin: 4px 0 0 0; }

/* ───── 周期 banner ───── */
.clock-banner {
    border-radius: 16px;
    padding: 18px 24px;
    border: 1px solid #d4e8df;
    box-shadow: 0 2px 10px rgba(58,138,110,0.08);
    margin-bottom: 12px;
}
.clock-phase   { font-size: 1.7rem; font-weight: 800; margin: 0; line-height: 1.2; }
.clock-desc    { font-size: 1rem;  color: #4a6b5e; margin: 4px 0 6px 0; font-weight: 500; }
.clock-assets  { font-size: 0.88rem; color: #5a8070; margin: 0; }
.clock-tag     { display: inline-block; padding: 2px 10px; border-radius: 12px; background: rgba(58,138,110,0.1);
                 color: #1e6e50; font-size: 0.72rem; font-weight: 700; margin-bottom: 6px; }

/* ───── Streamlit 微调 ───── */
.stButton > button {
    background: linear-gradient(135deg, #3a8a6e, #5abcb0);
    color: white; font-weight: 700; border: none; border-radius: 10px;
    padding: 6px 18px; box-shadow: 0 2px 6px rgba(58,138,110,0.25);
}
.stButton > button:hover { transform: translateY(-1px); box-shadow: 0 4px 10px rgba(58,138,110,0.35); }

div[data-baseweb="tab-list"] { gap: 4px; }

hr { border-color: rgba(58,138,110,0.18) !important; margin: 1rem 0 !important; }
</style>
""", unsafe_allow_html=True)


# ==========================================
#  2. 初始化 FRED API（支持 secrets 或硬编码）
# ==========================================
_FRED_API_KEY = "507dc70223e4743dfba25042ff8e1173"

@st.cache_resource
def get_fred_client():
    try:
        key = st.secrets.get("FRED_API_KEY", _FRED_API_KEY)
        return Fred(api_key=key)
    except Exception:
        return Fred(api_key=_FRED_API_KEY)

fred = get_fred_client()


# ==========================================
#  3. 指标字典（按展示顺序：增长→通胀→就业→利率→流动性→AI）
# ==========================================
FRED_CATEGORIES = {
    "增长 (Growth)": {
        "实际GDP (GDPC1)":                "GDPC1",
        "GDP环比增速 (A191RL1Q225SBEA)":  "A191RL1Q225SBEA",
        "工业生产指数 (INDPRO)":          "INDPRO",
        "零售销售 (RSAFS)":               "RSAFS",
        "个人消费支出 (PCE)":             "PCE",
        "耐用品订单 (DGORDER)":           "DGORDER",
        "建筑许可 (PERMIT)":              "PERMIT",
        "新屋开工 (HOUST)":               "HOUST",
        "领先经济指标 (USSLIND)":         "USSLIND",
        "芝加哥联储活动指数 (CFNAI)":     "CFNAI",
        "消费者信心指数 (UMCSENT)":       "UMCSENT",
    },
    "通胀 (Inflation)": {
        "CPI总体 (CPIAUCSL)":             "CPIAUCSL",
        "核心CPI (CPILFESL)":             "CPILFESL",
        "CPI住房分项 (CUSR0000SAH1)":     "CUSR0000SAH1",
        "PCE总体 (PCEPI)":                "PCEPI",
        "核心PCE (PCEPILFE)":             "PCEPILFE",
        "PPI总体 (PPIACO)":               "PPIACO",
        "PPI最终需求 (PPIFID)":           "PPIFID",
        "密歇根通胀预期1Y (MICH)":        "MICH",
        "5年盈亏平衡通胀率 (T5YIE)":      "T5YIE",
        "10年盈亏平衡通胀率 (T10YIE)":    "T10YIE",
    },
    "就业 (Employment)": {
        "失业率 (UNRATE)":                "UNRATE",
        "非农就业新增 (PAYEMS)":          "PAYEMS",
        "私人非农就业 (USPRIV)":          "USPRIV",
        "劳动力参与率 (CIVPART)":         "CIVPART",
        "就业人口比 (EMRATIO)":           "EMRATIO",
        "平均时薪 (CES0500000003)":       "CES0500000003",
        "平均每周工时 (AWHAETP)":         "AWHAETP",
        "初请失业金 (ICSA)":              "ICSA",
        "续请失业金 (CCSA)":              "CCSA",
        "职位空缺 JOLTS (JTSJOL)":        "JTSJOL",
    },
    "利率 (Interest Rates)": {
        "联邦基金利率 (FEDFUNDS)":        "FEDFUNDS",
        "有效联邦基金利率/日 (DFF)":      "DFF",
        "超额准备金利率 (IORB)":          "IORB",
        "2年期美债 (DGS2)":               "DGS2",
        "10年期美债 (DGS10)":             "DGS10",
        "10Y-2Y利差 (T10Y2Y)":           "T10Y2Y",
        "10Y-3M利差 (T10Y3M)":           "T10Y3M",
        "30年期抵押贷款利率 (MORTGAGE30US)": "MORTGAGE30US",
        "BAA企业债收益率 (BAA)":          "BAA",
        "高收益债利差OAS (BAMLH0A0HYM2)": "BAMLH0A0HYM2",
        "投资级债利差OAS (BAMLC0A0CM)":   "BAMLC0A0CM",
        "10年期TIPS实际利率 (DFII10)":    "DFII10",
    },
    "流动性 (Liquidity)": {
        "M2货币供应 (M2SL)":              "M2SL",
        "M1货币供应 (M1SL)":              "M1SL",
        "美联储资产负债表 (WALCL)":       "WALCL",
        "美联储持有国债 (TREAST)":        "TREAST",
        "隔夜逆回购余额 (RRPONTSYD)":     "RRPONTSYD",
        "银行准备金余额 (WRESBAL)":       "WRESBAL",
        "芝加哥联储金融条件 (NFCI)":      "NFCI",
        "圣路易斯金融压力 (STLFSI4)":     "STLFSI4",
        "TED利差 (TEDRATE)":              "TEDRATE",
        "SOFR隔夜融资利率 (SOFR)":        "SOFR",
        "贸易加权美元指数 (DTWEXBGS)":    "DTWEXBGS",
    },
    "AI代理指标 (AI Proxies)": {
        "半导体工业生产 (IPG3344N)":      "IPG3344N",
        "半导体价格PPI (PCU33443344)":    "PCU33443344",
    },
}


# ==========================================
#  4. 序列元数据（每条序列独立定义取数与渲染方式）
# ==========================================
# FRED units 说明：
#   lin = 原始值；pc1 = 月度同比%；pch = 环比%；pca = 季度年化%
SERIES_META = {
    # ── 增长 ──
    "GDPC1":            {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "实际GDP 同比 %"},
    "A191RL1Q225SBEA":  {"units": "lin", "display": "value",    "chart": "bar_abs",  "unit_str": "%",      "label": "GDP环比增速（季度年化 %）"},
    "INDPRO":           {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "工业生产 同比 %"},
    "RSAFS":            {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "零售销售 同比 %"},
    "PCE":              {"units": "pc1", "display": "value",    "chart": "line_yoy", "unit_str": "%",      "label": "个人消费 同比 %"},
    "DGORDER":          {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "耐用品订单 同比 %"},
    "PERMIT":           {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "建筑许可 同比 %"},
    "HOUST":            {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "新屋开工 同比 %"},
    "USSLIND":          {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "",       "label": "领先指标（指数）"},
    "CFNAI":            {"units": "lin", "display": "value",    "chart": "bar_abs",  "unit_str": "",       "label": "芝加哥联储活动指数"},
    "UMCSENT":          {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "",       "label": "密歇根消费者信心"},

    # ── 通胀 ──
    "CPIAUCSL":         {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "CPI总体 同比 %"},
    "CPILFESL":         {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "核心CPI 同比 %"},
    "CUSR0000SAH1":     {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "CPI住房 同比 %"},
    "PCEPI":            {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "PCE总体 同比 %"},
    "PCEPILFE":         {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "核心PCE 同比 %"},
    "PPIACO":           {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "PPI总体 同比 %"},
    "PPIFID":           {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "PPI最终需求 同比 %"},
    "MICH":             {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "密歇根通胀预期 1Y %"},
    "T5YIE":            {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "5年盈亏平衡通胀率 %"},
    "T10YIE":           {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "10年盈亏平衡通胀率 %"},

    # ── 就业 ──
    "UNRATE":           {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "失业率 %"},
    "PAYEMS":           {"units": "lin", "display": "mom_diff", "chart": "bar_abs",  "unit_str": " 千人",  "label": "非农就业 月增（千人）"},
    "USPRIV":           {"units": "lin", "display": "mom_diff", "chart": "bar_abs",  "unit_str": " 千人",  "label": "私人非农 月增（千人）"},
    "CIVPART":          {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "劳动力参与率 %"},
    "EMRATIO":          {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "就业人口比 %"},
    "CES0500000003":    {"units": "pc1", "display": "value",    "chart": "line_yoy", "unit_str": "%",      "label": "平均时薪 同比 %"},
    "AWHAETP":          {"units": "lin", "display": "value",    "chart": "line",     "unit_str": " 小时",  "label": "平均每周工时"},
    "ICSA":             {"units": "lin", "display": "value",    "chart": "line",     "unit_str": " 人",    "label": "初请失业金（人）"},
    "CCSA":             {"units": "lin", "display": "value",    "chart": "line",     "unit_str": " 人",    "label": "续请失业金（人）"},
    "JTSJOL":           {"units": "lin", "display": "value",    "chart": "line",     "unit_str": " 千人",  "label": "职位空缺 JOLTS（千人）"},

    # ── 利率 ──
    "FEDFUNDS":         {"units": "lin", "display": "value",    "chart": "step",     "unit_str": "%",      "label": "联邦基金利率 %"},
    "DFF":              {"units": "lin", "display": "value",    "chart": "step",     "unit_str": "%",      "label": "有效联邦基金利率 %"},
    "IORB":             {"units": "lin", "display": "value",    "chart": "step",     "unit_str": "%",      "label": "准备金利率 IORB %"},
    "DGS2":             {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "2年期美债 %"},
    "DGS10":            {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "10年期美债 %"},
    "T10Y2Y":           {"units": "lin", "display": "value",    "chart": "spread",   "unit_str": "%",      "label": "10Y-2Y利差 %"},
    "T10Y3M":           {"units": "lin", "display": "value",    "chart": "spread",   "unit_str": "%",      "label": "10Y-3M利差 %"},
    "MORTGAGE30US":     {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "30年抵押贷款利率 %"},
    "BAA":              {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "BAA企业债收益率 %"},
    "BAMLH0A0HYM2":     {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "高收益债利差 OAS %"},
    "BAMLC0A0CM":       {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "投资级债利差 OAS %"},
    "DFII10":           {"units": "lin", "display": "value",    "chart": "spread",   "unit_str": "%",      "label": "10年TIPS实际利率 %"},

    # ── 流动性 ──
    "M2SL":             {"units": "pc1", "display": "value",    "chart": "line_yoy", "unit_str": "%",      "label": "M2 同比 %"},
    "M1SL":             {"units": "pc1", "display": "value",    "chart": "line_yoy", "unit_str": "%",      "label": "M1 同比 %"},
    "WALCL":            {"units": "lin", "display": "value",    "chart": "line",     "unit_str": " 百万$", "label": "美联储资产负债表（百万$）"},
    "TREAST":           {"units": "lin", "display": "value",    "chart": "line",     "unit_str": " 百万$", "label": "联储持有国债（百万$）"},
    "RRPONTSYD":        {"units": "lin", "display": "value",    "chart": "line",     "unit_str": " 十亿$", "label": "隔夜逆回购余额（十亿$）"},
    "WRESBAL":          {"units": "lin", "display": "value",    "chart": "line",     "unit_str": " 十亿$", "label": "银行准备金（十亿$）"},
    "NFCI":             {"units": "lin", "display": "value",    "chart": "spread",   "unit_str": "",       "label": "芝加哥金融条件指数"},
    "STLFSI4":          {"units": "lin", "display": "value",    "chart": "spread",   "unit_str": "",       "label": "圣路易斯金融压力指数"},
    "TEDRATE":          {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "TED利差 %"},
    "SOFR":             {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "%",      "label": "SOFR %"},
    "DTWEXBGS":         {"units": "lin", "display": "value",    "chart": "line",     "unit_str": "",       "label": "贸易加权美元指数"},

    # ── AI 代理 ──
    "IPG3344N":         {"units": "pc1", "display": "value",    "chart": "line_yoy", "unit_str": "%",      "label": "半导体工业生产 同比 %"},
    "PCU33443344":      {"units": "pc1", "display": "value",    "chart": "bar_yoy",  "unit_str": "%",      "label": "半导体PPI 同比 %"},
}

# 兼容旧代码引用
PC1_SERIES      = {sid for sid, m in SERIES_META.items() if m["units"] == "pc1"}
MOM_DIFF_SERIES = {sid for sid, m in SERIES_META.items() if m["display"] == "mom_diff"}
UNIT_MAP        = {sid: m["unit_str"] for sid, m in SERIES_META.items()}
CHART_TYPE      = {sid: m["chart"]    for sid, m in SERIES_META.items()}


# ==========================================
#  5. 数据拉取（基于 SERIES_META，每个序列独立处理）
# ==========================================
@st.cache_data(ttl=43200)   # 12 小时缓存
def fetch_data_advanced(series_id, years=6):
    """
    根据 SERIES_META 中该序列的 units/display 配置精确拉取。
    - 不传 observation_end → FRED 返回最新已发布数据
    - 先在完整历史上计算衍生列，再截断展示窗口
    """
    meta        = SERIES_META.get(series_id, {"units": "lin", "display": "value"})
    req_units   = meta["units"]
    display     = meta["display"]
    today       = datetime.today()

    # 拉取窗口：比展示窗口多 2 年（保证 lag 计算）
    start_date  = today - relativedelta(years=years + 2)

    try:
        data = fred.get_series(
            series_id,
            observation_start=start_date,
            units=req_units,
        )
        if data is None or data.empty:
            return pd.DataFrame()

        df = pd.DataFrame({"Date": data.index, "Value": data.values})
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.dropna(subset=["Value"]).reset_index(drop=True)
        if df.empty:
            return pd.DataFrame()

        # 衍生列
        if display == "mom_diff":
            df["YoY"]        = df["Value"].diff(1)
            df["Value_Diff"] = df["Value"].diff(1)
        else:
            df["YoY"]        = df["Value"]
            df["Value_Diff"] = df["Value"].diff(1)

        display_start = today - relativedelta(years=years)
        result = df[df["Date"] >= display_start].copy()
        result = result.dropna(subset=["Value"])
        return result

    except Exception:
        return pd.DataFrame()


def warm_core_series_cache():
    core = (
        "INDPRO", "CPIAUCSL", "CPILFESL", "UNRATE", "FEDFUNDS",
        "IPG3344N", "DGS10", "DGS2", "T10Y2Y", "T10Y3M",
        "M2SL", "WALCL", "NFCI", "PAYEMS",
    )
    with ThreadPoolExecutor(max_workers=len(core)) as ex:
        futures = [ex.submit(fetch_data_advanced, sid, 6) for sid in core]
        for f in as_completed(futures):
            f.result()


def load_category_parallel(tab_name: str, years: int = 6) -> dict:
    ids = list(FRED_CATEGORIES[tab_name].values())
    if not ids:
        return {}
    out = {}
    with ThreadPoolExecutor(max_workers=min(12, len(ids))) as ex:
        fut_to_sid = {ex.submit(fetch_data_advanced, sid, years): sid for sid in ids}
        for fut in as_completed(fut_to_sid):
            out[fut_to_sid[fut]] = fut.result()
    return out


# ==========================================
#  6. ML 经济周期识别（HMM + 多因子评分）
# ==========================================
# 【本次修复要点】
#  1) 训练窗口 10 年 → 35 年：10 年样本只覆盖 1 次衰退（2020），
#     HMM 极易退化为单状态解，是"永远走规则模型"的主因之一。
#  2) 协方差从写死 "full" 改为 diag/full 网格 + 多随机种子重启 + min_covar 正则，
#     并剔除"某状态样本数过少"的退化解。
#  3) BIC 公式修正：m.score(X) 本身已是全样本对数似然，原代码又乘了 len(X)；
#     参数量按协方差类型正确计算（full 是 d(d+1)/2 而非 d²）。
#  4) 去掉最外层静默 except：任何降级都在 UI 里写明真实原因。
#  5) NaN 防御：状态画像取均值可能是 NaN，会污染评分。
#
# 特征（HMM 输入 10 维）：
#  INDPRO / CPIAUCSL / PCEPILFE / T10Y2Y / T10Y3M / UNRATE / FEDFUNDS / NFCI
#  + UNRATE_MOM（失业率月变化）+ INDPRO_MOM3 / CPI_MOM3（3 月动量）
_CLOCK_FEATURES = {
    "INDPRO":       ("YoY",   35),
    "CPIAUCSL":     ("Value", 35),
    "PCEPILFE":     ("Value", 35),
    "T10Y2Y":       ("Value", 35),
    "T10Y3M":       ("Value", 35),
    "UNRATE":       ("Value", 35),
    "FEDFUNDS":     ("Value", 35),
    "NFCI":         ("Value", 35),
    "BAMLH0A0HYM2": ("Value", 28),   # 1996-12 起，单独给短窗口
    "USSLIND":      ("Value", 35),
}

# 周期判断锚点
_NEUTRAL_FED_RATE     = 2.75   # FOMC SEP 长期点阵中位数附近
_FED_INFLATION_TARGET = 2.0    # Fed 通胀目标
_GROWTH_TREND         = 1.5    # INDPRO 长期 YoY 趋势线

# HMM 超参
_HMM_MIN_SAMPLES = 120         # 至少 10 年月度样本才允许拟合
_HMM_N_RANGE     = (3, 4, 5, 6)
_HMM_SEEDS       = (42, 7)     # 网格总量 = 4 状态数 × (diag 2 种子 + full 1 种子) = 12 次拟合
_HMM_MAX_ITER    = 300
_BLEND_NOW       = 0.6         # 实时观测权重
_BLEND_STATE     = 0.4         # 隐状态画像权重


def _to_monthly_series(df, col):
    s = df.set_index("Date")[col].copy()
    s.index = pd.to_datetime(s.index)
    return s.resample("MS").last()


def _num(v):
    """把 NaN / inf / pandas NA 统一成 None，避免污染评分。"""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if np.isfinite(f) else None


def _classify_phase(profile: dict):
    """
    多因子评分版周期分类。
    profile 字段（缺失即不参与该维度评分）：
      g, g_mom            - 增长水平 / 近 3 月动量(pp)
      i, i_core, i_mom    - CPI / 核心PCE / 通胀近 3 月动量(pp)
      spread2, spread3    - 10Y-2Y / 10Y-3M 利差(%)
      ff                  - 联邦基金利率(%)
      ur, ur_sahm         - 失业率 / Sahm 规则值(pp)
      nfci, hy, lei       - 金融条件 / HY OAS / 领先指标
    返回: (phase, description, color, assets, breakdown)
    """
    p = {k: _num(v) for k, v in profile.items()}

    g       = p.get("g")
    g_mom   = p.get("g_mom") or 0.0
    i       = p.get("i")
    i_core  = p.get("i_core")
    i_mom   = p.get("i_mom") or 0.0
    spread2 = p.get("spread2")
    spread3 = p.get("spread3")
    ff      = p.get("ff")
    ur_sahm = p.get("ur_sahm") or 0.0
    nfci    = p.get("nfci") or 0.0
    hy      = p.get("hy")
    lei     = p.get("lei")

    # 优先使用核心通胀（Fed 实际锚定），否则退化到 CPI
    i_eff = i_core if i_core is not None else i

    # ── 1. 各维度连续打分 (-1..+1) ──
    if g is not None:
        g_score = max(-1.0, min(1.0, (g - _GROWTH_TREND) / 3.0))
        g_score += max(-0.3, min(0.3, g_mom / 3.0))
        g_score = max(-1.0, min(1.0, g_score))
    else:
        g_score = 0.0

    if i_eff is not None:
        i_score = max(-1.0, min(1.0, (i_eff - _FED_INFLATION_TARGET) / 2.0))
        i_score += max(-0.3, min(0.3, i_mom / 1.5))
        i_score = max(-1.0, min(1.0, i_score))
    else:
        i_score = 0.0

    if ff is not None:
        policy_score = max(-1.0, min(1.0, (ff - _NEUTRAL_FED_RATE) / 2.5))
    else:
        policy_score = 0.0

    # ── 2. 衰退预警 / 复苏信号（多源加总） ──
    recession_alarm = 0.0
    if spread3 is not None and spread3 < 0:    recession_alarm += 0.4
    if spread2 is not None and spread2 < 0:    recession_alarm += 0.2
    if ur_sahm >= 0.5:                          recession_alarm += 0.6   # Sahm 规则触发
    elif ur_sahm >= 0.3:                        recession_alarm += 0.3
    if hy is not None and hy > 6.0:            recession_alarm += 0.3
    if nfci > 0.3:                              recession_alarm += 0.2
    recession_alarm = min(1.0, recession_alarm)

    recovery_signal = 0.0
    if g_mom > 1.0:                             recovery_signal += 0.4
    if i_mom < -0.3:                            recovery_signal += 0.2
    if lei is not None and lei > 0:            recovery_signal += 0.2
    if nfci < -0.2:                             recovery_signal += 0.2
    if policy_score < -0.2:                     recovery_signal += 0.2
    recovery_signal = min(1.0, recovery_signal)

    # 通胀粘性：>3% 且未明显回落 → 滞胀风险
    sticky_inflation = 0.0
    if i_eff is not None and i_eff > _FED_INFLATION_TARGET + 1.0:
        sticky_inflation = (i_eff - _FED_INFLATION_TARGET - 1.0) / 2.0
        if i_mom > -0.3:
            sticky_inflation *= 1.3

    # ── 3. 各阶段似然评分 ──
    scores = {}
    scores["衰退"] = (
        max(0, -g_score) * 1.2
        + recession_alarm * 1.5
        + (0.5 if ur_sahm >= 0.5 else 0)
        - recovery_signal * 0.6
    )
    scores["滞胀"] = (
        max(0, -g_score) * 0.8
        + max(0, sticky_inflation) * 1.5
        - recession_alarm * 0.4
    )
    scores["修复"] = (
        recovery_signal * 1.3
        + max(0, -i_score) * 0.6
        + (0.5 if g_mom > 0.5 and ff is not None and ff < _NEUTRAL_FED_RATE else 0)
        - recession_alarm * 0.5
        - max(0, sticky_inflation) * 0.8
    )
    scores["复苏"] = (
        max(0, g_score) * 1.2
        + (0.4 if i_eff is not None and i_eff < _FED_INFLATION_TARGET + 1.0 else 0)
        + (0.3 if g_mom > 0 else 0)
        - max(0, sticky_inflation) * 0.6
        - recession_alarm * 0.8
    )
    scores["过热"] = (
        max(0, g_score) * 0.8
        + max(0, i_score) * 1.3
        + (0.3 if i_mom > 0 else 0)
        + (0.3 if policy_score > 0.2 else 0)
        - recession_alarm * 0.6
    )

    phase = max(scores, key=scores.get)
    sorted_scores = sorted(scores.values(), reverse=True)
    margin = sorted_scores[0] - sorted_scores[1] if len(sorted_scores) > 1 else 1.0

    # ── 4. 阶段元数据 + 细分描述 ──
    phase_meta = {
        "复苏": ("📈 增长向上 / 通胀温和",   "#4caf8a", "股票 > 大宗 > 现金 > 债券"),
        "过热": ("🔥 增长强劲 / 通胀升温",   "#e07a5f", "大宗 > 股票 > 现金 > 债券"),
        "滞胀": ("☁️ 增长放缓 / 通胀粘性",   "#f2a65a", "现金 > 大宗 > 黄金 > 股债"),
        "衰退": ("🥶 增长收缩 / 风险偏好下行","#6a9fd8", "长债 > 现金 > 黄金 > 股票"),
        "修复": ("🌱 增长触底 / 通胀回落",   "#5abcb0", "长债 > 股票 > 现金 > 大宗"),
    }
    desc, color, assets = phase_meta[phase]

    if phase == "复苏" and g_mom > 1.0 and policy_score > 0:
        desc = "📈 中后期扩张 / 政策开始收敛"
    elif phase == "复苏" and g_mom > 0.5 and policy_score < -0.1:
        desc = "🌅 早期复苏 / 政策仍宽松"
    elif phase == "衰退" and ur_sahm >= 0.5:
        desc = "🥶 Sahm 规则触发 / 衰退确认"
    elif phase == "衰退" and recession_alarm > 0.7:
        desc = "🥶 多重预警 / 深度衰退"
    elif phase == "过热" and policy_score > 0.4:
        desc = "🔥 高位过热 / 政策深度紧缩"
    elif phase == "滞胀" and i_mom > 0.3:
        desc = "☁️ 通胀再加速 / 滞胀风险升温"
    elif phase == "修复" and recovery_signal > 0.7:
        desc = "🌱 修复加速 / 接近转入复苏"

    breakdown = {
        "scores":          {k: round(v, 3) for k, v in scores.items()},
        "g_score":         round(g_score, 3),
        "i_score":         round(i_score, 3),
        "policy_score":    round(policy_score, 3),
        "recession_alarm": round(recession_alarm, 3),
        "recovery_signal": round(recovery_signal, 3),
        "margin":          round(margin, 3),
    }
    return phase, desc, color, assets, breakdown


def _compute_sahm(unrate_series: pd.Series) -> float:
    """
    Sahm 规则：近 3 月失业率均值 - 过去 12 月内 3 月滚动均值的最低值。
    达到或超过 0.5pp 即为历史强衰退信号。
    """
    s = unrate_series.dropna()
    if len(s) < 15:
        return 0.0
    cur3 = float(s.iloc[-3:].mean())
    prev = s.iloc[-15:-3]
    rolling3 = prev.rolling(3).mean().dropna()
    if rolling3.empty:
        return 0.0
    return cur3 - float(rolling3.min())


def _n_params(n_states: int, n_dim: int, cov_type: str) -> float:
    """HMM 自由参数个数（用于 BIC）。"""
    trans = n_states * (n_states - 1)
    start = n_states - 1
    means = n_states * n_dim
    if cov_type == "full":
        cov = n_states * n_dim * (n_dim + 1) / 2
    elif cov_type == "spherical":
        cov = n_states
    else:                       # diag / tied 近似
        cov = n_states * n_dim
    return trans + start + means + cov


def _fit_best_hmm(X: np.ndarray):
    """
    在 (状态数 × 协方差类型 × 随机种子) 网格上拟合，按 BIC 选最优。
    返回 (best_dict, errors)；best_dict 为 None 表示全部失败。
    """
    best, best_any = None, None
    errors = []

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for n in _HMM_N_RANGE:
            for cov_type in ("diag", "full"):
                # full 协方差参数量 ~d²，状态数大时既慢又容易奇异 → 只在 n≤4 试一个种子
                if cov_type == "full" and n > 4:
                    continue
                seeds = _HMM_SEEDS if cov_type == "diag" else _HMM_SEEDS[:1]
                for seed in seeds:
                    try:
                        m = GaussianHMM(
                            n_components=n,
                            covariance_type=cov_type,
                            n_iter=_HMM_MAX_ITER,
                            tol=1e-4,
                            random_state=seed,
                            min_covar=1e-3,     # 正则，防协方差奇异
                        )
                        m.fit(X)
                        ll = float(m.score(X))
                        if not np.isfinite(ll):
                            continue
                        bic = -2 * ll + _n_params(n, X.shape[1], cov_type) * np.log(len(X))
                        cand = {"model": m, "bic": bic, "n": n,
                                "cov": cov_type, "seed": seed, "ll": ll}

                        if best_any is None or bic < best_any["bic"]:
                            best_any = cand

                        # 剔除退化解：任一状态样本数过少
                        counts = np.bincount(m.predict(X), minlength=n)
                        if counts.min() < max(6, int(len(X) * 0.02)):
                            continue
                        if best is None or bic < best["bic"]:
                            best = cand
                    except Exception as ex:      # noqa: BLE001
                        errors.append(f"n={n}/{cov_type}/seed={seed} → {type(ex).__name__}")

    return (best or best_any), errors


@st.cache_data(ttl=3600 * 6, show_spinner=False)
def calculate_ml_investment_clock():
    """
    周期识别流程：
      10 维宏观特征 → 月度对齐 → 标准化 → HMM（BIC 网格选型）
      → 当前隐状态画像 × 实时观测融合 → 多因子评分 → 阶段
    输出：phase, desc, color, assets, full_note, used_ml, confidence, history_df
    """
    # ── 1. 并行拉取特征 ──
    raw, sids = {}, list(_CLOCK_FEATURES.keys())
    with ThreadPoolExecutor(max_workers=len(sids)) as ex:
        fut = {ex.submit(fetch_data_advanced, sid, _CLOCK_FEATURES[sid][1]): sid
               for sid in sids}
        for f in as_completed(fut):
            raw[fut[f]] = f.result()

    core_required = ("INDPRO", "CPIAUCSL", "UNRATE", "FEDFUNDS", "T10Y2Y")
    missing_core = [s for s in core_required if raw.get(s, pd.DataFrame()).empty]
    if missing_core:
        return _fallback_rule_clock(
            raw.get("INDPRO", pd.DataFrame()),
            raw.get("CPIAUCSL", pd.DataFrame()),
            reason=f"FRED 核心序列拉取失败：{', '.join(missing_core)}（检查网络 / API Key 配额）",
        )

    # ── 2. 月度对齐 + 派生动量特征 ──
    series_list = []
    for sid in sids:
        df = raw.get(sid, pd.DataFrame())
        if df.empty:
            continue
        col = _CLOCK_FEATURES[sid][0]
        s = _to_monthly_series(df, col); s.name = sid
        series_list.append(s)
    combined = pd.concat(series_list, axis=1).sort_index()

    if "UNRATE" in combined:
        combined["UNRATE_MOM"] = combined["UNRATE"].diff(1)
    if "INDPRO" in combined:
        combined["INDPRO_MOM3"] = combined["INDPRO"].diff(3)
    if "CPIAUCSL" in combined:
        combined["CPI_MOM3"] = combined["CPIAUCSL"].diff(3)

    # ── 3. 当前实时 profile（先算，任何降级路径都能用上） ──
    core_in = [c for c in core_required if c in combined.columns]
    full = combined.dropna(subset=core_in)
    if full.empty:
        return _fallback_rule_clock(
            raw["INDPRO"], raw["CPIAUCSL"],
            reason="核心序列月度对齐后无重叠样本（可能是 FRED 发布延迟）",
        )
    latest = full.iloc[-1]
    prev3  = full.iloc[-4] if len(full) >= 4 else latest

    def _safe(col):
        return _num(latest.get(col)) if col in combined.columns else None

    profile_now = {
        "g":       _num(latest["INDPRO"]),
        "g_mom":   _num(latest["INDPRO"] - prev3["INDPRO"]),
        "i":       _num(latest["CPIAUCSL"]),
        "i_core":  _safe("PCEPILFE"),
        "i_mom":   _num(latest["CPIAUCSL"] - prev3["CPIAUCSL"]),
        "spread2": _num(latest["T10Y2Y"]),
        "spread3": _safe("T10Y3M"),
        "ff":      _num(latest["FEDFUNDS"]),
        "ur":      _num(latest["UNRATE"]),
        "ur_sahm": _compute_sahm(combined["UNRATE"]) if "UNRATE" in combined else 0.0,
        "nfci":    _safe("NFCI") or 0.0,
        "hy":      _safe("BAMLH0A0HYM2"),
        "lei":     _safe("USSLIND"),
    }
    obs_date = pd.to_datetime(full.index[-1]).strftime("%Y-%m")

    # ── 4. 依赖检查 ──
    if GaussianHMM is None:
        return _fallback_rule_clock(
            raw["INDPRO"], raw["CPIAUCSL"],
            reason=f"hmmlearn 不可用 → `{_HMM_IMPORT_ERR}`",
            profile_override=profile_now,
        )

    # ── 5. HMM 特征矩阵 ──
    hmm_cols = [c for c in ("INDPRO", "INDPRO_MOM3", "CPIAUCSL", "CPI_MOM3",
                            "UNRATE", "UNRATE_MOM", "FEDFUNDS",
                            "T10Y2Y", "T10Y3M", "NFCI")
                if c in combined.columns]
    hmm_data = combined[hmm_cols].dropna()
    if len(hmm_data) < _HMM_MIN_SAMPLES:
        return _fallback_rule_clock(
            raw["INDPRO"], raw["CPIAUCSL"],
            reason=(f"HMM 训练样本仅 {len(hmm_data)} 个月（要求 ≥ {_HMM_MIN_SAMPLES}）；"
                    f"当前特征集：{', '.join(hmm_cols)}"),
            profile_override=profile_now,
        )

    X_raw = hmm_data.values.astype(float)
    if StandardScaler is not None:
        X = StandardScaler().fit_transform(X_raw)
    else:
        mu, sigma = X_raw.mean(0), X_raw.std(0)
        sigma[sigma == 0] = 1.0
        X = (X_raw - mu) / sigma

    # ── 6. 网格拟合 ──
    best, fit_errors = _fit_best_hmm(X)
    if best is None:
        return _fallback_rule_clock(
            raw["INDPRO"], raw["CPIAUCSL"],
            reason=("HMM 全部拟合失败：" + ("；".join(fit_errors[:3]) if fit_errors else "未知原因")),
            profile_override=profile_now,
        )

    model, best_n, best_cov = best["model"], best["n"], best["cov"]
    states = model.predict(X)
    hmm_df = hmm_data.copy()
    hmm_df["state"] = states

    # ── 7. 各隐状态宏观画像（含状态内动量） ──
    state_profiles = {}
    for s in range(best_n):
        mask = hmm_df["state"] == s
        if mask.sum() < 2:
            continue
        sub = combined.loc[hmm_df.index[mask]]
        if len(sub) >= 6:
            g_mom_s = _num(sub["INDPRO"].iloc[-3:].mean()   - sub["INDPRO"].iloc[:3].mean()) or 0.0
            i_mom_s = _num(sub["CPIAUCSL"].iloc[-3:].mean() - sub["CPIAUCSL"].iloc[:3].mean()) or 0.0
        else:
            g_mom_s = i_mom_s = 0.0
        state_profiles[s] = {
            "g":       _num(sub["INDPRO"].mean()),
            "g_mom":   g_mom_s,
            "i":       _num(sub["CPIAUCSL"].mean()),
            "i_core":  _num(sub["PCEPILFE"].mean()) if "PCEPILFE" in sub else None,
            "i_mom":   i_mom_s,
            "spread2": _num(sub["T10Y2Y"].mean()),
            "spread3": _num(sub["T10Y3M"].mean()) if "T10Y3M" in sub else None,
            "ff":      _num(sub["FEDFUNDS"].mean()),
            "ur":      _num(sub["UNRATE"].mean()),
            "nfci":    _num(sub["NFCI"].mean()) if "NFCI" in sub else 0.0,
            "hy":      _num(sub["BAMLH0A0HYM2"].mean()) if "BAMLH0A0HYM2" in sub else None,
            "lei":     _num(sub["USSLIND"].mean()) if "USSLIND" in sub else None,
            "count":   int(mask.sum()),
        }

    current_state = int(states[-1])
    state_prof    = state_profiles.get(current_state, {})
    hmm_date      = pd.to_datetime(hmm_df.index[-1]).strftime("%Y-%m")

    # ── 8. 实时 60% × 状态画像 40% 融合 ──
    blended = {}
    for k in ("g", "g_mom", "i", "i_core", "i_mom",
              "spread2", "spread3", "ff", "ur", "nfci", "hy", "lei"):
        vn, vs = _num(profile_now.get(k)), _num(state_prof.get(k))
        if vn is None and vs is None:
            blended[k] = None
        elif vn is None:
            blended[k] = vs
        elif vs is None:
            blended[k] = vn
        else:
            blended[k] = _BLEND_NOW * vn + _BLEND_STATE * vs
    blended["ur_sahm"] = profile_now["ur_sahm"]   # Sahm 永远用最新

    phase, desc, color, assets, breakdown = _classify_phase(blended)

    # ── 9. 综合置信度 ──
    posteriors  = model.predict_proba(X)
    hmm_conf    = float(posteriors[-1, current_state])
    top2_idx    = int(np.argsort(posteriors[-1])[-2]) if best_n > 1 else current_state
    top2_conf   = float(posteriors[-1, top2_idx])
    margin_conf = min(1.0, max(0.0, breakdown["margin"]) / 2.0 + 0.5)
    confidence  = (hmm_conf * 0.6 + margin_conf * 0.4) * 100

    # ── 10. 历史阶段映射（状态号随机，映射到阶段后颜色才稳定） ──
    phase_map = {}
    for s, p in state_profiles.items():
        ph, _, col_, _, _ = _classify_phase(p)
        phase_map[s] = (ph, _phase_color(ph))
    history = hmm_df[["state"]].copy()
    history["phase_name"]  = history["state"].map(lambda s: phase_map.get(s, ("未知", "#aaaaaa"))[0])
    history["phase_color"] = history["state"].map(lambda s: phase_map.get(s, ("未知", "#aaaaaa"))[1])

    # ── 11. 说明文本 ──
    i_core_str = f"{profile_now['i_core']:.2f}%" if profile_now['i_core'] is not None else "—"
    sp3_str    = f"{profile_now['spread3']:+.2f}%" if profile_now['spread3'] is not None else "—"
    hy_part    = f" ｜ HY OAS {profile_now['hy']:.2f}%" if profile_now['hy'] is not None else ""
    scores_line = "  ｜  ".join(
        (f"**{k} {v:+.2f}**" if k == phase else f"{k} {v:+.2f}")
        for k, v in breakdown["scores"].items()
    )
    state_map_line = "、".join(
        f"#{s}→{phase_map.get(s, ('未知',''))[0]}({p['count']}月)"
        for s, p in sorted(state_profiles.items())
    )
    note_lines = [
        f"**模型**：Gaussian HMM ｜ BIC 最优 = **{best_n} 状态 / {best_cov} 协方差**"
        f"（训练样本 {len(hmm_data)} 个月，{pd.to_datetime(hmm_data.index[0]).strftime('%Y-%m')} 起，"
        f"{len(hmm_cols)} 维特征，logL {best['ll']:.1f}，BIC {best['bic']:.1f}）",
        f"**当前隐状态**：#{current_state}（历史出现 {state_prof.get('count','?')} 月，数据截至 {hmm_date}）"
        f" ｜ 次高状态 #{top2_idx} 后验 {top2_conf*100:.1f}%",
        f"**状态→阶段映射**：{state_map_line}",
        f"**置信度**：{confidence:.1f}% "
        f"（HMM 后验 {hmm_conf*100:.1f}% × 评分边际 {breakdown['margin']:.2f}）",
        "",
        f"**当前宏观画像**（观测截至 {obs_date}）：",
        f"- 增长：INDPRO 同比 **{profile_now['g']:.2f}%**（近 3 月动量 {profile_now['g_mom']:+.2f}pp）",
        f"- 通胀：CPI {profile_now['i']:.2f}% ｜ 核心 PCE {i_core_str}（近 3 月动量 {profile_now['i_mom']:+.2f}pp）",
        f"- 利率：FFR **{profile_now['ff']:.2f}%**（中性 {_NEUTRAL_FED_RATE:.2f}%）"
        f" ｜ 10Y-2Y {profile_now['spread2']:+.2f}% ｜ 10Y-3M {sp3_str}",
        f"- 就业：失业率 {profile_now['ur']:.1f}% ｜ Sahm 规则 **{profile_now['ur_sahm']:+.2f}pp**（≥0.50 触发衰退）",
        f"- 金融：NFCI {profile_now['nfci']:+.3f}（负=宽松）{hy_part}",
        "",
        f"**多因子评分**（实时 {_BLEND_NOW:.0%} × 状态画像 {_BLEND_STATE:.0%}）："
        f"增长 {breakdown['g_score']:+.2f} ｜ 通胀 {breakdown['i_score']:+.2f}"
        f" ｜ 政策 {breakdown['policy_score']:+.2f}"
        f" ｜ 衰退预警 {breakdown['recession_alarm']:.2f}"
        f" ｜ 复苏信号 {breakdown['recovery_signal']:.2f}",
        f"**阶段似然**：{scores_line}",
    ]
    if fit_errors:
        note_lines.append(
            f"\n<small>拟合网格中 {len(fit_errors)} 组失败（已自动跳过）："
            f"{'；'.join(fit_errors[:3])}</small>"
        )
    note_lines.append("\n_本判断由 HMM 隐状态识别 + 多因子评分两步生成，仅作研究参考，非投资建议。_")
    full_note = "\n".join(note_lines)

    return phase, desc, color, assets, full_note, True, confidence, history


def _fallback_rule_clock(growth_df, cpi_df, reason="", profile_override=None):
    """规则模型兜底。reason 会原样展示在 UI 上，便于定位为什么没走 HMM。"""
    if profile_override is not None:
        profile = dict(profile_override)
    else:
        if growth_df.empty or cpi_df.empty or len(growth_df) < 4 or len(cpi_df) < 4:
            note = f"**⚠️ 降级为规则模型**\n\n原因：{reason or '核心数据不足'}"
            return ("数据不足", "🔧 无法计算", "#aaaaaa",
                    "保持现金", note, False, 0.0, None)
        g_now  = _num(growth_df["YoY"].iloc[-1]) or 0.0
        g_prev = _num(growth_df["YoY"].iloc[-4]) if len(growth_df) >= 4 else g_now
        i_now  = _num(cpi_df["Value"].iloc[-1]) or 0.0
        i_prev = _num(cpi_df["Value"].iloc[-4]) if len(cpi_df) >= 4 else i_now
        profile = {
            "g": g_now, "g_mom": g_now - (g_prev or g_now),
            "i": i_now, "i_mom": i_now - (i_prev or i_now),
            "spread2": 0.5, "ff": 3.0,
        }

    phase, desc, color, assets, breakdown = _classify_phase(profile)
    scores_line = "  ｜  ".join(
        (f"**{k} {v:+.2f}**" if k == phase else f"{k} {v:+.2f}")
        for k, v in breakdown["scores"].items()
    )
    note = "\n".join([
        "**⚠️ 未使用 HMM，已降级为多因子规则评分**",
        "",
        f"**降级原因**：{reason or '未知'}",
        "",
        f"**依赖状态**：hmmlearn `{_HMM_IMPORT_ERR or '正常'}` ｜ scikit-learn `{_SK_IMPORT_ERR or '正常'}`",
        "",
        f"**规则评分结果**：{scores_line}",
        "",
        "_规则模型仅用增长/通胀/政策三维打分，不含隐状态信息，稳定性弱于 HMM。_",
    ])
    return phase, desc, color, assets, note, False, 0.0, None


# ==========================================
#  6b. 相位颜色辅助
# ==========================================
def _phase_color(phase: str) -> str:
    if "复苏" in phase: return "#4caf8a"
    if "过热" in phase: return "#e07a5f"
    if "滞胀" in phase: return "#f2a65a"
    if "修复" in phase: return "#5abcb0"
    if "衰退" in phase: return "#6a9fd8"
    return "#3a8a6e"


# ==========================================
#  6c. 联储 RSS（刷新周期：8 小时）
# ==========================================
_ET = ZoneInfo("America/New_York")
_FED_RSS_UA = {"User-Agent": "Mozilla/5.0 (compatible; MacroTrack/2.0)"}
_FED_RSS_ALL = "https://www.federalreserve.gov/feeds/speeches_and_testimony.xml"
_FED_BOARD_RSS = [
    ("Jerome H. Powell",     "https://www.federalreserve.gov/feeds/s_t_powell.xml"),
    ("Philip N. Jefferson",  "https://www.federalreserve.gov/feeds/s_t_jefferson.xml"),
    ("Michelle W. Bowman",   "https://www.federalreserve.gov/feeds/m_w_Bowman.xml"),
    ("Lisa D. Cook",         "https://www.federalreserve.gov/feeds/s_t_cook.xml"),
    ("Christopher J. Waller","https://www.federalreserve.gov/feeds/s_t_waller.xml"),
]

# 讲话刷新间隔：5 分钟 → 8 小时（缓存 TTL 与前端自动重跑周期保持一致）
_FED_REFRESH_SECONDS = 8 * 60 * 60
_FED_REFRESH_LABEL   = "每 8 小时自动刷新"


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
            pass
    return 0.0


def _parse_feed_entries(parsed):
    rows = []
    for e in getattr(parsed, "entries", []) or []:
        link = (e.get("link") or "").strip()
        if not link:
            continue
        rows.append({
            "title":   (e.get("title") or "（无标题）").strip(),
            "link":    link,
            "ts":      _entry_ts(e),
            "summary": _strip_html(e.get("summary", ""))[:400],
        })
    return rows


@st.cache_data(ttl=_FED_REFRESH_SECONDS)
def fetch_fed_speech_feeds():
    """返回 (rows, error_str, fetched_at_epoch)。fetched_at 是真实抓取时间，非渲染时间。"""
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

    with ThreadPoolExecutor(max_workers=min(8, len(_FED_BOARD_RSS))) as ex:
        futs = {ex.submit(load_one, name, url): name for name, url in _FED_BOARD_RSS}
        for fut in as_completed(futs):
            batch, err = fut.result()
            if err:
                errors.append(err)
            for row in batch:
                if row["link"] not in merged:
                    merged[row["link"]] = row

    rows = sorted(merged.values(), key=lambda r: r["ts"], reverse=True)
    if len(rows) < 3:
        try:
            parsed = feedparser.parse(_FED_RSS_ALL, request_headers=_FED_RSS_UA)
            seen = {r["link"] for r in rows}
            for row in sorted(_parse_feed_entries(parsed),
                              key=lambda r: r["ts"], reverse=True):
                if row["link"] not in seen:
                    row["speaker"] = "（聚合源）"
                    rows.append(row)
                    seen.add(row["link"])
            rows.sort(key=lambda r: r["ts"], reverse=True)
        except Exception as ex:
            errors.append(f"聚合源失败: {ex}")

    return rows, ("; ".join(errors[:2]) if errors else None), time.time()


# ==========================================
#  6d. 宏观事件倒计时
# ==========================================
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
    r = now_utc.astimezone(_ET); y, m = r.year, r.month
    for _ in range(28):
        fd = _first_friday(y, m)
        t = datetime(fd.year, fd.month, fd.day, 8, 30, tzinfo=_ET)
        if t.astimezone(timezone.utc) > now_utc:
            return t
        y, m = _add_month(y, m)


def _next_cpi(now_utc):
    r = now_utc.astimezone(_ET); y, m = r.year, r.month
    for _ in range(28):
        d2 = _second_tuesday(y, m)
        t = datetime(d2.year, d2.month, d2.day, 8, 30, tzinfo=_ET)
        if t.astimezone(timezone.utc) > now_utc:
            return t
        y, m = _add_month(y, m)


def _next_fomc(now_utc):
    for y, mo, d in _FOMC_DATES:
        t = datetime(y, mo, d, 14, 0, tzinfo=_ET)
        if t.astimezone(timezone.utc) > now_utc:
            return t


def _fmt_countdown(rem):
    if rem.total_seconds() <= 0:
        return "已到发布窗口"
    total = int(rem.total_seconds())
    d, r = divmod(total, 86400); h, r = divmod(r, 3600); m, s = divmod(r, 60)
    return f"{d}天 {h:02d}:{m:02d}:{s:02d}" if d > 0 else f"{h:02d}:{m:02d}:{s:02d}"


def _macro_countdown_strip_body():
    now_utc = datetime.now(timezone.utc)
    events = [
        ("📊 美国 CPI",      _next_cpi(now_utc),  "BLS 第二个周二 8:30 ET"),
        ("💼 非农就业 NFP",  _next_nfp(now_utc),  "BLS 当月首个周五 8:30 ET"),
        ("🏦 FOMC 利率决议", _next_fomc(now_utc), "联储声明约 14:00 ET"),
    ]
    c1, c2, c3 = st.columns(3)
    for col, (title, target, note) in zip((c1, c2, c3), events):
        with col:
            if target is None:
                st.markdown(
                    f'<div class="countdown-card"><p class="cd-note">{note}</p>'
                    f'<p class="cd-title">{title}</p><p class="cd-value">—</p></div>',
                    unsafe_allow_html=True,
                )
                continue
            rem = target.astimezone(timezone.utc) - now_utc
            urgent = timedelta(0) < rem < timedelta(hours=24)
            cls = "countdown-card urgent" if urgent else "countdown-card"
            st.markdown(
                f'<div class="{cls}">'
                f'<p class="cd-note">{note}</p>'
                f'<p class="cd-title">{title}</p>'
                f'<p class="cd-value">{_fmt_countdown(rem)}</p>'
                f'<p class="cd-when">发布（ET）{target.strftime("%Y-%m-%d %H:%M")}</p>'
                f'</div>',
                unsafe_allow_html=True,
            )


_macro_countdown_strip = (
    st.fragment(run_every=timedelta(seconds=5))(_macro_countdown_strip_body)
    if hasattr(st, "fragment") else _macro_countdown_strip_body
)


# ==========================================
#  7. 图表风格与智能渲染引擎
# ==========================================
FRESH_COLORS = {
    "primary":   "#3a8a6e",
    "secondary": "#6a9fd8",
    "accent":    "#e07a5f",
    "warm":      "#f2a65a",
    "purple":    "#9b88c4",
    "teal":      "#5abcb0",
    "rose":      "#d4727a",
    "olive":     "#8aaa5a",
    "palette":   ["#3a8a6e", "#6a9fd8", "#e07a5f", "#f2a65a",
                  "#9b88c4", "#5abcb0", "#d4727a", "#8aaa5a"],
}

_BASE_LAYOUT = dict(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(255,255,255,0.55)',
    font=dict(family="Nunito, sans-serif", color="#2d4a3e", size=11),
    margin=dict(l=10, r=10, t=46, b=10),
    hovermode="x unified",
    hoverlabel=dict(
        bgcolor="rgba(255,255,255,0.94)", bordercolor="#c8e0d8",
        font=dict(family="Nunito", color="#1f3d30", size=12),
    ),
    xaxis=dict(
        showgrid=False, linecolor="#d0e8df", tickcolor="#d0e8df",
        tickfont=dict(size=10, color="#7aad98"),
    ),
    yaxis=dict(
        showgrid=True, gridcolor="rgba(192,220,208,0.45)",
        zeroline=False,
        linecolor="#d0e8df", tickfont=dict(size=10, color="#7aad98"),
    ),
    legend=dict(
        bgcolor="rgba(255,255,255,0.75)", bordercolor="#d0e8df", borderwidth=1,
        font=dict(size=10, color="#2d4a3e"),
    ),
)


def _hex_rgba(hex_color, alpha):
    r, g, b = int(hex_color[1:3], 16), int(hex_color[3:5], 16), int(hex_color[5:7], 16)
    return f"rgba({r},{g},{b},{alpha})"


def render_regime_timeline(history_df, months=180):
    """HMM 历史机制时间轴：每月一根色条，颜色 = 映射后的经济阶段。"""
    if history_df is None or history_df.empty:
        return None
    h = history_df.tail(months).copy()
    h.index = pd.to_datetime(h.index)

    fig = go.Figure()
    for ph in h["phase_name"].unique():
        sub = h[h["phase_name"] == ph]
        fig.add_trace(go.Bar(
            x=sub.index, y=[1] * len(sub),
            name=ph,
            marker_color=sub["phase_color"].iloc[0],
            marker_line_width=0,
            width=1000 * 3600 * 24 * 28,
            hovertemplate="%{x|%Y-%m}<br><b>" + ph + "</b><extra></extra>",
        ))

    layout = dict(**_BASE_LAYOUT)
    layout["barmode"]  = "overlay"
    layout["bargap"]   = 0
    layout["height"]   = 190
    layout["hovermode"] = "closest"
    layout["yaxis"]    = dict(visible=False, range=[0, 1])
    layout["title"]    = dict(
        text=f"<b>HMM 历史机制时间轴</b>  <span style='font-size:10px;color:#9abfb0;'>"
             f"近 {min(months, len(h))} 个月</span>",
        font=dict(size=12, color="#1f3d30", family="Nunito"),
        x=0.01, xanchor="left",
    )
    fig.update_layout(**layout)
    return fig


def render_chart(series_id, metric_name, df, idx):
    """直接从 SERIES_META 读取 chart/unit_str/label，零条件判断、干净分发。
    标题末尾自动附上最新观测日期。"""
    if df.empty:
        return go.Figure()

    meta  = SERIES_META.get(series_id, {"chart": "line", "unit_str": "", "label": metric_name})
    ctype = meta["chart"]
    unit  = meta["unit_str"]
    label = meta["label"]
    color = FRESH_COLORS["palette"][idx % len(FRESH_COLORS["palette"])]

    try:
        last_date = pd.to_datetime(df["Date"].iloc[-1]).strftime("%Y-%m")
    except Exception:
        last_date = ""

    full_title = (
        f"<b>{label}</b>"
        + (f"  <span style='font-size:10px;color:#9abfb0;'>最新: {last_date}</span>"
           if last_date else "")
    )

    # ── 分发渲染 ──
    if ctype == "bar_yoy":
        y      = df["Value"]
        colors = [FRESH_COLORS["primary"] if v >= 0 else FRESH_COLORS["accent"] for v in y]
        fig    = go.Figure(go.Bar(
            x=df["Date"], y=y,
            marker_color=colors, marker_line_width=0, name=label,
            hovertemplate=f"<b>%{{y:.2f}}{unit}</b><extra></extra>",
        ))

    elif ctype == "line_yoy":
        y   = df["Value"]
        fig = go.Figure(go.Scatter(
            x=df["Date"], y=y, mode="lines",
            line=dict(width=2, color=color),
            fill="tozeroy", fillcolor=_hex_rgba(color, 0.10),
            name=label,
            hovertemplate=f"<b>%{{y:.2f}}{unit}</b><extra></extra>",
        ))

    elif ctype == "bar_abs":
        y      = df["Value"]
        colors = [FRESH_COLORS["primary"] if v >= 0 else FRESH_COLORS["accent"] for v in y]
        fig    = go.Figure(go.Bar(
            x=df["Date"], y=y,
            marker_color=colors, marker_line_width=0, name=label,
            hovertemplate=f"<b>%{{y:.2f}}{unit}</b><extra></extra>",
        ))
        if not y.empty and y.min() < 0:
            fig.add_hline(y=0, line_width=1, line_dash="dot",
                          line_color="rgba(200,80,60,0.45)")

    elif ctype == "step":
        fig = go.Figure(go.Scatter(
            x=df["Date"], y=df["Value"], mode="lines",
            line=dict(width=2.5, color=color, shape="hv"),
            fill="tozeroy", fillcolor=_hex_rgba(color, 0.08),
            name=label,
            hovertemplate=f"<b>%{{y:.2f}}{unit}</b><extra></extra>",
        ))

    elif ctype == "spread":
        fig = go.Figure()
        fig.add_hline(y=0, line_width=1.2, line_dash="dot",
                      line_color="rgba(200,80,60,0.55)")
        fig.add_trace(go.Scatter(
            x=df["Date"], y=df["Value"], mode="lines",
            line=dict(width=2, color=color),
            fill="tozeroy", fillcolor=_hex_rgba(color, 0.10),
            name=label,
            hovertemplate=f"<b>%{{y:.3f}}{unit}</b><extra></extra>",
        ))

    else:  # "line" — 原始值面积折线
        fig = go.Figure(go.Scatter(
            x=df["Date"], y=df["Value"], mode="lines",
            line=dict(width=2, color=color),
            fill="tozeroy", fillcolor=_hex_rgba(color, 0.09),
            name=label,
            hovertemplate=f"<b>%{{y:.2f}}{unit}</b><extra></extra>",
        ))

    layout = dict(**_BASE_LAYOUT)
    layout["title"] = dict(
        text=full_title,
        font=dict(size=12, color="#1f3d30", family="Nunito"),
        x=0.01, xanchor="left",
    )
    layout["height"] = 300
    fig.update_layout(**layout)
    return fig


# ==========================================
#  8. UI 主体
# ==========================================

# ── 顶部标题 ──
col_h1, col_h2 = st.columns([4, 1])
with col_h1:
    st.markdown(
        '<div>'
        '<p class="fresh-title">🌿 Macro Track Report·thinking</p>'
        '<p class="fresh-subtitle">美联储政策 · 宏观经济 · 投资时钟  '
        '|  数据来源: Federal Reserve Economic Data</p>'
        '</div>',
        unsafe_allow_html=True,
    )
with col_h2:
    st.markdown('<div style="height:8px;"></div>', unsafe_allow_html=True)
    if st.button("🔄 刷新数据"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

st.markdown("---")


# ── 数据新鲜度自检 ──
@st.cache_data(ttl=43200)
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
        st.info(
            f"📡 数据更新提示：FRED 已发布 GDPC1 至 {fred_latest}，"
            f"当前缓存截止 {local_latest}。点击右上角「🔄 刷新数据」即可获取最新。",
            icon="🔔",
        )


try:
    _show_freshness_banner()
except Exception:
    pass

# ── 预热缓存 ──
try:
    with st.spinner("正在加载宏观数据…"):
        warm_core_series_cache()
except Exception as _e:
    st.warning(f"⚠️ 数据预热未完成（{type(_e).__name__}），页面将按需逐个加载。")

# ── 经济周期（ML） ──
# 任何异常都不允许把整页打成 "Oh no."：出错时降级为规则模型并把 traceback 摆在页面上。
try:
    with st.spinner("正在拟合 HMM 机制模型（首次约 20–60 秒，结果缓存 6 小时）…"):
        (phase, desc, color, assets,
         clock_note, used_ml, confidence, history_df) = calculate_ml_investment_clock()
    _clock_tb = None
except Exception as _e:                       # noqa: BLE001
    _clock_tb = traceback.format_exc()
    phase, desc, color = "数据不足", "🔧 周期模块异常", "#aaaaaa"
    assets, used_ml, confidence, history_df = "保持现金", False, 0.0, None
    clock_note = (f"**❌ 周期模块抛出异常**\n\n`{type(_e).__name__}: {_e}`\n\n"
                  "展开下方 traceback 查看完整调用栈。")

phase_colors = {
    "复苏": "#4caf8a", "过热": "#e07a5f", "滞胀": "#f2a65a",
    "衰退": "#6a9fd8", "修复": "#5abcb0",
}
phase_bg = {
    "复苏": "rgba(76,175,138,0.10)", "过热": "rgba(224,122,95,0.10)",
    "滞胀": "rgba(242,166,90,0.10)",  "衰退": "rgba(106,159,216,0.10)",
    "修复": "rgba(90,188,176,0.10)",
}
phase_key = next((k for k in phase_colors if k in phase), "复苏")

ml_tag   = '🤖 HMM 机器学习' if used_ml else '📐 规则模型（降级）'
conf_str = f"｜置信度 {confidence:.1f}%" if used_ml and confidence > 0 else ""

st.markdown(
    f'<div class="clock-banner" style="background:{phase_bg[phase_key]};'
    f'border-left:6px solid {phase_colors[phase_key]};">'
    f'<span class="clock-tag">{ml_tag}{conf_str}</span>'
    f'<p class="clock-phase" style="color:{phase_colors[phase_key]};">{phase}</p>'
    f'<p class="clock-desc">{desc}</p>'
    f'<p class="clock-assets">建议资产配置顺序：<b>{assets}</b></p>'
    f'</div>',
    unsafe_allow_html=True,
)

# 时钟判断依据展开
with st.expander("📋 周期判断依据", expanded=not used_ml):
    st.markdown(clock_note, unsafe_allow_html=True)
    if _clock_tb:
        st.code(_clock_tb, language="text")
    if not used_ml:
        if _HMM_IMPORT_ERR or _SK_IMPORT_ERR:
            st.warning(
                "依赖缺失或版本不兼容。建议在当前环境执行：\n\n"
                "```bash\n"
                "pip install -U \"hmmlearn>=0.3.2\" \"scikit-learn>=1.3\" \"numpy<2.3\"\n"
                "```\n"
                "（Streamlit Cloud 请把这两行写进 `requirements.txt` 后 Reboot app）"
            )
        else:
            st.info("依赖正常，降级来自数据或拟合环节，具体原因见上方「降级原因」。")

# ── HMM 历史机制时间轴 ──
if used_ml and history_df is not None and not history_df.empty:
    with st.expander("🕰 历史机制时间轴（HMM 隐状态 → 经济阶段）", expanded=False):
        try:
            _tl = render_regime_timeline(history_df)
            if _tl is not None:
                st.plotly_chart(
                    _tl, use_container_width=True,
                    config={"displaylogo": False,
                            "modeBarButtonsToRemove": ["lasso2d", "select2d", "toImage"]},
                )
            st.caption("色块 = 该月所处的 HMM 隐状态映射出的经济阶段；可与下方指标趋势交叉验证。")
        except Exception as _e:
            st.warning(f"时间轴渲染失败：{type(_e).__name__}: {_e}")

# ── 倒计时 ──
st.markdown("#### ⏱ 重要发布倒计时")
_macro_countdown_strip()

st.markdown("---")

# ── 核心指标卡 ──
st.markdown("#### 📌 核心指标速览")
top_metrics = {
    "核心PCE":      ("PCEPILFE", "通胀"),
    "失业率":       ("UNRATE",   "就业"),
    "联邦基金利率": ("FEDFUNDS", "政策"),
    "工业产出":     ("INDPRO",   "增长"),
    "10Y-2Y利差":   ("T10Y2Y",   "衰退预警"),
    "M2同比":       ("M2SL",     "流动性"),
}

# 手机上 st.columns(6) 会挤成一排，改用两行各 3 列
_m_row1 = list(top_metrics.items())[:3]
_m_row2 = list(top_metrics.items())[3:]


def _render_metric_col(col, name, sid, sub):
    df = fetch_data_advanced(sid)
    with col:
        if df.empty:
            return
        latest = df.iloc[-1]
        unit   = UNIT_MAP.get(sid, "")
        if sid in PC1_SERIES:
            val_str   = f"{latest['Value']:.2f}%"
            delta_val = float(latest['Value_Diff']) if not pd.isna(latest['Value_Diff']) else 0
            delta_str = f"{delta_val:+.2f} pts"
            is_inv    = False
        elif sid in ("UNRATE", "FEDFUNDS"):
            val_str   = f"{latest['Value']:.2f}{unit}"
            delta_val = float(latest['Value_Diff']) if not pd.isna(latest['Value_Diff']) else 0
            delta_str = f"{delta_val:+.2f} pts"
            is_inv    = (sid == "UNRATE")
        elif sid in MOM_DIFF_SERIES:
            val_str   = f"{latest['Value']:.0f}{unit}"
            delta_val = float(latest['YoY']) if not pd.isna(latest['YoY']) else 0
            delta_str = f"{delta_val:+.0f} 千 MoM"
            is_inv    = False
        else:
            val_str   = f"{latest['Value']:.2f}{unit}"
            delta_val = float(latest['YoY']) if not pd.isna(latest['YoY']) else 0
            delta_str = f"{delta_val:+.1f}% YoY"
            is_inv    = False

        dc = "#e07a5f" if (delta_val > 0) == is_inv else "#4caf8a"
        arrow = "▲" if delta_val > 0 else "▼"
        st.markdown(f"""
        <div class="metric-card">
          <p class="metric-label">{sub}</p>
          <p class="metric-name">{name}</p>
          <p class="metric-value">{val_str}</p>
          <p class="metric-delta" style="color:{dc};">{arrow} {delta_str}</p>
        </div>
        """, unsafe_allow_html=True)


for row in [_m_row1, _m_row2]:
    cols = st.columns(3)
    for c, (name, (sid, sub)) in zip(cols, row):
        _render_metric_col(c, name, sid, sub)

st.markdown("---")

# ── 联储官员讲话（每 8 小时刷新） ──
st.markdown("#### 🏛 美联储官员最新讲话")

# 筛选控件在 fragment 外，避免自动刷新重置用户输入
_col_news, _col_filter = st.columns([3, 1])

with _col_filter:
    st.markdown(
        '<p style="margin-top:18px;font-weight:700;color:#5a8070;">🔍 筛选条件</p>',
        unsafe_allow_html=True,
    )
    only_personal = st.checkbox(
        "仅理事个人源", value=True,
        help="取消可包含地区联储主席等聚合源",
    )
    speech_q = st.text_input("标题关键词", "", placeholder="Powell / Inflation…")
    if st.button("↻ 立即重新拉取"):
        fetch_fed_speech_feeds.clear()
        st.rerun()


def _fed_news_body():
    try:
        _fed_rows, _err, _fetched_at = fetch_fed_speech_feeds()
    except Exception as _e:
        with _col_news:
            st.warning(f"联储 RSS 拉取异常：{type(_e).__name__}: {_e}")
        return
    fetched_str = datetime.fromtimestamp(_fetched_at).strftime("%m-%d %H:%M")
    next_str = datetime.fromtimestamp(
        _fetched_at + _FED_REFRESH_SECONDS
    ).strftime("%m-%d %H:%M")

    with _col_news:
        st.caption(
            f"🔄 {_FED_REFRESH_LABEL} ｜ 数据抓取于 {fetched_str} ｜ 下次自动更新 {next_str}"
        )

        if not _fed_rows:
            st.warning("暂无法拉取联储 RSS，请检查网络后点击「↻ 立即重新拉取」重试。")
            return

        filtered = [
            r for r in _fed_rows
            if (not only_personal or r.get("speaker") != "（聚合源）")
            and (not speech_q or speech_q.lower() in r["title"].lower())
        ]

        if not filtered:
            st.info("当前筛选条件下无条目，可放宽关键词或取消「仅理事个人源」。")
            return

        st.caption(f"共 **{len(filtered)}** 条，展示最新 5 条")
        for row in filtered[:5]:
            ts = row["ts"]
            dstr = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d") if ts > 0 else "—"
            speaker = row.get("speaker", "")
            summary_html = (
                f"<p style='margin:6px 0 0; color:#5a8070; font-size:0.82rem; line-height:1.5;'>"
                f"{row['summary'][:220]}…</p>"
                if row.get("summary") else ""
            )
            st.markdown(f"""
            <div class="fresh-card" style="margin-bottom:10px;">
              <p style="margin:0 0 4px; color:#7aad98; font-size:0.78rem; font-weight:600;">
                📅 {dstr} &nbsp;·&nbsp; {speaker}
              </p>
              <p style="margin:0; font-weight:700; font-size:0.97rem;">
                <a href="{row['link']}" target="_blank"
                   style="color:#1e6e50; text-decoration:none;">
                  {row['title']}
                </a>
              </p>
              {summary_html}
            </div>
            """, unsafe_allow_html=True)

        if len(filtered) > 5:
            with st.expander(f"查看更多（{len(filtered) - 5} 条）"):
                for row in filtered[5:]:
                    ts = row["ts"]
                    dstr = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d") if ts > 0 else "—"
                    st.markdown(
                        f"**{dstr}** · `{row.get('speaker','')}` — "
                        f"[{row['title']}]({row['link']})"
                    )


if hasattr(st, "fragment"):
    _fed_news_fragment = st.fragment(
        run_every=timedelta(seconds=_FED_REFRESH_SECONDS)
    )(_fed_news_body)
    _fed_news_fragment()
else:
    _fed_news_body()

st.markdown("---")

# ── 全量指标图表 ──
st.markdown("#### 📈 宏观指标趋势图（过去 5 年）")
_cat_keys = list(FRED_CATEGORIES.keys())
_selected_cat = st.radio("指标分类", _cat_keys, horizontal=True, key="chart_cat")
st.caption("切换分类后仅加载该分类数据，已加载数据自动缓存复用。")

with st.spinner("加载图表数据中…"):
    _cat_dfs = load_category_parallel(_selected_cat)

metrics_dict = FRED_CATEGORIES[_selected_cat]

chart_cols = st.columns(2)

for idx, (metric_name, series_id) in enumerate(metrics_dict.items()):
    df = _cat_dfs.get(series_id, pd.DataFrame())
    col = chart_cols[idx % 2]
    if df.empty:
        col.warning(f"⚠️ {metric_name} 数据获取失败")
        continue
    try:
        fig = render_chart(series_id, metric_name, df, idx)
        col.plotly_chart(
            fig, use_container_width=True,
            config={
                "displaylogo": False,
                "modeBarButtonsToRemove": ["lasso2d", "select2d", "toImage"],
            },
        )
    except Exception as e:
        col.warning(f"⚠️ {metric_name} 图表渲染失败：{e}")

# ── 底部注释 ──
st.markdown("---")
st.caption("📡 数据来源：Federal Reserve Economic Data (FRED) | 联储官网 RSS | 仅供学习研究，非投资建议。")
st.caption(f"🕐 页面构建时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}")
