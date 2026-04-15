import streamlit as st
import pandas as pd
import numpy as np
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
import plotly.express as px
from plotly.subplots import make_subplots
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# 1. 页面配置与小清新风格 CSS
# ==========================================
st.set_page_config(page_title="Marco Track | 宏观经济终端", page_icon="🌿", layout="wide")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@300;400;600;700&family=Crimson+Pro:ital,wght@0,300;0,400;1,300&display=swap');

    /* ── 全局背景 & 字体 ── */
    .stApp {
        background: linear-gradient(160deg, #f0f7f4 0%, #e8f4f0 40%, #f5f0eb 100%);
        color: #2d4a3e;
        font-family: 'Nunito', sans-serif;
    }

    /* ── 顶部标题 ── */
    h1 { font-family: 'Crimson Pro', Georgia, serif !important;
         font-weight: 300 !important; color: #2d4a3e !important;
         letter-spacing: 0.5px; font-size: 2.4rem !important; }
    h2 { font-family: 'Nunito', sans-serif !important;
         font-weight: 600 !important; color: #3a6b5a !important;
         font-size: 1.3rem !important; letter-spacing: 0.2px; }
    h3 { font-family: 'Nunito', sans-serif !important;
         font-weight: 700 !important; color: #3a6b5a !important; }

    /* ── 侧边栏 ── */
    section[data-testid="stSidebar"] {
        background: #e8f2ee !important;
        border-right: 1px solid #c8e0d8;
    }

    /* ── 卡片通用 ── */
    .fresh-card {
        background: rgba(255,255,255,0.72);
        border: 1px solid #d0e8df;
        border-radius: 16px;
        padding: 20px 22px;
        backdrop-filter: blur(8px);
        box-shadow: 0 2px 12px rgba(60,120,90,0.07);
        transition: box-shadow 0.25s, transform 0.25s;
        margin-bottom: 4px;
    }
    .fresh-card:hover { box-shadow: 0 6px 24px rgba(60,120,90,0.13); transform: translateY(-2px); }

    /* ── 时钟主卡 ── */
    .clock-card {
        background: linear-gradient(135deg, rgba(255,255,255,0.85) 0%, rgba(232,244,240,0.9) 100%);
        border: 1.5px solid #b8d8ce;
        border-radius: 20px;
        padding: 28px 30px;
        box-shadow: 0 4px 20px rgba(60,120,90,0.10);
        margin-bottom: 22px;
    }

    /* ── 指标数字卡 ── */
    .metric-card {
        background: rgba(255,255,255,0.80);
        border: 1px solid #d0e8df;
        border-radius: 14px;
        padding: 18px 16px;
        text-align: center;
        box-shadow: 0 2px 8px rgba(60,120,90,0.06);
        transition: transform 0.2s;
    }
    .metric-card:hover { transform: translateY(-3px); }
    .metric-label { color: #6a9e8e; font-size: 0.82rem; font-weight: 600;
                    letter-spacing: 0.5px; text-transform: uppercase; margin: 0 0 4px 0; }
    .metric-value { color: #1f3d30; font-size: 2rem; font-weight: 700; margin: 0; }
    .metric-delta { font-size: 0.82rem; margin: 4px 0 0 0; }

    /* ── 倒计时卡 ── */
    .countdown-card {
        background: rgba(255,255,255,0.75);
        border: 1px solid #d0e8df;
        border-radius: 14px;
        padding: 16px 18px;
        min-height: 128px;
        box-shadow: 0 2px 8px rgba(60,120,90,0.06);
        transition: border-color 0.2s;
    }
    @keyframes gentle-pulse {
        0%,100% { box-shadow: 0 2px 8px rgba(229,115,80,0.12); border-color: rgba(229,115,80,0.5); }
        50% { box-shadow: 0 4px 20px rgba(229,115,80,0.28); border-color: rgba(229,115,80,0.75); }
    }
    .countdown-card.urgent { animation: gentle-pulse 2.5s ease-in-out infinite; }
    .countdown-label { color: #8aad9e; font-size: 0.78rem; margin: 0 0 4px; }
    .countdown-title { color: #2d4a3e; font-size: 1rem; font-weight: 700; margin: 0 0 8px; }
    .countdown-time { color: #3a8a6e; font-size: 1.55rem; font-weight: 700;
                      font-variant-numeric: tabular-nums; margin: 0 0 4px; }
    .countdown-meta { color: #8aad9e; font-size: 0.76rem; margin: 0; }

    /* ── 分隔线 ── */
    hr { border: none; border-top: 1px solid #d0e8df !important; margin: 20px 0 !important; }

    /* ── 按钮 ── */
    .stButton > button {
        background: linear-gradient(135deg, #3a8a6e, #5aaa8a) !important;
        color: white !important; border: none !important;
        border-radius: 10px !important; font-weight: 600 !important;
        padding: 8px 20px !important; transition: opacity 0.2s !important;
    }
    .stButton > button:hover { opacity: 0.88 !important; }

    /* ── Radio 按钮：修复文字不显示问题 ── */
    div[data-testid="stRadio"] { gap: 6px; }
    div[data-testid="stRadio"] label {
        background: rgba(255,255,255,0.75) !important;
        border: 1px solid #c8e0d8 !important;
        border-radius: 8px !important;
        padding: 5px 14px !important;
        font-size: 0.86rem !important;
        color: #2d4a3e !important;
        cursor: pointer;
    }
    div[data-testid="stRadio"] label:hover {
        background: rgba(255,255,255,0.95) !important;
        border-color: #3a8a6e !important;
    }
    /* radio 选中状态文字颜色 */
    div[data-testid="stRadio"] label p { color: #2d4a3e !important; }
    div[data-testid="stRadio"] label span { color: #2d4a3e !important; }

    /* ── 文本输入框：修复黑色背景问题 ── */
    .stTextInput > div > div > input {
        background-color: rgba(255,255,255,0.88) !important;
        color: #2d4a3e !important;
        border: 1px solid #c8e0d8 !important;
        border-radius: 10px !important;
        font-family: 'Nunito', sans-serif !important;
    }
    .stTextInput > div > div > input::placeholder {
        color: #9abfb0 !important;
    }
    .stTextInput > div > div > input:focus {
        border-color: #3a8a6e !important;
        box-shadow: 0 0 0 2px rgba(58,138,110,0.15) !important;
    }
    /* 通用 input/textarea 强制亮色 */
    input, textarea, select {
        background-color: rgba(255,255,255,0.88) !important;
        color: #2d4a3e !important;
    }

    /* ── checkbox 修复 ── */
    .stCheckbox label { color: #1f3d30 !important; font-size: 0.9rem !important; font-weight: 600 !important; }
    .stCheckbox label p { color: #1f3d30 !important; }
    .stCheckbox span { color: #1f3d30 !important; }
    /* checkbox 勾选框本身 */
    .stCheckbox [data-testid="stCheckbox"] { accent-color: #3a8a6e; }

    /* ── selectbox ── */
    .stSelectbox > div > div {
        background: rgba(255,255,255,0.88) !important;
        color: #2d4a3e !important;
        border: 1px solid #c8e0d8 !important;
        border-radius: 10px !important;
    }

    /* ── 警告/信息框 ── */
    .stAlert { border-radius: 12px !important; }

    /* ── 展开器 ── */
    .streamlit-expanderHeader { color: #3a6b5a !important; font-weight: 600 !important; }
    details summary { color: #3a6b5a !important; }

    /* ── caption / 小字 ── */
    .stCaption, [data-testid="stCaptionContainer"] p {
        color: #8aad9e !important;
    }

    /* ── markdown 正文颜色 ── */
    .stMarkdown p { color: #2d4a3e; }

    /* ── ML标签 ── */
    .ml-badge {
        background: linear-gradient(135deg, #e8f4f0, #d4ece4);
        border: 1px solid #a8d4c4;
        border-radius: 8px;
        padding: 3px 10px;
        font-size: 0.76rem;
        color: #3a6b5a;
        font-weight: 600;
    }

    /* ── 图表容器：去掉会遮挡图表标题的白色背景块 ── */
    .chart-container {
        border: 1px solid #d0e8df;
        border-radius: 16px;
        padding: 4px 8px 8px 8px;
        margin-bottom: 14px;
        background: rgba(255,255,255,0.50);
        box-shadow: 0 2px 8px rgba(60,120,90,0.05);
        overflow: visible;
    }

    /* scrollbar */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: #f0f7f4; }
    ::-webkit-scrollbar-thumb { background: #b8d8ce; border-radius: 3px; }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. 初始化 FRED API（支持 secrets 或硬编码）
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
# 3. 指标字典（按展示顺序：增长→通胀→就业→利率→流动性→AI）
# ==========================================
FRED_CATEGORIES = {
    "增长 (Growth)": {
        "实际GDP (GDPC1)":                   "GDPC1",
        "GDP环比增速 (A191RL1Q225SBEA)":      "A191RL1Q225SBEA",
        "工业生产指数 (INDPRO)":               "INDPRO",
        "零售销售 (RSAFS)":                   "RSAFS",
        "个人消费支出 (PCE)":                  "PCE",
        "耐用品订单 (DGORDER)":               "DGORDER",
        "建筑许可 (PERMIT)":                  "PERMIT",
        "新屋开工 (HOUST)":                   "HOUST",
        "领先经济指标 (USSLIND)":              "USSLIND",
        "芝加哥联储活动指数 (CFNAI)":          "CFNAI",
        "消费者信心指数 (UMCSENT)":            "UMCSENT",
    },
    "通胀 (Inflation)": {
        "CPI总体 (CPIAUCSL)":                 "CPIAUCSL",
        "核心CPI (CPILFESL)":                 "CPILFESL",
        "CPI住房分项 (CUSR0000SAH1)":         "CUSR0000SAH1",
        "PCE总体 (PCEPI)":                    "PCEPI",
        "核心PCE (PCEPILFE)":                 "PCEPILFE",
        "PPI总体 (PPIACO)":                   "PPIACO",
        "PPI最终需求 (PPIFID)":               "PPIFID",
        "密歇根通胀预期1Y (MICH)":            "MICH",
        "5年盈亏平衡通胀率 (T5YIE)":          "T5YIE",
        "10年盈亏平衡通胀率 (T10YIE)":        "T10YIE",
    },
    "就业 (Employment)": {
        "失业率 (UNRATE)":                    "UNRATE",
        "非农就业新增 (PAYEMS)":               "PAYEMS",
        "私人非农就业 (USPRIV)":               "USPRIV",
        "劳动力参与率 (CIVPART)":              "CIVPART",
        "就业人口比 (EMRATIO)":               "EMRATIO",
        "平均时薪 (CES0500000003)":            "CES0500000003",
        "平均每周工时 (AWHAETP)":              "AWHAETP",
        "初请失业金 (ICSA)":                   "ICSA",
        "续请失业金 (CCSA)":                   "CCSA",
        "职位空缺 JOLTS (JTSJOL)":            "JTSJOL",
    },
    "利率 (Interest Rates)": {
        "联邦基金利率 (FEDFUNDS)":             "FEDFUNDS",
        "有效联邦基金利率/日 (DFF)":           "DFF",
        "超额准备金利率 (IORB)":               "IORB",
        "2年期美债 (DGS2)":                   "DGS2",
        "10年期美债 (DGS10)":                 "DGS10",
        "10Y-2Y利差 (T10Y2Y)":               "T10Y2Y",
        "10Y-3M利差 (T10Y3M)":               "T10Y3M",
        "30年期抵押贷款利率 (MORTGAGE30US)":   "MORTGAGE30US",
        "BAA企业债收益率 (BAA)":               "BAA",
        "高收益债利差OAS (BAMLH0A0HYM2)":     "BAMLH0A0HYM2",
        "投资级债利差OAS (BAMLC0A0CM)":       "BAMLC0A0CM",
        "10年期TIPS实际利率 (DFII10)":        "DFII10",
    },
    "流动性 (Liquidity)": {
        "M2货币供应 (M2SL)":                  "M2SL",
        "M1货币供应 (M1SL)":                  "M1SL",
        "美联储资产负债表 (WALCL)":            "WALCL",
        "美联储持有国债 (TREAST)":             "TREAST",
        "隔夜逆回购余额 (RRPONTSYD)":         "RRPONTSYD",
        "银行准备金余额 (WRESBAL)":            "WRESBAL",
        "芝加哥联储金融条件 (NFCI)":           "NFCI",
        "圣路易斯金融压力 (STLFSI4)":         "STLFSI4",
        "TED利差 (TEDRATE)":                  "TEDRATE",
        "SOFR隔夜融资利率 (SOFR)":            "SOFR",
        "贸易加权美元指数 (DTWEXBGS)":        "DTWEXBGS",
    },
    "AI代理指标 (AI Proxies)": {
        "半导体工业生产 (IPG3344N)":           "IPG3344N",
        "半导体价格PPI (PCU33443344)":         "PCU33443344",
    },
}

# ── pc1 接口（FRED 直接返回同比）──
PC1_SERIES = {
    "CPIAUCSL", "CPILFESL", "CUSR0000SAH1",
    "PCEPI", "PCEPILFE", "PPIACO", "PPIFID",
}

# ── 单位标注 ──
UNIT_MAP = {
    # 比率 / 利率
    "UNRATE": "%",   "CIVPART": "%",  "EMRATIO": "%",
    "FEDFUNDS": "%", "DFF": "%",      "IORB": "%",
    "DGS2": "%",     "DGS10": "%",
    "T10Y2Y": "%",   "T10Y3M": "%",
    "MORTGAGE30US": "%", "BAA": "%",
    "BAMLH0A0HYM2": "%", "BAMLC0A0CM": "%",
    "DFII10": "%",   "TEDRATE": "%",  "SOFR": "%",
    "MICH": "%",     "T5YIE": "%",    "T10YIE": "%",
    # 就业
    "PAYEMS": " 千人", "USPRIV": " 千人",
    "JTSJOL": " 千人", "ICSA": " 人",   "CCSA": " 人",
    "CES0500000003": " $/时", "AWHAETP": " 小时",
    # 货币 / 资产负债表
    "M2SL": " 十亿$",      "M1SL": " 十亿$",
    "WALCL": " 百万$",     "TREAST": " 百万$",
    "RRPONTSYD": " 十亿$", "WRESBAL": " 十亿$",
    # 增长
    "GDPC1": " 十亿$",     "RSAFS": " 百万$",
    "PCE": " 十亿$",       "DGORDER": " 百万$",
    "PERMIT": " 千套",     "HOUST": " 千套",
    "INDPRO": "",          "USSLIND": "",
    "CFNAI": "",           "UMCSENT": "",
    "A191RL1Q225SBEA": "%",
    # 其他
    "NFCI": "",   "STLFSI4": "",  "DTWEXBGS": "",
    "IPG3344N": "", "PCU33443344": "",
}

# ── 每个序列的图表渲染策略 ──
# "bar_yoy"  → 双色柱(同比%)
# "line_yoy" → 折线(同比%)
# "line"     → 渐变面积折线(原始值)
# "step"     → 阶梯线(政策利率)
# "spread"   → 利差专用(含0轴红线)
# "bar_abs"  → 柱状(绝对值,如CFNAI可负)
CHART_TYPE = {
    # 增长
    "GDPC1":              "line_yoy",
    "A191RL1Q225SBEA":    "bar_yoy",
    "INDPRO":             "line_yoy",
    "RSAFS":              "line_yoy",
    "PCE":                "line_yoy",
    "DGORDER":            "line_yoy",
    "PERMIT":             "line_yoy",
    "HOUST":              "line_yoy",
    "USSLIND":            "line",
    "CFNAI":              "bar_abs",
    "UMCSENT":            "line",
    # 通胀（pc1直接同比）
    "CPIAUCSL":           "bar_yoy",
    "CPILFESL":           "bar_yoy",
    "CUSR0000SAH1":       "bar_yoy",
    "PCEPI":              "bar_yoy",
    "PCEPILFE":           "bar_yoy",
    "PPIACO":             "bar_yoy",
    "PPIFID":             "bar_yoy",
    "MICH":               "line",
    "T5YIE":              "line",
    "T10YIE":             "line",
    # 就业
    "UNRATE":             "line",
    "PAYEMS":             "bar_abs",
    "USPRIV":             "bar_abs",
    "CIVPART":            "line",
    "EMRATIO":            "line",
    "CES0500000003":      "line_yoy",
    "AWHAETP":            "line",
    "ICSA":               "line",
    "CCSA":               "line",
    "JTSJOL":             "line",
    # 利率
    "FEDFUNDS":           "step",
    "DFF":                "step",
    "IORB":               "step",
    "DGS2":               "line",
    "DGS10":              "line",
    "T10Y2Y":             "spread",
    "T10Y3M":             "spread",
    "MORTGAGE30US":       "line",
    "BAA":                "line",
    "BAMLH0A0HYM2":       "line",
    "BAMLC0A0CM":         "line",
    "DFII10":             "spread",
    # 流动性
    "M2SL":               "line_yoy",
    "M1SL":               "line_yoy",
    "WALCL":              "line",
    "TREAST":             "line",
    "RRPONTSYD":          "line",
    "WRESBAL":            "line",
    "NFCI":               "spread",
    "STLFSI4":            "spread",
    "TEDRATE":            "line",
    "SOFR":               "line",
    "DTWEXBGS":           "line",
    # AI
    "IPG3344N":           "line_yoy",
    "PCU33443344":        "bar_yoy",
}

# 哪些序列的 YoY 直接用 Value（pc1已是同比），哪些要自己算
YOY_DISPLAY = {
    "GDPC1","A191RL1Q225SBEA","INDPRO","RSAFS","PCE","DGORDER",
    "PERMIT","HOUST","CPIAUCSL","CPILFESL","CUSR0000SAH1","PCEPI",
    "PCEPILFE","PPIACO","PPIFID","M2SL","M1SL",
    "CES0500000003","IPG3344N","PCU33443344",
}

# PAYEMS / USPRIV 用 MoM 差值（月增量）而非同比
MOM_DIFF_SERIES = {"PAYEMS", "USPRIV"}

# ==========================================
# 4. 数据拉取
# ==========================================
@st.cache_data(ttl=86400)
def fetch_data_advanced(series_id, years=6):
    end_date = datetime.today()
    start_date = end_date - relativedelta(years=years)
    req_units = 'pc1' if series_id in PC1_SERIES else 'lin'
    try:
        data = fred.get_series(series_id, observation_start=start_date,
                               observation_end=end_date, units=req_units)
        df = pd.DataFrame({'Date': data.index, 'Value': data.values}).dropna()

        if req_units == 'pc1':
            # FRED 已返回同比，直接存 YoY
            df['YoY'] = df['Value']
            df['Value_Diff'] = df['Value'].diff(1)

        elif series_id in MOM_DIFF_SERIES:
            # 非农/私人就业：月度新增人数（差值，千人）
            df['YoY'] = df['Value'].diff(1)       # MoM 增量
            df['Value_Diff'] = df['Value'].diff(1)

        else:
            n_lag = 4 if series_id in ("GDPC1", "A191RL1Q225SBEA") else 12
            if len(df) > n_lag:
                df['YoY'] = df['Value'].pct_change(n_lag) * 100
                df['Value_Diff'] = df['Value'].diff(1)
            else:
                df['YoY'] = 0.0
                df['Value_Diff'] = 0.0

        display_start = end_date - relativedelta(years=5)
        return df[df['Date'] >= display_start].copy()
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
# 5. ML 经济周期识别（HMM + 多指标合成）
# ==========================================
@st.cache_data(ttl=3600 * 6)
def calculate_ml_investment_clock():
    """
    使用隐马尔可夫模型 (Gaussian HMM) 对多维宏观特征进行无监督状态识别，
    再将学到的隐状态映射到美林时钟四象限。
    输入特征：
      - INDPRO YoY 同比（增长动能）
      - CPI YoY（通胀水平）
      - T10Y2Y 利差（宏观金融压力代理）
      - UNRATE 变动（就业边际）
    """
    try:
        from hmmlearn.hmm import GaussianHMM
        HMM_AVAILABLE = True
    except ImportError:
        HMM_AVAILABLE = False

    # ── 拉取多维特征 ──
    growth_df = fetch_data_advanced("INDPRO", years=10)
    cpi_df    = fetch_data_advanced("CPIAUCSL", years=10)
    spread_df = fetch_data_advanced("T10Y2Y", years=10)
    urate_df  = fetch_data_advanced("UNRATE", years=10)

    if any(df.empty for df in [growth_df, cpi_df, spread_df, urate_df]):
        return _fallback_rule_clock(growth_df, cpi_df)

    # ── 月度对齐 ──
    def to_monthly(df, col):
        s = df.set_index('Date')[col].copy()
        s.index = pd.to_datetime(s.index)
        return s.resample('MS').last()

    g_yoy   = to_monthly(growth_df, 'YoY')
    cpi_yoy = to_monthly(cpi_df, 'Value')   # pc1 系列，Value 即 YoY
    spread  = to_monthly(spread_df, 'Value')
    urate   = to_monthly(urate_df, 'Value')

    combined = pd.DataFrame({
        'g_yoy': g_yoy,
        'cpi_yoy': cpi_yoy,
        'spread': spread,
        'urate': urate,
    }).dropna()

    if len(combined) < 30:
        return _fallback_rule_clock(growth_df, cpi_df)

    # ── 特征标准化 ──
    from sklearn.preprocessing import StandardScaler
    scaler = StandardScaler()
    X = scaler.fit_transform(combined.values)

    # ── 最新观测值 ──
    latest = combined.iloc[-1]
    g_now  = float(latest['g_yoy'])
    i_now  = float(latest['cpi_yoy'])
    g_prev = float(combined['g_yoy'].iloc[-4])
    i_prev = float(combined['cpi_yoy'].iloc[-4])

    if HMM_AVAILABLE and len(X) >= 48:
        try:
            model = GaussianHMM(n_components=4, covariance_type="full",
                                n_iter=200, random_state=42, tol=1e-4)
            model.fit(X)
            states = model.predict(X)
            combined['state'] = states

            # ── 将 HMM 隐状态映射到四象限 ──
            # 按状态质心的 (g_yoy, cpi_yoy) 均值确定象限身份
            state_means = {}
            for s in range(4):
                mask = combined['state'] == s
                if mask.sum() > 0:
                    state_means[s] = (
                        combined.loc[mask, 'g_yoy'].mean(),
                        combined.loc[mask, 'cpi_yoy'].mean()
                    )

            # 当前状态
            current_state = int(states[-1])
            gm, im = state_means.get(current_state, (g_now, i_now))

            phase, desc, color, assets = _classify_quadrant(gm, im, g_prev, i_prev)

            # 置信度：当前状态的后验概率
            posteriors = model.predict_proba(X)
            confidence = float(posteriors[-1, current_state]) * 100

            # 历史状态序列（过去 36 个月）
            history = combined[['state']].copy()
            history['phase_color'] = history['state'].map(
                lambda s: _classify_quadrant(
                    state_means.get(s, (0, 0))[0],
                    state_means.get(s, (0, 0))[1],
                    0, 0
                )[2]
            )
            history['phase_name'] = history['state'].map(
                lambda s: _classify_quadrant(
                    state_means.get(s, (0, 0))[0],
                    state_means.get(s, (0, 0))[1],
                    0, 0
                )[0]
            )

            note = (f"HMM 4态模型在当前隐状态 #{current_state} 上的后验置信度 **{confidence:.1f}%**。"
                    f"该状态历史均值：增长同比 {gm:.2f}%，CPI同比 {im:.2f}%。"
                    f"模型综合了 INDPRO、CPI、10Y-2Y 利差与失业率四维特征。")

            return phase, desc, color, assets, note, True, confidence, history

        except Exception as e:
            pass  # HMM 失败 → 规则兜底

    # ── 规则兜底 ──
    return _fallback_rule_clock(growth_df, cpi_df)


def _classify_quadrant(g_mean, i_mean, g_prev, i_prev):
    """根据增长/通胀均值确定美林象限"""
    # 使用均值与全局均值对比
    g_up = g_mean > 2.0   # INDPRO 长期平均约 2%
    i_up = i_mean > 2.5   # Fed 目标 2%
    if g_up and not i_up:
        return "复苏 (Recovery)", "📈 增长强劲 / 通胀温和", "#4caf8a", "股票 > 债券 > 大宗"
    if g_up and i_up:
        return "过热 (Overheat)", "🔥 增长强劲 / 通胀升温", "#e07a5f", "大宗 > 股票 > 现金"
    if not g_up and i_up:
        return "滞胀 (Stagflation)", "☁️ 增长放缓 / 通胀顽固", "#f2a65a", "现金 > 大宗 > 债券"
    return "衰退 (Reflation)", "🥶 增长放缓 / 通胀回落", "#6a9fd8", "债券 > 现金 > 股票"


def _fallback_rule_clock(growth_df, cpi_df):
    if growth_df.empty or cpi_df.empty or len(growth_df) < 4 or len(cpi_df) < 4:
        return "数据不足", "🔧 无法计算", "#aaaaaa", "保持现金", "数据不足", False, 0.0, None
    g_now  = float(growth_df["YoY"].iloc[-1])
    g_prev = float(growth_df["YoY"].iloc[-4])
    i_now  = float(cpi_df["Value"].iloc[-1])
    i_prev = float(cpi_df["Value"].iloc[-4])
    phase, desc, color, assets = _classify_quadrant(g_now, i_now, g_prev, i_prev)
    note = "未安装 hmmlearn 或数据不足；使用简化四象限规则（INDPRO同比 × CPI同比）。"
    return phase, desc, color, assets, note, False, 0.0, None


# ==========================================
# 5b. 规则兜底（保留兼容）
# ==========================================
def _phase_color(phase: str) -> str:
    if "复苏" in phase: return "#4caf8a"
    if "过热" in phase: return "#e07a5f"
    if "滞胀" in phase: return "#f2a65a"
    if "衰退" in phase: return "#6a9fd8"
    return "#3a8a6e"


# ==========================================
# 5c. 联储 RSS
# ==========================================
_ET = ZoneInfo("America/New_York")
_FED_RSS_UA = {"User-Agent": "Mozilla/5.0 (compatible; MarcoTrack/2.0)"}
_FED_RSS_ALL = "https://www.federalreserve.gov/feeds/speeches_and_testimony.xml"
_FED_BOARD_RSS = [
    ("Jerome H. Powell",    "https://www.federalreserve.gov/feeds/s_t_powell.xml"),
    ("Philip N. Jefferson", "https://www.federalreserve.gov/feeds/s_t_jefferson.xml"),
    ("Michelle W. Bowman",  "https://www.federalreserve.gov/feeds/m_w_Bowman.xml"),
    ("Lisa D. Cook",        "https://www.federalreserve.gov/feeds/s_t_cook.xml"),
    ("Christopher J. Waller","https://www.federalreserve.gov/feeds/s_t_waller.xml"),
]


def _strip_html(text):
    if not text: return ""
    t = re.sub(r"<[^>]+>", " ", text)
    return html.unescape(re.sub(r"\s+", " ", t)).strip()


def _entry_ts(entry):
    tt = entry.get("published_parsed") or entry.get("updated_parsed")
    if tt:
        try: return time.mktime(tt)
        except: pass
    return 0.0


def _parse_feed_entries(parsed):
    rows = []
    for e in getattr(parsed, "entries", []) or []:
        link = (e.get("link") or "").strip()
        if not link: continue
        rows.append({"title": (e.get("title") or "（无标题）").strip(),
                     "link": link, "ts": _entry_ts(e),
                     "summary": _strip_html(e.get("summary", ""))[:400]})
    return rows


@st.cache_data(ttl=300)
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

    with ThreadPoolExecutor(max_workers=min(8, len(_FED_BOARD_RSS))) as ex:
        for name, url in _FED_BOARD_RSS:
            batch, err = load_one(name, url)
            if err: errors.append(err)
            for row in batch:
                if row["link"] not in merged:
                    merged[row["link"]] = row

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


# ==========================================
# 5d. 宏观事件倒计时
# ==========================================
_FOMC_DATES = [
    (2026,1,28),(2026,3,18),(2026,4,29),(2026,6,17),(2026,7,29),
    (2026,9,16),(2026,10,28),(2026,12,9),
]

def _add_month(y, m):
    return (y+1,1) if m==12 else (y, m+1)

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
        if t.astimezone(timezone.utc) > now_utc: return t
        y, m = _add_month(y, m)

def _next_cpi(now_utc):
    r = now_utc.astimezone(_ET); y, m = r.year, r.month
    for _ in range(28):
        d2 = _second_tuesday(y, m)
        t = datetime(d2.year, d2.month, d2.day, 8, 30, tzinfo=_ET)
        if t.astimezone(timezone.utc) > now_utc: return t
        y, m = _add_month(y, m)

def _next_fomc(now_utc):
    for y, mo, d in _FOMC_DATES:
        t = datetime(y, mo, d, 14, 0, tzinfo=_ET)
        if t.astimezone(timezone.utc) > now_utc: return t

def _fmt_countdown(rem):
    if rem.total_seconds() <= 0: return "已到发布窗口"
    total = int(rem.total_seconds())
    d, r = divmod(total, 86400); h, r = divmod(r, 3600); m, s = divmod(r, 60)
    return f"{d}天 {h:02d}:{m:02d}:{s:02d}" if d > 0 else f"{h:02d}:{m:02d}:{s:02d}"


def _macro_countdown_strip_body():
    now_utc = datetime.now(timezone.utc)
    events = [
        ("📊 美国 CPI", _next_cpi(now_utc), "BLS 第二个周二 8:30 ET"),
        ("💼 非农就业 NFP", _next_nfp(now_utc), "BLS 当月首个周五 8:30 ET"),
        ("🏦 FOMC 利率决议", _next_fomc(now_utc), "联储声明约 14:00 ET"),
    ]
    c1, c2, c3 = st.columns(3)
    for col, (title, target, note) in zip((c1,c2,c3), events):
        with col:
            if target is None:
                st.markdown(f'<div class="countdown-card"><p class="countdown-title">{title}</p>'
                            f'<p class="countdown-time">—</p></div>', unsafe_allow_html=True)
                continue
            rem = target.astimezone(timezone.utc) - now_utc
            urgent = timedelta(0) < rem < timedelta(hours=24)
            cls = "countdown-card urgent" if urgent else "countdown-card"
            st.markdown(
                f'<div class="{cls}">'
                f'<p class="countdown-label">{note}</p>'
                f'<p class="countdown-title">{title}</p>'
                f'<p class="countdown-time">{_fmt_countdown(rem)}</p>'
                f'<p class="countdown-meta">发布（ET）{target.strftime("%Y-%m-%d %H:%M")}</p>'
                f'</div>', unsafe_allow_html=True
            )

_macro_countdown_strip = (
    st.fragment(run_every=timedelta(seconds=5))(_macro_countdown_strip_body)
    if hasattr(st, "fragment") else _macro_countdown_strip_body
)

# ==========================================
# 6. 图表风格与智能渲染引擎
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
    "palette":   ["#3a8a6e","#6a9fd8","#e07a5f","#f2a65a",
                  "#9b88c4","#5abcb0","#d4727a","#8aaa5a"],
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


def _title_layout(title):
    return dict(
        text=f"<b>{title}</b>",
        font=dict(size=12, color="#1f3d30", family="Nunito"),
        x=0.01, xanchor='left',
    )


def _hex_rgba(hex_color, alpha):
    r, g, b = int(hex_color[1:3], 16), int(hex_color[3:5], 16), int(hex_color[5:7], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _apply(fig, title, height=300):
    layout = dict(**_BASE_LAYOUT)
    layout["title"] = _title_layout(title)
    layout["height"] = height
    fig.update_layout(**layout)
    return fig


# ── 各类图表构造函数 ──

def _chart_bar_yoy(df, series_id, label, unit_str):
    """双色同比柱状图（正绿负红）"""
    y = df['YoY'] if series_id not in MOM_DIFF_SERIES else df['YoY']
    colors = [FRESH_COLORS["primary"] if v >= 0 else FRESH_COLORS["accent"] for v in y]
    ylabel = "MoM 月增（千人）" if series_id in MOM_DIFF_SERIES else f"YoY %"
    fig = go.Figure(go.Bar(
        x=df['Date'], y=y,
        marker_color=colors, marker_line_width=0,
        name=label,
        hovertemplate=f'<b>%{{y:.2f}}{unit_str}</b><extra></extra>',
    ))
    fig.update_layout(yaxis_title=ylabel)
    return fig


def _chart_line_yoy(df, series_id, label, unit_str, color):
    """同比折线+渐变面积"""
    y = df['YoY']
    fig = go.Figure(go.Scatter(
        x=df['Date'], y=y, mode='lines',
        line=dict(width=2, color=color),
        fill='tozeroy', fillcolor=_hex_rgba(color, 0.10),
        name=label,
        hovertemplate=f'<b>%{{y:.2f}}{unit_str}</b><extra></extra>',
    ))
    fig.update_layout(yaxis_title="YoY %")
    return fig


def _chart_line(df, label, unit_str, color):
    """原始值渐变面积折线"""
    fig = go.Figure(go.Scatter(
        x=df['Date'], y=df['Value'], mode='lines',
        line=dict(width=2, color=color),
        fill='tozeroy', fillcolor=_hex_rgba(color, 0.09),
        name=label,
        hovertemplate=f'<b>%{{y:.2f}}{unit_str}</b><extra></extra>',
    ))
    fig.update_layout(yaxis_title=unit_str.strip() or "值")
    return fig


def _chart_step(df, label, unit_str, color):
    """阶梯线（政策利率专用）"""
    fig = go.Figure(go.Scatter(
        x=df['Date'], y=df['Value'],
        mode='lines', line=dict(width=2.5, color=color, shape='hv'),
        fill='tozeroy', fillcolor=_hex_rgba(color, 0.08),
        name=label,
        hovertemplate=f'<b>%{{y:.2f}}{unit_str}</b><extra></extra>',
    ))
    fig.update_layout(yaxis_title=unit_str.strip() or "%")
    return fig


def _chart_spread(df, label, unit_str, color):
    """利差图：含醒目0轴基准线，负区域橙红填充"""
    y = df['Value']
    # 分正负两段填充
    fig = go.Figure()
    fig.add_hline(y=0, line_width=1.2, line_dash="dot",
                  line_color="rgba(200,80,60,0.55)")
    fig.add_trace(go.Scatter(
        x=df['Date'], y=y, mode='lines',
        line=dict(width=2, color=color),
        fill='tozeroy',
        fillcolor=_hex_rgba(color, 0.10),
        name=label,
        hovertemplate=f'<b>%{{y:.3f}}{unit_str}</b><extra></extra>',
    ))
    fig.update_layout(yaxis_title=unit_str.strip() or "pts")
    return fig


def _chart_bar_abs(df, label, unit_str, color):
    """绝对值柱状图（CFNAI 等可负值序列，正负双色）"""
    y = df['Value']
    colors = [FRESH_COLORS["primary"] if v >= 0 else FRESH_COLORS["accent"] for v in y]
    fig = go.Figure(go.Bar(
        x=df['Date'], y=y,
        marker_color=colors, marker_line_width=0,
        name=label,
        hovertemplate=f'<b>%{{y:.3f}}{unit_str}</b><extra></extra>',
    ))
    if y.min() < 0:
        fig.add_hline(y=0, line_width=1, line_dash="dot",
                      line_color="rgba(200,80,60,0.45)")
    fig.update_layout(yaxis_title=unit_str.strip() or "值")
    return fig


def render_chart(series_id, metric_name, df, idx):
    """根据 CHART_TYPE 元数据分发渲染，返回 go.Figure"""
    ctype  = CHART_TYPE.get(series_id, "line")
    unit   = UNIT_MAP.get(series_id, "")
    color  = FRESH_COLORS["palette"][idx % len(FRESH_COLORS["palette"])]

    # 判断展示用的 title 后缀
    if ctype in ("bar_yoy", "line_yoy"):
        if series_id in MOM_DIFF_SERIES:
            suffix = "(MoM 月增)"
        else:
            suffix = "(YoY %)"
    elif ctype == "spread":
        suffix = f"({unit.strip()})" if unit else "(pts)"
    else:
        suffix = f"({unit.strip()})" if unit.strip() else ""

    title = f"{metric_name} {suffix}".strip()

    if ctype == "bar_yoy":
        fig = _chart_bar_yoy(df, series_id, metric_name, unit)
    elif ctype == "line_yoy":
        fig = _chart_line_yoy(df, series_id, metric_name, unit, color)
    elif ctype == "step":
        fig = _chart_step(df, metric_name, unit, color)
    elif ctype == "spread":
        fig = _chart_spread(df, metric_name, unit, color)
    elif ctype == "bar_abs":
        fig = _chart_bar_abs(df, metric_name, unit, color)
    else:  # "line"
        fig = _chart_line(df, metric_name, unit, color)

    # 高频日度数据列宽更细
    if ctype in ("bar_yoy", "bar_abs"):
        n = len(df)
        bw = 0.6 if n < 80 else (0.85 if n < 200 else 0.95)
        fig.update_traces(width=None)  # 让 plotly 自适应

    _apply(fig, title, height=300)
    return fig


# ==========================================
# 7. UI 主体
# ==========================================

# ── 顶部标题 ──
col_h1, col_h2 = st.columns([4, 1])
with col_h1:
    st.markdown("""
    <h1>🌿 Macro Track Reportthinking</h1>
    <p style="color:#6a9e8e; margin-top:-10px; font-size:0.95rem;">
        美联储政策 · 宏观经济 · 投资时钟 &nbsp;|&nbsp; 数据来源: Federal Reserve Economic Data
    </p>
    """, unsafe_allow_html=True)
with col_h2:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 刷新数据"):
        st.cache_data.clear()
        st.rerun()

st.markdown("---")

# ── 预热缓存 ──
with st.spinner("正在加载宏观数据…"):
    warm_core_series_cache()

# ── 经济周期（ML）──
(phase, desc, color, assets,
 clock_note, used_ml, confidence, history_df) = calculate_ml_investment_clock()

phase_colors = {
    "复苏": "#4caf8a", "过热": "#e07a5f", "滞胀": "#f2a65a", "衰退": "#6a9fd8"
}
phase_bg = {
    "复苏": "rgba(76,175,138,0.10)", "过热": "rgba(224,122,95,0.10)",
    "滞胀": "rgba(242,166,90,0.10)",  "衰退": "rgba(106,159,216,0.10)"
}
phase_key = next((k for k in phase_colors if k in phase), "复苏")

ml_tag = '<span class="ml-badge">🤖 HMM 机器学习</span>' if used_ml else '<span class="ml-badge">📐 规则模型</span>'
conf_str = f"｜置信度 {confidence:.1f}%" if used_ml and confidence > 0 else ""

st.markdown(f"""
<div class="clock-card">
  <div style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:16px;">
    <div>
      <div style="margin-bottom:8px;">{ml_tag}</div>
      <h3 style="margin:0; color:{color} !important; font-size:1.6rem;">
        当前经济阶段：{phase}
      </h3>
      <p style="margin:6px 0 0; color:#5a8a7a; font-size:1rem;">{desc} {conf_str}</p>
    </div>
    <div style="background:rgba(255,255,255,0.7); border:1px solid #d0e8df;
                border-radius:12px; padding:14px 20px; min-width:180px;">
      <p style="margin:0; color:#8aad9e; font-size:0.8rem; font-weight:600;">建议超配资产（示意）</p>
      <p style="margin:6px 0 0; color:#2d4a3e; font-size:1.1rem; font-weight:700;">{assets}</p>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# 时钟判断依据展开
with st.expander("📋 周期判断依据", expanded=False):
    st.markdown(clock_note)
    if not used_ml:
        st.info("💡 安装 `hmmlearn` 与 `scikit-learn` 可启用 HMM 机器学习周期识别。"
                "运行：`pip install hmmlearn scikit-learn`")

# ── 倒计时 ──
st.markdown("#### ⏱ 重要发布倒计时")
_macro_countdown_strip()

st.markdown("---")

# ── 核心指标卡 ──
st.markdown("#### 📌 核心指标速览")
top_metrics = {
    "核心PCE":    ("PCEPILFE", "通胀"),
    "失业率":      ("UNRATE",   "就业"),
    "联邦基金利率": ("FEDFUNDS", "政策"),
    "工业产出":    ("INDPRO",   "增长"),
    "10Y-2Y利差": ("T10Y2Y",   "衰退预警"),
    "M2同比":     ("M2SL",     "流动性"),
}
cols_m = st.columns(6)
for i, (name, (sid, sub)) in enumerate(top_metrics.items()):
    df = fetch_data_advanced(sid)
    with cols_m[i]:
        if not df.empty:
            latest = df.iloc[-1]
            unit = UNIT_MAP.get(sid, "")
            # 值显示
            if sid in PC1_SERIES:
                val_str = f"{latest['Value']:.2f}%"
                delta_val = float(latest['Value_Diff']) if not pd.isna(latest['Value_Diff']) else 0
                delta_str = f"{delta_val:+.2f} pts"
                is_inverse = False
            elif sid in ("UNRATE", "FEDFUNDS"):
                val_str = f"{latest['Value']:.2f}{unit}"
                delta_val = float(latest['Value_Diff']) if not pd.isna(latest['Value_Diff']) else 0
                delta_str = f"{delta_val:+.2f} pts"
                is_inverse = (sid == "UNRATE")
            elif sid in MOM_DIFF_SERIES:
                val_str = f"{latest['Value']:.0f}{unit}"
                delta_val = float(latest['YoY']) if not pd.isna(latest['YoY']) else 0
                delta_str = f"{delta_val:+.0f} 千人 MoM"
                is_inverse = False
            elif sid in YOY_DISPLAY:
                val_str = f"{latest['Value']:.1f}"
                delta_val = float(latest['YoY']) if not pd.isna(latest['YoY']) else 0
                delta_str = f"{delta_val:+.1f}% YoY"
                is_inverse = False
            else:
                val_str = f"{latest['Value']:.2f}{unit}"
                delta_val = float(latest['YoY']) if not pd.isna(latest['YoY']) else 0
                delta_str = f"{delta_val:+.1f}% YoY"
                is_inverse = False

            delta_color = "#e07a5f" if (delta_val > 0) == is_inverse else "#4caf8a"
            delta_arrow = "▲" if delta_val > 0 else "▼"

            st.markdown(f"""
            <div class="metric-card">
              <p class="metric-label">{sub}</p>
              <p style="color:#6a9e8e;font-size:0.76rem;margin:0 0 2px;">{name}</p>
              <p class="metric-value">{val_str}</p>
              <p class="metric-delta" style="color:{delta_color};">{delta_arrow} {delta_str}</p>
            </div>
            """, unsafe_allow_html=True)

st.markdown("---")

# ── 联储官员讲话（自动刷新）──
st.markdown("#### 🏛 美联储官员最新讲话")

# 筛选控件在 fragment 外，避免每次自动刷新重置用户输入
_col_news, _col_filter = st.columns([3, 1])

with _col_filter:
    st.markdown("""
    <div style="background:rgba(255,255,255,0.82); border:1px solid #b8d8ce;
                border-radius:14px; padding:16px 18px;">
      <p style="margin:0 0 10px; color:#2d4a3e; font-size:0.88rem;
                font-weight:700; letter-spacing:0.3px;">🔍 筛选条件</p>
    </div>
    """, unsafe_allow_html=True)
    only_personal = st.checkbox("仅理事个人源", value=True,
                                help="取消可包含地区联储主席等聚合源")
    speech_q = st.text_input("标题关键词", "", placeholder="Powell / Inflation…")


def _fed_news_body():
    _fed_rows, _ = fetch_fed_speech_feeds()
    now_str = datetime.now().strftime("%H:%M:%S")

    with _col_news:
        st.caption(f"🔄 每 5 分钟自动刷新 ｜ 上次更新 {now_str}")

        if not _fed_rows:
            st.warning("暂无法拉取联储 RSS，请检查网络后点击「刷新数据」重试。")
            return

        filtered = [r for r in _fed_rows
                    if (not only_personal or r.get("speaker") != "（聚合源）")
                    and (not speech_q or speech_q.lower() in r["title"].lower())]

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
    _fed_news_fragment = st.fragment(run_every=timedelta(seconds=300))(_fed_news_body)
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
n_metrics = len(metrics_dict)

# 超过 8 个指标时用 2 列，否则也用 2 列（保持一致）
chart_cols = st.columns(2)

for idx, (metric_name, series_id) in enumerate(metrics_dict.items()):
    df = _cat_dfs.get(series_id, pd.DataFrame())
    col = chart_cols[idx % 2]
    if df.empty:
        col.warning(f"⚠️ {metric_name} 数据获取失败")
        continue
    try:
        fig = render_chart(series_id, metric_name, df, idx)
        col.plotly_chart(fig, use_container_width=True,
                         config={"displaylogo": False,
                                 "modeBarButtonsToRemove": ["lasso2d","select2d","toImage"]}
                         )
    except Exception as e:
        col.warning(f"⚠️ {metric_name} 图表渲染失败：{e}")

# ── 底部注释 ──
st.markdown("---")
st.caption("📡 数据来源：Federal Reserve Economic Data (FRED) | 联储官网 RSS | 仅供学习研究，非投资建议。")
st.caption(f"🕐 页面构建时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}")
