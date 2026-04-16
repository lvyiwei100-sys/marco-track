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
st.set_page_config(page_title="Macro Track Reportthinking | 宏观经济终端", page_icon="🌿", layout="wide")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@300;400;600;700&family=Crimson+Pro:ital,wght@0,300;0,400;1,300&display=swap');

    /* ══════════════════════════════════════
       全局基础
    ══════════════════════════════════════ */
    .stApp {
        background: linear-gradient(160deg, #f0f7f4 0%, #e8f4f0 40%, #f5f0eb 100%);
        color: #2d4a3e;
        font-family: 'Nunito', sans-serif;
    }
    /* 缩小 Streamlit 默认左右内边距（手机特别需要） */
    .block-container {
        padding-left: 1rem !important;
        padding-right: 1rem !important;
        padding-top: 1.2rem !important;
        max-width: 1400px;
    }

    /* ══════════════════════════════════════
       标题
    ══════════════════════════════════════ */
    h1 {
        font-family: 'Crimson Pro', Georgia, serif !important;
        font-weight: 300 !important; color: #2d4a3e !important;
        letter-spacing: 0.5px;
        font-size: clamp(1.6rem, 5vw, 2.4rem) !important;
        line-height: 1.2 !important;
    }
    h2 {
        font-family: 'Nunito', sans-serif !important;
        font-weight: 600 !important; color: #3a6b5a !important;
        font-size: clamp(1rem, 3vw, 1.3rem) !important;
    }
    h3 {
        font-family: 'Nunito', sans-serif !important;
        font-weight: 700 !important; color: #3a6b5a !important;
        font-size: clamp(0.95rem, 3vw, 1.15rem) !important;
    }
    h4 { color: #3a6b5a !important; font-size: clamp(0.9rem, 2.5vw, 1.05rem) !important; }

    /* ══════════════════════════════════════
       侧边栏
    ══════════════════════════════════════ */
    section[data-testid="stSidebar"] {
        background: #e8f2ee !important;
        border-right: 1px solid #c8e0d8;
    }

    /* ══════════════════════════════════════
       通用卡片
    ══════════════════════════════════════ */
    .fresh-card {
        background: rgba(255,255,255,0.75);
        border: 1px solid #d0e8df;
        border-radius: 14px;
        padding: clamp(12px, 3vw, 20px) clamp(14px, 3vw, 22px);
        backdrop-filter: blur(8px);
        box-shadow: 0 2px 10px rgba(60,120,90,0.07);
        transition: box-shadow 0.25s, transform 0.2s;
        margin-bottom: 4px;
    }
    .fresh-card:hover {
        box-shadow: 0 6px 20px rgba(60,120,90,0.13);
        transform: translateY(-2px);
    }

    /* ══════════════════════════════════════
       经济周期主卡
    ══════════════════════════════════════ */
    .clock-card {
        background: linear-gradient(135deg,
            rgba(255,255,255,0.88) 0%,
            rgba(232,244,240,0.92) 100%);
        border: 1.5px solid #b8d8ce;
        border-radius: 18px;
        padding: clamp(16px, 4vw, 28px) clamp(16px, 4vw, 30px);
        box-shadow: 0 4px 20px rgba(60,120,90,0.10);
        margin-bottom: 18px;
    }
    /* 手机：标题字号缩小 */
    .clock-phase-title {
        font-size: clamp(1.1rem, 4vw, 1.6rem);
        font-weight: 700;
        margin: 0;
        line-height: 1.3;
    }
    .clock-desc {
        font-size: clamp(0.82rem, 2.5vw, 1rem);
        color: #5a8a7a;
        margin: 6px 0 0;
    }
    /* 资产推荐小卡（手机变全宽） */
    .assets-card {
        background: rgba(255,255,255,0.72);
        border: 1px solid #d0e8df;
        border-radius: 12px;
        padding: 12px 16px;
        min-width: 160px;
        flex-shrink: 0;
    }

    /* ══════════════════════════════════════
       核心指标卡
    ══════════════════════════════════════ */
    .metric-card {
        background: rgba(255,255,255,0.82);
        border: 1px solid #d0e8df;
        border-radius: 13px;
        padding: clamp(12px, 2.5vw, 18px) clamp(10px, 2vw, 16px);
        text-align: center;
        box-shadow: 0 2px 8px rgba(60,120,90,0.06);
        transition: transform 0.2s;
        height: 100%;
    }
    .metric-card:hover { transform: translateY(-3px); }
    .metric-label {
        color: #6a9e8e; font-size: clamp(0.68rem, 1.8vw, 0.82rem);
        font-weight: 600; letter-spacing: 0.4px;
        text-transform: uppercase; margin: 0 0 3px 0;
    }
    .metric-name {
        color: #7aad98; font-size: clamp(0.7rem, 1.8vw, 0.78rem);
        margin: 0 0 4px;
    }
    .metric-value {
        color: #1f3d30;
        font-size: clamp(1.3rem, 4vw, 2rem);
        font-weight: 700; margin: 0;
        line-height: 1.15;
    }
    .metric-delta { font-size: clamp(0.7rem, 1.8vw, 0.82rem); margin: 4px 0 0 0; }

    /* ══════════════════════════════════════
       倒计时卡
    ══════════════════════════════════════ */
    .countdown-card {
        background: rgba(255,255,255,0.78);
        border: 1px solid #d0e8df;
        border-radius: 13px;
        padding: clamp(12px, 3vw, 16px) clamp(12px, 3vw, 18px);
        min-height: 110px;
        box-shadow: 0 2px 8px rgba(60,120,90,0.06);
        transition: border-color 0.2s;
    }
    @keyframes gentle-pulse {
        0%,100% { box-shadow: 0 2px 8px rgba(229,115,80,0.12); border-color: rgba(229,115,80,0.5); }
        50%      { box-shadow: 0 4px 20px rgba(229,115,80,0.28); border-color: rgba(229,115,80,0.75); }
    }
    .countdown-card.urgent { animation: gentle-pulse 2.5s ease-in-out infinite; }
    .countdown-label { color: #8aad9e; font-size: clamp(0.68rem, 1.8vw, 0.78rem); margin: 0 0 3px; }
    .countdown-title { color: #2d4a3e; font-size: clamp(0.85rem, 2.5vw, 1rem); font-weight: 700; margin: 0 0 6px; }
    .countdown-time  { color: #3a8a6e; font-size: clamp(1.1rem, 4vw, 1.55rem);
                       font-weight: 700; font-variant-numeric: tabular-nums; margin: 0 0 3px; }
    .countdown-meta  { color: #8aad9e; font-size: clamp(0.66rem, 1.6vw, 0.76rem); margin: 0; }

    /* ══════════════════════════════════════
       Radio（分类选择器）
    ══════════════════════════════════════ */
    div[data-testid="stRadio"] label {
        background: rgba(255,255,255,0.75) !important;
        border: 1px solid #c8e0d8 !important;
        border-radius: 8px !important;
        padding: 4px 10px !important;
        font-size: clamp(0.76rem, 2vw, 0.86rem) !important;
        color: #2d4a3e !important;
        cursor: pointer;
        white-space: nowrap;
    }
    div[data-testid="stRadio"] label:hover {
        background: rgba(255,255,255,0.95) !important;
        border-color: #3a8a6e !important;
    }
    div[data-testid="stRadio"] label p,
    div[data-testid="stRadio"] label span { color: #2d4a3e !important; }

    /* ══════════════════════════════════════
       输入框 & checkbox
    ══════════════════════════════════════ */
    .stTextInput > div > div > input {
        background-color: rgba(255,255,255,0.90) !important;
        color: #2d4a3e !important;
        border: 1px solid #c8e0d8 !important;
        border-radius: 10px !important;
        font-size: clamp(0.82rem, 2vw, 0.95rem) !important;
    }
    .stTextInput > div > div > input::placeholder { color: #9abfb0 !important; }
    .stTextInput > div > div > input:focus {
        border-color: #3a8a6e !important;
        box-shadow: 0 0 0 2px rgba(58,138,110,0.15) !important;
    }
    input, textarea, select {
        background-color: rgba(255,255,255,0.90) !important;
        color: #2d4a3e !important;
    }
    .stCheckbox label {
        color: #1f3d30 !important;
        font-size: clamp(0.82rem, 2vw, 0.9rem) !important;
        font-weight: 600 !important;
    }
    .stCheckbox label p,
    .stCheckbox label span { color: #1f3d30 !important; }

    /* ══════════════════════════════════════
       按钮
    ══════════════════════════════════════ */
    .stButton > button {
        background: linear-gradient(135deg, #3a8a6e, #5aaa8a) !important;
        color: white !important; border: none !important;
        border-radius: 10px !important; font-weight: 600 !important;
        padding: 7px 18px !important; transition: opacity 0.2s !important;
        font-size: clamp(0.82rem, 2vw, 0.92rem) !important;
    }
    .stButton > button:hover { opacity: 0.88 !important; }

    /* ══════════════════════════════════════
       ML / 标签徽章
    ══════════════════════════════════════ */
    .ml-badge {
        display: inline-block;
        background: linear-gradient(135deg, #e8f4f0, #d4ece4);
        border: 1px solid #a8d4c4;
        border-radius: 8px;
        padding: 2px 10px;
        font-size: clamp(0.7rem, 1.8vw, 0.76rem);
        color: #3a6b5a;
        font-weight: 600;
    }

    /* ══════════════════════════════════════
       杂项
    ══════════════════════════════════════ */
    hr { border: none; border-top: 1px solid #d0e8df !important; margin: 18px 0 !important; }
    .stAlert { border-radius: 12px !important; }
    .streamlit-expanderHeader { color: #3a6b5a !important; font-weight: 600 !important; }
    details summary { color: #3a6b5a !important; }
    .stCaption, [data-testid="stCaptionContainer"] p { color: #8aad9e !important; font-size: clamp(0.72rem, 1.8vw, 0.82rem) !important; }
    .stMarkdown p { color: #2d4a3e; }

    /* ══════════════════════════════════════
       移动端媒体查询
    ══════════════════════════════════════ */
    @media (max-width: 640px) {
        /* 块容器紧凑 */
        .block-container {
            padding-left: 0.6rem !important;
            padding-right: 0.6rem !important;
        }

        /* 时钟卡内部改为竖排 */
        .clock-inner-flex {
            flex-direction: column !important;
            gap: 12px !important;
        }
        .assets-card { min-width: unset !important; width: 100% !important; }

        /* 指标卡字号再小一点 */
        .metric-value { font-size: 1.2rem !important; }

        /* 倒计时卡最小高度降低 */
        .countdown-card { min-height: 90px !important; }

        /* 图表高度在小屏降低 */
        .js-plotly-plot { min-height: 240px !important; }

        /* Radio 分类标签允许换行 */
        div[data-testid="stRadio"] label { white-space: normal !important; font-size: 0.76rem !important; }
    }

    @media (max-width: 480px) {
        h1 { font-size: 1.5rem !important; }
        .metric-value { font-size: 1.1rem !important; }
        .countdown-time { font-size: 1.1rem !important; }
    }

    /* scrollbar */
    ::-webkit-scrollbar { width: 5px; }
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

# ==========================================
# 4. 序列元数据表（每个序列独立定义取数方式）
# ==========================================
# 每条记录格式：
#   units    : 传给 FRED get_series 的 units 参数
#   display  : "value"=原值 | "yoy"=同比% | "mom_diff"=月度净增量
#   chart    : 图表渲染类型
#   unit_str : Y轴单位标注
#   label    : 图表标题后缀说明
#
# FRED units 说明：
#   lin  = 原始值
#   pc1  = 与上年同期相比的百分比变化（月度同比）
#   pch  = 与上期相比的百分比变化（季度环比）
#   pca  = 复合年增长率（季度，年化）

SERIES_META = {
    # ── 增长 (Growth) ──
    "GDPC1": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "实际GDP 同比 %"
    },
    "A191RL1Q225SBEA": {
        "units": "lin", "display": "value",
        "chart": "bar_abs", "unit_str": "%", "label": "GDP环比增速（季度年化 %）"
    },
    "INDPRO": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "工业生产 同比 %"
    },
    "RSAFS": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "零售销售 同比 %"
    },
    "PCE": {
        "units": "pc1", "display": "value",
        "chart": "line_yoy", "unit_str": "%", "label": "个人消费 同比 %"
    },
    "DGORDER": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "耐用品订单 同比 %"
    },
    "PERMIT": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "建筑许可 同比 %"
    },
    "HOUST": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "新屋开工 同比 %"
    },
    "USSLIND": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "", "label": "领先指标（指数）"
    },
    "CFNAI": {
        "units": "lin", "display": "value",
        "chart": "bar_abs", "unit_str": "", "label": "芝加哥联储活动指数"
    },
    "UMCSENT": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "", "label": "密歇根消费者信心"
    },
    # ── 通胀 (Inflation) ──
    "CPIAUCSL": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "CPI总体 同比 %"
    },
    "CPILFESL": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "核心CPI 同比 %"
    },
    "CUSR0000SAH1": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "CPI住房 同比 %"
    },
    "PCEPI": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "PCE总体 同比 %"
    },
    "PCEPILFE": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "核心PCE 同比 %"
    },
    "PPIACO": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "PPI总体 同比 %"
    },
    "PPIFID": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "PPI最终需求 同比 %"
    },
    "MICH": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "密歇根通胀预期 1Y %"
    },
    "T5YIE": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "5年盈亏平衡通胀率 %"
    },
    "T10YIE": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "10年盈亏平衡通胀率 %"
    },
    # ── 就业 (Employment) ──
    "UNRATE": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "失业率 %"
    },
    "PAYEMS": {
        "units": "lin", "display": "mom_diff",
        "chart": "bar_abs", "unit_str": " 千人", "label": "非农就业 月增（千人）"
    },
    "USPRIV": {
        "units": "lin", "display": "mom_diff",
        "chart": "bar_abs", "unit_str": " 千人", "label": "私人非农 月增（千人）"
    },
    "CIVPART": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "劳动力参与率 %"
    },
    "EMRATIO": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "就业人口比 %"
    },
    "CES0500000003": {
        "units": "pc1", "display": "value",
        "chart": "line_yoy", "unit_str": "%", "label": "平均时薪 同比 %"
    },
    "AWHAETP": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": " 小时", "label": "平均每周工时"
    },
    "ICSA": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": " 人", "label": "初请失业金（人）"
    },
    "CCSA": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": " 人", "label": "续请失业金（人）"
    },
    "JTSJOL": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": " 千人", "label": "职位空缺 JOLTS（千人）"
    },
    # ── 利率 (Interest Rates) ──
    "FEDFUNDS": {
        "units": "lin", "display": "value",
        "chart": "step", "unit_str": "%", "label": "联邦基金利率 %"
    },
    "DFF": {
        "units": "lin", "display": "value",
        "chart": "step", "unit_str": "%", "label": "有效联邦基金利率 %"
    },
    "IORB": {
        "units": "lin", "display": "value",
        "chart": "step", "unit_str": "%", "label": "准备金利率 IORB %"
    },
    "DGS2": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "2年期美债 %"
    },
    "DGS10": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "10年期美债 %"
    },
    "T10Y2Y": {
        "units": "lin", "display": "value",
        "chart": "spread", "unit_str": "%", "label": "10Y-2Y利差 %"
    },
    "T10Y3M": {
        "units": "lin", "display": "value",
        "chart": "spread", "unit_str": "%", "label": "10Y-3M利差 %"
    },
    "MORTGAGE30US": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "30年抵押贷款利率 %"
    },
    "BAA": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "BAA企业债收益率 %"
    },
    "BAMLH0A0HYM2": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "高收益债利差 OAS %"
    },
    "BAMLC0A0CM": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "投资级债利差 OAS %"
    },
    "DFII10": {
        "units": "lin", "display": "value",
        "chart": "spread", "unit_str": "%", "label": "10年TIPS实际利率 %"
    },
    # ── 流动性 (Liquidity) ──
    "M2SL": {
        "units": "pc1", "display": "value",
        "chart": "line_yoy", "unit_str": "%", "label": "M2 同比 %"
    },
    "M1SL": {
        "units": "pc1", "display": "value",
        "chart": "line_yoy", "unit_str": "%", "label": "M1 同比 %"
    },
    "WALCL": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": " 百万$", "label": "美联储资产负债表（百万$）"
    },
    "TREAST": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": " 百万$", "label": "联储持有国债（百万$）"
    },
    "RRPONTSYD": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": " 十亿$", "label": "隔夜逆回购余额（十亿$）"
    },
    "WRESBAL": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": " 十亿$", "label": "银行准备金（十亿$）"
    },
    "NFCI": {
        "units": "lin", "display": "value",
        "chart": "spread", "unit_str": "", "label": "芝加哥金融条件指数"
    },
    "STLFSI4": {
        "units": "lin", "display": "value",
        "chart": "spread", "unit_str": "", "label": "圣路易斯金融压力指数"
    },
    "TEDRATE": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "TED利差 %"
    },
    "SOFR": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "%", "label": "SOFR %"
    },
    "DTWEXBGS": {
        "units": "lin", "display": "value",
        "chart": "line", "unit_str": "", "label": "贸易加权美元指数"
    },
    # ── AI代理 ──
    "IPG3344N": {
        "units": "pc1", "display": "value",
        "chart": "line_yoy", "unit_str": "%", "label": "半导体工业生产 同比 %"
    },
    "PCU33443344": {
        "units": "pc1", "display": "value",
        "chart": "bar_yoy", "unit_str": "%", "label": "半导体PPI 同比 %"
    },
}

# 兼容旧代码引用
PC1_SERIES      = {sid for sid, m in SERIES_META.items() if m["units"] == "pc1"}
MOM_DIFF_SERIES = {sid for sid, m in SERIES_META.items() if m["display"] == "mom_diff"}
UNIT_MAP        = {sid: m["unit_str"] for sid, m in SERIES_META.items()}
CHART_TYPE      = {sid: m["chart"]    for sid, m in SERIES_META.items()}


# ==========================================
# 5. 数据拉取（基于 SERIES_META，每个序列独立处理）
# ==========================================
@st.cache_data(ttl=43200)   # 12 小时缓存
def fetch_data_advanced(series_id, years=6):
    """
    根据 SERIES_META 中该序列的 units/display 配置精确拉取。
    - 不传 observation_end → FRED 返回最新已发布数据
    - 先在完整历史上计算衍生列，再截断展示窗口
    """
    meta = SERIES_META.get(series_id, {"units": "lin", "display": "value"})
    req_units   = meta["units"]
    display     = meta["display"]
    today       = datetime.today()

    # 拉取窗口：比展示窗口多 2 年（保证 lag 计算）
    start_date  = today - relativedelta(years=years + 2)

    try:
        # ✅ 不传 observation_end，FRED 返回最新已发布数据
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

        # ── 衍生列（在完整历史上计算，截断前完成）──
        if display == "mom_diff":
            # 非农/私人就业：月度净增（千人）
            df["YoY"]        = df["Value"].diff(1)
            df["Value_Diff"] = df["Value"].diff(1)
        else:
            # units=pc1/pch/pca → FRED 已返回变化率，Value 即最终展示值
            # units=lin → Value 是原始水平值，YoY 列保留备用但图表用 Value
            df["YoY"]        = df["Value"]          # pc1 系列 Value 本身就是同比%
            df["Value_Diff"] = df["Value"].diff(1)

        # ── 截断展示窗口（计算完成后再截断）──
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
# 5. ML 经济周期识别（增强版 HMM）
# ==========================================
# 特征说明（7维）：
#   1. INDPRO YoY     — 工业生产同比，增长动能
#   2. CPIAUCSL YoY   — CPI同比，通胀水平
#   3. T10Y2Y         — 10Y-2Y利差，金融压力/衰退预警
#   4. UNRATE         — 失业率水平，劳动市场松紧
#   5. UNRATE MoM变动 — 失业率边际，就业拐点
#   6. FEDFUNDS       — 政策利率，货币政策立场
#   7. NFCI           — 芝加哥金融条件指数，流动性宽松度

_CLOCK_FEATURES = {
    "INDPRO":    ("YoY",   10),   # 工业产出同比
    "CPIAUCSL":  ("Value", 10),   # CPI同比（pc1）
    "T10Y2Y":    ("Value", 10),   # 10Y-2Y利差
    "UNRATE":    ("Value", 10),   # 失业率
    "FEDFUNDS":  ("Value", 10),   # 政策利率
    "NFCI":      ("Value", 10),   # 金融条件指数
    "T10YIE":    ("Value", 10),   # 10Y通胀预期（市场锚定）
}


def _to_monthly_series(df, col):
    s = df.set_index("Date")[col].copy()
    s.index = pd.to_datetime(s.index)
    return s.resample("MS").last()


def _phase_from_means(g_mean, i_mean, spread_mean, fedfunds_mean):
    """
    增强四象限映射：综合增长、通胀、利差、政策利率四维判断。
    利差为负（倒挂）且利率偏高 → 偏衰退；通胀高但增长弱 → 偏滞胀。
    """
    g_up = g_mean > 1.5        # INDPRO 同比
    i_up = i_mean > 2.5        # CPI 同比 > Fed 目标
    inverted = spread_mean < 0  # 收益率曲线倒挂

    # 滞胀：增长弱 + 通胀高（无论曲线是否倒挂）
    if not g_up and i_up:
        return "滞胀 (Stagflation)", "☁️ 增长放缓 / 通胀顽固", "#f2a65a", "现金 > 大宗 > 债券"
    # 衰退/修复：增长弱 + 通胀低 + 曲线倒挂 → 深衰退
    if not g_up and not i_up and inverted:
        return "衰退 (Recession)", "🥶 衰退深化 / 曲线倒挂", "#6a9fd8", "债券 > 现金 > 股票"
    # 衰退修复：增长弱 + 通胀低 + 曲线正常
    if not g_up and not i_up and not inverted:
        return "修复 (Reflation)", "🌱 衰退修复 / 通胀回落", "#5abcb0", "债券 > 股票 > 现金"
    # 过热：增长强 + 通胀高
    if g_up and i_up:
        return "过热 (Overheat)", "🔥 增长强劲 / 通胀升温", "#e07a5f", "大宗 > 股票 > 现金"
    # 复苏：增长强 + 通胀低
    return "复苏 (Recovery)", "📈 增长强劲 / 通胀温和", "#4caf8a", "股票 > 债券 > 大宗"


@st.cache_data(ttl=3600 * 6)
def calculate_ml_investment_clock():
    """
    增强版 HMM 经济周期识别：
    - 7 维宏观特征（增长/通胀/利率/流动性/预期）
    - BIC 准则自动选择最优状态数（3-6）
    - 动量修正：结合近3个月特征变化判断方向
    - 输出：阶段标签、置信度、特征快照、历史状态序列
    """
    try:
        from hmmlearn.hmm import GaussianHMM
        HMM_AVAILABLE = True
    except ImportError:
        HMM_AVAILABLE = False

    try:
        from sklearn.preprocessing import StandardScaler
        SK_AVAILABLE = True
    except ImportError:
        SK_AVAILABLE = False

    # ── 1. 并行拉取特征数据 ──
    raw = {}
    sids = list(_CLOCK_FEATURES.keys())
    with ThreadPoolExecutor(max_workers=len(sids)) as ex:
        fut = {ex.submit(fetch_data_advanced, sid, _CLOCK_FEATURES[sid][1]): sid
               for sid in sids}
        for f in as_completed(fut):
            raw[fut[f]] = f.result()

    if any(raw[s].empty for s in sids):
        gdf = raw.get("INDPRO", pd.DataFrame())
        cdf = raw.get("CPIAUCSL", pd.DataFrame())
        return _fallback_rule_clock(gdf, cdf)

    # ── 2. 月度对齐，构建特征矩阵 ──
    series_list = []
    names = []
    for sid in sids:
        col = _CLOCK_FEATURES[sid][0]
        s = _to_monthly_series(raw[sid], col)
        s.name = sid
        series_list.append(s)
        names.append(sid)

    combined = pd.concat(series_list, axis=1).dropna()

    # 补充特征：UNRATE 月度变动（就业拐点）
    if "UNRATE" in combined.columns:
        combined["UNRATE_MOM"] = combined["UNRATE"].diff(1)
        combined = combined.dropna()
        names.append("UNRATE_MOM")

    if len(combined) < 48:
        return _fallback_rule_clock(raw["INDPRO"], raw["CPIAUCSL"])

    # ── 3. 标准化 ──
    X_raw = combined.values
    if SK_AVAILABLE:
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        X = scaler.fit_transform(X_raw)
    else:
        # 手动标准化
        mu, sigma = X_raw.mean(0), X_raw.std(0)
        sigma[sigma == 0] = 1
        X = (X_raw - mu) / sigma

    # ── 最新快照 ──
    latest     = combined.iloc[-1]
    prev3      = combined.iloc[-4]   # 3个月前
    g_now      = float(latest["INDPRO"])
    i_now      = float(latest["CPIAUCSL"])
    sp_now     = float(latest["T10Y2Y"])
    ff_now     = float(latest["FEDFUNDS"])
    g_delta    = g_now - float(prev3["INDPRO"])     # 增长动量
    i_delta    = i_now - float(prev3["CPIAUCSL"])   # 通胀动量
    ur_now     = float(latest["UNRATE"])
    nfci_now   = float(latest["NFCI"])
    infl_exp   = float(latest["T10YIE"])

    if HMM_AVAILABLE:
        try:
            # ── 4. BIC 自动选择最优状态数 ──
            best_model, best_bic, best_n = None, np.inf, 4
            for n_states in range(3, 7):
                try:
                    m = GaussianHMM(
                        n_components=n_states,
                        covariance_type="full",
                        n_iter=300,
                        random_state=42,
                        tol=1e-5,
                    )
                    m.fit(X)
                    # BIC = -2*logL + k*ln(T)
                    log_prob = m.score(X) * len(X)
                    n_params = (n_states ** 2 - n_states +          # 转移矩阵
                                n_states * X.shape[1] +             # 均值
                                n_states * X.shape[1] ** 2)         # 协方差（full）
                    bic = -2 * log_prob + n_params * np.log(len(X))
                    if bic < best_bic:
                        best_bic, best_model, best_n = bic, m, n_states
                except Exception:
                    continue

            if best_model is None:
                raise RuntimeError("所有 HMM 拟合均失败")

            model  = best_model
            states = model.predict(X)
            combined = combined.copy()
            combined["state"] = states

            # ── 5. 计算每个隐状态的宏观均值 ──
            state_profiles = {}
            for s in range(best_n):
                mask = combined["state"] == s
                if mask.sum() > 2:
                    state_profiles[s] = {
                        "g":      combined.loc[mask, "INDPRO"].mean(),
                        "i":      combined.loc[mask, "CPIAUCSL"].mean(),
                        "spread": combined.loc[mask, "T10Y2Y"].mean(),
                        "ff":     combined.loc[mask, "FEDFUNDS"].mean(),
                        "ur":     combined.loc[mask, "UNRATE"].mean(),
                        "nfci":   combined.loc[mask, "NFCI"].mean(),
                        "count":  int(mask.sum()),
                    }

            current_state = int(states[-1])
            prof = state_profiles.get(current_state, {
                "g": g_now, "i": i_now, "spread": sp_now, "ff": ff_now,
                "ur": ur_now, "nfci": nfci_now,
            })

            # ── 6. 动量修正：若近3月增长/通胀动量方向与状态均值矛盾，降权并提示 ──
            momentum_note = ""
            raw_phase, raw_desc, raw_color, raw_assets = _phase_from_means(
                prof["g"], prof["i"], prof["spread"], prof["ff"]
            )
            # 增长动量修正
            if g_delta > 1.5 and "衰退" in raw_phase:
                momentum_note += "⚡ 增长动量向上（近3月 +{:.1f}%），衰退压力或减弱；".format(g_delta)
            elif g_delta < -1.5 and "复苏" in raw_phase:
                momentum_note += "⚠️ 增长动量转弱（近3月 {:.1f}%），复苏势头需观察；".format(g_delta)
            if i_delta > 0.5 and "衰退" in raw_phase:
                momentum_note += "🌡 通胀再加速（近3月 +{:.1f}%），警惕滞胀风险；".format(i_delta)

            phase, desc, color, assets = raw_phase, raw_desc, raw_color, raw_assets

            # ── 7. 后验置信度 ──
            posteriors = model.predict_proba(X)
            confidence = float(posteriors[-1, current_state]) * 100

            # ── 8. 历史状态序列（标注 phase_name）──
            history = combined[["state"]].copy()
            history["phase_name"] = history["state"].map(
                lambda s: _phase_from_means(
                    state_profiles.get(s, {}).get("g", 0),
                    state_profiles.get(s, {}).get("i", 0),
                    state_profiles.get(s, {}).get("spread", 0),
                    state_profiles.get(s, {}).get("ff", 5),
                )[0]
            )
            history["phase_color"] = history["state"].map(
                lambda s: _phase_from_means(
                    state_profiles.get(s, {}).get("g", 0),
                    state_profiles.get(s, {}).get("i", 0),
                    state_profiles.get(s, {}).get("spread", 0),
                    state_profiles.get(s, {}).get("ff", 5),
                )[2]
            )

            # ── 9. 构造判断说明 ──
            note_lines = [
                f"**模型**：Gaussian HMM，BIC 最优态数 **{best_n}**，当前隐状态 #{current_state}",
                f"**置信度**：{confidence:.1f}%（后验概率）",
                f"**特征快照**（该状态历史均值）：",
                f"- 工业产出同比 {prof['g']:.2f}% ｜ CPI同比 {prof['i']:.2f}% ｜ 通胀预期 {infl_exp:.2f}%",
                f"- 10Y-2Y利差 {prof['spread']:.2f}% ｜ 联邦基金利率 {prof['ff']:.2f}% ｜ 失业率 {ur_now:.1f}%",
                f"- 芝加哥金融条件指数 NFCI {nfci_now:.3f}（负=宽松）",
                f"**近3月动量**：增长 {g_delta:+.2f}%  |  通胀 {i_delta:+.2f}%",
            ]
            if momentum_note:
                note_lines.append(f"**动量修正提示**：{momentum_note}")
            note_lines.append("_以上为辅助判断，非投资建议。_")
            full_note = "\n\n".join(note_lines)

            return phase, desc, color, assets, full_note, True, confidence, history

        except Exception as e:
            pass  # 兜底

    # ── 规则兜底 ──
    return _fallback_rule_clock(raw["INDPRO"], raw["CPIAUCSL"])


def _fallback_rule_clock(growth_df, cpi_df):
    if growth_df.empty or cpi_df.empty or len(growth_df) < 4 or len(cpi_df) < 4:
        return "数据不足", "🔧 无法计算", "#aaaaaa", "保持现金", "数据不足", False, 0.0, None
    g_now  = float(growth_df["YoY"].iloc[-1]) if "YoY" in growth_df else 0
    i_now  = float(cpi_df["Value"].iloc[-1])
    phase, desc, color, assets = _phase_from_means(g_now, i_now, 0.5, 3.0)
    note = "未安装 hmmlearn / sklearn，或历史数据不足；使用 INDPRO同比 × CPI同比 简化规则。"
    return phase, desc, color, assets, note, False, 0.0, None


# ==========================================
# 5b. 相位颜色辅助
# ==========================================
def _phase_color(phase: str) -> str:
    if "复苏" in phase:   return "#4caf8a"
    if "过热" in phase:   return "#e07a5f"
    if "滞胀" in phase:   return "#f2a65a"
    if "修复" in phase:   return "#5abcb0"
    if "衰退" in phase:   return "#6a9fd8"
    return "#3a8a6e"


# ==========================================
# 5c. 联储 RSS
# ==========================================
_ET = ZoneInfo("America/New_York")
_FED_RSS_UA = {"User-Agent": "Mozilla/5.0 (compatible; MacroTrack/2.0)"}
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
    """
    直接从 SERIES_META 读取 chart/unit_str/label，零条件判断，干净分发。
    标题末尾自动附上最新观测日期。
    """
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

    # ── 分发渲染（与 SERIES_META chart 字段一一对应）──
    if ctype == "bar_yoy":
        # Value 列已经是同比%（FRED pc1 接口直接返回）
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
        # 原始值，正负双色柱（A191RL1Q225SBEA / CFNAI / 非农等）
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

    layout        = dict(**_BASE_LAYOUT)
    layout["title"] = dict(
        text=full_title,
        font=dict(size=12, color="#1f3d30", family="Nunito"),
        x=0.01, xanchor="left",
    )
    layout["height"] = 300
    fig.update_layout(**layout)
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
            f"📡 **数据更新提示**：FRED 已发布 GDPC1 至 **{fred_latest}**，"
            f"当前缓存截止 {local_latest}。点击右上角「🔄 刷新数据」即可获取最新。",
            icon="🔔",
        )

_show_freshness_banner()

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
  <div class="clock-inner-flex"
       style="display:flex; justify-content:space-between; align-items:flex-start;
              flex-wrap:wrap; gap:14px;">
    <div style="flex:1; min-width:0;">
      <div style="margin-bottom:8px;">{ml_tag}</div>
      <p class="clock-phase-title" style="color:{color};">
        当前经济阶段：{phase}
      </p>
      <p class="clock-desc">{desc} {conf_str}</p>
    </div>
    <div class="assets-card">
      <p style="margin:0; color:#8aad9e; font-size:0.78rem; font-weight:600;">建议超配资产（示意）</p>
      <p style="margin:6px 0 0; color:#2d4a3e; font-size:clamp(0.9rem,2.5vw,1.1rem); font-weight:700;">{assets}</p>
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
# 手机上 st.columns(6) 会挤成一排，改用两行各3列
_m_row1 = list(top_metrics.items())[:3]
_m_row2 = list(top_metrics.items())[3:]

def _render_metric_col(col, name, sid, sub):
    df = fetch_data_advanced(sid)
    with col:
        if df.empty:
            return
        latest  = df.iloc[-1]
        unit    = UNIT_MAP.get(sid, "")
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
