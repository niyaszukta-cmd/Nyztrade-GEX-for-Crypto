# ============================================================================
# NYZTrade CRYPTO GEX Dashboard
# BTC · ETH · XAU (Gold) Options via Deribit API
# Full GEX / VANNA / Cascade Analytics — Same as India Dashboard
# ============================================================================

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy.stats import norm
from datetime import datetime, timedelta
import pytz
import requests
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
import warnings
import hashlib
import json
import os
import pickle
from pathlib import Path
warnings.filterwarnings('ignore')

# ============================================================================
# PAGE CONFIG & STYLING
# ============================================================================

st.set_page_config(
    page_title="NYZTrade Crypto GEX — BTC ETH XAU",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Space+Grotesk:wght@300;400;500;600;700&display=swap');

    header[data-testid="stHeader"] a[href*="github"] { display: none !important; }

    :root {
        --bg-primary: #0a0e17;
        --bg-secondary: #111827;
        --bg-card: #1a2332;
        --bg-card-hover: #232f42;
        --accent-green: #10b981;
        --accent-red: #ef4444;
        --accent-blue: #3b82f6;
        --accent-purple: #8b5cf6;
        --accent-yellow: #f59e0b;
        --accent-cyan: #06b6d4;
        --text-primary: #f1f5f9;
        --text-secondary: #94a3b8;
        --text-muted: #64748b;
        --border-color: #2d3748;
    }

    .stApp { background: linear-gradient(135deg, var(--bg-primary) 0%, #0f172a 50%, var(--bg-primary) 100%); }

    .main-header {
        background: linear-gradient(135deg, rgba(59,130,246,0.1) 0%, rgba(139,92,246,0.1) 100%);
        border: 1px solid var(--border-color);
        border-radius: 16px; padding: 24px 32px; margin-bottom: 24px;
        backdrop-filter: blur(10px);
    }
    .main-title {
        font-family: 'Space Grotesk', sans-serif; font-size: 2.2rem; font-weight: 700;
        background: linear-gradient(135deg, #f59e0b, #8b5cf6, #06b6d4);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0;
    }
    .sub-title { font-family: 'JetBrains Mono', monospace; color: var(--text-secondary); font-size: 0.9rem; margin-top: 8px; }

    .metric-card {
        background: var(--bg-card); border: 1px solid var(--border-color);
        border-radius: 12px; padding: 20px; transition: all 0.3s ease;
    }
    .metric-card:hover { background: var(--bg-card-hover); transform: translateY(-2px); box-shadow: 0 8px 25px rgba(0,0,0,0.3); }
    .metric-card.positive { border-left: 4px solid var(--accent-green); }
    .metric-card.negative { border-left: 4px solid var(--accent-red); }
    .metric-card.neutral  { border-left: 4px solid var(--accent-yellow); }

    .metric-label  { font-family: 'JetBrains Mono', monospace; color: var(--text-muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 8px; }
    .metric-value  { font-family: 'Space Grotesk', sans-serif; font-size: 1.75rem; font-weight: 700; color: var(--text-primary); line-height: 1.2; }
    .metric-value.positive { color: var(--accent-green); }
    .metric-value.negative { color: var(--accent-red); }
    .metric-value.neutral  { color: var(--accent-yellow); }
    .metric-delta  { font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; margin-top: 8px; color: var(--text-secondary); }

    .live-indicator {
        display: inline-flex; align-items: center; gap: 8px; padding: 6px 14px;
        background: rgba(239,68,68,0.1); border: 1px solid rgba(239,68,68,0.3);
        border-radius: 20px;
    }
    .live-dot { width: 8px; height: 8px; background: var(--accent-red); border-radius: 50%; animation: blink 1.5s ease-in-out infinite; }
    .crypto-badge {
        display: inline-flex; align-items: center; gap: 6px; padding: 4px 12px;
        background: rgba(245,158,11,0.2); border: 1px solid rgba(245,158,11,0.4);
        border-radius: 12px; color: #fcd34d; font-size: 0.75rem; font-weight: 600;
    }
    .spike-legend {
        padding: 10px 16px; background: rgba(59,130,246,0.08);
        border: 1px solid rgba(59,130,246,0.25); border-radius: 10px;
        font-family: 'JetBrains Mono', monospace; font-size: 0.78rem;
        color: #94a3b8; line-height: 1.8;
    }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.3} }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# DERIBIT CONFIGURATION
# ============================================================================
# Deribit API is FREE — no auth needed for market data
# Get API keys from: https://www.deribit.com/account/mainaccount/api
# For reading market data: no keys needed (public endpoints)
# For trading: create Read-only API key on Deribit
#
# In .streamlit/secrets.toml (optional — only for trading):
#   DERIBIT_CLIENT_ID     = "your_client_id"
#   DERIBIT_CLIENT_SECRET = "your_client_secret"
# ============================================================================

DERIBIT_BASE = "https://www.deribit.com/api/v2"

# ── Contract Specs (equivalent to INDEX_CONFIG in India dashboard) ──────────
CRYPTO_CONFIG = {
    'BTC': {
        'contract_size':   1,
        'strike_interval': 1000,
        'pts_per_unit':    0.00001,   # empirical: ~$100k GEX → ~1pt BTC move
        'strike_cap_pts':  2000,      # BTC can move big per strike
        'currency':        'BTC',
        'unit_label':      'K',       # display in thousands USD
        'unit_divisor':    1e3,
        'emoji':           '₿',
        'color':           '#f59e0b',
    },
    'ETH': {
        'contract_size':   1,
        'strike_interval': 50,
        'pts_per_unit':    0.0001,
        'strike_cap_pts':  200,
        'currency':        'ETH',
        'unit_label':      'K',
        'unit_divisor':    1e3,
        'emoji':           '🔷',
        'color':           '#6366f1',
    },
    'XAU': {
        'contract_size':   1,        # 1 troy oz per contract on Deribit
        'strike_interval': 25,       # $25 intervals for gold (ATM ~$3100)
        'pts_per_unit':    0.002,    # empirical: gold moves ~$2 per 1K GEX
        'strike_cap_pts':  50,       # gold max ~$50 per strike cascade
        'currency':        'XAU',
        'unit_label':      'K',
        'unit_divisor':    1e3,
        'emoji':           '🥇',
        'color':           '#fbbf24',
    },
}

# ── Cache Manager ────────────────────────────────────────────────────────────
CACHE_DIR = Path("cache/crypto")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _cache_key(symbol: str, expiry: str) -> str:
    return hashlib.md5(f"{symbol}_{expiry}".encode()).hexdigest()[:12]

def _save_cache(key: str, df: pd.DataFrame, meta: dict):
    try:
        with open(CACHE_DIR / f"{key}.pkl", 'wb') as f:
            pickle.dump({'df': df, 'meta': meta, 'ts': time.time()}, f)
    except Exception:
        pass

def _load_cache(key: str, max_age: int = 60):
    try:
        p = CACHE_DIR / f"{key}.pkl"
        if not p.exists():
            return None, None
        with open(p, 'rb') as f:
            d = pickle.load(f)
        if time.time() - d['ts'] > max_age:
            return None, None
        return d['df'], d['meta']
    except Exception:
        return None, None

# ============================================================================
# DERIBIT API — DATA FETCHER
# ============================================================================

def deribit_get(method: str, params: dict = None) -> dict:
    """Generic Deribit public API call — no authentication needed."""
    url = f"{DERIBIT_BASE}/public/{method}"
    try:
        r = requests.get(url, params=params or {}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if 'result' in data:
            return data['result']
        return {}
    except Exception as e:
        # Silently return empty — caller handles missing strikes gracefully
        return {}


def get_deribit_instruments(currency: str) -> list:
    """Get all active options instruments for a currency."""
    result = deribit_get("get_instruments", {
        "currency": currency,
        "kind":     "option",
        "expired":  False,
    })
    return result if isinstance(result, list) else []


def get_deribit_expiries(currency: str) -> list:
    """Return sorted list of available expiry dates."""
    instruments = get_deribit_instruments(currency)
    expiries = set()
    for ins in instruments:
        # instrument_name format: BTC-31JAN25-50000-C
        parts = ins.get('instrument_name', '').split('-')
        if len(parts) >= 2:
            expiries.add(parts[1])
    return sorted(expiries)


def get_deribit_ticker(instrument_name: str) -> dict:
    """Get live ticker for one option instrument."""
    return deribit_get("ticker", {"instrument_name": instrument_name})


def fetch_options_chain(currency: str, expiry: str,
                        spot_price: float, atm_range: int = 12) -> pd.DataFrame:
    """
    Fetch full options chain for currency+expiry and compute Greeks.

    Returns DataFrame with columns matching India dashboard schema:
        strike, call_oi, put_oi, call_volume, put_volume,
        call_iv, put_iv, call_delta, put_delta,
        call_gamma, put_gamma, call_vanna, put_vanna,
        net_gex, net_vanna, net_dex, total_volume,
        call_oi_change, put_oi_change, enhanced_oi_gex,
        spot_price, timestamp
    """
    cfg = CRYPTO_CONFIG[currency]
    strike_interval = cfg['strike_interval']

    # Build ATM strike range
    atm_strike = round(spot_price / strike_interval) * strike_interval
    strikes_to_fetch = [
        atm_strike + i * strike_interval
        for i in range(-atm_range, atm_range + 1)
    ]

    rows = []
    progress = st.progress(0, text=f"Fetching {currency} options chain...")

    for idx, strike in enumerate(strikes_to_fetch):
        progress.progress((idx + 1) / len(strikes_to_fetch),
                          text=f"Fetching strike {strike:,.0f}...")

        call_name = f"{currency}-{expiry}-{int(strike)}-C"
        put_name  = f"{currency}-{expiry}-{int(strike)}-P"

        call_t = get_deribit_ticker(call_name)
        put_t  = get_deribit_ticker(put_name)
        time.sleep(0.05)  # gentle rate limiting

        if not call_t and not put_t:
            continue  # Strike doesn't exist for this expiry — skip silently

        # Greeks from Deribit (they compute BS Greeks server-side)
        c_greeks = call_t.get('greeks', {})
        p_greeks = put_t.get('greeks', {})

        call_iv     = call_t.get('mark_iv', 0) / 100.0
        put_iv      = put_t.get('mark_iv', 0)  / 100.0
        call_delta  = c_greeks.get('delta', 0)
        put_delta   = p_greeks.get('delta', 0)
        call_gamma  = c_greeks.get('gamma', 0)
        put_gamma   = p_greeks.get('gamma', 0)
        call_vanna  = c_greeks.get('vanna', 0)
        put_vanna   = p_greeks.get('vanna', 0)
        call_oi     = call_t.get('open_interest', 0)
        put_oi      = put_t.get('open_interest', 0)
        call_vol    = call_t.get('stats', {}).get('volume', 0)
        put_vol     = put_t.get('stats', {}).get('volume', 0)

        cs = cfg['contract_size']

        # GEX = OI × Gamma × Spot² × ContractSize  (same formula as India)
        net_gex_val   = (call_oi * call_gamma - put_oi * put_gamma) * spot_price ** 2 * cs
        net_vanna_val = (call_oi * call_vanna - put_oi * put_vanna) * cs
        net_dex_val   = (call_oi * call_delta + put_oi * put_delta) * cs

        rows.append({
            'strike':       strike,
            'call_oi':      call_oi,
            'put_oi':       put_oi,
            'call_volume':  call_vol,
            'put_volume':   put_vol,
            'call_iv':      call_iv * 100,
            'put_iv':       put_iv  * 100,
            'call_delta':   call_delta,
            'put_delta':    put_delta,
            'call_gamma':   call_gamma,
            'put_gamma':    put_gamma,
            'call_vanna':   call_vanna,
            'put_vanna':    put_vanna,
            'net_gex':      net_gex_val   / cfg['unit_divisor'],
            'net_vanna':    net_vanna_val / cfg['unit_divisor'],
            'net_dex':      net_dex_val   / cfg['unit_divisor'],
            'total_volume': call_vol + put_vol,
            # OI change placeholders (populated on refresh)
            'call_oi_change':  0.0,
            'put_oi_change':   0.0,
            'call_gex_flow':   0.0,
            'put_gex_flow':    0.0,
            'net_gex_flow':    0.0,
            'spot_price':   spot_price,
            'timestamp':    datetime.utcnow().replace(tzinfo=pytz.utc),
        })

    progress.empty()

    if not rows:
        st.error(
            f"No option data returned for {currency} {expiry}. "
            "Possible reasons:\n"
            "1. Expiry date has already passed — select a future expiry\n"
            "2. Deribit does not list XAU options (check currency dropdown)\n"
            "3. Strike range too narrow — try wider ATM range\n"
            "Tip: For XAU, try BTC or ETH first to confirm connectivity."
        )
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = _compute_enhanced_oi_gex_crypto(df, spot_price, cfg['unit_label'])
    return df


def _compute_enhanced_oi_gex_crypto(df: pd.DataFrame,
                                     spot_price: float,
                                     unit_label: str) -> pd.DataFrame:
    """
    Compute Enhanced OI GEX for crypto — identical logic to India dashboard.
    Uses total_volume as proxy for OI change when delta OI unavailable.
    """
    if df.empty:
        return df

    dist      = (df['strike'] - spot_price).abs()
    max_dist  = dist.max() if dist.max() > 0 else 1
    dist_w    = 1 - (dist / max_dist) * 0.5

    avg_iv   = (df['call_iv'].fillna(20) + df['put_iv'].fillna(20)) / 2
    iv_mean  = avg_iv.mean() if avg_iv.mean() > 0 else 20
    iv_adj   = (avg_iv / iv_mean).clip(0.5, 2.0)

    vol_mean = df['total_volume'].replace(0, np.nan).mean()
    vol_mean = vol_mean if (vol_mean and not np.isnan(vol_mean)) else 1
    vol_w    = (df['total_volume'].fillna(0) / vol_mean).clip(0.1, 3.0)

    # Use call_oi_change if available, else use OI as proxy
    call_delta_oi = df['call_oi_change'].abs().fillna(0)
    put_delta_oi  = df['put_oi_change'].abs().fillna(0)

    # Fallback: use scaled OI when no change data
    if call_delta_oi.sum() == 0:
        call_delta_oi = df['call_oi'].fillna(0) * 0.05
        put_delta_oi  = df['put_oi'].fillna(0)  * 0.05

    call_enh = call_delta_oi * df['call_gamma'].abs().fillna(0) * vol_w * iv_adj * dist_w
    put_enh  = put_delta_oi  * df['put_gamma'].abs().fillna(0)  * vol_w * iv_adj * dist_w

    # Scale to same unit as net_gex
    scale = df['net_gex'].abs().mean() / (call_enh + put_enh).abs().mean() \
            if (call_enh + put_enh).abs().mean() > 0 else 1

    df['enhanced_oi_gex'] = (call_enh - put_enh) * scale
    return df


def get_spot_price(currency: str) -> float:
    """Get current spot price for a currency."""
    index_name = {
        'BTC': 'btc_usd',
        'ETH': 'eth_usd',
        'XAU': 'xau_usd',  # Deribit uses xau_usd for gold index
    }.get(currency, 'btc_usd')
    result = deribit_get("get_index_price", {"index_name": index_name})
    price = float(result.get('index_price', 0))
    # Fallback defaults if API returns 0 (XAU may not have index on all plans)
    _fallbacks = {'BTC': 83000.0, 'ETH': 1800.0, 'XAU': 3100.0}
    return price if price > 0 else _fallbacks.get(currency, 0)

# ============================================================================
# PURE ANALYTICS FUNCTIONS
# (Identical to India dashboard — market-agnostic math)
# ============================================================================

def identify_gamma_flip_zones(df: pd.DataFrame, spot_price: float) -> List[Dict]:
    df_sorted = df.sort_values('strike').reset_index(drop=True)
    flip_zones = []
    for i in range(len(df_sorted) - 1):
        cur_gex = df_sorted.iloc[i]['net_gex']
        nxt_gex = df_sorted.iloc[i+1]['net_gex']
        cur_str = df_sorted.iloc[i]['strike']
        nxt_str = df_sorted.iloc[i+1]['strike']
        if (cur_gex > 0 and nxt_gex < 0) or (cur_gex < 0 and nxt_gex > 0):
            flip_str = cur_str + (nxt_str - cur_str) * (abs(cur_gex) / (abs(cur_gex) + abs(nxt_gex)))
            if spot_price < flip_str:
                direction, arrow, color = ("upward","↑","#ef4444") if cur_gex > 0 else ("downward","↓","#10b981")
            else:
                direction, arrow, color = ("downward","↓","#10b981") if cur_gex < 0 else ("upward","↑","#ef4444")
            flip_zones.append({
                'strike': flip_str, 'lower_strike': cur_str, 'upper_strike': nxt_str,
                'lower_gex': cur_gex, 'upper_gex': nxt_gex,
                'direction': direction, 'arrow': arrow, 'color': color,
                'flip_type': 'Positive→Negative' if cur_gex > 0 else 'Negative→Positive',
            })
    return flip_zones


def identify_vanna_flip_zones(df: pd.DataFrame, spot_price: float) -> List[Dict]:
    df_s = df.sort_values('strike').reset_index(drop=True)
    zones = []
    for i in range(len(df_s) - 1):
        cv = df_s.iloc[i]['net_vanna']
        nv = df_s.iloc[i+1]['net_vanna']
        cs = df_s.iloc[i]['strike']
        ns = df_s.iloc[i+1]['strike']
        if (cv > 0 and nv < 0) or (cv < 0 and nv > 0):
            fs = cs + (ns - cs) * (abs(cv) / (abs(cv) + abs(nv)))
            above = fs > spot_price
            pos2neg = cv > 0
            if above and pos2neg:
                role = 'RESISTANCE_CEILING'
            elif above and not pos2neg:
                role = 'VACUUM_ZONE'
            elif not above and pos2neg:
                role = 'TRAP_DOOR'
            else:
                role = 'SUPPORT_FLOOR'
            role_colors = {
                'RESISTANCE_CEILING': '#ef4444',
                'VACUUM_ZONE':        '#10b981',
                'TRAP_DOOR':          '#f59e0b',
                'SUPPORT_FLOOR':      '#06b6d4',
            }
            zones.append({
                'strike': fs, 'lower_strike': cs, 'upper_strike': ns,
                'lower_vanna': cv, 'upper_vanna': nv,
                'above_spot': above, 'pos2neg': pos2neg,
                'role': role, 'color': role_colors[role],
                'distance_pct': abs(fs - spot_price) / spot_price * 100,
            })
    return zones


def compute_iv_trend(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or 'timestamp' not in df.columns:
        return pd.DataFrame()
    g = df.groupby('timestamp').agg(
        call_iv=('call_iv', 'mean'),
        put_iv=('put_iv', 'mean'),
        spot_price=('spot_price', 'first'),
    ).reset_index().sort_values('timestamp')
    g['avg_iv']    = (g['call_iv'] + g['put_iv']) / 2
    g['iv_change'] = g['avg_iv'].diff().fillna(0)
    g['iv_skew']   = g['call_iv'] - g['put_iv']
    window = max(3, len(g) // 5)
    g['iv_ma']     = g['avg_iv'].rolling(window, min_periods=1).mean()

    def regime(row):
        if row['avg_iv'] > row['iv_ma'] * 1.03:
            return 'EXPANDING'
        elif row['avg_iv'] < row['iv_ma'] * 0.97:
            return 'COMPRESSING'
        return 'FLAT'
    g['iv_regime'] = g.apply(regime, axis=1)
    return g


def compute_gex_cascade(df: pd.DataFrame, spot_price: float,
                         unit_label: str, contract_size: int,
                         gex_col: str = 'net_gex',
                         vanna_zones=None, iv_regime: str = 'FLAT',
                         symbol: str = 'BTC') -> pd.DataFrame:
    """
    Cascade mathematics — identical to India dashboard.
    Maps GEX unwind at each strike to estimated price points.
    """
    if df.empty or gex_col not in df.columns:
        return pd.DataFrame()

    cfg = CRYPTO_CONFIG.get(symbol, CRYPTO_CONFIG['BTC'])
    pts_per_unit   = cfg['pts_per_unit']
    strike_cap_pts = cfg['strike_cap_pts']

    vanna_zones = vanna_zones or []

    VANNA_ADJ = {
        'SUPPORT_FLOOR':      {'COMPRESSING': -0.60, 'FLAT': -0.35, 'EXPANDING':  0.20},
        'TRAP_DOOR':          {'COMPRESSING':  0.00, 'FLAT':  0.00, 'EXPANDING':  0.30},
        'VACUUM_ZONE':        {'COMPRESSING':  0.00, 'FLAT':  0.00, 'EXPANDING': -0.50},
        'RESISTANCE_CEILING': {'COMPRESSING':  0.00, 'FLAT':  0.00, 'EXPANDING':  0.20},
    }

    df_s = df.copy()
    atm  = round(spot_price / cfg['strike_interval']) * cfg['strike_interval']

    bear_strikes = df_s[df_s['strike'] <= spot_price].sort_values('strike', ascending=False)
    bull_strikes = df_s[df_s['strike'] >  spot_price].sort_values('strike', ascending=True)

    rows = []
    for direction, subset in [('BEAR', bear_strikes), ('BULL', bull_strikes)]:
        cum_pts = 0
        for _, row in subset.iterrows():
            gex_val  = float(row[gex_col]) if gex_col in row.index else float(row['net_gex'])
            raw_pts  = min(abs(gex_val) * pts_per_unit, strike_cap_pts)

            # VANNA zone adjustment
            vanna_adj_pct = None
            vanna_adj_val = 0
            closest = None
            min_dist = float('inf')
            for z in vanna_zones:
                d = abs(z['strike'] - row['strike'])
                if d < min_dist:
                    min_dist = d; closest = z
            if closest and min_dist < cfg['strike_interval'] * 1.5:
                role = closest['role']
                adj  = VANNA_ADJ.get(role, {}).get(iv_regime, 0)
                if adj != 0:
                    vanna_adj_pct = adj
                    vanna_adj_val = raw_pts * adj

            adj_pts = raw_pts + vanna_adj_val
            adj_pts = max(0, adj_pts)
            cum_pts += adj_pts

            if gex_val < 0:
                effect   = 'Accelerates fall 🔴' if direction == 'BEAR' else 'Accelerates rise 🔴'
                role_str = 'BEAR_ACCEL' if direction == 'BEAR' else 'BULL_ACCEL'
            else:
                effect   = 'Brakes fall 🟢' if direction == 'BEAR' else 'Brakes rise 🟢'
                role_str = 'BEAR_BRAKE' if direction == 'BEAR' else 'BULL_BRAKE'

            vz_label = None
            if closest and min_dist < cfg['strike_interval'] * 1.5:
                icons = {'SUPPORT_FLOOR':'🛡️','TRAP_DOOR':'⚠️','VACUUM_ZONE':'🚀','RESISTANCE_CEILING':'🔴'}
                vz_label = icons.get(closest['role'],'📍') + ' ' + closest['role'].replace('_',' ') + ' @' + f"{closest['strike']:,.0f}"

            if vanna_adj_pct is not None:
                adj_str = f"{vanna_adj_pct*100:+.0f}% {'🛡️' if vanna_adj_pct < 0 else '⚡'}"
            else:
                adj_str = '—'

            rows.append({
                'strike':            row['strike'],
                'gex_raw':           gex_val,
                'gex_raw_disp':      f"{'+' if gex_val >= 0 else ''}{gex_val:.4f}{unit_label}",
                'pts_raw':           round(raw_pts, 2),
                'vanna_adj_pct':     adj_str,
                'pts_impact':        round(adj_pts, 2),
                'cumulative_pts':    round(cum_pts, 2),
                'role':              effect,
                'vanna_zone_label':  vz_label or '',
                'cascade_direction': direction,
            })

    return pd.DataFrame(rows)


def create_gex_chart(df: pd.DataFrame, spot_price: float,
                     unit_label: str = 'K', symbol: str = 'BTC') -> go.Figure:
    """Standard GEX bar chart — identical to India dashboard."""
    cfg  = CRYPTO_CONFIG.get(symbol, CRYPTO_CONFIG['BTC'])
    emoj = cfg['emoji']
    df_s = df.sort_values('strike').reset_index(drop=True)

    colors = ['#10b981' if v >= 0 else '#ef4444' for v in df_s['net_gex']]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_s['strike'], x=df_s['net_gex'], orientation='h',
        marker_color=colors,
        hovertemplate='Strike: %{y:,.0f}<br>GEX: %{x:.4f}' + unit_label + '<extra></extra>',
        name='Net GEX',
    ))
    fig.add_hline(y=spot_price, line=dict(color='#fbbf24', width=2, dash='dash'),
                  annotation_text=f'Spot {spot_price:,.0f}',
                  annotation_font=dict(color='#fbbf24', size=12))

    flip_zones = identify_gamma_flip_zones(df_s, spot_price)
    for fz in flip_zones[:3]:
        fig.add_hline(y=fz['strike'], line=dict(color=fz['color'], width=1, dash='dot'),
                      annotation_text=f"Flip {fz['arrow']} {fz['strike']:,.0f}",
                      annotation_font=dict(color=fz['color'], size=10))

    fig.update_layout(
        title=f'{emoj} {symbol} Standard GEX — Gamma Exposure by Strike',
        xaxis_title=f'Net GEX ({unit_label})',
        yaxis_title='Strike Price (USD)',
        height=600, template='plotly_dark',
        plot_bgcolor='rgba(15,23,42,0.8)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='JetBrains Mono', color='#f1f5f9'),
        showlegend=True,
    )
    return fig


def create_enhanced_oi_gex_chart(df: pd.DataFrame, spot_price: float,
                                  unit_label: str = 'K', symbol: str = 'BTC') -> go.Figure:
    """Enhanced OI GEX — Purple/Gold bars (same as India dashboard)."""
    cfg  = CRYPTO_CONFIG.get(symbol, CRYPTO_CONFIG['BTC'])
    emoj = cfg['emoji']
    if 'enhanced_oi_gex' not in df.columns:
        df = _compute_enhanced_oi_gex_crypto(df, spot_price, unit_label)
    df_s = df.sort_values('strike').reset_index(drop=True)

    colors = ['#8b5cf6' if v >= 0 else '#f59e0b' for v in df_s['enhanced_oi_gex']]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_s['strike'], x=df_s['enhanced_oi_gex'], orientation='h',
        marker_color=colors,
        hovertemplate='Strike: %{y:,.0f}<br>Enh OI GEX: %{x:.4f}' + unit_label + '<extra></extra>',
        name='Enhanced OI GEX',
    ))
    fig.add_hline(y=spot_price, line=dict(color='#fbbf24', width=2, dash='dash'),
                  annotation_text=f'Spot {spot_price:,.0f}',
                  annotation_font=dict(color='#fbbf24', size=12))

    vanna_zones = identify_vanna_flip_zones(df_s, spot_price)
    role_colors = {'RESISTANCE_CEILING':'#ef4444','VACUUM_ZONE':'#10b981',
                   'TRAP_DOOR':'#f59e0b','SUPPORT_FLOOR':'#06b6d4'}
    role_icons  = {'RESISTANCE_CEILING':'🔴','VACUUM_ZONE':'🚀','TRAP_DOOR':'⚠️','SUPPORT_FLOOR':'🛡️'}
    for vz in vanna_zones[:5]:
        fig.add_hline(y=vz['strike'],
                      line=dict(color=role_colors[vz['role']], width=1.5, dash='dot'),
                      annotation_text=role_icons[vz['role']] + ' ' + vz['role'].replace('_',' '),
                      annotation_font=dict(color=role_colors[vz['role']], size=10))

    fig.update_layout(
        title=f'{emoj} {symbol} Enhanced OI GEX — OI Change × Greeks × Vol × IV × Distance',
        xaxis_title=f'Enhanced OI GEX ({unit_label})',
        yaxis_title='Strike Price (USD)',
        height=600, template='plotly_dark',
        plot_bgcolor='rgba(15,23,42,0.8)', paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='JetBrains Mono', color='#f1f5f9'),
    )
    return fig


def create_vanna_chart(df: pd.DataFrame, spot_price: float,
                       unit_label: str = 'K', symbol: str = 'BTC') -> go.Figure:
    """VANNA exposure chart."""
    cfg  = CRYPTO_CONFIG.get(symbol, CRYPTO_CONFIG['BTC'])
    df_s = df.sort_values('strike').reset_index(drop=True)

    vanna_zones = identify_vanna_flip_zones(df_s, spot_price)
    colors = ['#ec4899' if v >= 0 else '#be185d' for v in df_s['net_vanna']]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_s['strike'], x=df_s['net_vanna'], orientation='h',
        marker_color=colors,
        hovertemplate='Strike: %{y:,.0f}<br>VANNA: %{x:.4f}' + unit_label + '<extra></extra>',
        name='Net VANNA',
    ))
    fig.add_hline(y=spot_price, line=dict(color='#fbbf24', width=2, dash='dash'),
                  annotation_text=f'Spot {spot_price:,.0f}',
                  annotation_font=dict(color='#fbbf24', size=12))

    role_colors = {'RESISTANCE_CEILING':'#ef4444','VACUUM_ZONE':'#10b981',
                   'TRAP_DOOR':'#f59e0b','SUPPORT_FLOOR':'#06b6d4'}
    role_icons  = {'RESISTANCE_CEILING':'🔴','VACUUM_ZONE':'🚀','TRAP_DOOR':'⚠️','SUPPORT_FLOOR':'🛡️'}
    for vz in vanna_zones[:5]:
        fig.add_hline(y=vz['strike'],
                      line=dict(color=role_colors[vz['role']], width=1.5, dash='dot'),
                      annotation_text=role_icons[vz['role']] + ' ' + vz['role'].replace('_',' '),
                      annotation_font=dict(color=role_colors[vz['role']], size=10))

    fig.update_layout(
        title=f'🌊 {symbol} VANNA Exposure — Dealer Delta Sensitivity to IV',
        xaxis_title=f'Net VANNA ({unit_label})',
        yaxis_title='Strike Price (USD)',
        height=600, template='plotly_dark',
        plot_bgcolor='rgba(15,23,42,0.8)', paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='JetBrains Mono', color='#f1f5f9'),
    )
    return fig


def create_oi_chart(df: pd.DataFrame, spot_price: float,
                    symbol: str = 'BTC') -> go.Figure:
    """OI distribution chart."""
    df_s = df.sort_values('strike').reset_index(drop=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df_s['strike'], x=df_s['call_oi'], orientation='h',
        name='Call OI', marker_color='rgba(16,185,129,0.7)',
        hovertemplate='Strike: %{y:,.0f}<br>Call OI: %{x:,.0f}<extra></extra>',
    ))
    fig.add_trace(go.Bar(
        y=df_s['strike'], x=-df_s['put_oi'], orientation='h',
        name='Put OI', marker_color='rgba(239,68,68,0.7)',
        hovertemplate='Strike: %{y:,.0f}<br>Put OI: %{x:,.0f}<extra></extra>',
    ))
    fig.add_hline(y=spot_price, line=dict(color='#fbbf24', width=2, dash='dash'),
                  annotation_text=f'Spot {spot_price:,.0f}',
                  annotation_font=dict(color='#fbbf24', size=12))
    fig.update_layout(
        title=f'📋 {symbol} Open Interest Distribution',
        barmode='overlay', xaxis_title='Open Interest (contracts)',
        yaxis_title='Strike Price (USD)',
        height=600, template='plotly_dark',
        plot_bgcolor='rgba(15,23,42,0.8)', paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='JetBrains Mono', color='#f1f5f9'),
    )
    return fig


def create_iv_smile_chart(df: pd.DataFrame, spot_price: float,
                           symbol: str = 'BTC') -> go.Figure:
    """IV Smile / Skew chart — unique to crypto, very informative."""
    df_s = df.sort_values('strike').reset_index(drop=True)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_s['strike'], y=df_s['call_iv'],
        name='Call IV', mode='lines+markers',
        line=dict(color='#10b981', width=2),
        marker=dict(size=6),
        hovertemplate='Strike: %{x:,.0f}<br>Call IV: %{y:.1f}%<extra></extra>',
    ))
    fig.add_trace(go.Scatter(
        x=df_s['strike'], y=df_s['put_iv'],
        name='Put IV', mode='lines+markers',
        line=dict(color='#ef4444', width=2),
        marker=dict(size=6),
        hovertemplate='Strike: %{x:,.0f}<br>Put IV: %{y:.1f}%<extra></extra>',
    ))
    fig.add_vline(x=spot_price, line=dict(color='#fbbf24', width=2, dash='dash'),
                  annotation_text=f'Spot {spot_price:,.0f}',
                  annotation_font=dict(color='#fbbf24', size=12))
    fig.update_layout(
        title=f'📈 {symbol} IV Smile / Skew — Implied Volatility by Strike',
        xaxis_title='Strike Price (USD)', yaxis_title='Implied Volatility (%)',
        height=500, template='plotly_dark',
        plot_bgcolor='rgba(15,23,42,0.8)', paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='JetBrains Mono', color='#f1f5f9'),
    )
    return fig


# ============================================================================
# LANDING PAGE
# ============================================================================

LANDING_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Space+Grotesk:wght@300;400;500;600;700&display=swap');
.lp-hero {
    background: linear-gradient(135deg, #1a0533 0%, #0f172a 45%, #1a1033 100%);
    border: 1px solid rgba(168,85,247,0.35); border-radius: 20px;
    padding: 36px 40px 28px 40px; margin-bottom: 20px; position: relative; overflow: hidden;
}
.lp-hero::before {
    content: ''; position: absolute; top: -60px; right: -60px;
    width: 280px; height: 280px; border-radius: 50%;
    background: radial-gradient(circle, rgba(245,158,11,0.15) 0%, transparent 70%);
}
.lp-headline {
    font-family: 'Space Grotesk', sans-serif; font-size: 2.0rem; font-weight: 700;
    background: linear-gradient(135deg, #ffffff 0%, #f59e0b 40%, #8b5cf6 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    line-height: 1.25; margin-bottom: 10px;
}
.lp-subline {
    font-family: 'JetBrains Mono', monospace; font-size: 0.88rem;
    color: rgba(255,255,255,0.55); line-height: 1.6; margin-bottom: 18px;
}
.lp-badge-row { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 14px; }
.lp-badge {
    display: inline-flex; align-items: center; gap: 6px; padding: 4px 12px;
    border-radius: 20px; background: rgba(255,255,255,0.07);
    border: 1px solid rgba(255,255,255,0.15);
    font-family: 'JetBrains Mono', monospace; font-size: 0.70rem; color: rgba(255,255,255,0.75);
}
.lp-badge-dot { width: 6px; height: 6px; border-radius: 50%; background: #f59e0b; animation: lp-pulse 2s ease-in-out infinite; }
.lp-social-row { display: flex; gap: 10px; margin-bottom: 4px; }
.lp-social-btn {
    display: inline-flex; align-items: center; gap: 7px; padding: 7px 16px;
    border-radius: 22px; text-decoration: none !important;
    font-family: 'JetBrains Mono', monospace; font-size: 0.73rem; font-weight: 600;
}
.lp-social-yt  { background: rgba(255,0,0,0.18); border: 1px solid rgba(255,0,0,0.35); color: #ff6b6b !important; }
.lp-social-li  { background: rgba(10,102,194,0.20); border: 1px solid rgba(10,102,194,0.40); color: #74b3f5 !important; }
.lp-metrics { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }
.lp-metric { flex: 1; min-width: 120px; background: rgba(255,255,255,0.05); border: 1px solid rgba(245,158,11,0.25); border-radius: 12px; padding: 14px 18px; text-align: center; }
.lp-metric-val { font-family: 'Space Grotesk', sans-serif; font-size: 1.4rem; font-weight: 700; background: linear-gradient(135deg, #f59e0b, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
.lp-metric-lbl { font-family: 'JetBrains Mono', monospace; font-size: 0.68rem; color: rgba(255,255,255,0.40); margin-top: 3px; }
.lp-features { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px; margin-bottom: 20px; }
.lp-feature { background: rgba(255,255,255,0.04); border: 1px solid rgba(245,158,11,0.18); border-radius: 12px; padding: 16px 18px; }
.lp-feature-icon { font-size: 1.4rem; margin-bottom: 7px; }
.lp-feature-title { font-family: 'Space Grotesk', sans-serif; font-size: 0.88rem; font-weight: 600; color: #ffffff; margin-bottom: 4px; }
.lp-feature-desc  { font-family: 'JetBrains Mono', monospace; font-size: 0.72rem; color: rgba(255,255,255,0.45); line-height: 1.55; }
.lp-disclaimer { font-family: 'JetBrains Mono', monospace; font-size: 0.66rem; color: rgba(255,255,255,0.28); text-align: center; margin-top: 6px; }
@keyframes lp-pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
</style>
"""

def show_landing() -> bool:
    st.markdown(LANDING_CSS, unsafe_allow_html=True)
    hero = (
        '<div class="lp-hero">'
        '<div class="lp-badge-row">'
        '<span class="lp-badge"><span class="lp-badge-dot"></span>LIVE &#8212; Deribit API</span>'
        '<span class="lp-badge"><span class="lp-badge-dot"></span>&#x20BF; BTC &middot; &#x1F537; ETH &middot; &#x1F947; XAU Gold</span>'
        '<span class="lp-badge"><span class="lp-badge-dot"></span>24/7 Global Markets</span>'
        '</div>'
        '<div class="lp-headline">Crypto &amp; Gold<br>GEX / VANNA Analytics</div>'
        '<div class="lp-subline">'
        'Institutional-grade Gamma Exposure &middot; VANNA Cascade &middot; Dealer Flow analytics<br>'
        'for BTC, ETH and XAU options &mdash; powered by Deribit &mdash; by NYZTrade Analytics'
        '</div>'
        '<div class="lp-social-row">'
        '<a class="lp-social-btn lp-social-yt" href="https://www.youtube.com/@nyztrade" target="_blank">&#9654; YouTube &#8212; @nyztrade</a>'
        '<a class="lp-social-btn lp-social-li" href="https://www.linkedin.com/in/drniyas/" target="_blank">in&nbsp; LinkedIn &#8212; Dr. Niyas N</a>'
        '</div>'
        '</div>'
    )
    st.markdown(hero, unsafe_allow_html=True)

    metrics = (
        '<div class="lp-metrics">'
        '<div class="lp-metric"><div class="lp-metric-val">3</div><div class="lp-metric-lbl">Assets: BTC ETH XAU</div></div>'
        '<div class="lp-metric"><div class="lp-metric-val">6</div><div class="lp-metric-lbl">Analytics Tabs</div></div>'
        '<div class="lp-metric"><div class="lp-metric-val">FREE</div><div class="lp-metric-lbl">Deribit API</div></div>'
        '<div class="lp-metric"><div class="lp-metric-val">24/7</div><div class="lp-metric-lbl">Live Global Data</div></div>'
        '</div>'
    )
    st.markdown(metrics, unsafe_allow_html=True)

    features = [
        ("🎯", "Standard GEX",        "Total dealer gamma by strike — structural walls and flip zones"),
        ("🚀", "Enhanced OI GEX",      "OI change weighted by Greeks, Volume, IV and Distance — intraday signal"),
        ("🌊", "VANNA Cascade",        "Flip zone detection — Vacuum, Support Floor, Trap Door, Resistance Ceiling"),
        ("📐", "Cascade Mathematics",  "Per-instrument calibrated scalars — estimated price move per strike unwind"),
        ("📈", "IV Smile / Skew",      "Call vs Put IV by strike — market fear/greed structure visualized"),
        ("📋", "OI Distribution",      "Call vs Put open interest — pin risk and max pain levels"),
    ]
    feat_cards = "".join(
        '<div class="lp-feature">'
        '<div class="lp-feature-icon">{}</div>'
        '<div class="lp-feature-title">{}</div>'
        '<div class="lp-feature-desc">{}</div>'
        '</div>'.format(icon, title, desc)
        for icon, title, desc in features
    )
    st.markdown('<div class="lp-section-label" style="font-family:JetBrains Mono,monospace;font-size:0.68rem;color:rgba(245,158,11,0.65);text-transform:uppercase;letter-spacing:0.1em;margin-bottom:10px;">&#9889; Platform Features</div>', unsafe_allow_html=True)
    st.markdown('<div class="lp-features">{}</div>'.format(feat_cards), unsafe_allow_html=True)

    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        entered = st.button("🚀  Enter Dashboard  →", use_container_width=True,
                            type="primary", key="lp_enter_button")
    st.markdown(
        '<div class="lp-disclaimer">&#9888;&#65039; For educational and research purposes only. '
        'Not financial advice. &nbsp;&middot;&nbsp; '
        '&copy; 2026 NYZTrade Analytics. All rights reserved.</div>',
        unsafe_allow_html=True,
    )
    return entered


# ============================================================================
# MAIN APP
# ============================================================================

def _render_cascade(cascade_df: pd.DataFrame, label: str, unit_label: str):
    """Render cascade table with metrics — identical UX to India dashboard."""
    if cascade_df.empty:
        st.info(f"{label} data not available.")
        return

    for direction, emoji, spot_label in [('BEAR','🐻','Below Spot'), ('BULL','🐂','Above Spot')]:
        sub = cascade_df[cascade_df['cascade_direction'] == direction].head(10)
        st.markdown(f"**{emoji} {direction.title()} Cascade ({spot_label})**")
        accel = sub[sub['gex_raw'] < 0]
        brake = sub[sub['gex_raw'] >= 0]
        accel_pts = accel['pts_impact'].sum()
        brake_pts = brake['pts_impact'].sum()
        m1, m2 = st.columns(2)
        m1.metric("🔴 Cascade Fuel (Neg GEX)",   f"~{accel_pts:.0f} pts",
                  delta="Accelerating" if accel_pts > 50 else "Low fuel",
                  delta_color="inverse" if direction == 'BEAR' else "normal")
        m2.metric("🟢 Absorption (Pos GEX)", f"~{brake_pts:.0f} pts",
                  delta="Absorbing move" if brake_pts > accel_pts else "Weak absorption",
                  delta_color="normal" if direction == 'BEAR' else "inverse")
        net_pts   = max(0, accel_pts - brake_pts * 0.5)
        net_color = "#ef4444" if direction == 'BEAR' else "#10b981"
        st.markdown(
            '<div style="background:rgba(15,23,42,0.7);border-left:3px solid {};'
            'padding:6px 12px;border-radius:4px;font-size:0.82rem;margin-bottom:6px;">'
            '&#9889; <b>Estimated Net Realised: ~{:.0f} pts</b> (fuel &minus; 50% absorption)</div>'.format(
                net_color, net_pts),
            unsafe_allow_html=True)

        sub2 = pd.concat([accel, brake]).reset_index(drop=True)
        sub2['Type'] = sub2['gex_raw'].apply(lambda x: '🔴 Fuel' if x < 0 else '🟢 Brake')
        disp_cols = ['strike', 'gex_raw_disp', 'pts_raw', 'vanna_adj_pct',
                     'pts_impact', 'cumulative_pts', 'role', 'Type']
        avail = [c for c in disp_cols if c in sub2.columns]
        rename = {
            'strike': 'Strike', 'gex_raw_disp': 'GEX', 'pts_raw': 'Raw Pts',
            'vanna_adj_pct': 'VANNA Adj', 'pts_impact': 'Adj Pts',
            'cumulative_pts': 'Cum. Pts', 'role': 'Effect', 'Type': 'Type',
        }
        st.dataframe(sub2[avail].rename(columns=rename),
                     use_container_width=True, height=280)


def main():
    # ── Landing gate ──────────────────────────────────────────────────────────
    if 'app_entered' not in st.session_state:
        st.session_state.app_entered = False
    if not st.session_state.app_entered:
        if show_landing():
            st.session_state.app_entered = True
            st.rerun()
        return

    # ── Header ────────────────────────────────────────────────────────────────
    utc_now = datetime.utcnow().strftime('%H:%M:%S UTC')
    st.markdown(
        '<div class="main-header">'
        '<div style="display:flex;justify-content:space-between;align-items:center;">'
        '<div>'
        '<h1 class="main-title">&#x20BF; NYZTrade Crypto GEX Dashboard</h1>'
        '<p class="sub-title">BTC &middot; ETH &middot; XAU Gold | GEX / VANNA / Cascade | Deribit API | 24/7 Live</p>'
        '</div>'
        '<div class="live-indicator"><div class="live-dot"></div>'
        '<span style="color:#ef4444;font-family:JetBrains Mono,monospace;font-size:0.8rem;">'
        + utc_now + ' UTC</span></div>'
        '</div></div>',
        unsafe_allow_html=True,
    )

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### ⚙️ Configuration")

        currency = st.selectbox("🪙 Asset", ["BTC", "ETH", "XAU"],
                                 format_func=lambda x: {"BTC":"₿ Bitcoin","ETH":"🔷 Ethereum","XAU":"🥇 Gold (XAU)"}[x])
        cfg = CRYPTO_CONFIG[currency]

        st.markdown(
            '<div class="crypto-badge">📊 {}</div>'.format(
                f"Deribit | Contract: {cfg['contract_size']} {currency} | Strike Δ: ${cfg['strike_interval']:,}"),
            unsafe_allow_html=True)
        if currency == 'XAU':
            st.info(
                "🥇 **XAU (Gold) options** are available on Deribit but with limited liquidity. "
                "If expiries fail to load, try **BTC or ETH** first to confirm your connection is working, "
                "then switch back to XAU."
            )

        st.markdown("---")

        # Live spot price
        if st.button("🔄 Refresh Spot Price"):
            st.session_state.pop('spot_price', None)

        if 'spot_price' not in st.session_state or \
           st.session_state.get('spot_currency') != currency:
            with st.spinner(f"Fetching {currency} spot..."):
                sp = get_spot_price(currency)
                st.session_state['spot_price']    = sp
                st.session_state['spot_currency'] = currency
        spot_price = st.session_state['spot_price']

        if spot_price > 0:
            st.metric(f"{cfg['emoji']} {currency} Spot", f"${spot_price:,.2f}")
        else:
            # Sensible defaults per asset
            _default_spots = {'BTC': 83000.0, 'ETH': 1800.0, 'XAU': 3100.0}
            _step          = {'BTC': 500.0,   'ETH': 10.0,    'XAU': 5.0}
            spot_price = st.number_input("Manual Spot Price ($)",
                value=_default_spots.get(currency, 83000.0),
                min_value=1.0,
                step=_step.get(currency, 100.0))
        st.markdown("---")
        st.markdown("### 📅 Expiry Selection")

        if st.button("🔄 Load Expiries"):
            st.session_state.pop('expiries', None)

        if 'expiries' not in st.session_state or \
           st.session_state.get('expiry_currency') != currency:
            with st.spinner("Loading expiries..."):
                expiries = get_deribit_expiries(currency)
                st.session_state['expiries']        = expiries
                st.session_state['expiry_currency'] = currency
        else:
            expiries = st.session_state['expiries']

        if not expiries:
            from datetime import datetime, timedelta
            # Generate next 4 Friday expiries as fallback
            today = datetime.utcnow()
            fallback = []
            d = today
            while len(fallback) < 4:
                d += timedelta(days=1)
                if d.weekday() == 4:  # Friday
                    fallback.append(d.strftime('%d%b%y').upper())
            expiries = fallback
            st.warning("Could not load expiries from Deribit. Using next Fridays as fallback — click Load Expiries to retry.")

        selected_expiry = st.selectbox("📆 Expiry", expiries)

        st.markdown("---")
        st.markdown("### 🎯 Strike Range")
        atm_range = st.slider("ATM ± N strikes", 5, 20, 12)
        st.caption(f"Will fetch {atm_range*2+1} strikes centered on ATM")

        st.markdown("---")
        fetch_btn = st.button("🚀 Fetch Options Chain", type="primary",
                               use_container_width=True)
        refresh_btn = st.button("🔄 Refresh Data", use_container_width=True)

        st.markdown("---")
        if st.button("🏠 Back to Home", use_container_width=True):
            st.session_state.app_entered = False
            st.rerun()

    # ── Data fetch ────────────────────────────────────────────────────────────
    if fetch_btn or refresh_btn:
        cache_key = _cache_key(currency, selected_expiry)
        # always refresh on explicit fetch; use 60s cache on refresh
        max_age   = 0 if fetch_btn else 60
        df, meta  = _load_cache(cache_key, max_age)
        if df is None:
            with st.spinner(f"Fetching {currency} {selected_expiry} options chain from Deribit..."):
                df = fetch_options_chain(currency, selected_expiry,
                                         spot_price, atm_range)
            if df is not None and not df.empty:
                meta = {
                    'symbol': currency, 'expiry': selected_expiry,
                    'spot_price': spot_price, 'fetch_time': datetime.utcnow().isoformat(),
                    'unit_label': cfg['unit_label'], 'contract_size': cfg['contract_size'],
                    'total_records': len(df),
                }
                _save_cache(cache_key, df, meta)
                st.session_state['crypto_df']   = df
                st.session_state['crypto_meta'] = meta
                st.success(f"✅ Fetched {len(df)} strikes for {currency} {selected_expiry}")
        else:
            st.session_state['crypto_df']   = df
            st.session_state['crypto_meta'] = meta
            st.info("Loaded from cache (< 60s old)")

    # ── Main display ──────────────────────────────────────────────────────────
    if 'crypto_df' in st.session_state and st.session_state['crypto_df'] is not None:
        df   = st.session_state['crypto_df']
        meta = st.session_state['crypto_meta']
        unit_label = meta.get('unit_label', 'K')

        if df.empty:
            st.error("No data available for this expiry. Try another expiry date.")
            return

        # ── Top metrics ───────────────────────────────────────────────────────
        net_gex_total   = df['net_gex'].sum()
        net_vanna_total = df['net_vanna'].sum()
        net_dex_total   = df['net_dex'].sum()
        total_oi        = df['call_oi'].sum() + df['put_oi'].sum()
        pcr             = df['put_oi'].sum() / max(df['call_oi'].sum(), 1)
        flip_zones      = identify_gamma_flip_zones(df, spot_price)

        c1, c2, c3, c4, c5, c6 = st.columns(6)
        for col, label, val, sub, clr in [
            (c1, 'NET GEX',   f'{net_gex_total:.4f}{unit_label}',
             '🟢 Bullish' if net_gex_total > 0 else '🔴 Bearish',
             'positive' if net_gex_total > 0 else 'negative'),
            (c2, 'NET VANNA', f'{net_vanna_total:.4f}{unit_label}',
             'Vol Sensitivity', 'positive' if net_vanna_total > 0 else 'negative'),
            (c3, 'NET DEX',   f'{net_dex_total:.4f}{unit_label}',
             'Delta Exposure', 'neutral'),
            (c4, 'P/C RATIO', f'{pcr:.2f}',
             '🐻 Bearish' if pcr > 1 else '🟢 Bullish', 'neutral'),
            (c5, 'TOTAL OI',  f'{total_oi:,.0f}',
             'Contracts', 'neutral'),
            (c6, 'FLIP ZONES', str(len(flip_zones)),
             'GEX Crossovers', 'neutral'),
        ]:
            with col:
                st.markdown(
                    '<div class="metric-card {}">'
                    '<div class="metric-label">{}</div>'
                    '<div class="metric-value {}">{}</div>'
                    '<div class="metric-delta">{}</div>'
                    '</div>'.format(clr, label, clr, val, sub),
                    unsafe_allow_html=True)

        st.markdown("---")

        # ── Meta info bar ─────────────────────────────────────────────────────
        st.markdown(
            '<div style="display:flex;gap:12px;align-items:center;margin-bottom:16px;flex-wrap:wrap;">'
            '<span class="crypto-badge">&#x20BF; {}</span>'
            '<span style="padding:6px 12px;background:rgba(16,185,129,0.2);border:1px solid rgba(16,185,129,0.4);'
            'border-radius:8px;color:#10b981;font-family:JetBrains Mono,monospace;font-size:0.8rem;">&#9679; LIVE</span>'
            '<span style="color:#94a3b8;font-family:JetBrains Mono,monospace;font-size:0.85rem;">'
            '{} | Expiry: {} | Spot: ${} | Records: {}</span>'
            '</div>'.format(
                currency,
                currency,
                selected_expiry,
                f"{spot_price:,.2f}",
                meta.get('total_records', len(df)),
            ),
            unsafe_allow_html=True,
        )

        # ── TABS ──────────────────────────────────────────────────────────────
        tabs = st.tabs([
            "🎯 Standard GEX",
            "🚀 Enhanced OI GEX",
            "🌊 VANNA Exposure",
            "📐 Cascade Mathematics",
            "📈 IV Smile / Skew",
            "📋 OI Distribution",
            "📁 Data Table",
        ])

        # Tab 0 — Standard GEX
        with tabs[0]:
            st.markdown(f"### 🎯 {currency} Standard Gamma Exposure")
            st.plotly_chart(create_gex_chart(df, spot_price, unit_label, currency),
                            use_container_width=True)
            c1, c2, c3 = st.columns(3)
            c1.metric("Positive GEX", f"{df[df['net_gex']>0]['net_gex'].sum():.4f}{unit_label}")
            c2.metric("Negative GEX", f"{df[df['net_gex']<0]['net_gex'].sum():.4f}{unit_label}")
            c3.metric("GEX Flip Zones", str(len(flip_zones)))

        # Tab 1 — Enhanced OI GEX
        with tabs[1]:
            st.markdown(f"### 🚀 {currency} Enhanced OI GEX")
            st.caption("OI-weighted GEX incorporating Greeks · Volume · IV · Distance. Purple = Positive, Gold = Negative.")
            if 'enhanced_oi_gex' not in df.columns:
                df = _compute_enhanced_oi_gex_crypto(df, spot_price, unit_label)
            st.plotly_chart(create_enhanced_oi_gex_chart(df, spot_price, unit_label, currency),
                            use_container_width=True)
            g1, g2, g3 = st.columns(3)
            g1.metric("🟣 Positive OI GEX", f"{df['enhanced_oi_gex'].clip(lower=0).sum():.4f}{unit_label}")
            g2.metric("🟡 Negative OI GEX", f"{df['enhanced_oi_gex'].clip(upper=0).sum():.4f}{unit_label}")
            g3.metric("⚡ Net Enhanced GEX", f"{df['enhanced_oi_gex'].sum():.4f}{unit_label}")

        # Tab 2 — VANNA
        with tabs[2]:
            st.markdown(f"### 🌊 {currency} VANNA Exposure")
            st.markdown("""<div class="spike-legend">
            🔴 <b style="color:#ef4444">Resistance Ceiling</b> = POS→NEG flip above spot — IV↑ forces dealers to SELL delta<br>
            🚀 <b style="color:#10b981">Vacuum Zone</b> = NEG→POS flip above spot — IV↑ forces dealers to BUY delta<br>
            ⚠️ <b style="color:#f59e0b">Trap Door</b> = POS→NEG flip below spot — drop accelerates<br>
            🛡️ <b style="color:#06b6d4">Support Floor</b> = NEG→POS flip below spot — IV compression holds price
            </div>""", unsafe_allow_html=True)
            st.plotly_chart(create_vanna_chart(df, spot_price, unit_label, currency),
                            use_container_width=True)
            vanna_zones = identify_vanna_flip_zones(df, spot_price)
            v1, v2, v3, v4 = st.columns(4)
            v1.metric("Total VANNA Zones",  len(vanna_zones))
            v2.metric("🚀 Vacuum Zones",    sum(1 for z in vanna_zones if z['role']=='VACUUM_ZONE'))
            v3.metric("🔴 Resistance",       sum(1 for z in vanna_zones if z['role']=='RESISTANCE_CEILING'))
            v4.metric("⚠️ Trap Doors",       sum(1 for z in vanna_zones if z['role']=='TRAP_DOOR'))

        # Tab 3 — Cascade Mathematics
        with tabs[3]:
            st.markdown(f"### 📐 {currency} Cascade Mathematics")
            st.markdown(
                '<div style="background:rgba(15,23,42,0.8);border-left:4px solid #8b5cf6;'
                'padding:12px 16px;border-radius:6px;margin-bottom:12px;font-size:0.85rem;line-height:1.7;">'
                '<b>How Cascade Math Works</b><br>'
                'When price breaks a strike, dealers holding short gamma <b>must hedge by selling/buying futures.</b><br>'
                'Left = Standard GEX (structural total OI) | '
                '<b>Right = Enhanced OI GEX (intraday fresh positioning &#8212; PRIMARY)</b>'
                '</div>',
                unsafe_allow_html=True,
            )

            vanna_zones  = identify_vanna_flip_zones(df, spot_price)
            iv_df        = compute_iv_trend(df)
            iv_regime    = 'FLAT'
            if not iv_df.empty and 'iv_regime' in iv_df.columns:
                iv_regime = str(iv_df.iloc[-1]['iv_regime'])

            iv_color = {'EXPANDING':'#ef4444','COMPRESSING':'#10b981','FLAT':'#94a3b8'}.get(iv_regime,'#94a3b8')
            st.markdown(
                '<div style="background:rgba(6,182,212,0.08);border:1px solid rgba(6,182,212,0.3);'
                'padding:10px 14px;border-radius:6px;font-size:0.82rem;margin-bottom:12px;">'
                'IV Regime: <b style="color:{};">{}</b> &nbsp;|&nbsp; '
                'VANNA Zones: <b>{}</b> &nbsp;|&nbsp; '
                'GEX Flip Zones: <b>{}</b>'
                '</div>'.format(iv_color, iv_regime, len(vanna_zones), len(flip_zones)),
                unsafe_allow_html=True,
            )

            cas_col1, cas_col2 = st.columns(2)
            with cas_col1:
                st.markdown("##### 📊 Standard GEX Cascade (reference)")
                st.caption("Total accumulated OI — structural levels")
                orig_cascade = compute_gex_cascade(
                    df, spot_price, unit_label, cfg['contract_size'],
                    gex_col='net_gex', vanna_zones=vanna_zones,
                    iv_regime=iv_regime, symbol=currency)
                _render_cascade(orig_cascade, "Standard GEX", unit_label)

            with cas_col2:
                st.markdown("##### 🚀 Enhanced OI GEX Cascade (PRIMARY)")
                st.caption("Fresh OI change — intraday cascade picture")
                enh_cascade = compute_gex_cascade(
                    df, spot_price, unit_label, cfg['contract_size'],
                    gex_col='enhanced_oi_gex', vanna_zones=vanna_zones,
                    iv_regime=iv_regime, symbol=currency)
                _render_cascade(enh_cascade, "Enhanced OI GEX", unit_label)

            # Combined summary
            st.markdown("---")
            st.markdown("##### 🎯 Dealer Flow Bias")
            try:
                eb = enh_cascade[(enh_cascade['cascade_direction']=='BEAR') & (enh_cascade['gex_raw']<0)]['pts_impact'].sum() if not enh_cascade.empty else 0
                eu = enh_cascade[(enh_cascade['cascade_direction']=='BULL') & (enh_cascade['gex_raw']<0)]['pts_impact'].sum() if not enh_cascade.empty else 0
                if eb > eu:
                    bias = "🐻 BEARISH DEALER FLOW"; bias_color = '#ef4444'
                    note = f"Bear accelerators: ~{eb:.0f} pts vs Bull: ~{eu:.0f} pts"
                elif eu > eb:
                    bias = "🐂 BULLISH DEALER FLOW"; bias_color = '#10b981'
                    note = f"Bull accelerators: ~{eu:.0f} pts vs Bear: ~{eb:.0f} pts"
                else:
                    bias = "&#9878; NEUTRAL"; bias_color = '#94a3b8'
                    note = "Balanced dealer positioning"
                st.markdown(
                    '<div style="background:rgba(15,23,42,0.9);border:1px solid {};'
                    'padding:12px 16px;border-radius:8px;text-align:center;">'
                    '<span style="font-size:1.1rem;font-weight:700;color:{};">{}</span><br>'
                    '<span style="font-size:0.85rem;color:#e2e8f0;">{}</span>'
                    '</div>'.format(bias_color, bias_color, bias, note),
                    unsafe_allow_html=True,
                )
            except Exception:
                st.info("Fetch data to see dealer flow bias")

        # Tab 4 — IV Smile
        with tabs[4]:
            st.markdown(f"### 📈 {currency} IV Smile / Skew")
            st.markdown("""<div class="spike-legend">
            📈 <b>IV Skew</b> = Call IV &gt; Put IV → Market pricing upside risk (bullish fear / FOMO)<br>
            📉 <b>Reverse Skew</b> = Put IV &gt; Call IV → Market pricing downside risk (bearish hedge demand)<br>
            🏔️ <b>IV Smile peak</b> = ATM has lowest IV, OTM higher → normal crypto structure
            </div>""", unsafe_allow_html=True)
            st.plotly_chart(create_iv_smile_chart(df, spot_price, currency),
                            use_container_width=True)
            avg_call_iv = df['call_iv'].mean()
            avg_put_iv  = df['put_iv'].mean()
            skew        = avg_call_iv - avg_put_iv
            i1, i2, i3, i4 = st.columns(4)
            i1.metric("Avg Call IV",  f"{avg_call_iv:.1f}%")
            i2.metric("Avg Put IV",   f"{avg_put_iv:.1f}%")
            i3.metric("IV Skew",      f"{skew:+.1f}%",
                      delta="Call Skew" if skew > 0 else "Put Skew")
            i4.metric("IV Regime",    iv_regime)

        # Tab 5 — OI Distribution
        with tabs[5]:
            st.markdown(f"### 📋 {currency} Open Interest Distribution")
            st.plotly_chart(create_oi_chart(df, spot_price, currency),
                            use_container_width=True)
            max_pain = df.loc[(df['call_oi'] + df['put_oi']).idxmax(), 'strike']
            o1, o2, o3, o4 = st.columns(4)
            o1.metric("Total Call OI",  f"{df['call_oi'].sum():,.0f}")
            o2.metric("Total Put OI",   f"{df['put_oi'].sum():,.0f}")
            o3.metric("P/C Ratio",      f"{pcr:.2f}")
            o4.metric("Max OI Strike",  f"${max_pain:,.0f}")

        # Tab 6 — Data Table
        with tabs[6]:
            st.markdown(f"### 📁 {currency} Options Data")
            st.markdown(f"""
            **Asset**: {currency} | **Expiry**: {selected_expiry}
            | **Spot**: ${spot_price:,.2f}
            | **Fetch Time**: {meta.get('fetch_time','N/A')}
            | **Records**: {len(df)}
            """)
            disp_cols = ['strike','net_gex','net_vanna','net_dex',
                         'call_oi','put_oi','call_iv','put_iv',
                         'call_gamma','put_gamma','total_volume','enhanced_oi_gex']
            avail = [c for c in disp_cols if c in df.columns]
            st.dataframe(df[avail], use_container_width=True, height=400)
            st.download_button(
                "📥 Download CSV",
                data=df[avail].to_csv(index=False),
                file_name=f"nyztrade_{currency}_{selected_expiry}.csv",
                mime="text/csv",
                use_container_width=True,
            )

    else:
        # Welcome state
        st.markdown("""
        <div style="text-align:center;padding:60px 20px;">
            <div style="font-size:3rem;margin-bottom:16px;">₿ 🔷 🥇</div>
            <h2 style="font-family:'Space Grotesk',sans-serif;color:#f1f5f9;">
                Welcome to NYZTrade Crypto GEX
            </h2>
            <p style="font-family:'JetBrains Mono',monospace;color:#94a3b8;font-size:0.9rem;">
                Select an asset, load expiries, then click<br>
                <b style="color:#f59e0b;">🚀 Fetch Options Chain</b> to begin
            </p>
        </div>
        """, unsafe_allow_html=True)

    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align:center;padding:16px;color:#64748b;">
        <p style="font-family:'JetBrains Mono',monospace;font-size:0.82rem;">
            NYZTrade Crypto GEX &nbsp;|&nbsp; BTC &middot; ETH &middot; XAU &nbsp;|&nbsp;
            Powered by Deribit API &nbsp;|&nbsp; GEX / VANNA / Cascade Analytics<br>
            &#9888;&#65039; For educational and research purposes only. Not financial advice.
        </p>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
