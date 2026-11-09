"""
Binance USDT-M Futures botu (7/24, 3'te 2 İndikatör Filtresi) - DİNAMİK HACİM FİLTRELİ (%50M - 700M)
"""
import os
import json
import time
import logging
import threading
import requests
import ccxt
import pandas as pd
from flask import Flask, jsonify

app = Flask(__name__)

# ===================== AYARLAR =====================
TIMEFRAME = "5m"
LOOP_SECONDS = 180      # Döngü süresi (3 dakika)
MIN_SCORE = 2           
LEV_BY_SCORE = {2: 5, 3: 8} 
MARGIN_PCT = 0.30
TRANCHES = 3
ADD_ATR = 1.0
MAX_OPEN = 3
SL_ATR = 1.5
TP1_ROI = 30.0          
TP2_ROI = 100.0     
TRAIL_ATR = 1.0     
MAX_DRAWDOWN = 0.20
STATE_FILE = "bot_state.json"
TESTNET = False
MAX_COIN_PRICE = 500.0  

# İstediğin Hacim Filtresi Aralıkları (USDT cinsinden)
MIN_VOLUME = 50_000_000     # 50 Milyon USDT
MAX_VOLUME = 700_000_000    # 700 Milyon USDT
# ===================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()],
    force=True
)
log = logging.getLogger("bot")

PROXY_URL = os.getenv("PROXY_URL")
print(f"Proxy kontrol: {PROXY_URL}")

exchange_config = {
    "apiKey": os.getenv("BINANCE_API_KEY"),
    "secret": os.getenv("BINANCE_API_SECRET"),
    "enableRateLimit": True,
    "options": {"adjustForTimeDifference": True, "defaultType": "future"},
}

if PROXY_URL:
    exchange_config["proxies"] = {
        'http': PROXY_URL,
        'https': PROXY_URL,
    }
    print("Proxy aktif babam!")

ex = ccxt.binanceusdm(exchange_config)
if TESTNET:
    ex.set_sandbox_mode(True)

state = {}
state_lock = threading.Lock()

def get_dynamic_symbols():
    """50M - 700M hacim aralığındaki ve 500 USDT altındaki altcoinleri seçer"""
    try:
        tickers = ex.fetch_tickers()
        valid_tickers = {}
        for symbol, data in tickers.items():
            if symbol.endswith(':USDT') and 'quoteVolume' in data:
                base = symbol.split('/')[0]
                if base in ['USDC', 'BUSD', 'FDUSD', 'TUSD', 'BTC', 'ETH', 'BNB', 'SOL']:
                    continue
                
                last_price = data.get('last') or 0
                vol = data.get('quoteVolume') or 0
                
                # Fiyat ve Belirttiğin Hacim Aralığı Filtresi
                if 0 < last_price <= MAX_COIN_PRICE and MIN_VOLUME <= vol <= MAX_VOLUME:
                    valid_tickers[symbol] = vol
        
        sorted_symbols = sorted(valid_tickers.keys(), key=lambda x: valid_tickers[x], reverse=True)
        top_symbols = sorted_symbols[:12]  # En uygun ilk 12 coin
        
        if not top_symbols:
            top_symbols = ["XRP/USDT:USDT", "DOGE/USDT:USDT", "AVAX/USDT:USDT", "ADA/USDT:USDT", "LINK/USDT:USDT"]
            
        print(f"50M-700M Hacim Aralığında Seçilen Coinler: {top_symbols}")
        return top_symbols
    except Exception as e:
        print(f"Dinamik coin seçme hatası: {e}")
        return ["XRP/USDT:USDT", "DOGE/USDT:USDT", "AVAX/USDT:USDT", "ADA/USDT:USDT", "LINK/USDT:USDT"]

def add_indicators(df):
    c, h, l = df["close"], df["high"], df["low"]
    df["ema9"] = c.ewm(span=9, adjust=False).mean()
    df["ema21"] = c.ewm(span=21, adjust=False).mean()
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1/14, adjust=False).mean()
    delta = c.diff()
    up = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    dn = (-delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
    df["rsi"] = 100 - 100 / (1 + up / dn.replace(0, 1e-12))
    macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    df["hist"] = macd - macd.ewm(span=9, adjust=False).mean()
    return df

def balance():
    return float(ex.fetch_balance()["USDT"]["total"])

def tranche_qty(sym, price, lev):
    notional = max(balance() * MARGIN_PCT / TRANCHES * lev, 10)
    qty = float(ex.amount_to_precision(sym, notional / price))
    m = ex.market(sym)
    min_amt = (m["limits"]["amount"]["min"] or 0)
    if qty < min_amt:
        qty = float(ex.amount_to_precision(sym, min_amt))
    return qty

def get_signal(sym):
    raw = ex.fetch_ohlcv(sym, TIMEFRAME, limit=100)
    df = pd.DataFrame(raw, columns=["t", "open", "high", "low", "close", "volume"])
    df = add_indicators(df)
    r, p = df.iloc[-2], df.iloc[-3]
    
    votes = []
    votes.append(1 if r["ema9"] > r["ema21"] else -1)
    votes.append(1 if 50 < r["rsi"] < 70 else (-1 if 30 < r["rsi"] < 50 else 0))
    votes.append(1 if (r["hist"] > 0 and r["hist"] > p["hist"]) else (-1 if (r["hist"] < 0 and r["hist"] < p["hist"]) else 0))
    
    score = sum(votes)
    return score, float(r["atr"])

def get_pos(sym):
    for p in ex.fetch_positions([sym]):
        amt = float(p.get("contracts") or 0)
        if amt > 0:
            side = p.get("side")
            if not side:
                side = "long" if float(p.get("notional", 0)) > 0 else "short"
            return {
                "side": side, 
                "amt": amt, 
                "entry": float(p["entryPrice"] if p.get("entryPrice") else 0)
            }
    return None

def cancel_orders(sym):
    for params in ({}, {"trigger": True}):
        try: ex.cancel_all_orders(sym, params)
        except: pass

def place_stop(sym, side, amt, price):
    cancel_orders(sym)
    close_side = "sell" if side == "long" else "buy"
    ex.create_order(sym, "STOP_MARKET", close_side, float(ex.amount_to_precision(sym, amt)), None, {"stopPrice": float(ex.price_to_precision(sym, price)), "reduceOnly": True})
