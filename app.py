"""
Binance USDT-M Futures botu (7/24, 3'te 2 İndikatör Filtresi) - DİNAMİK HACİM FİLTRELİ (20M - 800M)
"""
import os
import json
import time
import logging
import threading
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

# Güncellenen Hacim Filtresi Aralıkları (USDT cinsinden)
MIN_VOLUME = 20_000_000     # 20 Milyon USDT
MAX_VOLUME = 800_000_000    # 800 Milyon USDT
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
    """20M - 800M hacim aralığındaki ve 500 USDT altındaki altcoinleri seçer"""
    try:
        tickers = ex.fetch_tickers()
        valid_tickers = {}
        for symbol, data in tickers.items():
            if '/USDT:USDT' in symbol or symbol.endswith(':USDT'):
                base = symbol.split('/')[0]
                if base in ['USDC', 'BUSD', 'FDUSD', 'TUSD', 'BTC', 'ETH', 'BNB', 'SOL']:
                    continue
                
                last_price = data.get('last') or 0
                vol = data.get('quoteVolume') or 0
                
                # Fiyat ve Güncel Hacim Aralığı Filtresi
                if 0 < last_price <= MAX_COIN_PRICE and MIN_VOLUME <= vol <= MAX_VOLUME:
                    valid_tickers[symbol] = vol
        
        sorted_symbols = sorted(valid_tickers.keys(), key=lambda x: valid_tickers[x], reverse=True)
        top_symbols = sorted_symbols[:12]
        return top_symbols
    except Exception as e:
        log.error(f"Semboller çekilirken hata oluştu: {e}")
        return []

@app.route("/")
def index():
    return jsonify({"status": "running", "message": "Bot aktif ve çalışıyor!"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
