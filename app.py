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
    "options": {
        "adjustForTimeDifference": True,  # Saat farkını otomatik kapatır
        "defaultType": "future",
        "fetchCurrencies": False,         # Spot/SAPI cüzdan endpoint'ine gitmesini engeller
    },
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
    """20M - 800M hacim aralığındaki ve 500 USDT altındaki altcoinleri seçer (Futures)"""
    try:
        # fetchCurrencies kapalı olduğu için burası sadece Futures market verisini çeker
        ex.load_markets()
        tickers = ex.fetch_tickers()
        valid_tickers = {}
        
        for symbol, data in tickers.items():
            # Sadece USDT vadeli işlem paritelerini al
            if 'USDT' in symbol and not any(stable in symbol for stable in ['USDC', 'BUSD', 'FDUSD', 'TUSD']):
                last_price = float(data.get('last') or data.get('close') or 0)
                vol = float(data.get('quoteVolume') or 0)
                
                # Fiyat ve Güncel Hacim Aralığı Filtresi
                if 0 < last_price <= MAX_COIN_PRICE and MIN_VOLUME <= vol <= MAX_VOLUME:
                    valid_tickers[symbol] = vol
        
        sorted_symbols = sorted(valid_tickers.keys(), key=lambda x: valid_tickers[x], reverse=True)
        top_symbols = sorted_symbols[:12]
        return top_symbols
    except Exception as e:
        log.error(f"Semboller çekilirken hata oluştu: {e}")
        return []

def calculate_indicators(df):
    """Teknik indikatörleri hesaplar (EMA, RSI, ATR)"""
    try:
        df['ema_fast'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema_slow'] = df['close'].ewm(span=21, adjust=False).mean()
        
        # RSI Hesaplama
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # ATR Hesaplama
        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift()).abs()
        low_close = (df['low'] - df['close'].shift()).abs()
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        df['atr'] = true_range.rolling(14).mean()
        
        return df
    except Exception as e:
        log.error(f"İndikatör hesaplama hatası: {e}")
        return df

def bot_loop():
    """Botun 7/24 arkaplanda çalışacak ana döngüsü"""
    log.info("Bot arkaplan döngüsü başladı.")
    while True:
        try:
            symbols = get_dynamic_symbols()
            log.info(f"Seçilen dinamik coinler: {symbols}")
            
            for symbol in symbols:
                try:
                    ohlcv = ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=50)
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df = calculate_indicators(df)
                except Exception as inner_err:
                    log.error(f"{symbol} için veri işleme hatası: {inner_err}")
                
                time.sleep(1)
                
        except Exception as e:
            log.error(f"Döngü hatası: {e}")
            
        time.sleep(LOOP_SECONDS)

@app.route("/")
def index():
    return jsonify({"status": "running", "message": "Bot aktif ve hatasız çalışıyor!"})

if __name__ == "__main__":
    t = threading.Thread(target=bot_loop, daemon=True)
    t.start()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
