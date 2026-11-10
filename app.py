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
TESTNET = False         # Render IP engeli sürerse True yapıp test edebilirsin
MAX_COIN_PRICE = 500.0  

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
    "timeout": 40000,  # Timeout süresi uzatıldı
    "options": {
        "adjustForTimeDifference": True,
        "defaultType": "future",
        "fetchCurrencies": False,
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
    """Alternatif endpoint ve güvenli bağlantı ile coinleri seçer"""
    for attempt in range(4):
        try:
            # exchangeInfo yerine doğrudan public ticker verisini çekerek engeli aşmaya çalışır
            tickers = ex.fetch_tickers()
            valid_tickers = {}
            
            for symbol, data in tickers.items():
                if 'USDT' in symbol and not any(stable in symbol for stable in ['USDC', 'BUSD', 'FDUSD', 'TUSD']):
                    last_price = float(data.get('last') or data.get('close') or 0)
                    vol = float(data.get('quoteVolume') or 0)
                    
                    if 0 < last_price <= MAX_COIN_PRICE and MIN_VOLUME <= vol <= MAX_VOLUME:
                        valid_tickers[symbol] = vol
            
            sorted_symbols = sorted(valid_tickers.keys(), key=lambda x: valid_tickers[x], reverse=True)
            top_symbols = sorted_symbols[:12]
            if top_symbols:
                return top_symbols
        except Exception as e:
            log.warning(f"Bağlantı denemesi {attempt+1} hata aldı: {e}")
            time.sleep(8)
            
    log.error("Tüm denemelere rağmen semboller çekilemedi!")
    return []

def calculate_indicators(df):
    try:
        df['ema_fast'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema_slow'] = df['close'].ewm(span=21, adjust=False).mean()
        
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
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

def check_signals(df):
    if len(df) < 2:
        return 0, "HOLD"
    
    last = df.iloc[-1]
    score = 0
    signal = "HOLD"
    
    if last['ema_fast'] > last['ema_slow']:
        score += 1
    if 45 <= last['rsi'] <= 65:
        score += 1
    if last['close'] > last['ema_fast']:
        score += 1

    if score >= MIN_SCORE:
        signal = "BUY"
        
    return score, signal

def bot_loop():
    log.info("Bot arkaplan döngüsü başladı.")
    while True:
        try:
            symbols = get_dynamic_symbols()
            log.info(f"Seçilen dinamik coinler: {symbols}")
            
            if not symbols:
                log.warning("Coin listesi boş, 30 saniye sonra tekrar denenecek...")
                time.sleep(30)
                continue

            try:
                positions = ex.fetch_positions()
                open_positions = [p for p in positions if float(p.get('contracts', 0)) > 0]
            except Exception as pos_err:
                log.error(f"Pozisyonlar çekilemedi: {pos_err}")
                open_positions = []

            for symbol in symbols:
                if len(open_positions) >= MAX_OPEN:
                    break
                    
                if any(p['symbol'] == symbol for p in open_positions):
                    continue

                try:
                    ohlcv = ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=50)
                    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    df = calculate_indicators(df)
                    
                    score, signal = check_signals(df)
                    log.info(f"{symbol} | Skor: {score} | Sinyal: {signal}")

                    if signal == "BUY":
                        lev = LEV_BY_SCORE.get(score, 5)
                        try:
                            ex.set_leverage(lev, symbol)
                        except Exception:
                            pass

                        balance = ex.fetch_balance()
                        usdt_free = float(balance['USDT']['free'])
                        margin_to_use = usdt_free * MARGIN_PCT
                        
                        last_price = df.iloc[-1]['close']
                        notional = margin_to_use * lev
                        amount = notional / last_price

                        amount = float(ex.amount_to_precision(symbol, amount))
                        
                        if amount > 0:
                            order = ex.create_market_buy_order(symbol, amount)
                            log.info(f"🚀 İŞLEM AÇILDI: {symbol} | Miktar: {amount} | Kaldıraç: {lev}x")
                            time.sleep(2)
                            
                except Exception as inner_err:
                    log.error(f"{symbol} işlem hatası: {inner_err}")
                
                time.sleep(1)
                
        except Exception as e:
            log.error(f"Döngü hatası: {e}")
            
        time.sleep(LOOP_SECONDS)

@app.route("/")
def index():
    return jsonify({"status": "running", "message": "Bot aktif ve çalışıyor!"})

if __name__ == "__main__":
    t = threading.Thread(target=bot_loop, daemon=True)
    t.start()
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
k
