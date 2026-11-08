"""
Binance USDT-M Futures botu (7/24, 3'te 2 İndikatör Filtresi, long + short) - RENDER UYUMLU
- BTC/ETH gibi pahalı coinleri eler, ADA/XRP/DOGE gibi altcoinlere odaklanır
- Proxy destekli
- Flask ile 7/24 ayakta
- Rate-limit korumalı ve Ortalama Maliyet Düzeltmeli
- ROI Bazlı Kâr Al (Take Profit) ve İzleyen Stop (Trailing Stop) Mekanizmalı
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
TIMEFRAME = "5m"        # Daha çevik ve hızlı tepki için 5 dakikalık periyot
LOOP_SECONDS = 360  # Rate limit önlemi için döngü süresi
MIN_SCORE = 2       # 3 indikatörden en az 2'si uyarsa işlem açar (3'te 2 kuralı)
LEV_BY_SCORE = {2: 5, 3: 8} # Skora göre kaldıraç
MARGIN_PCT = 0.30
TRANCHES = 3
ADD_ATR = 1.0
MAX_OPEN = 3
SL_ATR = 1.5
TP1_ROI = 50.0      # ROI %50 olduğunda ilk kâr al (%50 satış)
TP2_ROI = 100.0     # ROI %100 olduğunda ikinci kâr al (kalanın %50'si)
TRAIL_ATR = 1.0     # İzleyen stop mesafesi
MAX_DRAWDOWN = 0.20
STATE_FILE = "bot_state.json"
TESTNET = False
MAX_COIN_PRICE = 500.0  # BTC ve ETH gibi pahalı coinleri elemek için üst fiyat sınırı (USDT)
# ===================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot.log", encoding="utf-8")],
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
    """Binance piyasasındaki hacmi yüksek, ancak fiyatı 500 USDT'den ucuz olan altcoinleri seçer (ADA, XRP tarzı)"""
    try:
        tickers = ex.fetch_tickers()
        valid_tickers = {}
        for symbol, data in tickers.items():
            if symbol.endswith(':USDT') and 'quoteVolume' in data:
                base = symbol.split('/')[0]
                if base in ['USDC', 'BUSD', 'FDUSD', 'TUSD', 'BTC', 'ETH', 'BNB', 'SOL']:
                    continue
                
                last_price = data.get('last') or 0
                if 0 < last_price <= MAX_COIN_PRICE:
                    valid_tickers[symbol] = data['quoteVolume'] or 0
        
        sorted_symbols = sorted(valid_tickers.keys(), key=lambda x: valid_tickers[x], reverse=True)
        top_symbols = sorted_symbols[:10]
        log.info(f"Dinamik seçilen en hacimli altcoinler: {top_symbols}")
        return top_symbols
    except Exception as e:
        log.error(f"Dinamik coin seçme hatası: {e}")
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
    raw = ex.fetch_ohlcv(sym, TIMEFRAME, limit=200)
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
            return {
                "side": p["side"], 
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
    log.info(f"{sym} stop -> {price:.6f}")

def market_order(sym, side, qty, reduce=False):
    params = {"reduceOnly": True} if reduce else {}
    return ex.create_order(sym, "market", side, qty, None, params)

def close_all(sym, why):
    cancel_orders(sym)
    pos = get_pos(sym)
    if pos:
        side = "sell" if pos["side"] == "long" else "buy"
        market_order(sym, side, pos["amt"], reduce=True)
        log.info(f"{sym} KAPATILDI ({why})")
    with state_lock:
        state.pop(sym, None)

def close_part(sym, pos, fraction, why):
    qty = float(ex.amount_to_precision(sym, pos["amt"] * fraction))
    if qty <= 0 or qty >= pos["amt"]:
        close_all(sym, why)
        return False
    side = "sell" if pos["side"] == "long" else "buy"
    market_order(sym, side, qty, reduce=True)
    log.info(f"{sym} kismi satis {fraction:.0%} ({why})")
    return True

def open_first(sym, direction, score, atr, price):
    lev = LEV_BY_SCORE.get(min(abs(score), 3), 5)
    try: ex.set_margin_mode("isolated", sym)
    except: pass
    ex.set_leverage(lev, sym)
    qty = tranche_qty(sym, price, lev)
    if qty <= 0: return
    market_order(sym, "buy" if direction == 1 else "sell", qty)
    pos = get_pos(sym)
    if not pos: return
    
    sl = pos["entry"] - direction * SL_ATR * atr
    
    with state_lock:
        state[sym] = {
            "tranches": 1, 
            "last_add": pos["entry"], 
            "atr": atr, 
            "sl": sl, 
            "tp1": False, 
            "tp2": False, 
            "lev": lev
        }
    
    place_stop(sym, pos["side"], pos["amt"], sl)
    log.info(f"{sym} {'LONG' if direction == 1 else 'SHORT'} acildi (Skor: {score})")

def manage(sym):
    score, atr = get_signal(sym)
    price = float(ex.fetch_ticker(sym)["last"])
    pos = get_pos(sym)
    
    log.info(f"Tarama -> {sym} | Fiyat: {price} | Skor: {score}/3")
    
    with state_lock:
        is_in_state = sym in state
        st = state.get(sym)

    if not pos:
        if is_in_state:
            with state_lock:
                state.pop(sym, None)
            cancel_orders(sym)
        
        with state_lock:
            active_count = len(state)
        
        if active_count >= MAX_OPEN: return
        if score >= MIN_SCORE: open_first(sym, 1, score, atr, price)
        elif score <= -MIN_SCORE: open_first(sym, -1, score, atr, price)
        return

    d = 1 if pos["side"] == "long" else -1
    
    if st is None:
        st = {
            "tranches": 1, 
            "last_add": pos["entry"], 
            "atr": atr, 
            "sl": pos["entry"] - d * SL_ATR * atr, 
            "tp1": False, 
            "tp2": False, 
            "lev": 5
        }
        with state_lock:
            state[sym] = st
        place_stop(sym, pos["side"], pos["amt"], st["sl"])

    # Stop-loss kontrolü
    if (price - st["sl"]) * d <= 0: 
        return close_all(sym, "stop")
    
    # Ters sinyal kontrolü
    if score * d <= -MIN_SCORE: 
        return close_all(sym, "ters sinyal")
    
    avg_entry = pos["entry"]
    
    # Binance ekranındaki ROI oranını tam hesapla (Kaldıraç dahil yüzdelik kâr)
    roi_pct = ((price - avg_entry) * d / avg_entry) * st["lev"] * 100
    log.info(f"{sym} Anlık ROI: %{roi_pct:.2f}")
    
    # TP1 Kontrolü (ROI %50'yi geçtiği an yarısını satıp karı ayırır)[span_0](start_span)[span_0](end_span)
    if not st["tp1"] and roi_pct >= TP1_ROI:
        if close_part(sym, pos, 0.5, "TP1 (%50 ROI)"):
            st["tp1"] = True
            # Stop seviyesini giriş maliyetinin çok az üstüne çekerek riski sıfırla
            st["sl"] = avg_entry + d * 0.001 * avg_entry
            pos = get_pos(sym)
            if pos:
                place_stop(sym, pos["side"], pos["amt"], st["sl"])
        return
        
    # TP2 Kontrolü (ROI %100 olduğunda kalan yarısı için)
    if st["tp1"] and not st["tp2"] and roi_pct >= TP2_ROI:
        if close_part(sym, pos, 0.5, "TP2 (%100 ROI)"):
            st["tp2"] = True
            pos = get_pos(sym)
            if pos:
                place_stop(sym, pos["side"], pos["amt"], st["sl"])
        return
        
    # Trailing Stop Mekanizması (Fiyat lehimize gittikçe karı arkadan takip edip kilitleme)
    if st["tp1"]:
        new_sl = price - d * TRAIL_ATR * st["atr"]
        if (new_sl - st["sl"]) * d > 0:
            st["sl"] = new_sl
            pos = get_pos(sym)
            if pos:
                place_stop(sym, pos["side"], pos["amt"], st["sl"])
        return
        
    # Kademeli Ekleme (DCA) Kontrolü
    if (st["tranches"] < TRANCHES and score * d >= MIN_SCORE and (price - st["last_add"]) * d >= ADD_ATR * st["atr"]):
        qty = tranche_qty(sym, price, st["lev"])
        if qty > 0:
            market_order(sym, "buy" if d == 1 else "sell", qty)
            st["tranches"] += 1
            st["last_add"] = price
            pos = get_pos(sym)
            if pos:
                place_stop(sym, pos["side"], pos["amt"], st["sl"])

def save_state():
    with state_lock:
        with open(STATE_FILE, "w") as f: json.dump(state, f)

def bot_loop():
    global state
    ex.load_markets()
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                with state_lock:
                    state = json.load(f)
        except:
            with state_lock:
                state = {}
                
    start_bal = balance()
    log.info(f"Bot basladi (ROI Bazlı Kâr Al Aktif) | bakiye={start_bal:.2f} USDT")
    
    while True:
        try:
            if balance() < start_bal * (1 - MAX_DRAWDOWN):
                for p in ex.fetch_positions():
                    if float(p.get("contracts") or 0) > 0:
                        close_all(p['symbol'], "drawdown")
                save_state()
                break
                
            current_symbols = get_dynamic_symbols()
                
            for sym in current_symbols:
                try:
                    manage(sym)
                    time.sleep(1.5)
                except Exception as e: 
                    log.error(f"{sym} hata: {e}")
            save_state()
        except Exception as e: 
            log.error(f"Dongu hatasi: {e}")
        time.sleep(LOOP_SECONDS)

@app.route('/')
def home():
    try:
        bal = balance()
        with state_lock:
            keys = list(state.keys())
        return f"BOT CANLI BABAM! (ROI Kâr Al Modu) | Bakiye: {bal} USDT | Pozisyonlar: {keys} | Proxy: OK"
    except Exception as e:
        return f"Bot ayakta ama Binance hatasi: {e} | Proxy: {PROXY_URL}"

@app.route('/ip')
def get_ip():
    import requests
    return jsonify({"ip": requests.get('https://api.ipify.org').text})

threading.Thread(target=bot_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
