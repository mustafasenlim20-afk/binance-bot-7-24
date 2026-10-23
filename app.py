"""
Binance USDT-M Futures botu (7/24, long + short) - RENDER UYUMLU
- Proxy destekli
- Flask ile 7/24 ayakta
"""
import os
import json
import time
import logging
import threading
import ccxt
import pandas as pd
from flask import Flask

app = Flask(__name__)

# ===================== AYARLAR =====================
SYMBOLS = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT", "XRP/USDT:USDT"]
TIMEFRAME = "15m"
LOOP_SECONDS = 30
MIN_SCORE = 4
LEV_BY_SCORE = {3: 5, 4: 7, 5: 10}
MARGIN_PCT = 0.10
TRANCHES = 3
ADD_ATR = 1.0
MAX_OPEN = 3
SL_ATR = 1.5
TP1_ATR = 1.5
TP2_ATR = 3.0
TRAIL_ATR = 1.2
MAX_DRAWDOWN = 0.20
STATE_FILE = "bot_state.json"
TESTNET = False
# ===================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot.log", encoding="utf-8")],
)
log = logging.getLogger("bot")

# ===== PROXY EKLENDI BABAM - BURASI COZUM =====
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

def add_indicators(df):
    c, h, l = df["close"], df["high"], df["low"]
    df["ema9"] = c.ewm(span=9, adjust=False).mean()
    df["ema21"] = c.ewm(span=21, adjust=False).mean()
    delta = c.diff()
    up = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    dn = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    df["rsi"] = 100 - 100 / (1 + up / dn.replace(0, 1e-12))
    macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    df["hist"] = macd - macd.ewm(span=9, adjust=False).mean()
    df["bb_mid"] = c.rolling(20).mean()
    std = c.rolling(20).std()
    df["bb_up"] = df["bb_mid"] + 2 * std
    df["bb_lo"] = df["bb_mid"] - 2 * std
    ll, hh = l.rolling(14).min(), h.rolling(14).max()
    k = 100 * (c - ll) / (hh - ll).replace(0, 1e-12)
    df["k"] = k.rolling(3).mean()
    df["d"] = df["k"].rolling(3).mean()
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / 14, adjust=False).mean()
    return df

def get_signal(sym):
    raw = ex.fetch_ohlcv(sym, TIMEFRAME, limit=200)
    df = pd.DataFrame(raw, columns=["t", "open", "high", "low", "close", "volume"])
    df = add_indicators(df)
    r, p = df.iloc[-2], df.iloc[-3]
    votes = []
    votes.append(1 if r.ema9 > r.ema21 else -1)
    votes.append(1 if 50 < r.rsi < 70 else (-1 if 30 < r.rsi < 50 else 0))
    votes.append(1 if (r.hist > 0 and r.hist > p.hist) else (-1 if (r.hist < 0 and r.hist < p.hist) else 0))
    votes.append(1 if r.bb_mid < r.close < r.bb_up else (-1 if r.bb_lo < r.close < r.bb_mid else 0))
    votes.append(1 if (r.k > r.d and r.k < 80) else (-1 if (r.k < r.d and r.k > 20) else 0))
    return sum(votes), float(r.atr)

def get_pos(sym):
    for p in ex.fetch_positions([sym]):
        amt = float(p.get("contracts") or 0)
        if amt > 0:
            return {"side": p["side"], "amt": amt, "entry": float(p["entryPrice"])}
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

def balance():
    return float(ex.fetch_balance()["USDT"]["total"])

def tranche_qty(sym, price, lev):
    notional = balance() * MARGIN_PCT / TRANCHES * lev
    qty = float(ex.amount_to_precision(sym, notional / price))
    m = ex.market(sym)
    min_amt = (m["limits"]["amount"]["min"] or 0)
    min_cost = (m["limits"]["cost"]["min"] or 0)
    if qty < min_amt or qty * price < min_cost:
        log.warning(f"{sym} parca cok kucuk")
        return 0.0
    return qty

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
    lev = LEV_BY_SCORE.get(min(abs(score), 5), 5)
    try: ex.set_margin_mode("isolated", sym)
    except: pass
    ex.set_leverage(lev, sym)
    qty = tranche_qty(sym, price, lev)
    if qty <= 0: return
    market_order(sym, "buy" if direction == 1 else "sell", qty)
    pos = get_pos(sym)
    if not pos: return
    sl = pos["entry"] - direction * SL_ATR * atr
    state[sym] = {"tranches": 1, "last_add": pos["entry"], "atr": atr, "sl": sl, "tp1": False, "tp2": False, "lev": lev}
    place_stop(sym, pos["side"], pos["amt"], sl)
    log.info(f"{sym} {'LONG' if direction == 1 else 'SHORT'} acildi")

def manage(sym):
    score, atr = get_signal(sym)
    price = float(ex.fetch_ticker(sym)["last"])
    pos = get_pos(sym)
    if not pos:
        if sym in state:
            state.pop(sym)
            cancel_orders(sym)
        if len(state) >= MAX_OPEN: return
        if score >= MIN_SCORE: open_first(sym, 1, score, atr, price)
        elif score <= -MIN_SCORE: open_first(sym, -1, score, atr, price)
        return
    d = 1 if pos["side"] == "long" else -1
    st = state.get(sym)
    if st is None:
        st = {"tranches": 1, "last_add": pos["entry"], "atr": atr, "sl": pos["entry"] - d * SL_ATR * atr, "tp1": False, "tp2": False, "lev": 5}
        state[sym] = st
        place_stop(sym, pos["side"], pos["amt"], st["sl"])
    if (price - st["sl"]) * d <= 0: return close_all(sym, "stop")
    if score * d <= -MIN_SCORE: return close_all(sym, "ters sinyal")
    profit = (price - pos["entry"]) * d
    if not st["tp1"] and profit >= TP1_ATR * st["atr"]:
        if close_part(sym, pos, 0.5, "TP1"):
            st["tp1"] = True
            st["sl"] = pos["entry"] + d * 0.1 * st["atr"]
            pos = get_pos(sym)
            place_stop(sym, pos["side"], pos["amt"], st["sl"])
        return
    if st["tp1"] and not st["tp2"] and profit >= TP2_ATR * st["atr"]:
        if close_part(sym, pos, 0.5, "TP2"):
            st["tp2"] = True
            pos = get_pos(sym)
            place_stop(sym, pos["side"], pos["amt"], st["sl"])
        return
    if st["tp1"]:
        new_sl = price - d * TRAIL_ATR * st["atr"]
        if (new_sl - st["sl"]) * d > 0:
            st["sl"] = new_sl
            place_stop(sym, pos["side"], pos["amt"], st["sl"])
        return
    if (st["tranches"] < TRANCHES and score * d >= MIN_SCORE and (price - st["last_add"]) * d >= ADD_ATR * st["atr"]):
        qty = tranche_qty(sym, price, st["lev"])
        if qty > 0:
            market_order(sym, "buy" if d == 1 else "sell", qty)
            st["tranches"] += 1
            st["last_add"] = price
            pos = get_pos(sym)
            place_stop(sym, pos["side"], pos["amt"], st["sl"])

def save_state():
    with open(STATE_FILE, "w") as f: json.dump(state, f)

def bot_loop():
    global state
    ex.load_markets()
    if os.path.exists(STATE_FILE):
        try: state = json.load(open(STATE_FILE))
        except: state = {}
    start_bal = balance()
    log.info(f"Bot basladi | bakiye={start_bal:.2f} USDT")
    while True:
        try:
            if balance() < start_bal * (1 - MAX_DRAWDOWN):
                for s in SYMBOLS: close_all(s, "drawdown")
                save_state()
                break
            for sym in SYMBOLS:
                try: manage(sym)
                except Exception as e: log.error(f"{sym} hata: {e}")
            save_state()
        except Exception as e: log.error(f"Dongu hatasi: {e}")
        time.sleep(LOOP_SECONDS)

@app.route('/')
def home():
    try:
        bal = balance()
        return f"BOT CANLI BABAM! Bakiye: {bal} USDT | Pozisyonlar: {list(state.keys())} | Proxy: OK"
    except Exception as e:
        return f"Bot ayakta ama Binance hatasi: {e} | Proxy: {PROXY_URL}"

# Botu arka planda baslat
threading.Thread(target=bot_loop, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
