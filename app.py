"""
Binance USDT-M Futures botu (7/24, 3'te 2 Indikator Filtresi) - DINAMIK HACIM FILTRELI (20M - 800M)
Versiyon: Stop-loss + ROI %20'de pozisyonun %30'u kapanir + sadece kapanmis mum
"""
import os
import re
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
LOOP_SECONDS = 180
MIN_SCORE = 2
LEV_BY_SCORE = {2: 5, 3: 8}
MARGIN_PCT = 0.30
MAX_OPEN = 3
SL_ATR = 1.5            # Stop-loss: giris - 1.5 x ATR
TP1_ROI = 20.0          # ROI %20 olunca
TP1_CLOSE_PCT = 0.30    # pozisyonun %30'unu kapat
TRAIL_ATR = 1.5         # Gezen stop mesafesi: en yuksek fiyat - 1.5 x ATR
TRAIL_CHECK_SECONDS = 20  # Gezen stop kac saniyede bir kontrol edilsin
STATE_FILE = "bot_state.json"
TESTNET = False
MAX_COIN_PRICE = 500.0
MIN_VOLUME = 20_000_000
MAX_VOLUME = 800_000_000
# ===================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()],
    force=True,
)
log = logging.getLogger("bot")

PROXY_URL = os.getenv("PROXY_URL")
PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

exchange_config = {
    "apiKey": os.getenv("BINANCE_API_KEY"),
    "secret": os.getenv("BINANCE_API_SECRET"),
    "enableRateLimit": True,
    "timeout": 30000,
    "options": {
        "adjustForTimeDifference": True,
        "defaultType": "future",
    },
}
if PROXY_URL:
    exchange_config["proxies"] = PROXIES

ex = ccxt.binanceusdm(exchange_config)
if TESTNET:
    ex.set_sandbox_mode(True)


# ---------------- BAN / HIZ KORUMASI ----------------
ban_until = 0.0  # epoch saniye; bu zamana kadar Binance'e istek atilmaz
ban_lock = threading.Lock()


def note_ban(err):
    """Hata metninde Binance yasagi veya bolge engeli varsa bekleme suresini ayarlar.
    Yasakliysa True doner."""
    global ban_until
    text = str(err)
    wait_until = 0.0
    m = re.search(r"banned until (\d{10,})", text)
    if m:
        wait_until = int(m.group(1)) / 1000.0 + 15  # bitisten 15 sn sonra dene
    elif "418" in text or "-1003" in text or "429" in text or "Too many requests" in text:
        wait_until = time.time() + 120               # sure yoksa 2 dk bekle
    elif "restricted location" in text or "451" in text:
        wait_until = time.time() + 600               # bolge engeli: 10 dk bekle
    if wait_until:
        with ban_lock:
            if wait_until > ban_until:
                ban_until = wait_until
                left = int(ban_until - time.time())
                log.error(f"⛔ Binance yasagi/limit algilandi, {left} sn istek atilmayacak.")
        return True
    return False


def wait_if_banned():
    """Yasak suruyorsa bitene kadar uyur (en fazla 60 sn'lik parcalarla)."""
    while True:
        left = ban_until - time.time()
        if left <= 0:
            return
        time.sleep(min(left, 60))


def get_dynamic_symbols():
    try:
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        response = requests.get(url, timeout=15)
        data = response.json()
        if not isinstance(data, list):
            log.error(f"Beklenmeyen veri formati: {data}")
            note_ban(data)
            return []

        valid = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol", "")
            if not symbol.endswith("USDT"):
                continue
            if any(s in symbol for s in ["USDC", "BUSD", "FDUSD", "TUSD"]):
                continue
            try:
                price = float(item.get("lastPrice", 0))
                vol = float(item.get("quoteVolume", 0))
            except (ValueError, TypeError):
                continue
            if 0 < price <= MAX_COIN_PRICE and MIN_VOLUME <= vol <= MAX_VOLUME:
                valid[f"{symbol[:-4]}/USDT:USDT"] = vol

        return sorted(valid, key=valid.get, reverse=True)[:12]
    except Exception as e:
        log.error(f"Semboller cekilirken hata: {e}")
        return []


def calculate_indicators(df):
    df["ema_fast"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=21, adjust=False).mean()

    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))

    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    df["atr"] = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1).rolling(14).mean()
    return df


def check_signals(df):
    """Son KAPANMIS muma bakar (df'de acik mum yok)."""
    if len(df) < 25:
        return 0, "HOLD"
    last = df.iloc[-1]
    if pd.isna(last["rsi"]) or pd.isna(last["atr"]):
        return 0, "HOLD"

    score = 0
    if last["ema_fast"] > last["ema_slow"]:
        score += 1
    if 45 <= last["rsi"] <= 65:
        score += 1
    if last["close"] > last["ema_fast"]:
        score += 1

    return score, ("BUY" if score >= MIN_SCORE else "HOLD")


def place_protection(symbol, entry, amount, atr, lev):
    """Borsa tarafinda SL ve ROI %20 kismi kar alma emirlerini koyar.
    SL konamazsa False doner (pozisyon kapatilmali)."""
    sl_price = float(ex.price_to_precision(symbol, entry - SL_ATR * atr))
    # ROI %20 = fiyat hareketi %20 / kaldirac
    tp_price = float(ex.price_to_precision(symbol, entry * (1 + (TP1_ROI / 100) / lev)))
    tp_amount = float(ex.amount_to_precision(symbol, amount * TP1_CLOSE_PCT))

    try:
        sl_order = ex.create_order(symbol, "STOP_MARKET", "sell", amount, None,
                                   {"stopPrice": sl_price, "reduceOnly": True})
        log.info(f"🛑 SL kondu: {symbol} @ {sl_price}")
    except Exception as e:
        log.error(f"{symbol} SL konamadi: {e}")
        return None

    if tp_amount > 0:
        try:
            ex.create_order(symbol, "TAKE_PROFIT_MARKET", "sell", tp_amount, None,
                            {"stopPrice": tp_price, "reduceOnly": True})
            log.info(f"🎯 TP kondu (%{TP1_CLOSE_PCT*100:.0f} kapanir): {symbol} @ {tp_price} (ROI %{TP1_ROI})")
        except Exception as e:
            log.error(f"{symbol} TP konamadi: {e}")
    return {"sl_id": sl_order.get("id"), "sl_price": sl_price}


# ---------------- GEZEN STOP (TRAILING) ----------------
trail_state = {}
trail_lock = threading.Lock()


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(trail_state, f)
    except Exception as e:
        log.error(f"State kaydedilemedi: {e}")


def load_state():
    try:
        with open(STATE_FILE) as f:
            trail_state.update(json.load(f))
        log.info(f"State yuklendi: {list(trail_state.keys())}")
    except Exception:
        pass


def cancel_safe(order_id, symbol):
    """Normal ve kosullu emir iptalini dener."""
    for params in ({}, {"trigger": True}):
        try:
            ex.cancel_order(order_id, symbol, params)
            return True
        except Exception:
            continue
    return False


def cancel_all_safe(symbol):
    for params in ({}, {"trigger": True}):
        try:
            ex.cancel_all_orders(symbol, params)
        except Exception:
            pass


def trail_loop():
    """Fiyat yukseldikce SL'yi yukari ceker, asla asagi indirmez."""
    log.info("Gezen stop dongusu basladi.")
    while True:
        wait_if_banned()
        try:
            with trail_lock:
                symbols = list(trail_state.keys())

            if symbols:
                sizes = {p["symbol"]: float(p.get("contracts") or 0) for p in ex.fetch_positions()}

                for symbol in symbols:
                    try:
                        st = trail_state.get(symbol)
                        if not st:
                            continue

                        size = sizes.get(symbol, 0)
                        if size <= 0:  # pozisyon kapandi, artan emirleri temizle
                            cancel_all_safe(symbol)
                            with trail_lock:
                                trail_state.pop(symbol, None)
                                save_state()
                            log.info(f"{symbol} kapandi, gezen stop birakildi.")
                            continue

                        price = float(ex.fetch_ticker(symbol)["last"])
                        st["high"] = max(st["high"], price)
                        new_sl = float(ex.price_to_precision(symbol, st["high"] - TRAIL_ATR * st["atr"]))

                        # Sadece yukari: en az 0.1 ATR fark olsun (gereksiz emir spami olmasin)
                        if new_sl > st["sl_price"] + 0.1 * st["atr"] and new_sl < price:
                            amount = float(ex.amount_to_precision(symbol, size))
                            new_order = ex.create_order(symbol, "STOP_MARKET", "sell", amount, None,
                                                        {"stopPrice": new_sl, "reduceOnly": True})
                            cancel_safe(st["sl_id"], symbol)  # once yenisi kondu, sonra eskisi iptal
                            log.info(f"📈 {symbol} gezen stop: {st['sl_price']} -> {new_sl}")
                            st["sl_id"] = new_order.get("id")
                            st["sl_price"] = new_sl

                        with trail_lock:
                            save_state()
                    except Exception as e:
                        log.error(f"{symbol} gezen stop hatasi: {e}")
                        if note_ban(e):
                            break
        except Exception as e:
            log.error(f"Gezen stop dongu hatasi: {e}")
            note_ban(e)

        time.sleep(TRAIL_CHECK_SECONDS)


def get_open_symbols():
    positions = ex.fetch_positions()
    return [p["symbol"] for p in positions if float(p.get("contracts") or 0) > 0]


def bot_loop():
    log.info("Bot arkaplan dongusu basladi.")
    while True:
        wait_if_banned()
        try:
            try:
                open_symbols = get_open_symbols()
            except Exception as e:
                log.error(f"Pozisyonlar cekilemedi: {e}")
                note_ban(e)
                time.sleep(30)
                continue  # pozisyon bilgisi yoksa yeni islem acma

            if len(open_symbols) >= MAX_OPEN:
                log.info(f"Pozisyon limiti dolu ({len(open_symbols)}/{MAX_OPEN}), yeni tarama yapilmadi.")
                time.sleep(LOOP_SECONDS)
                continue

            symbols = get_dynamic_symbols()
            log.info(f"Secilen coinler: {symbols}")
            if not symbols:
                time.sleep(30)
                continue

            for symbol in symbols:
                if len(open_symbols) >= MAX_OPEN:
                    break
                if symbol in open_symbols:
                    continue

                try:
                    ohlcv = ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=60)
                    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                    df = df.iloc[:-1].copy()  # acik mumu at
                    df = calculate_indicators(df)

                    score, signal = check_signals(df)
                    log.info(f"{symbol} | Skor: {score} | Sinyal: {signal}")
                    if signal != "BUY":
                        continue

                    lev = LEV_BY_SCORE.get(score, 5)
                    try:
                        ex.set_margin_mode("isolated", symbol)
                    except Exception:
                        pass
                    try:
                        ex.set_leverage(lev, symbol)
                    except Exception:
                        pass

                    usdt_free = float(ex.fetch_balance()["USDT"]["free"])
                    last = df.iloc[-1]
                    amount = float(ex.amount_to_precision(symbol, usdt_free * MARGIN_PCT * lev / last["close"]))
                    if amount <= 0:
                        continue

                    order = ex.create_market_buy_order(symbol, amount)
                    entry = float(order.get("average") or order.get("price") or last["close"])
                    log.info(f"🚀 ISLEM ACILDI: {symbol} | Miktar: {amount} | Kaldirac: {lev}x | Giris: {entry}")
                    open_symbols.append(symbol)

                    prot = place_protection(symbol, entry, amount, float(last["atr"]), lev)
                    if prot:
                        with trail_lock:
                            trail_state[symbol] = {
                                "atr": float(last["atr"]),
                                "high": entry,
                                "sl_price": prot["sl_price"],
                                "sl_id": prot["sl_id"],
                            }
                            save_state()
                    else:
                        log.error(f"{symbol} SL yok, pozisyon aciliyor kapatiliyor!")
                        ex.create_order(symbol, "market", "sell", amount, None, {"reduceOnly": True})
                        open_symbols.remove(symbol)
                    time.sleep(2)

                except Exception as inner_err:
                    log.error(f"{symbol} islem hatasi: {inner_err}")
                    if note_ban(inner_err):
                        break  # yasak varsa kalan coinlere istek atma

                time.sleep(1)

        except Exception as e:
            log.error(f"Dongu hatasi: {e}")
            note_ban(e)

        time.sleep(LOOP_SECONDS)


_started = False
_start_lock = threading.Lock()


def start_bot_once():
    global _started
    with _start_lock:
        if not _started:
            _started = True
            load_state()
            threading.Thread(target=bot_loop, daemon=True).start()
            threading.Thread(target=trail_loop, daemon=True).start()


start_bot_once()  # gunicorn ile de baslasin (1 worker kullan)


@app.route("/")
def index():
    return jsonify({"status": "running"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
