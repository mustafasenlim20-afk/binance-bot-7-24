"""
Binance USDT-M Futures botu (7/24) - WEBSOCKET VERI + REST SADECE ISLEM
- Fiyat ve mum verisi WebSocket'ten gelir (REST'e istek atilmaz)
- REST sadece: al/sat, stop koy, gezen stop guncelle, pozisyon senkronu (60 sn'de 1 istek)
- Acilista her coinin gecmis mumlari REST'ten BIR KEZ cekilir (yavas, aralikli), sonrasi hep WebSocket
- WebSocket calismazsa: kontrollu REST yedek modu (az coin, yavas, ban korumali)
- 20M-800M hacim araligindaki TUM coinler taranir, 0-100 trend puani, LONG + SHORT
- ATR stop-loss, kismi kar al, KADEMELI ATR gezen stop, stop sonrasi bekleme, gunluk zarar limiti
- Render'da yeniden baslarsa acik pozisyonlari sahiplenir

Gerekli paketler (requirements.txt): ccxt, pandas, flask, gunicorn, websocket-client
"""
import os
import re
import json
import time
import logging
import threading
from urllib.parse import urlparse

import ccxt
import pandas as pd
from flask import Flask, jsonify

try:
    import websocket  # pip paketi adi: websocket-client
except ImportError:
    websocket = None

app = Flask(__name__)

# ===================== AYARLAR =====================
TIMEFRAME = "15m"          # ana zaman dilimi
HTF = "1h"                 # ust zaman dilimi (yon teyidi)
LOOP_SECONDS = 180         # tarama araligi

# --- Coin secimi (HEPSI taranir) ---
MIN_VOLUME = 20_000_000
MAX_VOLUME = 800_000_000
MAX_COIN_PRICE = 500.0

# --- Trend puani (0-100) ---
MIN_TREND_SCORE = 50       # bunun altindaki coine girilmez (dusurursen daha cok islem acar)
STRONG_SCORE = 70          # bu puan ve ustu = guclu trend -> yuksek kaldirac
HTF_CHECK_TOP = 10         # ust zaman dilimi teyidi en iyi kac coine yapilsin
HTF_PTS = 15               # ust zaman dilimi uyumlu ise puan (belirsizse yarisi, ters ise elenir)
ADX_MIN = 20               # bunun altinda trend yok sayilir
MIN_ATR_PCT = 0.0025       # ATR / fiyat en az %0,25 (cok durgun coinde komisyon yer)
MIN_REL_VOL = 0.7          # son 3 mum hacmi / onceki 20 mum ortalamasi en az bu olsun
RSI_MAX_LONG = 75          # long'da RSI bunun ustundeyse trend yorulmus
RSI_MIN_SHORT = 25         # short'ta RSI bunun altindaysa trend yorulmus
MAX_EXT_ATR = 4.0          # fiyat EMA21'den 4 ATR'den fazla uzaksa gec kalinmis, girme
BREAKOUT_BARS = 12         # kirilim icin bakilan onceki mum sayisi

# --- Islem ---
LEV_NORMAL = 3             # puan < STRONG_SCORE
LEV_STRONG = 5             # puan >= STRONG_SCORE
MARGIN_PCT = 0.30          # bos bakiyenin %30'u marjin
MAX_OPEN = 3
SL_ATR = 1.5               # Stop-loss: giris -/+ 1.5 x ATR
TP1_ATR = 2.0              # Ilk kar al: giris +/- 2 x ATR
TP1_CLOSE_PCT = 0.50       # ilk kar alda pozisyonun %50'si kapanir, kalan gezen stopla gider

# --- Kademeli gezen stop (kar buyudukce daralir) ---
# Kar (ATR cinsinden) -> takip carpani. Kar sinirin altindaysa o carpan kullanilir.
TRAIL_TIERS = ((1.0, 3.0), (2.0, 2.0))   # kar < 1 ATR: 3x | kar 1-2 ATR: 2x
TRAIL_TIGHT_ATR = 1.2      # kar 2 ATR'yi gecince en dar takip (1.2 x ATR)
BREAKEVEN_AT_ATR = 1.0     # kar bu kadar ATR olunca stop en az girise cekilir (zarar etmeme)

COOLDOWN_SECONDS = 2 * 3600   # bir coin kapaninca bu sure tekrar girilmez
DAILY_LOSS_LIMIT_PCT = 10.0   # gun basi bakiyeye gore bu kadar zarar olursa gun sonuna kadar yeni islem yok
ALLOW_LONG = True
ALLOW_SHORT = True
STATE_FILE = "bot_state.json"
TESTNET = False

# --- WebSocket / istek ayarlari ---
WS_URL = "wss://fstream.binance.com/stream"
KLINE_KEEP = 150            # her coin icin bellekte tutulan mum sayisi
SEED_LIMIT = 100            # acilista REST'ten cekilen gecmis mum sayisi
SEED_SLEEP = 0.5            # gecmis mum cekerken coinler arasi bekleme (sn)
WS_STALE_SECONDS = 45       # WS'ten bu kadar sn veri gelmezse "calismiyor" sayilir
KLINE_STALE_SECONDS = 180   # bir coinin mumu bu kadar sn guncellenmezse bayat sayilir
TRAIL_TICK_SECONDS = 2      # WS aciksa gezen stop kontrol sikligi (REST istegi atmaz)
TRAIL_CHECK_SECONDS = 20    # WS yoksa REST yedek kontrol sikligi
POSITION_SYNC_SECONDS = 60  # pozisyon kapandi mi kontrolu (tek REST istegi)
HTF_CACHE_SECONDS = 600     # 1s verisi bu sure onbellekte tutulur
FALLBACK_MAX_COINS = 30     # WS yoksa REST yedek modunda taranacak en fazla coin
FALLBACK_SLEEP = 0.5        # REST yedek modunda coinler arasi bekleme (sn)
REST_TICKER_MIN_GAP = 120   # WS yoksa coin listesi REST'ten en az bu aralikla cekilir
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


def _tf_ms(tf):
    return int(tf[:-1]) * {"m": 60_000, "h": 3_600_000, "d": 86_400_000}[tf[-1]]


TF_MS = _tf_ms(TIMEFRAME)


# ---------------- BAN / HIZ KORUMASI ----------------
ban_until = 0.0
ban_lock = threading.Lock()


def note_ban(err):
    global ban_until
    text = str(err)
    wait_until = 0.0
    m = re.search(r"banned until (\d{10,})", text)
    if m:
        wait_until = int(m.group(1)) / 1000.0 + 15
    elif "418" in text or "-1003" in text or "429" in text or "Too many requests" in text:
        wait_until = time.time() + 120
    elif "restricted location" in text or "451" in text:
        wait_until = time.time() + 600
    if wait_until:
        with ban_lock:
            if wait_until > ban_until:
                ban_until = wait_until
                left = int(ban_until - time.time())
                log.error(f"⛔ Binance yasagi/limit algilandi, {left} sn istek atilmayacak.")
        return True
    return False


def wait_if_banned():
    while True:
        left = ban_until - time.time()
        if left <= 0:
            return
        time.sleep(min(left, 60))


# ---------------- DURUM (state) ----------------
trail_state = {}
cooldown = {}
day_state = {}
trail_lock = threading.Lock()
opening_symbols = set()
open_lock = threading.Lock()


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"trail": trail_state, "cooldown": cooldown, "day": day_state}, f)
    except Exception as e:
        log.error(f"State kaydedilemedi: {e}")


def load_state():
    try:
        with open(STATE_FILE) as f:
            loaded = json.load(f)
        if isinstance(loaded.get("trail"), dict):
            trail = loaded["trail"]
            cooldown.update({k: float(v) for k, v in loaded.get("cooldown", {}).items()})
            day_state.update(loaded.get("day", {}))
        else:
            trail = loaded
        for st in trail.values():
            st.setdefault("side", "long")
            st.setdefault("ext", st.pop("high", st.get("sl_price", 0)))
        trail_state.update(trail)
        log.info(f"State yuklendi: {list(trail_state.keys())}")
    except Exception:
        pass


# =====================================================================
#                      WEBSOCKET VERI KATMANI
# =====================================================================
ws_lock = threading.Lock()
tickers = {}          # "BTCUSDT" -> {"price": float, "vol": float(24s quote hacim)}
tickers_ts = 0.0      # son WS ticker mesaji zamani
klines = {}           # "BTCUSDT" -> [[t, o, h, l, c, v, kapandi_mi], ...]
kline_ts = {}         # "BTCUSDT" -> son guncelleme zamani
dirty = set()         # mum boslugu olan (yeniden yuklenmesi gereken) coinler
subscribed = set()    # kline'ina abone olunan coinler
ws_app = None
ws_connected = False
rest_ticker_ts = 0.0
COLS = ["timestamp", "open", "high", "low", "close", "volume", "closed"]


def to_raw(symbol):
    """'BTC/USDT:USDT' -> 'BTCUSDT'"""
    return symbol.split("/")[0] + "USDT"


def to_ccxt(raw):
    """'BTCUSDT' -> 'BTC/USDT:USDT'"""
    return f"{raw[:-4]}/USDT:USDT"


def ws_healthy():
    return bool(websocket) and ws_connected and (time.time() - tickers_ts) < WS_STALE_SECONDS


def ws_on_open(*args):
    global ws_connected
    ws_connected = True
    log.info("✅ WebSocket baglandi.")
    with ws_lock:
        params = ["!miniTicker@arr"] + [f"{s.lower()}@kline_{TIMEFRAME}" for s in subscribed]
    ws_send_subscribe(params)


def ws_on_close(*args):
    global ws_connected
    ws_connected = False
    log.warning("WebSocket koptu, yeniden baglanilacak.")


def ws_on_error(*args):
    log.error(f"WebSocket hatasi: {args[-1] if args else '?'}")


def ws_on_message(*args):
    global tickers_ts
    try:
        msg = json.loads(args[-1])
    except Exception:
        return
    if not isinstance(msg, dict):
        return
    data = msg.get("data")
    if data is None:       # abonelik cevabi vb.
        return
    stream = msg.get("stream", "")

    if stream == "!miniTicker@arr":
        now = time.time()
        with ws_lock:
            for it in data:
                try:
                    tickers[it["s"]] = {"price": float(it["c"]), "vol": float(it["q"])}
                except (KeyError, ValueError, TypeError):
                    continue
            tickers_ts = now

    elif "@kline_" in stream:
        try:
            k = data["k"]
            raw = data["s"]
            row = [int(k["t"]), float(k["o"]), float(k["h"]), float(k["l"]),
                   float(k["c"]), float(k["v"]), bool(k["x"])]
        except (KeyError, ValueError, TypeError):
            return
        with ws_lock:
            arr = klines.setdefault(raw, [])
            if not arr:
                arr.append(row)
            elif arr[-1][0] == row[0]:
                arr[-1] = row
            elif row[0] > arr[-1][0]:
                if row[0] - arr[-1][0] > TF_MS * 1.5:
                    dirty.add(raw)           # arada mum kacti, REST ile yeniden yuklenecek
                arr.append(row)
                if len(arr) > KLINE_KEEP:
                    del arr[0]
            kline_ts[raw] = time.time()


def ws_send_subscribe(params):
    app_ = ws_app
    if app_ is None or not ws_connected or not params:
        return
    try:
        for i in range(0, len(params), 50):
            app_.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": params[i:i + 50],
                "id": int(time.time() * 1000) % 1_000_000_000,
            }))
            time.sleep(0.15)     # Binance: saniyede en fazla 10 mesaj
    except Exception as e:
        log.error(f"WS abonelik gonderilemedi: {e}")


def ws_proxy_kwargs():
    if not PROXY_URL:
        return {}
    u = urlparse(PROXY_URL)
    kw = {
        "http_proxy_host": u.hostname,
        "http_proxy_port": u.port,
        "proxy_type": "socks5" if u.scheme.startswith("socks") else "http",
    }
    if u.username:
        kw["http_proxy_auth"] = (u.username, u.password or "")
    return kw


def ws_thread():
    """Surekli calisir; kopunca kendini yeniden baglar."""
    global ws_app, ws_connected
    if websocket is None:
        log.error("❌ 'websocket-client' paketi yok! requirements.txt'ye ekle. REST yedek modunda calisacak.")
        return
    fails = 0
    while True:
        started = time.time()
        try:
            ws_app = websocket.WebSocketApp(
                WS_URL,
                on_open=ws_on_open,
                on_message=ws_on_message,
                on_error=ws_on_error,
                on_close=ws_on_close,
            )
            ws_app.run_forever(ping_interval=20, ping_timeout=10, **ws_proxy_kwargs())
        except Exception as e:
            log.error(f"WebSocket dongu hatasi: {e}")
        ws_connected = False
        fails = 0 if time.time() - started > 60 else min(fails + 1, 6)
        time.sleep(min(5 * (fails + 1), 60))


# ---------------- Coin listesi (WS onbellekten) ----------------
def symbols_from_cache():
    with ws_lock:
        snap = dict(tickers)
    valid = {}
    for raw, t in snap.items():
        if not raw.endswith("USDT"):
            continue
        if any(s in raw for s in ["USDC", "BUSD", "FDUSD", "TUSD"]):
            continue
        if 0 < t["price"] <= MAX_COIN_PRICE and MIN_VOLUME <= t["vol"] <= MAX_VOLUME:
            valid[to_ccxt(raw)] = t["vol"]
    return sorted(valid, key=valid.get, reverse=True)


def refresh_tickers_rest():
    """Sadece WS calismiyorsa: tek istekle tum coinleri ceker."""
    global rest_ticker_ts
    wait_if_banned()
    data = ex.fetch_tickers()
    with ws_lock:
        for t in data.values():
            info = t.get("info") or {}
            try:
                tickers[info["symbol"]] = {
                    "price": float(info["lastPrice"]),
                    "vol": float(info["quoteVolume"]),
                }
            except (KeyError, ValueError, TypeError):
                continue
    rest_ticker_ts = time.time()


def get_dynamic_symbols():
    if not ws_healthy() and time.time() - rest_ticker_ts > REST_TICKER_MIN_GAP:
        try:
            refresh_tickers_rest()
        except Exception as e:
            log.error(f"Semboller (REST yedek) cekilirken hata: {e}")
            note_ban(e)
    return symbols_from_cache()


# ---------------- Mum verisi ----------------
def is_fresh(raw):
    with ws_lock:
        arr = klines.get(raw)
        return bool(arr) and len(arr) >= 60 and raw not in dirty \
            and (time.time() - kline_ts.get(raw, 0)) < KLINE_STALE_SECONDS


def ensure_streams(symbols):
    with ws_lock:
        new = [to_raw(s) for s in symbols if to_raw(s) not in subscribed]
        subscribed.update(new)
    if new:
        ws_send_subscribe([f"{r.lower()}@kline_{TIMEFRAME}" for r in new])


def seed_symbol(symbol):
    """Gecmis mumlari REST'ten BIR KEZ yukler (sonrasi WS besler)."""
    raw = to_raw(symbol)
    ohlcv = ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=SEED_LIMIT)
    rows = [[int(r[0]), r[1], r[2], r[3], r[4], r[5], True] for r in ohlcv]
    if rows:
        rows[-1][6] = False       # sonuncu mum henuz olusuyor
    with ws_lock:
        klines[raw] = rows
        kline_ts[raw] = time.time()
        dirty.discard(raw)


def prepare_data(symbols, skip):
    ensure_streams(symbols)
    need = [s for s in symbols if s not in skip and not is_fresh(to_raw(s))]
    if not need:
        return
    log.info(f"Gecmis mum yukleniyor (REST, aralikli): {len(need)} coin")
    for s in need:
        wait_if_banned()
        try:
            seed_symbol(s)
        except Exception as e:
            log.error(f"{s} gecmis mum hatasi: {e}")
            if note_ban(e):
                break
        time.sleep(SEED_SLEEP)


# ---------------- INDIKATORLER ----------------
def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def calculate_indicators(df):
    df["ema_fast"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=21, adjust=False).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))

    prev_close = df["close"].shift()
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr"] = tr.rolling(14).mean()

    up = df["high"].diff()
    down = -df["low"].diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    tr_s = tr.ewm(alpha=1 / 14, adjust=False).mean()
    df["plus_di"] = 100 * plus_dm.ewm(alpha=1 / 14, adjust=False).mean() / tr_s
    df["minus_di"] = 100 * minus_dm.ewm(alpha=1 / 14, adjust=False).mean() / tr_s
    di_sum = (df["plus_di"] + df["minus_di"]).replace(0, float("nan"))
    dx = 100 * (df["plus_di"] - df["minus_di"]).abs() / di_sum
    df["adx"] = dx.ewm(alpha=1 / 14, adjust=False).mean()
    return df


def get_df(symbol):
    """WebSocket onbelleginden DataFrame (REST istegi YOK)."""
    raw = to_raw(symbol)
    with ws_lock:
        rows = [r[:] for r in klines.get(raw, [])]
    if len(rows) < 2:
        return None
    df = pd.DataFrame(rows, columns=COLS)
    if not rows[-1][6]:          # olusmakta olan mumu at, sadece kapanmislar kalsin
        df = df.iloc[:-1]
    return calculate_indicators(df.copy())


def fetch_df_rest(symbol, timeframe, limit=100):
    """REST ile mum (sadece yedek mod, 1s teyidi ve pozisyon sahiplenme icin)."""
    ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.iloc[:-1].copy()
    return calculate_indicators(df)


# ---------------- TREND PUANLAMA ----------------
def evaluate(df):
    if len(df) < 60:
        return None, "veri_az"
    last = df.iloc[-1]
    needed = ["adx", "rsi", "atr", "plus_di", "minus_di", "ema_fast", "ema_slow"]
    if any(pd.isna(last[k]) for k in needed) or pd.isna(df["adx"].iloc[-4]):
        return None, "veri_eksik"

    close = float(last["close"])
    atr = float(last["atr"])
    adx = float(last["adx"])
    rsi = float(last["rsi"])
    if close <= 0 or atr <= 0:
        return None, "veri_eksik"

    if atr / close < MIN_ATR_PCT:
        return None, "durgun"
    if adx < ADX_MIN:
        return None, "adx_dusuk"

    ef, es = float(last["ema_fast"]), float(last["ema_slow"])
    if ef > es and last["plus_di"] > last["minus_di"] and close > es:
        side = "long"
    elif ef < es and last["minus_di"] > last["plus_di"] and close < es:
        side = "short"
    else:
        return None, "yon_belirsiz"

    if side == "long" and rsi > RSI_MAX_LONG:
        return None, "rsi_asiri"
    if side == "short" and rsi < RSI_MIN_SHORT:
        return None, "rsi_asiri"
    if abs(close - es) / atr > MAX_EXT_ATR:
        return None, "gec_kalmis"

    vol_recent = float(df["volume"].iloc[-3:].mean())
    vol_base = float(df["volume"].iloc[-23:-3].mean())
    rv = vol_recent / vol_base if vol_base > 0 else 0.0
    if rv < MIN_REL_VOL:
        return None, "hacim_dusuk"

    adx_pts = 25 * clamp((adx - ADX_MIN) / (40 - ADX_MIN), 0, 1)
    rising_pts = 10 if adx > float(df["adx"].iloc[-4]) else 0
    spread_pts = 10 * clamp(abs(ef - es) / atr, 0, 1)
    vol_pts = 20 * clamp((rv - 0.8) / 1.2, 0, 1)
    if side == "long":
        rsi_pts = 10 if 50 <= rsi <= 68 else (5 if 45 <= rsi <= RSI_MAX_LONG else 0)
        breakout = close > float(df["high"].iloc[-1 - BREAKOUT_BARS:-1].max())
    else:
        rsi_pts = 10 if 32 <= rsi <= 50 else (5 if RSI_MIN_SHORT <= rsi <= 55 else 0)
        breakout = close < float(df["low"].iloc[-1 - BREAKOUT_BARS:-1].min())
    breakout_pts = 10 if breakout else 0

    score = adx_pts + rising_pts + spread_pts + vol_pts + rsi_pts + breakout_pts
    return {
        "side": side, "score": score, "atr": atr, "price": close,
        "adx": adx, "rv": rv, "rsi": rsi, "htf": "-",
    }, "ok"


htf_cache = {}   # symbol -> (zaman, df)


def htf_alignment(symbol, side):
    now = time.time()
    cached = htf_cache.get(symbol)
    if cached and now - cached[0] < HTF_CACHE_SECONDS:
        df = cached[1]
    else:
        wait_if_banned()
        df = fetch_df_rest(symbol, HTF)
        htf_cache[symbol] = (now, df)
    if len(df) < 30:
        return "belirsiz"
    last = df.iloc[-1]
    up = last["ema_fast"] > last["ema_slow"] and last["close"] > last["ema_slow"]
    down = last["ema_fast"] < last["ema_slow"] and last["close"] < last["ema_slow"]
    if side == "long":
        return "ok" if up else ("ters" if down else "belirsiz")
    return "ok" if down else ("ters" if up else "belirsiz")


def scan_market(skip):
    symbols = get_dynamic_symbols()
    ws_ok = ws_healthy()
    log.info(f"Hacim araligindaki coin sayisi: {len(symbols)} | "
             f"veri kaynagi: {'WebSocket' if ws_ok else 'REST YEDEK'}")
    if not symbols:
        return []

    if ws_ok:
        prepare_data(symbols, skip)
    else:
        symbols = symbols[:FALLBACK_MAX_COINS]
        log.warning(f"⚠️ WebSocket hazir degil: REST yedek modu, sadece ilk {len(symbols)} coin, yavas taranacak.")

    reasons = {}
    stage1 = []

    for symbol in symbols:
        if symbol in skip:
            continue
        try:
            if ws_ok:
                if not is_fresh(to_raw(symbol)):
                    reasons["veri_yok"] = reasons.get("veri_yok", 0) + 1
                    continue
                df = get_df(symbol)
                if df is None:
                    reasons["veri_yok"] = reasons.get("veri_yok", 0) + 1
                    continue
            else:
                wait_if_banned()
                df = fetch_df_rest(symbol, TIMEFRAME)
                time.sleep(FALLBACK_SLEEP)
        except Exception as e:
            log.error(f"{symbol} veri hatasi: {e}")
            if note_ban(e):
                return []
            continue
        cand, reason = evaluate(df)
        if cand is None:
            reasons[reason] = reasons.get(reason, 0) + 1
            continue
        if (cand["side"] == "long" and not ALLOW_LONG) or (cand["side"] == "short" and not ALLOW_SHORT):
            continue
        cand["symbol"] = symbol
        stage1.append(cand)

    stage1.sort(key=lambda c: c["score"], reverse=True)
    final = []
    for cand in stage1[:HTF_CHECK_TOP]:
        try:
            htf = htf_alignment(cand["symbol"], cand["side"])
        except Exception as e:
            log.error(f"{cand['symbol']} ust zaman hatasi: {e}")
            if note_ban(e):
                break
            continue
        if htf == "ters":
            reasons["ust_trend_ters"] = reasons.get("ust_trend_ters", 0) + 1
            continue
        cand["score"] += HTF_PTS if htf == "ok" else HTF_PTS / 2
        cand["htf"] = htf
        final.append(cand)

    final.sort(key=lambda c: c["score"], reverse=True)
    log.info(f"Elenenler: {reasons} | Trend adayi: {len(stage1)} | Ust trend teyidi gecen: {len(final)}")
    for c in final[:5]:
        log.info(f"  ⭐ {c['symbol']} {c['side'].upper()} puan={c['score']:.0f} adx={c['adx']:.0f} "
                 f"hacimx={c['rv']:.1f} rsi={c['rsi']:.0f} 1s={c['htf']}")
    return [c for c in final if c["score"] >= MIN_TREND_SCORE]


# ---------------- KORUMA EMIRLERI ----------------
def place_protection(symbol, entry, amount, atr, side="long"):
    exit_side = "sell" if side == "long" else "buy"
    if side == "long":
        sl_raw = entry - SL_ATR * atr
        tp_raw = entry + TP1_ATR * atr
    else:
        sl_raw = entry + SL_ATR * atr
        tp_raw = entry - TP1_ATR * atr
    sl_price = float(ex.price_to_precision(symbol, sl_raw))
    tp_price = float(ex.price_to_precision(symbol, tp_raw))
    tp_amount = float(ex.amount_to_precision(symbol, amount * TP1_CLOSE_PCT))

    try:
        sl_order = ex.create_order(symbol, "STOP_MARKET", exit_side, amount, None,
                                   {"stopPrice": sl_price, "reduceOnly": True})
        log.info(f"🛑 SL kondu: {symbol} @ {sl_price}")
    except Exception as e:
        log.error(f"{symbol} SL konamadi: {e}")
        return None

    if tp_amount > 0:
        try:
            ex.create_order(symbol, "TAKE_PROFIT_MARKET", exit_side, tp_amount, None,
                            {"stopPrice": tp_price, "reduceOnly": True})
            log.info(f"🎯 TP kondu (%{TP1_CLOSE_PCT*100:.0f} kapanir): {symbol} @ {tp_price}")
        except Exception as e:
            log.error(f"{symbol} TP konamadi: {e}")
    return {"sl_id": sl_order.get("id"), "sl_price": sl_price}


def cancel_safe(order_id, symbol):
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


def find_stop_order(symbol, exit_side):
    best = (None, None)
    for params in ({}, {"trigger": True}):
        try:
            orders = ex.fetch_open_orders(symbol, params=params)
        except Exception:
            continue
        for o in orders:
            typ = str(o.get("type", "")).lower()
            if o.get("side") != exit_side or "stop" not in typ:
                continue
            price = o.get("stopPrice") or o.get("triggerPrice") or (o.get("info") or {}).get("stopPrice")
            try:
                price = float(price)
            except (TypeError, ValueError):
                continue
            if price <= 0:
                continue
            if best[0] is None:
                best = (o.get("id"), price)
            elif exit_side == "sell" and price > best[1]:
                best = (o.get("id"), price)
            elif exit_side == "buy" and price < best[1]:
                best = (o.get("id"), price)
    return best


def adopt_position(symbol, pos):
    side = pos.get("side")
    size = float(pos.get("contracts") or 0)
    entry = float(pos.get("entryPrice") or 0)
    if side not in ("long", "short") or size <= 0 or entry <= 0:
        return False
    df = fetch_df_rest(symbol, TIMEFRAME)
    atr = float(df.iloc[-1]["atr"]) if len(df) else 0.0
    if not atr or atr != atr:
        return False
    price = float(ex.fetch_ticker(symbol)["last"])
    exit_side = "sell" if side == "long" else "buy"
    sl_id, sl_price = find_stop_order(symbol, exit_side)
    if sl_id is None:
        raw = price - SL_ATR * atr if side == "long" else price + SL_ATR * atr
        sl_price = float(ex.price_to_precision(symbol, raw))
        amount = float(ex.amount_to_precision(symbol, size))
        order = ex.create_order(symbol, "STOP_MARKET", exit_side, amount, None,
                                {"stopPrice": sl_price, "reduceOnly": True})
        sl_id = order.get("id")
        log.warning(f"🛟 {symbol} stop'suz pozisyon bulundu, SL kondu @ {sl_price}")
    ext = max(entry, price) if side == "long" else min(entry, price)
    with trail_lock:
        trail_state[symbol] = {
            "side": side, "atr": atr, "ext": ext, "entry": entry, "sl_price": sl_price,
            "sl_id": sl_id, "amount": float(ex.amount_to_precision(symbol, size)),
        }
        save_state()
    log.info(f"🤝 {symbol} ({side}) pozisyonu sahiplenildi, gezen stop surecek. SL: {sl_price}")
    return True


# ---------------- GEZEN STOP (TRAILING) ----------------
def trail_multiplier(profit_atr):
    for limit, mult in TRAIL_TIERS:
        if profit_atr < limit:
            return mult
    return TRAIL_TIGHT_ATR


def update_trailing_stop_logic(symbol, price):
    """Anlik fiyatla gezen stopu hesaplar. REST'e SADECE stop gercekten tasinacaksa gider."""
    st = trail_state.get(symbol)
    if not st:
        return

    is_long = st["side"] == "long"
    exit_side = "sell" if is_long else "buy"
    entry = float(st["entry"])
    atr = st["atr"]

    if is_long:
        if price <= st["ext"]:
            return
        st["ext"] = max(st["ext"], price)
        profit_atr = (st["ext"] - entry) / atr
        mult = trail_multiplier(profit_atr)
        raw_sl = st["ext"] - mult * atr
        if profit_atr >= BREAKEVEN_AT_ATR:
            raw_sl = max(raw_sl, entry)
        new_sl = float(ex.price_to_precision(symbol, raw_sl))
        move_ok = new_sl > st["sl_price"] + 0.1 * atr and new_sl < price
    else:
        if price >= st["ext"]:
            return
        st["ext"] = min(st["ext"], price)
        profit_atr = (entry - st["ext"]) / atr
        mult = trail_multiplier(profit_atr)
        raw_sl = st["ext"] + mult * atr
        if profit_atr >= BREAKEVEN_AT_ATR:
            raw_sl = min(raw_sl, entry)
        new_sl = float(ex.price_to_precision(symbol, raw_sl))
        move_ok = new_sl < st["sl_price"] - 0.1 * atr and new_sl > price

    if move_ok:
        wait_if_banned()
        try:
            amount_now = st.get("amount")
            new_order = ex.create_order(symbol, "STOP_MARKET", exit_side, amount_now, None,
                                        {"stopPrice": new_sl, "reduceOnly": True})
            cancel_safe(st["sl_id"], symbol)
            log.info(f"📈 {symbol} gezen stop: {st['sl_price']} -> {new_sl} (kar {profit_atr:.1f} ATR)")
            st["sl_id"] = new_order.get("id")
            st["sl_price"] = new_sl
            with trail_lock:
                save_state()
        except Exception as e:
            log.error(f"{symbol} gezen stop guncelleme hatasi: {e}")
            note_ban(e)


def sync_positions(symbols):
    """TEK REST istegiyle: kapanan pozisyonlari temizle, kalan miktari guncelle."""
    wait_if_banned()
    positions = ex.fetch_positions()
    open_map = {}
    for p in positions:
        c = float(p.get("contracts") or 0)
        if c > 0:
            open_map[p["symbol"]] = c
    for symbol in symbols:
        if symbol in open_map:
            with trail_lock:
                st = trail_state.get(symbol)
                if st:
                    st["amount"] = float(ex.amount_to_precision(symbol, open_map[symbol]))
        else:
            cancel_all_safe(symbol)
            with trail_lock:
                trail_state.pop(symbol, None)
                cooldown[symbol] = time.time() + COOLDOWN_SECONDS
                save_state()
            log.info(f"{symbol} kapandi, {COOLDOWN_SECONDS // 60} dk bekleme basladi.")


def trail_loop():
    log.info("Gezen stop dongusu basladi (WS fiyati, REST sadece stop tasirken).")

    # Ilk acilista mevcut pozisyonlari sahiplen
    try:
        wait_if_banned()
        for p in ex.fetch_positions():
            c = float(p.get("contracts") or 0)
            if c > 0:
                symbol = p["symbol"]
                with trail_lock:
                    known = symbol in trail_state
                if not known:
                    adopt_position(symbol, p)
    except Exception as e:
        log.error(f"Baslangic pozisyon sahiplenme hatasi: {e}")
        note_ban(e)

    last_sync = time.time()
    while True:
        interval = TRAIL_TICK_SECONDS
        try:
            wait_if_banned()
            with trail_lock:
                symbols = list(trail_state.keys())

            if symbols:
                prices = {}
                if ws_healthy():
                    with ws_lock:
                        for s in symbols:
                            t = tickers.get(to_raw(s))
                            if t:
                                prices[s] = t["price"]
                else:
                    # WS yok: yedek olarak tek istekle fiyatlar, yavas aralikla
                    interval = TRAIL_CHECK_SECONDS
                    tk = ex.fetch_tickers(symbols)
                    for s in symbols:
                        if s in tk and tk[s].get("last"):
                            prices[s] = float(tk[s]["last"])

                for symbol, price in prices.items():
                    try:
                        update_trailing_stop_logic(symbol, price)
                    except Exception as e:
                        log.error(f"{symbol} gezen stop hatasi: {e}")
                        note_ban(e)

                if time.time() - last_sync >= POSITION_SYNC_SECONDS:
                    last_sync = time.time()
                    sync_positions(symbols)
        except Exception as e:
            log.error(f"Gezen stop dongu hatasi: {e}")
            note_ban(e)
            interval = TRAIL_CHECK_SECONDS

        time.sleep(interval)


# ---------------- ISLEM ACMA ----------------
def get_open_symbols():
    positions = ex.fetch_positions()
    return [p["symbol"] for p in positions if float(p.get("contracts") or 0) > 0]


def daily_loss_hit():
    try:
        total = float(ex.fetch_balance()["USDT"]["total"])
    except Exception as e:
        log.error(f"Bakiye cekilemedi: {e}")
        note_ban(e)
        return False
    today = time.strftime("%Y-%m-%d", time.gmtime())
    with trail_lock:
        if day_state.get("day") != today or not day_state.get("start"):
            day_state["day"] = today
            day_state["start"] = total
            save_state()
        start = float(day_state["start"])
    loss_pct = (start - total) / start * 100 if start > 0 else 0.0
    if loss_pct >= DAILY_LOSS_LIMIT_PCT:
        log.warning(f"🚫 Gunluk zarar limiti doldu (%{loss_pct:.1f}). Gun sonuna kadar yeni islem yok.")
        return True
    return False


def open_trade(cand, open_symbols):
    symbol, side, atr = cand["symbol"], cand["side"], cand["atr"]
    lev = LEV_STRONG if cand["score"] >= STRONG_SCORE else LEV_NORMAL
    with open_lock:
        opening_symbols.add(symbol)
    try:
        try:
            ex.set_margin_mode("isolated", symbol)
        except Exception:
            pass
        try:
            ex.set_leverage(lev, symbol)
        except Exception:
            pass

        usdt_free = float(ex.fetch_balance()["USDT"]["free"])
        amount = float(ex.amount_to_precision(symbol, usdt_free * MARGIN_PCT * lev / cand["price"]))
        if amount <= 0:
            return False

        if side == "long":
            order = ex.create_market_buy_order(symbol, amount)
        else:
            order = ex.create_market_sell_order(symbol, amount)
        entry = float(order.get("average") or order.get("price") or cand["price"])
        log.info(f"🚀 ISLEM ACILDI ({side.upper()}): {symbol} | Puan: {cand['score']:.0f} | "
                 f"Miktar: {amount} | Kaldirac: {lev}x | Giris: {entry}")
        open_symbols.append(symbol)

        prot = place_protection(symbol, entry, amount, atr, side)
        if prot:
            with trail_lock:
                trail_state[symbol] = {
                    "side": side, "atr": atr, "ext": entry, "entry": entry,
                    "sl_price": prot["sl_price"], "sl_id": prot["sl_id"], "amount": amount,
                }
                save_state()
        else:
            log.error(f"{symbol} SL yok, pozisyon kapatiliyor!")
            close_side = "sell" if side == "long" else "buy"
            ex.create_order(symbol, "market", close_side, amount, None, {"reduceOnly": True})
            open_symbols.remove(symbol)
        return True
    finally:
        with open_lock:
            opening_symbols.discard(symbol)


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
                continue

            if len(open_symbols) >= MAX_OPEN:
                log.info(f"Pozisyon limiti dolu ({len(open_symbols)}/{MAX_OPEN}), yeni tarama yapilmadi.")
                time.sleep(LOOP_SECONDS)
                continue

            if daily_loss_hit():
                time.sleep(LOOP_SECONDS)
                continue

            now = time.time()
            with trail_lock:
                for s in [s for s, t in cooldown.items() if t <= now]:
                    cooldown.pop(s, None)
                skip = set(open_symbols) | set(cooldown.keys())

            candidates = scan_market(skip)
            if not candidates:
                log.info("Bu turda esigi gecen trend yok, islem acilmadi.")

            for cand in candidates:
                if len(open_symbols) >= MAX_OPEN:
                    break
                if cand["symbol"] in open_symbols:
                    continue
                try:
                    open_trade(cand, open_symbols)
                    time.sleep(2)
                except Exception as inner_err:
                    log.error(f"{cand['symbol']} islem hatasi: {inner_err}")
                    if note_ban(inner_err):
                        break

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
            threading.Thread(target=ws_thread, daemon=True).start()
            threading.Thread(target=bot_loop, daemon=True).start()
            threading.Thread(target=trail_loop, daemon=True).start()


start_bot_once()


@app.route("/")
def index():
    return jsonify({
        "status": "running",
        "websocket": ws_healthy(),
        "izlenen_coin": len(subscribed),
        "takip_edilen": list(trail_state.keys()),
        "bekleyen_coin": len(cooldown),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
