"""
Binance USDT-M Futures botu (7/24) - WEBSOCKET VERI + REST SADECE ISLEM  (SADE SURUM)

STRATEJI (sade cekirdek):
  1) ON ELEME: tum coinlerin fiyat+hacmi !miniTicker@arr'dan izlenir; son 15 dk'da en hizli hareket eden ilk PRESCREEN_TOP
     coin "aday" olur. Detayli analiz sadece adaylara yapilir.
  2) YON: fiyat EMA50 ustundeyse sadece LONG, altindaysa sadece SHORT.
  3) TETIK: EMA50 yonunde TAZE EMA9/21 kesisimi (son EMA_CROSS_BARS mum) + EMA21 egimi yonde.
  4) ZORUNLU HACIM SICRAMASI (>= VOL_SPIKE_MIN) ve sicrama yonle uyumlu mumda olmali.
  5) GEC KALMA FILTRELERI: EMA21'den uzaklik, 24s hareket, dikey mum, tetikten bu yana ATR, 1s pump.
  6) ONAYLAR (hepsi WebSocket, OI haric):
       - CVD (kumulatif taker alim-satim farki): mum akisindaki taker-alim hacminden (kline 'V' alani) hesaplanir.
         CVD yonun tersineyse GIRILMEZ; guclu destekliyorsa +1 onay.
       - FUNDING: !markPrice@arr akisindan ('r' alani). Kalabalik tarafa (long'da cok yuksek / short'ta cok negatif funding) girilmez;
         sakinse +1 onay.
       - OPEN INTEREST: Binance'te OI icin WebSocket akisi YOK. Sadece en iyi OI_CHECK_TOP adaya, onbellekli, ban korumali tek REST istegi.
         OI artiyorsa +1 onay, belirgin dusuyorsa elenir. Veri gelmezse sonuc degismez (fail-open).
  7) Adaylar (onay sayisi, hacim sicramasi) sirasina gore dizilir; en az MIN_CONFIRM onay gerekir.

ERKEN TETIKLER (olusan mum, en az 5 dk dolmus, hacim sureye oranlanir):
  ema_canli : olusan mumda yeni EMA9/21 kesisimi (EMA50 yonunde)
  kirilim   : son 20 mumun tepesi/dibi kirildi, fiyat seviyeden en fazla 0.5 ATR uzakta
  donus     : son 20 mumun dibi/tepesi temizlendi ve fiyat geri dondu (EMA50 / EMA egimi / 1s filtresi aranmaz)
  Koruma: 3 tarama tutunma, igne filtresi, guclu CVD, yarim risk.
  3'te 2: super hacim, OI artiyor, funding sakin. (donus: super hacim VE sakin funding, OI aranmaz)
  Mevcut "ema" tetigi (kapanmis mum) eskisi gibi calisir.

KAGIT MODU (PAPER_MODE = True): emir verilmez; her sinyalin stop mu kar-al mi once geldigi tetik bazinda sayilir (/paper).

YONETIM: ATR stop-loss (marjinde en fazla %45 zarar = 15x'te %3 fiyat), kismi kar al (stop mesafesinin 1.5 kati karda pozisyonun %50'si), daralan ATR gezen stop,
  stop sonrasi bekleme, gunluk zarar limiti, TEK POZISYON, 15x kaldirac (olmazsa 10/8/5/3x), risk bazli pozisyon boyutu,
  BTC filtresi, islem gunlugu (trades.csv + /journal), kagit modu (TRADING_ENABLED=0), yedek stop, kar-al garantisi (tam 1 TP),
  rotasyon (bayat pozisyonu kapat, daha onayli adaya gec), ban korumasi.
  /scan adresi: hareketli coinler, adaylar, acik pozisyonlar.

Gerekli paketler (requirements.txt): ccxt, pandas, numpy, flask, gunicorn  (websocket icin EK PAKET GEREKMEZ)
"""
import os
import re
import sys
import csv
import errno
import json
import time
import logging
import threading
import socket
import ssl
import base64
import hashlib
import struct
from collections import deque
from urllib.parse import urlparse, unquote

import ccxt
import numpy as np
import pandas as pd
from flask import Flask, jsonify

app = Flask(__name__)

# ===================== LOG =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()],
    force=True,
)
log = logging.getLogger("bot")

# ===================== AYARLAR =====================
TIMEFRAME = "15m"          # ana zaman dilimi
HTF = "1h"                 # ust zaman dilimi (yon teyidi, sadece filtre)
LOOP_SECONDS = 30          # tarama araligi (sadece WebSocket verisi)
ACCOUNT_CHECK_SECONDS = 180   # pozisyon/bakiye REST kontrolu ve rotasyon araligi (istek sayisi artmasin)
SUMMARY_LOG_SECONDS = 180  # tarama ozeti en fazla bu aralikla loga yazilir
FAILED_OPEN_COOLDOWN = 10 * 60   # islem acilamayan coin bu sure tekrar denenmez

# --- Coin secimi ---
MIN_VOLUME = 20_000_000
MAX_VOLUME = 1_500_000_000
MAX_COIN_PRICE = 500.0

# --- ON ELEME (hizli coin bulucu, sadece WebSocket verisi) ---
PRESCREEN_WINDOW = 15 * 60       # fiyat/hacim degisimine bakilan pencere (sn)
PRESCREEN_WARMUP = 12 * 60       # en az bu kadar fiyat gecmisi birikmeden on eleme calismaz (acilis isinmasi)
PRESCREEN_TOP = 100              # her turda en hareketli kac coin detayli analize girsin
PRESCREEN_MIN_MOVE_PCT = 0.3     # pencerede fiyat en az %0,3 oynamamissa aday olmaz
HIST_SAMPLE_SECONDS = 10         # fiyat gecmisi ornekleme sikligi (sn)
MAX_MOVE_1H = 0.15               # son 1 saatte %15'ten fazla oynamis coine girme (pump'a gec kalma)

# --- Sade cekirdek ---
HTF_CHECK_TOP = 10         # ust zaman dilimi teyidi en iyi kac coine yapilsin (ters ise elenir)
MIN_ATR_PCT = 0.005        # ATR / fiyat en az %0,5 (cok durgun coinde komisyon yer)
ATR_REF_BARS = 96          # "durgun mu" kontrolunde bakilan uzun vadeli ATR ortalamasi (96 mum = 1 gun)
ATR_FLOOR_RATIO = 0.6      # stop icin kullanilan ATR en az (1 gunluk ortalama ATR) x bu kadar olsun. 0 = kapali

# EMA yonu ve taze kesisim
EMA_CROSS_BARS = 4         # EMA9/21 kesisimi en fazla kac mum once olmus olabilir
EMA_SLOPE_BARS = 3         # EMA21 egimi icin kac mum geriye bakilir
EMA_TREND_PERIOD = 50      # yon filtresi: fiyat bunun ustunde = long, altinda = short

# Zorunlu hacim sicramasi
VOL_SPIKE_MIN = 1.5        # son 2 mumun en yuksek hacmi / onceki 20 mum ortalamasi en az bu olsun

# Gec kalma filtreleri (anti-chase)
ENTRY_MAX_EXT_ATR = 3.0    # fiyat EMA21'den bu kadar ATR'den fazla uzaksa girme
MAX_MOVE_24H = 0.30        # long: son 24s dibinden, short: son 24s zirvesinden %30'dan fazla gitmisse girme
MOVE_24H_BARS = 96         # 24 saat = 96 adet 15 dk mum
MAX_CANDLE_ATR = 3.0       # son kapanmis mumun (yuksek-dusuk) araligi bu kadar ATR'den buyukse "dikey mum": girme
MAX_SINCE_TRIGGER_ATR = 3.0   # EMA kesisiminden bu yana fiyat bu kadar ATR gittiyse tetik eskimis sayilir

# --- ONAYLAR: CVD / FUNDING / OI ---
# CVD: kline akisindaki taker-alim hacminden (V) hesaplanir. Mum CVD farki = taker alim - taker satim = 2*V - hacim.
CVD_BARS = 4               # son kac mumun CVD'sine bakilir
CVD_MIN = 0.03             # CVD orani (toplam CVD farki / toplam hacim) yon tarafinda en az bu olmali; altindaysa girilmez
CVD_STRONG = 0.10          # bu ve ustu = guclu CVD teyidi (+1 onay)
# FUNDING: !markPrice@arr akisindan. Oran olarak (0.0001 = %0,01).
FUNDING_ENABLED = True
FUNDING_MAX = 0.0005       # long'da funding > bu (%0,05) ya da short'ta funding < -bu ise kalabalik tarafa girilmez
FUNDING_CALM = 0.0001      # long'da funding <= bu, short'ta >= -bu ise "sakin" sayilir (+1 onay)
FUNDING_STALE_SECONDS = 300   # funding verisi bu kadar sn guncellenmediyse yok sayilir (fail-open)
# OPEN INTEREST: WebSocket akisi yok -> sadece en iyi adaylara, onbellekli tek REST istegi, fail-open.
OI_ENABLED = True
OI_PERIOD = "15m"          # OI gecmisi periyodu
OI_BARS = 5                # 5 nokta = son ~1 saatlik degisim
OI_RISE_PCT = 0.5          # OI bu kadar % artmissa +1 onay
OI_DROP_PCT = 1.0          # OI bu kadar % dusmusse aday elenir
OI_CHECK_TOP = 3           # en iyi kac aday icin OI sorulsun
OI_CACHE_SECONDS = 300
MIN_CONFIRM = 1            # girmek icin gereken en az onay sayisi (CVD guclu / funding sakin / OI artiyor). 0 = onay sart degil

# --- ERKEN TETIKLER ---
EARLY_ENABLED = True
LIVE_MIN_ELAPSED = 5 * 60      # olusan mumun en az bu kadari dolmus olmali (sn)
LIVE_STALE_SECONDS = 20        # olusan mum WS'ten bu kadar sn guncellenmediyse kullanilmaz
BREAK_BARS = 20                # kirilim/donus icin bakilan kapanmis mum sayisi
BREAK_MAX_ATR = 0.5            # kirilimda fiyat seviyeden en fazla bu kadar ATR uzakta
REV_VOL_MIN = 2.0              # donus icin en az hacim kati
EARLY_CVD_MIN = 0.10           # erken tetiklerde zorunlu CVD (donuste sadece olusan mum)
WICK_ZONE = 0.30               # long: fiyat mumun ust %30'unda, short: alt %30'unda
HOLD_SCANS = 3                 # sinyal arka arkaya bu kadar taramada gecerli olmali
HOLD_MIN_GAP = 20              # iki sayim arasi en az (sn)
HOLD_MAX_GAP = 90              # bu kadar sn gorulmezse sayac sifirlanir
SUPER_VOL = 2.5                # "super hacim" esigi
EARLY_NEED = 2                 # super hacim / OI artiyor / funding sakin: en az kac tanesi
EARLY_RISK_FACTOR = 0.5        # erken tetikte risk carpani (%1 -> %0.5)

# --- KAGIT MODU SAYACI ---
PAPER_MODE = False             # True: emir YOK, sadece sanal sonuc sayilir. Gercek islem icin False yap.
PAPER_FEE_PCT = 0.10           # gidis-donus komisyon (fiyatin %'si); kayma ve funding odemesi dahil degil
PAPER_MAX_HOURS = 24           # bu surede stop/kar-al gelmezse o anki fiyattan kapanir
PAPER_MAX_GAP = 120            # fiyat takibinde bu kadar sn bosluk olursa sinyal sayilmaz
PAPER_MIN_SAMPLE = 50
PAPER_STATE_FILE = "paper_state.json"

# --- Islem ---
LEVERAGE = 15
LEV_FALLBACKS = (10, 8, 5, 3)   # coin 15x'i desteklemezse sirayla bu kaldiraclar denenir
RISK_PCT = 1.0             # her islemde stop olursa hesabin (toplam bakiye) yaklasik en fazla %1'i gider
MARGIN_PCT = 0.30          # ...ama marjin hicbir zaman bos bakiyenin %30'unu gecmez (ust sinir)
MAX_OPEN = 1               # ayni anda tek pozisyon
SL_ATR = 2.0               # Stop-loss: giris -/+ 2 x ATR
SL_MAX_MARGIN_PCT = 45.0   # ...ama stop marjinde en fazla bu kadar zarar ettirecek yerde olur
TP1_RR = 1.5               # Ilk kar al mesafesi = stop mesafesi x 1.5 (2 ATR stopta 3 ATR karda)
TP1_CLOSE_PCT = 0.50       # ilk kar alda pozisyonun %50'si kapanir, kalan %50 gezen stopla gider

# --- DARALAN gezen stop (ATR'ye bagli) ---
TRAIL_START_ATR = 1.0      # kar bu kadar ATR olmadan gezen stop calismaz
TRAIL_TIERS = ((3.0, 1.5),)   # kar < 3 ATR: 1.5 x ATR geriden takip
TRAIL_TIGHT_ATR = 1.0      # kar 3 ATR'yi gecince en dar takip (1 x ATR)
TRAIL_MIN_DIST_PCT = 1.0   # takip mesafesi hicbir zaman fiyatin %1'inden az olmaz
TRAIL_MIN_STEP_ATR = 0.25  # stop en az bu kadar ATR ilerlemeden REST'e gidilmez
TRAIL_MIN_INTERVAL = 15    # ayni coinde iki stop guncellemesi arasi en az bu kadar sn
BREAKEVEN_AT_ATR = 1.5     # kar bu kadar ATR olunca stop en az girise cekilir
FEE_BUFFER_PCT = 0.12      # stop girise cekilirken eklenen komisyon payi (fiyatin %'si)

COOLDOWN_SECONDS = 2 * 3600   # bir coin kapaninca bu sure tekrar girilmez
DAILY_LOSS_LIMIT_PCT = 10.0   # gun basi bakiyeye gore bu kadar zarar olursa gun sonuna kadar yeni islem yok
LOSS_LOG_EVERY = 3600         # "limit doldu" uyarisi en fazla bu kadar sn'de bir loga yazilir
BACKUP_STOP_GAP_PCT = 0.5     # fiyat stopu bu kadar (%) gecip pozisyon hala aciksa bot kendisi kapatir
ALLOW_LONG = True
ALLOW_SHORT = True
STATE_FILE = "bot_state.json"
TESTNET = False

# PAPER_MODE True ise ya da ortam degiskeni TRADING_ENABLED=0 ise bot yeni emir vermez (kagit modu).
TRADING_ENABLED = (not PAPER_MODE) and os.getenv("TRADING_ENABLED", "1") != "0"

# --- BTC FILTRESI: piyasa yonune ters islem acma ---
BTC_FILTER_ENABLED = True
BTC_WINDOW = 15 * 60       # BTC degisimine bakilan pencere (sn)
BTC_MIN_SPAN = 8 * 60      # pencerede en az bu kadar fiyat gecmisi yoksa filtre calismaz
BTC_BLOCK_PCT = 0.5        # BTC pencerede %0,5'ten fazla dusmusse long, fazla cikmissa short acilmaz

# --- ISLEM GUNLUGU ---
JOURNAL_FILE = "trades.csv"
JOURNAL_FIELDS = ["time", "event", "symbol", "side", "confirm", "lev", "entry", "amount", "sl", "tp",
                  "risk_usdt", "pnl_usdt", "fee_usdt", "net_usdt", "hold_min", "reason"]

# --- ROTASYON: bayat pozisyonu kapat, onayli yeni trende gir ---
ROTATE_ENABLED = True
ROTATE_MIN_AGE_SECONDS = 45 * 60   # pozisyon en az bu kadar suredir acik olmali
ROTATE_PNL_MIN = -6.0              # marjine gore kar/zarar % bu bandin icindeyse "sikismis" sayilir
ROTATE_PNL_MAX = 6.0               # (+6'nin ustu kardadir: gezen stop yonetsin, dokunma)
ROTATE_MIN_CONFIRM = 2             # yeni coinin en az bu kadar onayi olmali
ROTATE_GAP_SECONDS = 20 * 60       # iki rotasyon arasi en az bu kadar sure
ROTATE_MAX_PER_DAY = 6             # gunluk en fazla rotasyon

# --- WebSocket / istek ayarlari ---
# Fiyat, funding ve mum akislari /market altinda.
WS_URLS = ["wss://fstream.binance.com/market/stream", "wss://fstream.binance.com/stream"]
WS_IDLE_RECONNECT = 30      # WS bagli ama bu kadar sn hic mesaj gelmezse yeniden baglan
WS_RESUB_MIN_AGE = 150      # yeni aday coin akislari icin yeniden baglanmadan once baglantinin en az bu kadar sn yasi olmali
MAX_STREAMS = 150           # ayni anda en fazla bu kadar coinin mum akisi tutulur
KLINE_KEEP = 200            # her coin icin bellekte tutulan mum sayisi
SEED_LIMIT = 200            # yeni aday icin REST'ten cekilen gecmis mum sayisi
SEED_SLEEP = 1.0            # gecmis mum cekerken coinler arasi bekleme (sn)
SEED_PER_CYCLE = 8          # her tarama turunda en fazla bu kadar coinin gecmis mumu yuklenir
WS_STALE_SECONDS = 45       # WS'ten bu kadar sn veri gelmezse "calismiyor" sayilir
KLINE_STALE_SECONDS = 600   # bir coinin mumu bu kadar sn guncellenmezse bayat sayilir
TRAIL_TICK_SECONDS = 2      # WS aciksa gezen stop kontrol sikligi (REST istegi atmaz)
TRAIL_CHECK_SECONDS = 20    # WS yoksa REST yedek kontrol sikligi
POSITION_SYNC_SECONDS = 60  # pozisyon kapandi mi kontrolu (tek REST istegi)
HTF_CACHE_SECONDS = 600     # (sadece REST yedek modu) 1s verisi bu sure onbellekte tutulur
FALLBACK_MAX_COINS = 30     # WS yoksa REST yedek modunda taranacak en fazla coin
FALLBACK_SLEEP = 0.5        # REST yedek modunda coinler arasi bekleme (sn)
REST_TICKER_MIN_GAP = 120   # WS yoksa coin listesi REST'ten en az bu aralikla cekilir

# ===================== TEKIL KOPYA KILIDI (LOCK) =====================
LOCK_FILE = "bot.lock"
lock_fd = None


def acquire_lock():
    global lock_fd
    lock_fd = open(LOCK_FILE, "a+")
    try:
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(lock_fd.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.seek(0)
        lock_fd.truncate()
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
    except (IOError, OSError) as e:
        if e.errno in (errno.EACCES, errno.EAGAIN):
            log.error("⛔ KRITIK: Botun baska bir kopyasi zaten calisiyor! Bu kopya kapatiliyor.")
            sys.exit(0)
        raise


acquire_lock()

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
ban_strikes = 0            # ust uste kac kez limit yendi (ustel bekleme icin)
last_ban_ts = 0.0
BAN_FILE = "ban_state.json"


def _save_ban():
    """Ban suresini dosyaya yazar: bot yeniden baslarsa banli IP'ye hemen istek atip cezayi uzatmasin."""
    try:
        with open(BAN_FILE, "w") as f:
            json.dump({"until": ban_until}, f)
    except Exception:
        pass


def _load_ban():
    global ban_until
    try:
        with open(BAN_FILE) as f:
            ban_until = max(ban_until, float(json.load(f).get("until", 0)))
        if ban_until > time.time():
            log.warning(f"Onceki ban suruyor, {int(ban_until - time.time())} sn istek atilmayacak.")
    except Exception:
        pass


def note_ban(err):
    global ban_until, ban_strikes, last_ban_ts
    text = str(err)
    now = time.time()
    wait_until = 0.0
    m = re.search(r"banned until (\d{10,})", text)
    if m:
        wait_until = int(m.group(1)) / 1000.0 + 15
    elif "418" in text or "-1003" in text or "429" in text or "Too many requests" in text:
        with ban_lock:
            if now >= ban_until:                  # zaten beklemedeyken gelen gec hatalar sayilmaz
                if now - last_ban_ts > 900:       # 15 dk sakin gectiyse sayac sifirlanir
                    ban_strikes = 0
                ban_strikes = min(ban_strikes + 1, 5)
                last_ban_ts = now
            n = max(ban_strikes, 1)
        wait_until = now + min(120 * 2 ** (n - 1), 1800)    # 2, 4, 8, 16, 30 dk
    elif "restricted location" in text or "451" in text:
        wait_until = now + 600
    if wait_until:
        with ban_lock:
            if wait_until > ban_until:
                ban_until = wait_until
                left = int(ban_until - time.time())
                log.error(f"⛔ Binance yasagi/limit algilandi, {left} sn istek atilmayacak.")
                _save_ban()
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
    """DIKKAT: cagiran taraf trail_lock'u tutuyor olmali."""
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
            st.setdefault("orig_amount", st.get("amount", 0))     # kar-al takibi icin ilk miktar
            st.setdefault("tp_state", "pending")                  # pending: 1 kar-al beklenir | done: hic kar-al kalmamali
            st.setdefault("opened_at", time.time())               # rotasyon icin pozisyon yasi
        trail_state.update(trail)
        log.info(f"State yuklendi: {list(trail_state.keys())}")
    except Exception:
        pass


# ---------------- ISLEM GUNLUGU (journal) ----------------
journal_lock = threading.Lock()


def journal(event, symbol, **kw):
    """Her acilis/kapanisi hem loga ('JURNAL' satiri) hem trades.csv'ye yazar.
    Not: Render ucretsiz planda dosyalar yeniden baslatmada silinebilir; log satirlari ve Binance islem gecmisi yedektir."""
    row = {"time": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), "event": event, "symbol": symbol}
    row.update({k: v for k, v in kw.items() if k in JOURNAL_FIELDS})
    log.info("📒 JURNAL " + json.dumps(row, ensure_ascii=False))
    try:
        with journal_lock:
            new = not os.path.exists(JOURNAL_FILE)
            with open(JOURNAL_FILE, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=JOURNAL_FIELDS, extrasaction="ignore")
                if new:
                    w.writeheader()
                w.writerow(row)
    except Exception as e:
        log.warning(f"Gunluk dosyaya yazilamadi: {e}")


def journal_close(symbol, st, reason):
    """Kapanan pozisyonun gerceklesen kar/zararini borsadan (tek istek) okuyup gunluge yazar. REST: kilidin disinda cagir."""
    if not st:
        return
    opened_at = float(st.get("opened_at") or time.time())
    pnl = fee = net = None
    try:
        wait_if_banned()
        trades = ex.fetch_my_trades(symbol, since=int(opened_at * 1000) - 60_000, limit=100)
        pnl = 0.0
        fee = 0.0
        for t in trades:
            info = t.get("info") or {}
            pnl += float(info.get("realizedPnl") or 0)
            fee += float((t.get("fee") or {}).get("cost") or 0)
        net = pnl - fee
    except Exception as e:
        log.warning(f"{symbol} gunluk icin islem gecmisi okunamadi: {e}")
        note_ban(e)
    journal("close", symbol, side=st.get("side"), lev=st.get("lev"), entry=st.get("entry"),
            pnl_usdt=round(pnl, 4) if pnl is not None else "",
            fee_usdt=round(fee, 4) if fee is not None else "",
            net_usdt=round(net, 4) if net is not None else "",
            hold_min=round((time.time() - opened_at) / 60),
            reason=f"{reason} [tetik={st.get('trigger', 'ema')}]")


# =====================================================================
#     WEBSOCKET VERI KATMANI  (hicbir paket gerektirmez: sadece Python'un kendi kutuphaneleri)
# =====================================================================
class MiniWS:
    """Kucuk RFC 6455 websocket istemcisi (ws:// ve wss://). Sunucuya paket kurmaya gerek birakmaz."""
    GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    MAX_FRAME = 32 * 1024 * 1024

    def __init__(self, url, proxy_url=None, ssl_context=None, connect_timeout=15.0, read_timeout=1.0):
        self.url = url
        self.proxy_url = proxy_url
        self.ssl_context = ssl_context
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.sock = None
        self.buf = bytearray()
        self.send_lock = threading.Lock()
        self.frag_data = bytearray()

    @staticmethod
    def _read_http_head(sock):
        data = bytearray()
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("sunucu HTTP basligi gelmeden baglantiyi kapatti")
            data += chunk
            if len(data) > 65536:
                raise ConnectionError("HTTP basligi cok buyuk")
        head, _, rest = bytes(data).partition(b"\r\n\r\n")
        return head, rest

    @staticmethod
    def _xor(data, mask):
        n = len(data)
        if n == 0:
            return b""
        full = (mask * (n // 4 + 1))[:n]
        return (int.from_bytes(data, "big") ^ int.from_bytes(full, "big")).to_bytes(n, "big")

    def connect(self):
        u = urlparse(self.url)
        secure = u.scheme == "wss"
        host = u.hostname
        port = u.port or (443 if secure else 80)
        path = (u.path or "/") + (("?" + u.query) if u.query else "")

        proxy = urlparse(self.proxy_url) if self.proxy_url else None
        if proxy is not None and (not proxy.hostname or proxy.scheme.startswith("socks")):
            proxy = None        # socks desteklenmiyor: dogrudan baglanmayi dene

        if proxy is not None:
            sock = socket.create_connection((proxy.hostname, proxy.port or 8080), timeout=self.connect_timeout)
            req = f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n"
            if proxy.username:
                cred = f"{unquote(proxy.username)}:{unquote(proxy.password or '')}".encode()
                req += "Proxy-Authorization: Basic " + base64.b64encode(cred).decode() + "\r\n"
            sock.sendall((req + "\r\n").encode())
            try:
                head, _ = self._read_http_head(sock)
            except Exception:
                sock.close()
                raise
            status = head.split(b"\r\n", 1)[0].decode("latin1")
            if len(status.split()) < 2 or status.split()[1] != "200":
                sock.close()
                raise ConnectionError(f"proxy CONNECT reddetti: {status}")
        else:
            sock = socket.create_connection((host, port), timeout=self.connect_timeout)

        try:
            if secure:
                ctx = self.ssl_context or ssl.create_default_context()
                sock = ctx.wrap_socket(sock, server_hostname=host)
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            key = base64.b64encode(os.urandom(16)).decode()
            host_hdr = host if port in (80, 443) else f"{host}:{port}"
            sock.sendall((
                f"GET {path} HTTP/1.1\r\nHost: {host_hdr}\r\nUpgrade: websocket\r\n"
                f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
                f"Sec-WebSocket-Version: 13\r\nUser-Agent: Mozilla/5.0\r\n\r\n"
            ).encode())
            head, rest = self._read_http_head(sock)
            lines = head.decode("latin1").split("\r\n")
            status = lines[0]
            parts = status.split()
            if len(parts) < 2 or parts[1] != "101":
                body = rest[:200].decode("utf-8", "replace").strip()
                raise ConnectionError(f"el sikisma reddedildi: {status} {body}")
            headers = {}
            for ln in lines[1:]:
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    headers[k.strip().lower()] = v.strip()
            expect = base64.b64encode(hashlib.sha1(key.encode() + self.GUID).digest()).decode()
            if headers.get("sec-websocket-accept") != expect:
                raise ConnectionError("el sikisma dogrulanamadi (Sec-WebSocket-Accept)")
            self.buf = bytearray(rest)      # basliktan sonra gelmis olabilecek ilk cerceve baytlari
            sock.settimeout(self.read_timeout)
            self.sock = sock
        except Exception:
            try:
                sock.close()
            except Exception:
                pass
            raise

    def _fill(self):
        try:
            chunk = self.sock.recv(65536)
        except socket.timeout:
            return False
        except ssl.SSLWantReadError:
            return False
        if not chunk:
            raise ConnectionError("sunucu baglantiyi kapatti")
        self.buf += chunk
        return True

    def _frame(self):
        b = self.buf
        if len(b) < 2:
            return None
        b1, b2 = b[0], b[1]
        fin = bool(b1 & 0x80)
        op = b1 & 0x0F
        masked = bool(b2 & 0x80)
        ln = b2 & 0x7F
        pos = 2
        if ln == 126:
            if len(b) < 4:
                return None
            ln = struct.unpack(">H", bytes(b[2:4]))[0]
            pos = 4
        elif ln == 127:
            if len(b) < 10:
                return None
            ln = struct.unpack(">Q", bytes(b[2:10]))[0]
            pos = 10
        mask = b""
        if masked:
            if len(b) < pos + 4:
                return None
            mask = bytes(b[pos:pos + 4])
            pos += 4
        if ln > self.MAX_FRAME:
            raise ConnectionError("cerceve cok buyuk")
        if len(b) < pos + ln:
            return None
        payload = bytes(b[pos:pos + ln])
        del b[:pos + ln]
        if masked:
            payload = self._xor(payload, mask)
        return fin, op, payload

    def _send(self, op, payload=b""):
        n = len(payload)
        hdr = bytearray([0x80 | op])
        if n < 126:
            hdr.append(0x80 | n)
        elif n < 65536:
            hdr.append(0x80 | 126)
            hdr += struct.pack(">H", n)
        else:
            hdr.append(0x80 | 127)
            hdr += struct.pack(">Q", n)
        mask = os.urandom(4)
        data = bytes(hdr) + mask + self._xor(payload, mask)
        with self.send_lock:
            self.sock.sendall(data)

    def send_pong(self):
        self._send(0xA, b"")

    def send_text(self, text):
        self._send(0x1, text.encode())

    def recv(self):
        """Tam bir mesaj (metin) dondurur. Veri yoksa ~1 sn sonra None doner."""
        while True:
            fr = self._frame()
            if fr is None:
                if not self._fill():
                    return None
                continue
            fin, op, payload = fr
            if op == 0x9:               # ping -> pong
                self._send(0xA, payload)
                continue
            if op == 0xA:               # pong
                continue
            if op == 0x8:               # close
                try:
                    self._send(0x8, payload[:2])
                except Exception:
                    pass
                raise ConnectionError("sunucu baglantiyi kapatti (close)")
            if op in (0x1, 0x2):
                self.frag_data = bytearray(payload)
            elif op == 0x0:
                self.frag_data += payload
            else:
                continue
            if fin:
                data = bytes(self.frag_data)
                self.frag_data = bytearray()
                return data.decode("utf-8", "replace")

    def close(self):
        s = self.sock
        self.sock = None
        if s is None:
            return
        try:
            self.sock = s
            self._send(0x8, b"")
        except Exception:
            pass
        self.sock = None
        try:
            s.close()
        except Exception:
            pass


ws_lock = threading.Lock()
tickers = {}          # "BTCUSDT" -> {"price": float, "vol": float(24s quote hacim)}
tick_hist = {}        # "BTCUSDT" -> deque([(zaman, fiyat, 24s hacim), ...]) on eleme icin kisa fiyat gecmisi
funding = {}          # "BTCUSDT" -> funding orani (float, ornek 0.0001 = %0,01)  (!markPrice@arr 'r' alani)
funding_ts = 0.0      # son funding mesaji zamani
hot_ts = {}           # "BTCUSDT" -> en son "hareketli aday" olma zamani (eski akislari temizlemek icin)
tickers_ts = 0.0      # son WS ticker mesaji zamani
first_tick_ts = 0.0   # ilk WS ticker mesaji zamani
klines = {}           # "BTCUSDT" -> [[t, o, h, l, c, v, kapandi_mi, taker_alim_hacmi], ...]
kline_ts = {}         # "BTCUSDT" -> son guncelleme zamani
live_ts = {}          # "BTCUSDT" -> olusan mumun SADECE WebSocket'ten son guncellenme zamani
dirty = set()         # mum boslugu olan (yeniden yuklenmesi gereken) coinler
subscribed = set()    # mum akisi istenen coinler
ws_connected = False
rest_ticker_ts = 0.0
last_scan = {"time": 0.0, "kaynak": "-", "adaylar": [], "hareketli": []}   # /scan adresi icin son tarama sonucu
COLS = ["timestamp", "open", "high", "low", "close", "volume", "closed", "tbv"]
STABLE_BASES = {"USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "AEUR", "EURI", "USD1", "XUSD"}
START_TS = time.time()
HIST_MAXLEN = int((PRESCREEN_WINDOW + 300) / HIST_SAMPLE_SECONDS) + 10


def to_raw(symbol):
    """'BTC/USDT:USDT' -> 'BTCUSDT'"""
    return symbol.split("/")[0] + "USDT"


def to_ccxt(raw):
    """'BTCUSDT' -> 'BTC/USDT:USDT'"""
    return f"{raw[:-4]}/USDT:USDT"


def ws_healthy():
    """Fiyat akisi taze mi? (kisa kopmalarda onbellekteki fiyat kullanilmaya devam eder)"""
    return (time.time() - tickers_ts) < WS_STALE_SECONDS


def wait_for_ws(timeout=30):
    """Bot yeni acildiysa websocket'in veri almasini bekler (ilk tarama bos yere REST yedege dusmesin)."""
    if time.time() - START_TS > 300:
        return
    t0 = time.time()
    while time.time() - t0 < timeout:
        if ws_healthy() and first_tick_ts and time.time() - first_tick_ts >= 8:
            return
        time.sleep(1)


def ws_on_message(text):
    global tickers_ts, first_tick_ts, funding_ts
    try:
        msg = json.loads(text)
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
                    raw = it["s"]
                    price = float(it["c"])
                    vol = float(it["q"])
                except (KeyError, ValueError, TypeError):
                    continue
                tickers[raw] = {"price": price, "vol": vol}
                h = tick_hist.get(raw)
                if h is None:
                    h = tick_hist[raw] = deque(maxlen=HIST_MAXLEN)
                if not h or now - h[-1][0] >= HIST_SAMPLE_SECONDS:
                    h.append((now, price, vol))
            tickers_ts = now
            if not first_tick_ts:
                first_tick_ts = now

    elif stream.startswith("!markPrice@arr"):
        # Mark price akisi: 'r' = guncel funding orani (REST gerekmez)
        now = time.time()
        with ws_lock:
            for it in data:
                try:
                    funding[it["s"]] = float(it["r"])
                except (KeyError, ValueError, TypeError):
                    continue
            funding_ts = now

    elif "@kline_" in stream:
        try:
            k = data["k"]
            raw = data["s"]
            row = [int(k["t"]), float(k["o"]), float(k["h"]), float(k["l"]),
                   float(k["c"]), float(k["v"]), bool(k["x"]), float(k["V"])]   # V = taker alim hacmi (CVD icin)
        except (KeyError, ValueError, TypeError):
            return
        with ws_lock:
            arr = klines.setdefault(raw, [])
            if not arr:
                arr.append(row)
            elif arr[-1][0] == row[0]:
                arr[-1] = row
            elif row[0] > arr[-1][0]:
                if row[0] - arr[-1][0] > TF_MS * 1.5 or not arr[-1][6]:
                    dirty.add(raw)           # mum kacti ya da onceki mumun kapanisi gelmedi (yarim kaldi): REST ile yeniden yuklenecek
                arr[-1][6] = True            # yeni mum geldiyse onceki kapanmistir
                arr.append(row)
                if len(arr) > KLINE_KEEP:
                    del arr[0]
            kline_ts[raw] = time.time()
            if arr[-1][0] == row[0]:
                live_ts[raw] = time.time()


def ws_build_url(base):
    """Binance 'birlesik akis' adresi: fiyat akisi + funding akisi + istenen coinlerin mum akislari."""
    with ws_lock:
        syms = sorted(subscribed)
    streams = ["!miniTicker@arr"]
    if FUNDING_ENABLED:
        streams.append("!markPrice@arr")
    streams += [f"{r.lower()}@kline_{TIMEFRAME}" for r in syms]
    return f"{base}?streams={'/'.join(streams)}", set(syms)


def ws_thread():
    """Surekli calisir; kopunca ya da yeni coin eklenince kendini yeniden baglar."""
    global ws_connected
    idx = 0
    fails = 0
    while True:
        base = WS_URLS[idx % len(WS_URLS)]
        url, conn_syms = ws_build_url(base)
        started = time.time()
        got_data = False
        client = None
        try:
            client = MiniWS(url, proxy_url=PROXY_URL)
            client.connect()
            ws_connected = True
            log.info(f"✅ WebSocket baglandi: {base} (fiyat + funding akisi + {len(conn_syms)} mum akisi)")
            last_rx = time.time()
            last_pong = last_rx
            while True:
                msg = client.recv()
                now = time.time()
                if msg is not None:
                    last_rx = now
                    if not got_data:
                        got_data = True
                        log.info("📡 WebSocket veri akisi basladi.")
                    ws_on_message(msg)
                elif now - last_rx > WS_IDLE_RECONNECT:
                    raise ConnectionError(f"{WS_IDLE_RECONNECT} sn boyunca veri gelmedi")
                if now - last_pong > 60:
                    client.send_pong()       # istenmemis pong'a Binance izin veriyor (baglantiyi canli tutar)
                    last_pong = now
                with ws_lock:
                    missing = len(subscribed - conn_syms)
                if missing and now - started > WS_RESUB_MIN_AGE:
                    log.info(f"WebSocket yeni coin akislari icin yeniden baglaniyor (+{missing} coin).")
                    break
                if now - started > 23 * 3600:      # Binance baglantiyi 24 saatte keser
                    break
        except Exception as e:
            log.error(f"WebSocket hatasi ({base}): {type(e).__name__}: {e}")
        finally:
            ws_connected = False
            if client is not None:
                client.close()

        if got_data:
            fails = 0
            time.sleep(1)
        else:
            fails += 1
            idx += 1                                # diger adresi dene
            time.sleep(min(3 * fails, 30))


# ---------------- Coin listesi (WS onbellekten) ----------------
def symbols_from_cache():
    with ws_lock:
        snap = dict(tickers)
    valid = {}
    for raw, t in snap.items():
        if not raw.endswith("USDT"):
            continue
        if raw[:-4] in STABLE_BASES:
            continue
        if 0 < t["price"] <= MAX_COIN_PRICE and MIN_VOLUME <= t["vol"] <= MAX_VOLUME:
            valid[to_ccxt(raw)] = t["vol"]
    return sorted(valid, key=valid.get, reverse=True)


def is_tradfi(symbol):
    """Hisse/emtia/doviz vadelisi (TradFi-Perps) mi? Bilgi yoksa False doner (yanlis eleme olmasin)."""
    try:
        info = ((ex.markets or {}).get(symbol) or {}).get("info") or {}
        ctype = str(info.get("contractType", "")).upper()
        utype = str(info.get("underlyingType", "")).upper()
        subs = [str(x).upper() for x in (info.get("underlyingSubType") or [])]
        if "TRADIFI" in ctype or "TRADFI" in ctype:
            return True
        if utype in ("EQUITY", "COMMODITY", "TRADFI", "TRADIFI", "FX", "FOREX"):
            return True
        return any("TRADFI" in s or "TRADIFI" in s for s in subs)
    except Exception:
        return False


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


# ---------------- ON ELEME (hizli coin bulucu) ----------------
def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def prescreen_movers(symbols):
    """Sadece WS fiyat gecmisinden (REST YOK) son PRESCREEN_WINDOW sn'de en hizli hareket eden coinleri bulur.
    Isinma bitmediyse None doner; bitmis ama kimse esigi gecmediyse bos liste doner."""
    with ws_lock:
        hist = {s: list(tick_hist.get(to_raw(s), ())) for s in symbols}
    ready = 0
    out = []
    for s in symbols:
        h = hist[s]
        if len(h) < 2:
            continue
        t_now, p_now, v_now = h[-1]
        target = t_now - PRESCREEN_WINDOW
        ref = None
        for smp in h:                           # en eskiden yeniye: pencerenin icindeki ilk ornek
            if smp[0] >= target:
                ref = smp
                break
        if ref is None:
            continue
        span = t_now - ref[0]
        if span < PRESCREEN_WARMUP or ref[1] <= 0:
            continue
        ready += 1
        chg = (p_now / ref[1] - 1.0) * 100.0
        base = v_now * span / 86400.0
        ratio = (v_now - ref[2]) / base if base > 0 else 0.0
        if abs(chg) < PRESCREEN_MIN_MOVE_PCT:
            continue
        heat = abs(chg) * clamp(ratio, 0.5, 3.0)
        out.append({"symbol": s, "chg": chg, "ratio": ratio, "heat": heat})
    if ready == 0:
        return None
    out.sort(key=lambda m: m["heat"], reverse=True)
    return out[:PRESCREEN_TOP]


def mark_hot(symbols):
    now = time.time()
    with ws_lock:
        for s in symbols:
            hot_ts[to_raw(s)] = now


def window_change_pct(raw):
    """Bir coinin son BTC_WINDOW sn'deki % degisimi (sadece WS fiyat gecmisi, REST YOK)."""
    with ws_lock:
        h = list(tick_hist.get(raw, ()))
    if len(h) < 2:
        return None
    t_now, p_now, _ = h[-1]
    target = t_now - BTC_WINDOW
    ref = None
    for smp in h:
        if smp[0] >= target:
            ref = smp
            break
    if ref is None or ref[1] <= 0 or t_now - ref[0] < BTC_MIN_SPAN:
        return None
    return (p_now / ref[1] - 1.0) * 100.0


def btc_change_pct():
    """BTC'nin son BTC_WINDOW sn'deki % degisimi (sadece WS). Yeterli gecmis yoksa None (filtre karar vermez)."""
    return window_change_pct("BTCUSDT")


def funding_rate(raw):
    """WS'ten gelen guncel funding orani. Veri yoksa ya da bayatsa None (fail-open)."""
    if not FUNDING_ENABLED:
        return None
    with ws_lock:
        if time.time() - funding_ts > FUNDING_STALE_SECONDS:
            return None
        return funding.get(raw)


oi_cache = {}     # symbol -> (zaman, oi_degisim_yuzde veya None)


def oi_change_pct(symbol):
    """Son ~OI_BARS*OI_PERIOD icindeki Open Interest % degisimi. Binance'te OI icin websocket akisi olmadigi icin REST:
    tek istek, OI_CACHE_SECONDS onbellekli, ban varken istek atmaz. Hata/veri yoksa None (fail-open)."""
    now = time.time()
    cached = oi_cache.get(symbol)
    if cached and now - cached[0] < OI_CACHE_SECONDS:
        return cached[1]
    if ban_until > now:
        return None
    chg = None
    try:
        rows = ex.fetch_open_interest_history(symbol, OI_PERIOD, None, OI_BARS)
        rows = sorted(rows or [], key=lambda r: r.get("timestamp") or 0)
        vals = []
        for r in rows:
            v = r.get("openInterestValue")
            if v is None:
                v = r.get("openInterestAmount")
            if v is not None:
                vals.append(float(v))
        if len(vals) >= 2 and vals[0] > 0:
            chg = (vals[-1] / vals[0] - 1.0) * 100.0
    except Exception as e:
        log.warning(f"{symbol} OI okunamadi (sonuc degismedi): {e}")
        note_ban(e)
    oi_cache[symbol] = (now, chg)
    return chg


# ---------------- Mum verisi ----------------
def is_fresh(raw):
    with ws_lock:
        arr = klines.get(raw)
        return bool(arr) and len(arr) >= 60 and raw not in dirty \
            and (time.time() - kline_ts.get(raw, 0)) < KLINE_STALE_SECONDS


def ensure_streams(symbols):
    """Yeni adaylarin mum akislarini kaydeder; websocket kendini yeniden baglayarak bunlari ekler.
    Yer kalmazsa en eski hareketli olmus (ve acik pozisyonu olmayan) coinlerin akislari cikarilir."""
    with trail_lock:
        protect = {to_raw(s) for s in trail_state}
    with ws_lock:
        new = [to_raw(s) for s in symbols if to_raw(s) not in subscribed]
        room = MAX_STREAMS - len(subscribed)
        if len(new) > room:
            need = len(new) - room
            cands = sorted((r for r in subscribed if r not in protect and r not in new),
                           key=lambda r: hot_ts.get(r, 0.0))
            for r in cands[:need]:
                subscribed.discard(r)
                klines.pop(r, None)
                kline_ts.pop(r, None)
                live_ts.pop(r, None)
                dirty.discard(r)
            room = MAX_STREAMS - len(subscribed)
        new = new[:max(0, room)]
        subscribed.update(new)
    if new:
        log.info(f"{len(new)} aday coin icin mum akisi isteniyor.")


def seed_symbol(symbol):
    """Gecmis mumlari REST'ten BIR KEZ yukler (sonrasi WS besler).
    Ham Binance klines ucu kullanilir: taker alim hacmi (CVD icin) de gelir. Olmazsa fetch_ohlcv'ye dusulur (CVD bos kalir)."""
    raw = to_raw(symbol)
    rows = None
    if hasattr(ex, "fapiPublicGetKlines"):
        data = ex.fapiPublicGetKlines({"symbol": raw, "interval": TIMEFRAME, "limit": SEED_LIMIT})
        rows = [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]), True, float(r[9])]
                for r in data]
    if rows is None:
        ohlcv = ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=SEED_LIMIT)
        rows = [[int(r[0]), r[1], r[2], r[3], r[4], r[5], True, float("nan")] for r in ohlcv]
    if rows:
        rows[-1][6] = False       # sonuncu mum henuz olusuyor
    with ws_lock:
        existing = klines.get(raw, [])
        if rows:
            last_t = rows[-1][0]
            same = [r for r in existing if r[0] == last_t]
            if same:
                rows[-1] = same[-1]                       # WS'teki surum daha tazedir
            rows = rows + [r for r in existing if r[0] > last_t]
        klines[raw] = rows[-KLINE_KEEP:]
        kline_ts[raw] = time.time()
        dirty.discard(raw)


def prepare_data(symbols, skip):
    ensure_streams(symbols)
    need = [s for s in symbols if s not in skip and not is_fresh(to_raw(s))]
    if not need:
        return
    total_need = len(need)
    need = need[:SEED_PER_CYCLE]      # en hareketli coinler once, kalanlar sonraki turda
    log.info(f"Gecmis mum yukleniyor (REST, bir kerelik, aralikli): {len(need)}/{total_need} coin")
    for s in need:
        wait_if_banned()
        try:
            seed_symbol(s)
        except Exception as e:
            log.error(f"{s} gecmis mum hatasi: {e}")
            if note_ban(e):
                break
        time.sleep(SEED_SLEEP)


# ---------------- INDIKATORLER (sadece gerekenler) ----------------
def calculate_indicators(df):
    df["ema_fast"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=21, adjust=False).mean()
    df["ema_trend"] = df["close"].ewm(span=EMA_TREND_PERIOD, adjust=False).mean()

    prev_close = df["close"].shift()
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr"] = tr.rolling(14).mean()

    # CVD: mum bazinda taker alim - taker satim = 2*taker_alim - toplam hacim (taker_alim yoksa NaN)
    if "tbv" not in df.columns:
        df["tbv"] = np.nan
    df["cvd_delta"] = 2.0 * df["tbv"] - df["volume"]
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


def get_live_df(symbol):
    """Olusmakta olan mum DAHIL DataFrame. Dondurur: (df, mumun dolan orani) ya da None.
    Mum en az LIVE_MIN_ELAPSED dolmus, WS'ten taze ve onceki mumla bitisik olmali."""
    raw = to_raw(symbol)
    now = time.time()
    with ws_lock:
        rows = [r[:] for r in klines.get(raw, [])]
        seen = live_ts.get(raw, 0.0)
    if len(rows) < 61 or rows[-1][6] or now - seen > LIVE_STALE_SECONDS:
        return None
    elapsed = now - rows[-1][0] / 1000.0
    if not (LIVE_MIN_ELAPSED <= elapsed < TF_MS / 1000.0):
        return None
    if rows[-1][0] - rows[-2][0] != TF_MS:
        return None
    df = pd.DataFrame(rows, columns=COLS)
    return calculate_indicators(df), elapsed / (TF_MS / 1000.0)


def htf_df_from_ws(symbol):
    """1 saatlik mumlari, WS/onbellekteki 15 dk mumlardan URETIR (REST istegi YOK).
    Sadece tamamlanmis saatler kullanilir."""
    raw = to_raw(symbol)
    with ws_lock:
        rows = [r[:] for r in klines.get(raw, [])]
    if len(rows) < 20:
        return None
    df = pd.DataFrame(rows, columns=COLS)
    df = df[df["closed"]].copy()
    if df.empty:
        return None
    per_hour = max(int(3_600_000 // TF_MS), 1)
    df["hour"] = df["timestamp"] // 3_600_000
    df = df[df.groupby("hour")["timestamp"].transform("count") >= per_hour]
    if df.empty:
        return None
    g = df.groupby("hour")
    h = pd.DataFrame({
        "timestamp": g["timestamp"].first(),
        "open": g["open"].first(),
        "high": g["high"].max(),
        "low": g["low"].min(),
        "close": g["close"].last(),
        "volume": g["volume"].sum(),
    }).reset_index(drop=True)
    if len(h) < 30:
        return None
    return calculate_indicators(h)


def fetch_df_rest(symbol, timeframe, limit=100):
    """REST ile mum (sadece yedek mod ve pozisyon sahiplenme icin). CVD verisi yoktur (NaN)."""
    ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.iloc[:-1].copy()
    return calculate_indicators(df)


# ---------------- GIRIS DEGERLENDIRMESI (sade cekirdek) ----------------
def _pumped_1h(df, close):
    """Son 1 saatte MAX_MOVE_1H'den fazla oynamis mi? (pump/dump'a gec kalma)"""
    per_hour = max(int(3_600_000 // TF_MS), 1)
    if len(df) <= per_hour:
        return False
    ref_close = float(df["close"].iloc[-1 - per_hour])
    return ref_close > 0 and abs(close / ref_close - 1.0) > MAX_MOVE_1H


def cvd_direction(df, side, bars=CVD_BARS):
    """Son 'bars' mumun CVD orani (toplam CVD farki / toplam hacim), yon tarafinda pozitif = para yone akiyor.
    Taker verisi yoksa None (fail-open)."""
    d = df["cvd_delta"].iloc[-bars:]
    v = df["volume"].iloc[-bars:]
    if len(d) < bars or d.isna().any():
        return None
    vs = float(v.sum())
    if vs <= 0:
        return None
    ratio = float(d.sum()) / vs
    return ratio if side == "long" else -ratio


def evaluate(df):
    """Sade giris kurali. Sira: durgunluk -> yon (EMA50) -> taze EMA9/21 kesisimi -> gec kalma filtreleri ->
    ZORUNLU hacim sicramasi -> CVD teyidi. Funding/OI/ust zaman dilimi scan_market'te uygulanir."""
    if len(df) < 60:
        return None, "veri_az"
    last = df.iloc[-1]
    if any(pd.isna(last[k]) for k in ("ema_fast", "ema_slow", "ema_trend", "atr")) \
            or pd.isna(df["ema_slow"].iloc[-1 - EMA_SLOPE_BARS]):
        return None, "veri_eksik"

    n = len(df)
    close = float(last["close"])
    atr = float(last["atr"])
    if close <= 0 or atr <= 0:
        return None, "veri_eksik"

    atr_ref = float(df["atr"].iloc[-ATR_REF_BARS:].mean())
    if atr_ref != atr_ref:
        atr_ref = atr
    if max(atr, atr_ref) / close < MIN_ATR_PCT:      # hem su an hem gun ortalamasi durgunsa: komisyon yer, girme
        return None, "durgun"
    atr = max(atr, ATR_FLOOR_RATIO * atr_ref)        # stop asiri dar olmasin

    # --- YON: EMA50'nin hangi tarafinda ---
    ema50 = float(last["ema_trend"])
    es = float(last["ema_slow"])
    if close > ema50:
        side = "long"
    elif close < ema50:
        side = "short"
    else:
        return None, "yon_belirsiz"

    # --- TETIK: EMA50 yonunde TAZE EMA9/21 kesisimi ---
    ef_s, es_s = df["ema_fast"], df["ema_slow"]
    side_state = (ef_s > es_s) if side == "long" else (ef_s < es_s)
    if not bool(side_state.iloc[-1]):
        return None, "ema_kesisim_yok"
    start = n - 1 - EMA_CROSS_BARS
    win = side_state.iloc[start:n - 1]
    if bool(win.all()):
        return None, "ema_kesisim_yok"                 # kesisim eski: taze degil
    k = start + int(np.flatnonzero(~win.to_numpy()).max()) + 1       # kesisimin gerceklestigi mum
    slope = float(es_s.iloc[-1]) - float(es_s.iloc[-1 - EMA_SLOPE_BARS])
    if (slope <= 0) if side == "long" else (slope >= 0):
        return None, "ema_egim_ters"
    run_atr = ((close - float(df["close"].iloc[k])) * (1 if side == "long" else -1)) / atr
    if run_atr > MAX_SINCE_TRIGGER_ATR:
        return None, "tetik_eski"                      # kesisimden bu yana cok gitmis

    # --- GEC KALMA FILTRELERI ---
    if abs(close - es) / atr > ENTRY_MAX_EXT_ATR:
        return None, "gec_kalmis"                      # fiyat EMA21'den cok uzak
    if len(df) >= MOVE_24H_BARS:
        w24 = df.iloc[-MOVE_24H_BARS:]
        if side == "long":
            base = float(w24["low"].min())
            if base > 0 and close / base - 1.0 > MAX_MOVE_24H:
                return None, "24s_asiri"
        else:
            top = float(w24["high"].max())
            if top > 0 and 1.0 - close / top > MAX_MOVE_24H:
                return None, "24s_asiri"
    if float(last["high"] - last["low"]) > MAX_CANDLE_ATR * atr:
        return None, "dikey_mum"
    if _pumped_1h(df, close):
        return None, "pump_1s"

    # --- ZORUNLU HACIM SICRAMASI (yonle uyumlu mumda) ---
    vol_base = float(df["volume"].iloc[-22:-2].mean())
    v2 = df["volume"].iloc[-2:]
    spike = float(v2.max()) / vol_base if vol_base > 0 else 0.0
    if spike < VOL_SPIKE_MIN:
        return None, "hacim_dusuk"
    spike_candle = df.iloc[-2:].loc[v2.idxmax()]
    if (side == "long" and spike_candle["close"] < spike_candle["open"]) or \
       (side == "short" and spike_candle["close"] > spike_candle["open"]):
        return None, "hacim_ters"

    # --- CVD TEYIDI: taker akisi yonun tersineyse girme ---
    cvd = cvd_direction(df, side)
    if cvd is not None and cvd < CVD_MIN:
        return None, "cvd_ters"
    cvd_strong = cvd is not None and cvd >= CVD_STRONG

    return {
        "side": side, "atr": atr, "price": close, "rv": spike, "htf": "-",
        "cvd": cvd, "cvd_strong": cvd_strong, "confirm": 0, "trigger": "ema",
    }, "ok"


def evaluate_live(df, frac):
    """Olusan mumdaki erken tetikler (ema_canli / kirilim / donus). df'nin son satiri olusan mumdur,
    frac mumun dolan orani. Dondurur: aday listesi (tutunma, funding, 3'te 2 scan_market'te uygulanir)."""
    if len(df) < 61 or frac <= 0:
        return []
    live, prev = df.iloc[-1], df.iloc[-2]
    closed = df.iloc[:-1]
    close, op = float(live["close"]), float(live["open"])
    high, low = float(live["high"]), float(live["low"])
    atr = float(prev["atr"])                       # ATR sadece kapanmis mumlardan
    keys = ("ema_fast", "ema_slow", "ema_trend")
    if any(pd.isna(live[k]) for k in keys) or pd.isna(atr) or atr <= 0 or close <= 0 or high <= low:
        return []
    atr_ref = float(closed["atr"].iloc[-ATR_REF_BARS:].mean())
    if atr_ref != atr_ref:
        atr_ref = atr
    if max(atr, atr_ref) / close < MIN_ATR_PCT:
        return []
    atr = max(atr, ATR_FLOOR_RATIO * atr_ref)
    if high - low > MAX_CANDLE_ATR * atr or _pumped_1h(df, close):
        return []
    vol_base = float(closed["volume"].iloc[-20:].mean())
    if not vol_base > 0:
        return []
    rv = float(live["volume"]) / frac / vol_base
    if rv < VOL_SPIKE_MIN:
        return []

    pos = (close - low) / (high - low)             # 1 = mumun tepesinde, 0 = dibinde
    lvl_high = float(closed["high"].iloc[-BREAK_BARS:].max())
    lvl_low = float(closed["low"].iloc[-BREAK_BARS:].min())
    ema50, ef, es = float(live["ema_trend"]), float(live["ema_fast"]), float(live["ema_slow"])
    out = []
    for side in ("long", "short"):
        d = 1 if side == "long" else -1
        if (close - op) * d <= 0:                  # mum yonle ayni renkte olmali
            continue
        if (pos < 1 - WICK_ZONE) if side == "long" else (pos > WICK_ZONE):
            continue                               # igne filtresi
        if len(df) >= MOVE_24H_BARS:
            w24 = df.iloc[-MOVE_24H_BARS:]
            ref = float(w24["low"].min()) if side == "long" else float(w24["high"].max())
            if ref > 0 and (close / ref - 1.0) * d > MAX_MOVE_24H:
                continue

        def add(trigger, cvd_bars):
            cvd = cvd_direction(df, side, cvd_bars)
            if cvd is None or cvd < EARLY_CVD_MIN:
                return
            out.append({"side": side, "atr": atr, "price": close, "rv": rv, "htf": "-", "cvd": cvd,
                        "cvd_strong": True, "confirm": 0, "trigger": trigger})

        # --- DONUS: dip/tepe temizlendi, fiyat geri dondu ---
        if rv >= REV_VOL_MIN:
            tip = low if side == "long" else high
            level = lvl_low if side == "long" else lvl_high
            swept = (level - tip) * d > 0 and (close - level) * d > 0
            if swept and abs(close - tip) <= sl_distance(close, atr, LEVERAGE):
                add("donus", 1)

        # --- trend yonu isteyen tetikler ---
        if (close - ema50) * d <= 0 or abs(close - es) / atr > ENTRY_MAX_EXT_ATR:
            continue
        level = lvl_high if side == "long" else lvl_low
        if 0 < (close - level) * d <= BREAK_MAX_ATR * atr:
            add("kirilim", CVD_BARS)
        crossed = (ef - es) * d > 0 and (float(prev["ema_fast"]) - float(prev["ema_slow"])) * d <= 0
        slope = (es - float(df["ema_slow"].iloc[-1 - EMA_SLOPE_BARS])) * d
        if crossed and slope > 0:
            add("ema_canli", CVD_BARS)
    return out


hold_state = {}    # (coin, tetik, yon) -> {"n": sayi, "ts": son sayim, "seen": son gorulme}


def update_hold(cands, now=None):
    """Tutunma sarti: sinyal arka arkaya HOLD_SCANS taramada gorulen adaylari dondurur.
    Bu taramada gorulmeyen sinyalin sayaci silinir."""
    now = time.time() if now is None else now
    passed, seen = [], set()
    for c in cands:
        key = (c["symbol"], c["trigger"], c["side"])
        seen.add(key)
        h = hold_state.get(key)
        if h is None or now - h["seen"] > HOLD_MAX_GAP:
            h = hold_state[key] = {"n": 1, "ts": now, "seen": now}
        else:
            h["seen"] = now
            if now - h["ts"] >= HOLD_MIN_GAP:
                h["n"] += 1
                h["ts"] = now
        if h["n"] >= HOLD_SCANS:
            passed.append(c)
    for key in [k for k in hold_state if k not in seen]:
        del hold_state[key]
    return passed


def confirm_early(cands, reasons):
    """3'te 2 sarti: super hacim / OI artiyor / funding sakin. Donus: super hacim VE sakin funding (OI aranmaz).
    OI sadece digerlerinden en az biri saglaniyorsa ve en fazla OI_CHECK_TOP coin icin sorulur (onbellekli)."""
    out, asked = [], set()
    for c in sorted(cands, key=lambda x: x["rv"], reverse=True):
        sup = c["rv"] >= SUPER_VOL
        calm = bool(c.get("calm"))
        if c["trigger"] == "donus":
            if sup and calm:
                c["confirm"] = 2
                out.append(c)
            else:
                reasons["donus_onay_yok"] = reasons.get("donus_onay_yok", 0) + 1
            continue
        rising = False
        if OI_ENABLED and (sup or calm) and (c["symbol"] in asked or len(asked) < OI_CHECK_TOP):
            asked.add(c["symbol"])
            chg = oi_change_pct(c["symbol"])
            c["oi"] = chg
            if chg is not None:
                if chg <= -OI_DROP_PCT:
                    reasons["oi_dusuyor"] = reasons.get("oi_dusuyor", 0) + 1
                    continue
                rising = chg >= OI_RISE_PCT
        score = int(sup) + int(calm) + int(rising)
        if score >= EARLY_NEED:
            c["confirm"] = score
            out.append(c)
        else:
            reasons["erken_3te2_yok"] = reasons.get("erken_3te2_yok", 0) + 1
    return out


htf_cache = {}   # symbol -> (zaman, df)   (sadece REST yedek modu)


def htf_alignment(symbol, side):
    df = None
    if ws_healthy() and is_fresh(to_raw(symbol)):
        df = htf_df_from_ws(symbol)          # REST yok: 15 dk mumlardan uretilen 1s mumlar
    if df is None:
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


last_summary_log = 0.0


def scan_market(skip):
    global last_scan, last_summary_log
    wait_for_ws()
    verbose = time.time() - last_summary_log >= SUMMARY_LOG_SECONDS
    symbols = get_dynamic_symbols()
    tradfi = {s for s in symbols if is_tradfi(s)}
    if tradfi:
        symbols = [s for s in symbols if s not in tradfi]
        if verbose:
            names = ", ".join(sorted(s.split("/")[0] for s in tradfi)[:12])
            log.info(f"TradFi (hisse/emtia) vadeliler elendi: {len(tradfi)} adet ({names}...)")
    ws_ok = ws_healthy()
    if verbose:
        log.info(f"Hacim araligindaki coin sayisi: {len(symbols)} | "
                 f"veri kaynagi: {'WebSocket' if ws_ok else 'REST YEDEK'}")
    if not symbols:
        return []

    movers_view = []
    if ws_ok:
        movers = prescreen_movers(symbols)
        if movers is None:
            if verbose:
                last_summary_log = time.time()
                log.info(f"⏳ Isinma: fiyat gecmisi birikiyor (en az {PRESCREEN_WARMUP // 60} dk). "
                         f"Bu sure islem acilmaz.")
            last_scan = {"time": time.time(), "kaynak": "WebSocket (isinma)", "adaylar": [], "hareketli": []}
            return []
        hot = [m["symbol"] for m in movers if m["symbol"] not in skip]
        movers_view = [{"coin": m["symbol"], "degisim_yuzde": round(m["chg"], 2),
                        "hacim_x": round(m["ratio"], 1)} for m in movers]
        if movers and verbose:
            top = ", ".join(f"{m['symbol'].split('/')[0]} {m['chg']:+.1f}%" for m in movers[:8])
            log.info(f"🔥 Hareketli coinler ({len(movers)}): {top}")
        mark_hot(hot)
        prepare_data(hot, skip)       # sadece bu adaylarin akisi acilir ve gecmis mumu (bir kez) yuklenir
        hot_set = set(hot)
        symbols = hot + [s for s in symbols if s not in hot_set and is_fresh(to_raw(s))]
    else:
        symbols = symbols[:FALLBACK_MAX_COINS]
        log.warning(f"⚠️ WebSocket hazir degil: REST yedek modu, sadece ilk {len(symbols)} coin, yavas taranacak.")

    reasons = {}
    stage1 = []
    early_raw = []

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
        if EARLY_ENABLED and ws_ok:
            live = get_live_df(symbol)
            if live is not None:
                for c in evaluate_live(*live):
                    if (c["side"] == "long" and ALLOW_LONG) or (c["side"] == "short" and ALLOW_SHORT):
                        c["symbol"] = symbol
                        early_raw.append(c)
        cand, reason = evaluate(df)
        if cand is None:
            reasons[reason] = reasons.get(reason, 0) + 1
            continue
        if (cand["side"] == "long" and not ALLOW_LONG) or (cand["side"] == "short" and not ALLOW_SHORT):
            continue
        cand["symbol"] = symbol
        stage1.append(cand)

    # --- FUNDING (WS): kalabalik tarafa girme; sakinse +1 onay ---
    funded = []
    for cand in stage1:
        f = funding_rate(to_raw(cand["symbol"])) if ws_ok else None
        cand["funding"] = f
        if f is not None:
            if (cand["side"] == "long" and f > FUNDING_MAX) or (cand["side"] == "short" and f < -FUNDING_MAX):
                reasons["funding_kalabalik"] = reasons.get("funding_kalabalik", 0) + 1
                continue
            calm = f <= FUNDING_CALM if cand["side"] == "long" else f >= -FUNDING_CALM
        else:
            calm = False
        cand["confirm"] = int(bool(cand["cvd_strong"])) + int(calm)
        funded.append(cand)

    funded.sort(key=lambda c: (c["confirm"], c["rv"]), reverse=True)

    # --- ERKEN ADAYLAR: funding -> tutunma -> 3'te 2 ---
    early_funded = []
    for cand in early_raw:
        f = funding_rate(to_raw(cand["symbol"]))
        cand["funding"] = f
        long_side = cand["side"] == "long"
        if f is not None and ((long_side and f > FUNDING_MAX) or (not long_side and f < -FUNDING_MAX)):
            reasons["funding_kalabalik"] = reasons.get("funding_kalabalik", 0) + 1
            continue
        cand["calm"] = f is not None and (f <= FUNDING_CALM if long_side else f >= -FUNDING_CALM)
        early_funded.append(cand)
    early = confirm_early(update_hold(early_funded), reasons)

    # --- UST ZAMAN DILIMI: ters ise ele (puan yok, sadece filtre) ---
    final = []
    for cand in funded[:HTF_CHECK_TOP]:
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
        cand["htf"] = htf
        final.append(cand)

    # erken adaylarda ust zaman filtresi (donus haric)
    early_final = []
    for cand in early:
        if cand["trigger"] != "donus":
            try:
                cand["htf"] = htf_alignment(cand["symbol"], cand["side"])
            except Exception as e:
                log.error(f"{cand['symbol']} ust zaman hatasi: {e}")
                if note_ban(e):
                    break
                continue
            if cand["htf"] == "ters":
                reasons["ust_trend_ters"] = reasons.get("ust_trend_ters", 0) + 1
                continue
        early_final.append(cand)

    # --- OPEN INTEREST (REST, sadece en iyi adaylar, onbellekli, fail-open) ---
    if OI_ENABLED and ws_ok and final:
        checked = final[:OI_CHECK_TOP]
        dropped = []
        for c in checked:
            chg = oi_change_pct(c["symbol"])
            c["oi"] = chg
            if chg is None:
                continue
            if chg <= -OI_DROP_PCT:
                dropped.append(c)
                reasons["oi_dusuyor"] = reasons.get("oi_dusuyor", 0) + 1
            elif chg >= OI_RISE_PCT:
                c["confirm"] += 1
        if dropped:
            final = [c for c in final if c not in dropped]

    final = [c for c in final if c["confirm"] >= MIN_CONFIRM] + early_final
    final.sort(key=lambda c: (c["confirm"], c["rv"]), reverse=True)

    if verbose or early_final:
        last_summary_log = time.time()
        log.info(f"Analiz edilen: {len(symbols)} | Elenenler: {reasons} | Tetikli aday: {len(stage1)} | "
                 f"Erken sinyal (bekleyen): {len(hold_state)} | Filtreleri gecen: {len(final)}")
    for c in (final[:5] if (verbose or early_final) else []):
        cvd_txt = f"{c['cvd']:+.2f}" if c.get("cvd") is not None else "-"
        fr_txt = f"{c['funding'] * 100:+.3f}%" if c.get("funding") is not None else "-"
        oi_txt = f"{c['oi']:+.2f}%" if c.get("oi") is not None else "-"
        log.info(f"  ⭐ {c['symbol']} {c['side'].upper()} tetik={c['trigger']} onay={c['confirm']} hacimx={c['rv']:.1f} "
                 f"1s={c['htf']} cvd={cvd_txt} funding={fr_txt} oi={oi_txt}")
    last_scan = {
        "time": time.time(),
        "kaynak": "WebSocket" if ws_ok else "REST yedek",
        "hareketli": movers_view,
        "adaylar": [{"coin": c["symbol"], "yon": c["side"], "tetik": c["trigger"], "onay": c["confirm"],
                     "hacim_x": round(c["rv"], 1), "ust_trend": c["htf"],
                     "cvd": round(c["cvd"], 3) if c.get("cvd") is not None else None,
                     "funding_yuzde": round(c["funding"] * 100, 4) if c.get("funding") is not None else None,
                     "oi_degisim_yuzde": round(c["oi"], 2) if c.get("oi") is not None else None}
                    for c in final[:10]],
    }

    ready = list(final)
    if BTC_FILTER_ENABLED and ws_ok and ready:
        btc = btc_change_pct()
        if btc is not None:
            kept = [c for c in ready
                    if not ((c["side"] == "long" and btc <= -BTC_BLOCK_PCT)
                            or (c["side"] == "short" and btc >= BTC_BLOCK_PCT))]
            if len(kept) < len(ready):
                log.info(f"🛡️ BTC filtresi: BTC son {BTC_WINDOW // 60} dk %{btc:+.2f}, "
                         f"ters yondeki {len(ready) - len(kept)} aday elendi.")
            ready = kept
    return ready


# ---------------- KORUMA EMIRLERI ----------------
def sl_distance(price, atr, lev):
    """Stop mesafesi: SL_ATR x ATR, ama marjinde SL_MAX_MARGIN_PCT zarari gecmeyecek kadar (hangisi yakinsa)."""
    cap = price * (SL_MAX_MARGIN_PCT / 100.0) / max(float(lev), 1.0)
    return min(SL_ATR * atr, cap)


def tp_price_for(symbol, entry, atr, side, lev):
    """Ilk kar-al fiyati: stop mesafesinin TP1_RR kati kadar kar yonunde."""
    dist = sl_distance(entry, atr, lev) * TP1_RR
    raw = entry + dist if side == "long" else entry - dist
    return float(ex.price_to_precision(symbol, raw))


def calc_tp_amount(symbol, amount):
    """Ilk kar alda kapanacak miktar = pozisyonun TP1_CLOSE_PCT'si.
    Yuvarlama yuzunden miktar yariyi belirgin asarsa 0 doner: TP konmaz (yanlis buyuk kar-al pozisyonun tamamini kapatir)."""
    try:
        tp_amount = float(ex.amount_to_precision(symbol, amount * TP1_CLOSE_PCT))
    except Exception as e:
        log.warning(f"{symbol} kar-al miktari hesaplanamadi: {e}")
        return 0.0
    if tp_amount <= 0 or tp_amount > amount * (TP1_CLOSE_PCT + 0.10):
        return 0.0
    return tp_amount


def place_tp_once(symbol, exit_side, tp_amount, tp_price):
    """Kar-al emrini TAM BIR KERE koyar (once borsaya bakar, sonra sayar, fazlasini iptal eder).
    Basarili ise kalan kar-al emrinin id'sini, degilse None dondurur.
    Emirler okunamiyorsa yeni emir GONDERMEZ (cift kar-al olmasin); dedupe_stops sonra tekrar dener."""
    try:
        existing = scan_exit_orders(symbol, exit_side)[1]
    except OrderReadError:
        log.warning(f"{symbol}: emirler okunamadi, kar-al simdilik konmadi, sonra tekrar denenecek.")
        return None
    if existing:
        log.warning(f"⚠️ {symbol}: borsada zaten {len(existing)} kar-al emri var, yeni kar-al KONMADI.")
        try:
            return enforce_single_tp(symbol, exit_side)
        except OrderReadError:
            return existing[-1][0]

    new_id = None
    try:
        order = ex.create_order(symbol, "TAKE_PROFIT_MARKET", exit_side, tp_amount, None,
                                {"stopPrice": tp_price, "reduceOnly": True})
        new_id = order.get("id")
    except Exception as e:
        log.error(f"{symbol} TP gonderilirken hata: {e}")
        note_ban(e)
        time.sleep(2)                      # zaman asimiysa emir gitmis olabilir: kontrol et, KORUMASIZCA tekrar gonderme
        try:
            if not scan_exit_orders(symbol, exit_side)[1]:
                return None
        except OrderReadError:
            return None
        log.warning(f"⚠️ {symbol}: hata alindi ama kar-al emri borsada gorunuyor, tekrar gonderilmedi.")

    time.sleep(1)                          # borsa listesine yansisin
    try:
        return enforce_single_tp(symbol, exit_side) or new_id
    except OrderReadError:
        return new_id                      # emir gonderildi, sadece liste okunamadi


def enforce_single_tp(symbol, exit_side):
    """Borsada bu yonde birden fazla kar-al varsa EN YENISI kalir (id en buyuk), digerleri iptal edilir."""
    tps = scan_exit_orders(symbol, exit_side)[1]
    if not tps:
        return None
    tps.sort(key=lambda x: str(x[0]).zfill(20))
    keep_id = tps[-1][0]
    if len(tps) > 1:
        removed = sum(1 for oid, _ in tps[:-1] if cancel_safe(oid, symbol))
        log.warning(f"🧹 {symbol}: {len(tps)} kar-al emri bulundu (iki kar-al = pozisyonun TAMAMI kapanir!), "
                    f"{removed} fazla iptal edildi. Birden fazla bot kopyasi calisiyor olabilir!")
    return keep_id


def place_protection(symbol, entry, amount, atr, side="long", lev=1):
    """SL: giris -/+ sl_distance. TP1: stop mesafesinin TP1_RR kati, pozisyonun TP1_CLOSE_PCT'si, TAM 1 emir."""
    exit_side = "sell" if side == "long" else "buy"
    sl_dist = sl_distance(entry, atr, lev)
    sl_raw = entry - sl_dist if side == "long" else entry + sl_dist
    sl_price = float(ex.price_to_precision(symbol, sl_raw))
    tp_price = tp_price_for(symbol, entry, atr, side, lev)
    tp_amount = calc_tp_amount(symbol, amount)

    try:
        sl_order = ex.create_order(symbol, "STOP_MARKET", exit_side, amount, None,
                                   {"stopPrice": sl_price, "reduceOnly": True})
        log.info(f"🛑 SL kondu: {symbol} @ {sl_price}")
    except Exception as e:
        log.error(f"{symbol} SL konamadi: {e}")
        return None

    tp_id = None
    if tp_amount > 0:
        tp_id = place_tp_once(symbol, exit_side, tp_amount, tp_price)
        if tp_id is not None:
            roi = abs(tp_price - entry) / entry * lev * 100
            log.info(f"🎯 TP kondu (%{TP1_CLOSE_PCT*100:.0f} kapanir, marjinde ~%{roi:.1f}): "
                     f"{symbol} @ {tp_price} miktar={tp_amount}")
        else:
            log.error(f"{symbol} TP konamadi")
    else:
        log.warning(f"{symbol}: kar-al miktari uygun degil (yuvarlama), TP konmadi. SL aktif.")
    return {"sl_id": sl_order.get("id"), "sl_price": sl_price,
            "tp_id": tp_id, "tp_price": tp_price, "tp_amount": tp_amount}


def cancel_safe(order_id, symbol):
    last_err = None
    for params in ({}, {"trigger": True}):
        try:
            ex.cancel_order(order_id, symbol, params)
            return True
        except Exception as e:
            last_err = e
            continue
    log.warning(f"{symbol} emir iptal edilemedi (id={order_id}): {last_err}")
    return False


def cancel_all_safe(symbol):
    for params in ({}, {"trigger": True}):
        try:
            ex.cancel_all_orders(symbol, params)
        except Exception:
            pass


def _raw_type(o):
    """Emrin Binance'teki gercek tipi (ccxt'nin birlestirilmis 'type' alani STOP_MARKET'i 'market' gosterebilir)."""
    info = o.get("info") or {}
    for k in ("orderType", "origType", "type"):
        v = info.get(k)
        if v:
            return str(v).upper()
    return str(o.get("type") or "").upper()


class OrderReadError(Exception):
    """Acik emirler okunamadi. "Emir yok" ile KARISTIRILMAMALI."""


def scan_exit_orders(symbol, exit_side):
    """Borsadaki bu yone ait cikis emirlerini TEK taramada okur.
    Dondurur: ([(id, fiyat) stop-loss], [(id, fiyat) kar-al], {id: miktar}).
    Iki sorgudan biri bile basarisizsa OrderReadError firlatir (eksik liste dondurmez)."""
    stops, tps, amounts = {}, {}, {}
    for params in ({}, {"trigger": True}):
        try:
            orders = ex.fetch_open_orders(symbol, params=params)
        except Exception as e:
            log.warning(f"{symbol} acik emirler okunamadi {params}: {e}")
            note_ban(e)
            raise OrderReadError(f"{symbol}: {e}") from e
        for o in orders:
            if o.get("side") != exit_side:
                continue
            info = o.get("info") or {}
            raw = _raw_type(o)
            is_tp = raw.startswith("TAKE_PROFIT")
            if not (is_tp or raw.startswith("STOP") or o.get("stopLossPrice")):
                continue
            price = (o.get("stopLossPrice") or o.get("stopPrice") or o.get("triggerPrice")
                     or info.get("stopPrice") or info.get("triggerPrice"))
            try:
                price = float(price)
            except (TypeError, ValueError):
                continue
            if price <= 0 or o.get("id") is None:
                continue
            (tps if is_tp else stops)[o["id"]] = price
            try:
                amt = o.get("amount")
                if amt is None:
                    amt = info.get("origQty") or info.get("quantity")
                if amt is not None:
                    amounts[o["id"]] = float(amt)
            except (TypeError, ValueError):
                pass
    return list(stops.items()), list(tps.items()), amounts


def dedupe_stops(symbol):
    """Her dakika calisir. (1) KAR-AL: pozisyon basina en fazla 1 kar-al; ilk kar-al dolduysa hic kalmasin;
    kar-al miktari yariyi asiyorsa duzelt. (2) STOP-LOSS: birden fazla varsa en siki olani tut, fazlasini iptal et."""
    with trail_lock:
        st = trail_state.get(symbol)
        if not st:
            return
        exit_side = "sell" if st["side"] == "long" else "buy"
        tp_state = st.get("tp_state", "pending")
        orig = float(st.get("orig_amount") or 0)
        cur = float(st.get("amount") or 0)
    try:
        stops, tps, amounts = scan_exit_orders(symbol, exit_side)  # REST, kilidin disinda (tek tarama)
    except OrderReadError:
        log.warning(f"{symbol}: emirler okunamadi, hicbir emre ve pozisyona dokunulmadi (sonraki turda tekrar).")
        return

    # ---------- KAR-AL EMIRLERI ----------
    if tp_state != "done" and orig > 0 and 0 < cur < orig * 0.9:
        tp_state = "done"
        with trail_lock:
            cu = trail_state.get(symbol)
            if cu:
                cu["tp_state"] = "done"
                save_state()
        log.info(f"🎯 {symbol}: ilk kar-al gerceklesti ({orig} -> {cur}); bundan sonra hic kar-al emri kalmayacak.")

    if tp_state == "done":
        if tps:
            removed_tp = sum(1 for oid, _ in tps if cancel_safe(oid, symbol))
            log.warning(f"🧹 {symbol}: ilk kar-al tamamlandi ama {len(tps)} kar-al emri kalmisti "
                        f"(kalan yariyi kapatirdi), {removed_tp} tanesi iptal edildi.")
    else:
        if len(tps) > 1:
            tps.sort(key=lambda x: str(x[0]).zfill(20))        # id'ler artar: en yeniyi tut
            removed_tp = sum(1 for oid, _ in tps[:-1] if cancel_safe(oid, symbol))
            log.warning(f"🧹 {symbol}: {len(tps)} kar-al emri bulundu (iki kar-al = pozisyonun TAMAMI kapanir!), "
                        f"{removed_tp} fazla kar-al iptal edildi. Birden fazla bot kopyasi calisiyor olabilir!")
            tps = tps[-1:]
        if tps and orig > 0:
            oid, tpx = tps[0]
            amt = amounts.get(oid)
            expected = calc_tp_amount(symbol, orig)
            if amt is not None and expected > 0 and amt > expected * 1.2:
                log.warning(f"🧹 {symbol}: kar-al miktari {amt}, olmasi gereken {expected}. Duzeltiliyor.")
                if cancel_safe(oid, symbol):
                    place_tp_once(symbol, exit_side, expected, tpx)

    if tp_state != "done":
        if tps:
            with trail_lock:
                ct = trail_state.get(symbol)
                if ct:
                    ct["notp_n"] = 0
        elif stops:
            restore_missing_tp(symbol)       # ilk kar-al henuz dolmadi ama borsada yok: tekrar koy

    # ---------- STOP-LOSS EMIRLERI ----------
    if not stops:
        restore_missing_stop(symbol)         # borsada hic stop yok: yenisini koy (konamazsa kapat)
        return
    with trail_lock:
        cs = trail_state.get(symbol)
        if cs:
            cs["nostop_n"] = 0
    if len(stops) <= 1:
        return
    keep_id, keep_price = (max if exit_side == "sell" else min)(stops, key=lambda x: x[1])
    removed = 0
    for oid, _ in stops:
        if oid != keep_id and cancel_safe(oid, symbol):
            removed += 1
    with trail_lock:
        cur_st = trail_state.get(symbol)
        if cur_st:
            cur_st["sl_id"] = keep_id
            cur_st["sl_price"] = keep_price
            save_state()
    log.info(f"🧹 {symbol}: {len(stops)} stop bulundu, {removed} fazla stop iptal edildi, kalan SL: {keep_price}")


def restore_missing_tp(symbol):
    """Ilk kar-al konamadiysa / kaybolduysa tekrar koyar. Ust uste 2 kontrolde gorunmezse ve pozisyon
    hala TAM ise calisir (dolmus kar-alin ustune ikinci kez kar-al konmasin)."""
    with trail_lock:
        st = trail_state.get(symbol)
        if not st:
            return
        st["notp_n"] = st.get("notp_n", 0) + 1
        if st["notp_n"] < 2:
            return
        snap = dict(st)
    side = snap["side"]
    exit_side = "sell" if side == "long" else "buy"
    orig = float(snap.get("orig_amount") or 0)
    try:
        pos_amt = fetch_position_amount(symbol)
        if orig <= 0 or pos_amt <= 0:
            return
        tp_amount = calc_tp_amount(symbol, orig)
        if pos_amt < orig * 0.9 or tp_amount <= 0:
            with trail_lock:
                cur = trail_state.get(symbol)
                if cur:
                    cur["tp_state"] = "done"
                    save_state()
            return
        tp_price = float(snap.get("tp_price") or tp_price_for(
            symbol, float(snap["entry"]), float(snap["atr"]), side, float(snap.get("lev") or LEVERAGE)))
        if place_tp_once(symbol, exit_side, tp_amount, tp_price) is not None:
            with trail_lock:
                cur = trail_state.get(symbol)
                if cur:
                    cur["notp_n"] = 0
                    cur["tp_price"] = tp_price
                    save_state()
            log.info(f"🎯 {symbol}: eksik kar-al tekrar kondu @ {tp_price} miktar={tp_amount}")
    except Exception as e:
        log.warning(f"{symbol} kar-al yenilenemedi, sonra tekrar denenecek: {e}")
        note_ban(e)


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
    stops, tps, _ = scan_exit_orders(symbol, exit_side)
    log.info(f"{symbol}: borsada {len(stops)} stop-loss, {len(tps)} kar-al emri bulundu")
    sl_id, sl_price = None, None
    if stops:
        sl_id, sl_price = (max if exit_side == "sell" else min)(stops, key=lambda x: x[1])
        extra = [oid for oid, _ in stops if oid != sl_id]
        for oid in extra:
            cancel_safe(oid, symbol)
        if extra:
            log.info(f"🧹 {symbol}: {len(extra)} fazla stop temizlendi")
    if sl_id is None:
        try:
            lev_pos = float(pos.get("leverage") or LEVERAGE)
        except (TypeError, ValueError):
            lev_pos = float(LEVERAGE)
        d = sl_distance(price, atr, lev_pos)
        raw = price - d if side == "long" else price + d
        sl_price = float(ex.price_to_precision(symbol, raw))
        amount = float(ex.amount_to_precision(symbol, size))
        order = ex.create_order(symbol, "STOP_MARKET", exit_side, amount, None,
                                {"stopPrice": sl_price, "reduceOnly": True})
        sl_id = order.get("id")
        log.warning(f"🛟 {symbol} stop'suz pozisyon bulundu, SL kondu @ {sl_price}")
    ext = max(entry, price) if side == "long" else min(entry, price)
    opened_at = time.time()
    try:
        ut = (pos.get("info") or {}).get("updateTime")      # borsanin son guncelleme zamani (yaklasik pozisyon yasi)
        if ut:
            opened_at = min(opened_at, float(ut) / 1000.0)
    except (TypeError, ValueError):
        pass
    with trail_lock:
        try:
            lev = float(pos.get("leverage") or LEVERAGE)
        except (TypeError, ValueError):
            lev = float(LEVERAGE)
        size_amt = float(ex.amount_to_precision(symbol, size))
        trail_state[symbol] = {
            "side": side, "atr": atr, "ext": ext, "entry": entry, "sl_price": sl_price,
            "sl_id": sl_id, "amount": size_amt, "lev": lev,
            "orig_amount": size_amt, "tp_state": "pending" if tps else "done",
            "tp_price": tps[-1][1] if tps else None,
            "opened_at": opened_at,
        }
        save_state()
    log.info(f"🤝 {symbol} ({side}) pozisyonu sahiplenildi, gezen stop surecek. SL: {sl_price}")
    return True


# ---------------- DARALAN GEZEN STOP (TRAILING) ----------------
def trail_multiplier(profit_atr):
    """Kar (ATR cinsinden) buyudukce takip mesafesi (ATR carpani) daralir."""
    for limit, mult in TRAIL_TIERS:
        if profit_atr < limit:
            return mult
    return TRAIL_TIGHT_ATR


def update_trailing_stop_logic(symbol, price):
    """Anlik fiyatla gezen stopu hesaplar. REST'e SADECE stop gercekten tasinacaksa gider.
    Paylasilan state trail_lock altinda okunur/yazilir; REST cagrilari kilidin DISINDA yapilir."""
    with trail_lock:
        st = trail_state.get(symbol)
        if not st:
            return

        is_long = st["side"] == "long"
        exit_side = "sell" if is_long else "buy"
        entry = float(st["entry"])
        atr = st["atr"]

        # Fiyat yeni uc yapmasa da hesap en iyi fiyattan (ext) tekrar yapilir:
        # bekleme suresine veya REST hatasina takilan stop guncellemesi kaybolmaz.
        if is_long:
            st["ext"] = max(st["ext"], price)
            profit_atr = (st["ext"] - entry) / atr
            if profit_atr < TRAIL_START_ATR:
                return
            mult = trail_multiplier(profit_atr)
            dist = max(mult * atr, st["ext"] * TRAIL_MIN_DIST_PCT / 100.0)
            raw_sl = st["ext"] - dist
            if profit_atr >= BREAKEVEN_AT_ATR:
                raw_sl = max(raw_sl, entry * (1 + FEE_BUFFER_PCT / 100.0))
            new_sl = float(ex.price_to_precision(symbol, raw_sl))
            move_ok = new_sl > st["sl_price"] + TRAIL_MIN_STEP_ATR * atr and new_sl < price
        else:
            st["ext"] = min(st["ext"], price)
            profit_atr = (entry - st["ext"]) / atr
            if profit_atr < TRAIL_START_ATR:
                return
            mult = trail_multiplier(profit_atr)
            dist = max(mult * atr, st["ext"] * TRAIL_MIN_DIST_PCT / 100.0)
            raw_sl = st["ext"] + dist
            if profit_atr >= BREAKEVEN_AT_ATR:
                raw_sl = min(raw_sl, entry * (1 - FEE_BUFFER_PCT / 100.0))
            new_sl = float(ex.price_to_precision(symbol, raw_sl))
            move_ok = new_sl < st["sl_price"] - TRAIL_MIN_STEP_ATR * atr and new_sl > price

        if not move_ok or time.time() - st.get("last_move_ts", 0) < TRAIL_MIN_INTERVAL:
            return
        st["last_move_ts"] = time.time()
        old_sl_id = st["sl_id"]
        old_sl_price = st["sl_price"]
        amount_now = st.get("amount")

    # ---- kilit disinda: REST islemleri ----
    wait_if_banned()
    try:
        # Kismi kar alindiysa pozisyon kuculmustur: stopu GUNCEL miktarla koy
        try:
            amount_now = fetch_position_amount(symbol) or amount_now
        except Exception as e:
            log.warning(f"{symbol} guncel pozisyon miktari okunamadi, eski miktar kullanilacak: {e}")
            note_ban(e)

        new_order = ex.create_order(symbol, "STOP_MARKET", exit_side, amount_now, None,
                                    {"stopPrice": new_sl, "reduceOnly": True})
        cancel_safe(old_sl_id, symbol)
        log.info(f"📈 {symbol} gezen stop: {old_sl_price} -> {new_sl} (kar {profit_atr:.1f} ATR, takip {mult} ATR)")
        with trail_lock:
            cur = trail_state.get(symbol)
            if cur is st:
                cur["sl_id"] = new_order.get("id")
                cur["sl_price"] = new_sl
                cur["amount"] = amount_now
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
            try:
                dedupe_stops(symbol)          # her dakika: fazladan kar-al/stop kaldiysa temizle
            except Exception as e:
                log.warning(f"{symbol} fazla emir temizligi hatasi: {e}")
                note_ban(e)
        else:
            cancel_all_safe(symbol)
            with trail_lock:
                closed_st = trail_state.pop(symbol, None)
                cooldown[symbol] = time.time() + COOLDOWN_SECONDS
                save_state()
            log.info(f"{symbol} kapandi, {COOLDOWN_SECONDS // 60} dk bekleme basladi.")
            journal_close(symbol, closed_st, "borsada kapandi (stop / kar-al / gezen stop)")


# ---------------- YEDEK STOP (borsadaki stop calismazsa bot kendisi kapatir) ----------------
def fetch_position_amount(symbol):
    """Borsadaki pozisyonun GUNCEL miktari (1 REST istegi). Pozisyon yoksa 0.0. Hata olursa istisna firlatir."""
    for p in ex.fetch_positions([symbol]):
        if p.get("symbol") == symbol:
            c = float(p.get("contracts") or 0)
            if c > 0:
                return float(ex.amount_to_precision(symbol, c))
    return 0.0


def close_tracked_position(symbol, neden, journal_reason):
    """Takip edilen pozisyonu market emriyle kapatir, emirleri siler, durumu temizler, gunluge yazar."""
    with trail_lock:
        st0 = trail_state.get(symbol)
        if not st0:
            return False
        side = st0["side"]
    wait_if_banned()
    amount = None
    try:
        amount = fetch_position_amount(symbol) or None
    except Exception as e:
        log.error(f"{symbol} {neden}: pozisyon miktari okunamadi: {e}")
        note_ban(e)
        return False
    if not amount:
        return False                # zaten kapanmis: sync_positions temizler
    if not emergency_close(symbol, side, amount, neden=neden):
        return False
    cancel_all_safe(symbol)
    with trail_lock:
        closed_st = trail_state.pop(symbol, None)
        cooldown[symbol] = time.time() + COOLDOWN_SECONDS
        save_state()
    journal_close(symbol, closed_st, journal_reason)
    return True


def restore_missing_stop(symbol):
    """Borsada gercekten stop-loss yoksa (liste basariyla okunduysa) yenisini koyar.
    Ust uste 2 kontrolde stop gorunmezse harekete gecer. Okuma / baglanti / ban hatasinda pozisyon KAPATILMAZ;
    sadece fiyat stopu gectiyse ya da borsa stop emrini REDDEDERSE kapatilir."""
    with trail_lock:
        st = trail_state.get(symbol)
        if not st:
            return
        st["nostop_n"] = st.get("nostop_n", 0) + 1
        n = st["nostop_n"]
        side, entry, atr = st["side"], float(st["entry"]), float(st["atr"])
        lev = float(st.get("lev") or LEVERAGE)
        amount = float(st.get("amount") or 0)
        target = float(st.get("sl_price") or 0)          # son takip edilen stop (gezen stop ilerlediyse orasi)
    if n < 2:
        return
    exit_side = "sell" if side == "long" else "buy"
    try:
        price = float(ex.fetch_ticker(symbol)["last"])
        pos_amt = fetch_position_amount(symbol)
        if target <= 0:
            d = sl_distance(entry, atr, lev)
            target = entry - d if side == "long" else entry + d
        sl_price = float(ex.price_to_precision(symbol, target))
    except Exception as e:
        log.warning(f"{symbol}: eksik stop icin veri okunamadi, pozisyona dokunulmadi: {e}")
        note_ban(e)
        return
    if pos_amt <= 0:
        return                                           # pozisyon kapanmis: sync_positions temizler
    already_past = price <= sl_price if side == "long" else price >= sl_price
    if already_past:
        log.warning(f"🆘 {symbol}: stop yoktu ve fiyat stop seviyesini gecmis, pozisyon kapatiliyor.")
        close_tracked_position(symbol, "stop yok + fiyat gecmis", "stop yoktu, fiyat stopu gecmisti")
        return
    try:
        order = ex.create_order(symbol, "STOP_MARKET", exit_side, pos_amt or amount, None,
                                {"stopPrice": sl_price, "reduceOnly": True})
    except Exception as e:
        log.error(f"{symbol} eksik stop yenilenemedi: {e}")
        if note_ban(e) or isinstance(e, ccxt.NetworkError):
            log.warning(f"{symbol}: baglanti/limit hatasi, pozisyon kapatilmadi, sonraki turda tekrar denenecek.")
            return
        log.warning(f"🆘 {symbol}: borsa stop emrini reddetti, pozisyon kapatiliyor.")
        close_tracked_position(symbol, "stop konamadi", "stop yoktu ve konamadi")
        return
    with trail_lock:
        cur = trail_state.get(symbol)
        if cur:
            cur["sl_id"] = order.get("id")
            cur["sl_price"] = sl_price
            cur["nostop_n"] = 0
            save_state()
    log.warning(f"🛟 {symbol}: borsada stop yoktu, yeni SL kondu @ {sl_price}")


def backup_stop_check(symbol, price):
    """Fiyat stopu BACKUP_STOP_GAP_PCT kadar gectiginde pozisyonu bot kendisi market emriyle kapatir.
    Kontrol WS fiyatiyla yapilir (REST'e SADECE tetiklenince gider)."""
    with trail_lock:
        st = trail_state.get(symbol)
        if not st:
            return
        side = st["side"]
        sl = float(st["sl_price"])
        if time.time() - st.get("backup_ts", 0) < 20:       # ust uste tekrar denemesin
            return
    gap = BACKUP_STOP_GAP_PCT / 100.0
    breach = price <= sl * (1 - gap) if side == "long" else price >= sl * (1 + gap)
    if not breach:
        return
    with trail_lock:
        st["backup_ts"] = time.time()
    log.warning(f"🛟 {symbol} fiyat stopu gecti ({price} / SL {sl}), yedek stop kapatiyor.")
    close_tracked_position(symbol, "yedek stop", "yedek stop (borsa stopu calismadi)")


adopt_needed = threading.Event()      # takip edilmeyen pozisyon olabilir: sahiplenme basarana kadar tekrar denenir
adopt_needed.set()


def adopt_untracked():
    """Borsada acik olup takip edilmeyen pozisyonlari sahiplenir. Hepsi tamamsa True."""
    all_ok = True
    for p in ex.fetch_positions():
        if float(p.get("contracts") or 0) <= 0:
            continue
        symbol = p["symbol"]
        with trail_lock:
            known = symbol in trail_state
        if known:
            continue
        with open_lock:
            opening = symbol in opening_symbols
        if opening:
            all_ok = False
            continue
        try:
            if not adopt_position(symbol, p):
                all_ok = False
        except Exception as e:
            log.error(f"{symbol} sahiplenme hatasi, tekrar denenecek: {e}")
            note_ban(e)
            all_ok = False
    return all_ok


def trail_loop():
    log.info("Gezen stop dongusu basladi (WS fiyati, REST sadece stop tasirken).")

    last_sync = 0.0        # acilista hemen bir temizlik turu
    last_adopt = 0.0
    while True:
        interval = TRAIL_TICK_SECONDS
        try:
            wait_if_banned()
            if adopt_needed.is_set() and time.time() - last_adopt >= POSITION_SYNC_SECONDS:
                last_adopt = time.time()
                adopt_needed.clear()
                try:
                    if not adopt_untracked():
                        adopt_needed.set()
                except Exception:
                    adopt_needed.set()
                    raise
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
                        backup_stop_check(symbol, price)
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


# ---------------- KAGIT MODU SAYACI ----------------
paper_lock = threading.Lock()
paper_open_trades = {}     # "coin|tetik" -> sanal islem
paper_cool = {}            # "coin|tetik" -> tekrar sayilmayacagi zaman
paper_stats = {}           # tetik -> sayaclar
PAPER_TRIGGERS = ("ema", "ema_canli", "kirilim", "donus")


def _paper_save():
    """Cagiran taraf paper_lock'u tutuyor olmali."""
    try:
        tmp = PAPER_STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"open": paper_open_trades, "cool": paper_cool, "stats": paper_stats}, f)
        os.replace(tmp, PAPER_STATE_FILE)
    except Exception as e:
        log.warning(f"Kagit modu sayaci kaydedilemedi: {e}")


def paper_load():
    try:
        with open(PAPER_STATE_FILE) as f:
            d = json.load(f)
        with paper_lock:
            paper_open_trades.update(d.get("open", {}))
            paper_cool.update(d.get("cool", {}))
            paper_stats.update(d.get("stats", {}))
    except Exception:
        pass


def paper_record(cand):
    """Sinyali sanal islem olarak acar. Her tetik ayri sayilir (biri digerini engellemez)."""
    symbol, side, trigger = cand["symbol"], cand["side"], cand.get("trigger", "ema")
    key = f"{symbol}|{trigger}"
    now = time.time()
    with ws_lock:
        t = tickers.get(to_raw(symbol))
    price = float(t["price"]) if t else float(cand["price"])
    d = sl_distance(price, cand["atr"], LEVERAGE)
    sgn = 1 if side == "long" else -1
    with paper_lock:
        if key in paper_open_trades or paper_cool.get(key, 0) > now or d <= 0 or price <= 0:
            return False
        tr = {"symbol": symbol, "side": side, "trigger": trigger, "entry": price,
              "sl": price - sgn * d, "tp": price + sgn * d * TP1_RR, "opened": now, "checked": now}
        paper_open_trades[key] = tr
        _paper_save()
    log.info(f"📝 [KAGIT] {side.upper()} {symbol} tetik={trigger} onay={cand['confirm']} fiyat={price} "
             f"SL={tr['sl']:.8g} TP1={tr['tp']:.8g} hacimx={cand.get('rv', 0):.1f} (emir verilmedi)")
    journal("signal", symbol, side=side, confirm=cand["confirm"], lev=LEVERAGE, entry=price,
            sl=round(tr["sl"], 10), tp=round(tr["tp"], 10), reason=f"kagit modu tetik={trigger}")
    return True


def paper_check(prices, now=None):
    """Acik sanal islemleri anlik fiyatlarla kontrol eder. Sonuc R cinsinden: 1R = stop mesafesi."""
    now = time.time() if now is None else now
    done = []
    with paper_lock:
        for key, tr in list(paper_open_trades.items()):
            gap = now - tr["checked"]
            price = prices.get(tr["symbol"])
            if gap > PAPER_MAX_GAP:                       # arada ne oldugu bilinmiyor: sayma
                paper_open_trades.pop(key)
                done.append((tr, "veri_boslugu", None, None))
                continue
            if price is None:
                continue
            tr["checked"] = now
            sgn = 1 if tr["side"] == "long" else -1
            if (price - tr["sl"]) * sgn <= 0:
                result, exit_price = "stop", price         # stop kaymasi: gorulen fiyat
            elif (price - tr["tp"]) * sgn >= 0:
                result, exit_price = "kar_al", tr["tp"]
            elif now - tr["opened"] >= PAPER_MAX_HOURS * 3600:
                result, exit_price = "sure_doldu", price
            else:
                continue
            paper_open_trades.pop(key)
            paper_cool[key] = now + COOLDOWN_SECONDS
            move = (exit_price - tr["entry"]) / tr["entry"] * sgn * 100.0
            net = move - PAPER_FEE_PCT
            r = net / (abs(tr["entry"] - tr["sl"]) / tr["entry"] * 100.0)
            st = paper_stats.setdefault(tr["trigger"], {"n": 0, "kar_al": 0, "stop": 0, "sure_doldu": 0,
                                                       "net_pct": 0.0, "net_r": 0.0})
            st["n"] += 1
            st[result] += 1
            st["net_pct"] += net
            st["net_r"] += r
            done.append((tr, result, net, r))
        if done:
            _paper_save()
    for tr, result, net, r in done:
        if net is None:
            log.info(f"📝 [KAGIT] {tr['symbol']} tetik={tr['trigger']}: veri boslugu, sayilmadi.")
        else:
            log.info(f"📊 [KAGIT SONUC] {tr['symbol']} {tr['side'].upper()} tetik={tr['trigger']} {result} "
                     f"net=%{net:+.2f} fiyat ({r:+.2f}R)")
    return done


def paper_summary():
    out = {}
    with paper_lock:
        for trig in PAPER_TRIGGERS:
            st = paper_stats.get(trig) or {}
            n = st.get("n", 0)
            out[trig] = {
                "kapanan": n,
                "kar_al": st.get("kar_al", 0),
                "stop": st.get("stop", 0),
                "sure_doldu": st.get("sure_doldu", 0),
                "kar_al_orani_yuzde": round(st["kar_al"] / n * 100, 1) if n else None,
                "net_toplam_R": round(st["net_r"], 2) if n else None,
                "islem_basi_R": round(st["net_r"] / n, 3) if n else None,
                "net_toplam_fiyat_yuzde": round(st["net_pct"], 2) if n else None,
                "acik": sum(1 for t in paper_open_trades.values() if t["trigger"] == trig),
                "yeterli_veri": n >= PAPER_MIN_SAMPLE,
            }
    return out


def paper_loop():
    last_report = time.time()
    while True:
        try:
            if ws_healthy():
                with paper_lock:
                    syms = {t["symbol"] for t in paper_open_trades.values()}
                with ws_lock:
                    prices = {s: tickers[to_raw(s)]["price"] for s in syms if to_raw(s) in tickers}
                paper_check(prices)
            if time.time() - last_report >= 3600:
                last_report = time.time()
                for trig, v in paper_summary().items():
                    log.info(f"📊 [KAGIT OZET] {trig}: kapanan={v['kapanan']} kar-al={v['kar_al']} stop={v['stop']} "
                             f"net={v['net_toplam_R']}R islem basi={v['islem_basi_R']}R acik={v['acik']}")
        except Exception as e:
            log.error(f"Kagit modu sayaci hatasi: {e}")
        time.sleep(2)


# ---------------- ISLEM ACMA ----------------
def get_open_symbols():
    positions = ex.fetch_positions()
    return [p["symbol"] for p in positions if float(p.get("contracts") or 0) > 0]


last_loss_log = 0.0


def daily_loss_hit():
    global last_loss_log
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
        if time.time() - last_loss_log > LOSS_LOG_EVERY:
            log.warning(f"🚫 Gunluk zarar limiti doldu (%{loss_pct:.1f}). Gun sonuna kadar yeni islem yok.")
            last_loss_log = time.time()
        return True
    return False


def emergency_close(symbol, side, amount, neden="korumasiz pozisyon"):
    """Pozisyonu market emriyle (reduceOnly) kapatir; 3 kez dener. Basarili mi diye True/False doner."""
    close_side = "sell" if side == "long" else "buy"
    for attempt in range(1, 4):
        try:
            wait_if_banned()
            ex.create_order(symbol, "market", close_side, amount, None, {"reduceOnly": True})
            log.warning(f"🔒 {symbol} kapatildi ({neden}, deneme {attempt}).")
            return True
        except Exception as e:
            log.error(f"{symbol} kapatma hatasi ({neden}, deneme {attempt}/3): {e}")
            note_ban(e)
            time.sleep(2)
    return False


def min_notional(symbol):
    """Borsanin bu coin icin en kucuk emir degeri (USDT). Bilgi yoksa 5 varsayilir."""
    try:
        m = (ex.markets or {}).get(symbol) or {}
        c = ((m.get("limits") or {}).get("cost") or {}).get("min")
        return float(c) if c else 5.0
    except Exception:
        return 5.0


def open_trade(cand, open_symbols):
    symbol, side, atr = cand["symbol"], cand["side"], cand["atr"]
    lev = LEVERAGE

    if not TRADING_ENABLED:
        paper_record(cand)          # KAGIT MODU: emir verilmez, sadece sanal sonuc sayilir
        return False

    trigger = cand.get("trigger", "ema")
    risk_pct = RISK_PCT * (1.0 if trigger == "ema" else EARLY_RISK_FACTOR)

    with open_lock:
        opening_symbols.add(symbol)
    try:
        # Bu coinde pozisyon yok: onceki islemden kalan artik stop/kar-al emirleri varsa temizle.
        cancel_all_safe(symbol)
        try:
            ex.set_margin_mode("isolated", symbol)
        except Exception:
            pass

        # Kaldirac: once hedef (15x), coin desteklemezse sirayla daha dusuk degerler denenir.
        lev_ok = None
        for try_lev in (lev,) + LEV_FALLBACKS:
            try:
                ex.set_leverage(try_lev, symbol)
                lev_ok = try_lev
                break
            except Exception as e:
                log.warning(f"{symbol} {try_lev}x kaldirac ayarlanamadi: {e}")
                note_ban(e)
        if lev_ok is None:
            log.warning(f"⏭️ {symbol}: kaldirac ayarlanamadi, islem atlandi.")
            return False
        if lev_ok != lev:
            log.info(f"{symbol}: {lev}x desteklenmiyor, {lev_ok}x kullanilacak.")
        lev = lev_ok

        # ---- RISK BAZLI POZISYON BOYUTU ----
        bal = ex.fetch_balance()["USDT"]
        usdt_free = float(bal["free"])
        usdt_total = float(bal["total"])
        sl_dist = sl_distance(cand["price"], atr, lev)
        if sl_dist <= 0 or usdt_total <= 0:
            return False
        risk_usdt = usdt_total * risk_pct / 100.0
        amount_risk = risk_usdt / sl_dist
        amount_cap = usdt_free * MARGIN_PCT * lev / cand["price"]
        amount = float(ex.amount_to_precision(symbol, min(amount_risk, amount_cap)))
        if amount <= 0:
            return False
        min_cost = min_notional(symbol)
        if amount * cand["price"] < min_cost * 1.02:
            # Kucuk bakiyede yarim riskli erken tetik borsa alt sinirinin altinda kalabilir:
            # alt sinira yukselt, ama stop zarari %RISK_PCT'yi ve marjin sinirini gecmiyorsa.
            bumped = float(ex.amount_to_precision(symbol, min_cost * 1.05 / cand["price"]))
            if bumped * sl_dist <= usdt_total * RISK_PCT / 100.0 and bumped <= amount_cap:
                amount = bumped
        if amount * cand["price"] < min_cost * 1.02:
            log.warning(f"⏭️ {symbol}: hesaplanan pozisyon degeri {amount * cand['price']:.2f} USDT, borsa en az "
                        f"{min_cost:.0f} USDT istiyor, bu islem atlandi.")
            return False
        real_risk = amount * sl_dist
        log.info(f"⚖️ {symbol} boyut: risk hedefi {risk_usdt:.2f} USDT, stop mesafesi %{sl_dist / cand['price'] * 100:.2f}, "
                 f"stop olursa ~{real_risk:.2f} USDT kayip, marjin ~{amount * cand['price'] / lev:.2f} USDT")

        if side == "long":
            order = ex.create_market_buy_order(symbol, amount)
        else:
            order = ex.create_market_sell_order(symbol, amount)
        entry = float(order.get("average") or order.get("price") or cand["price"])
        log.info(f"🚀 ISLEM ACILDI ({side.upper()}): {symbol} | Tetik: {trigger} | Onay: {cand['confirm']} | "
                 f"Miktar: {amount} | Kaldirac: {lev}x | Giris: {entry}")
        open_symbols.append(symbol)

        prot = place_protection(symbol, entry, amount, atr, side, lev)
        if prot:
            with trail_lock:
                trail_state[symbol] = {
                    "side": side, "atr": atr, "ext": entry, "entry": entry,
                    "sl_price": prot["sl_price"], "sl_id": prot["sl_id"], "amount": amount,
                    "lev": lev,
                    "orig_amount": amount,
                    "tp_state": "pending" if prot.get("tp_amount", 0) > 0 else "done",
                    "tp_price": prot["tp_price"],
                    "opened_at": time.time(), "trigger": trigger,
                }
                save_state()
            journal("open", symbol, side=side, confirm=cand["confirm"], lev=lev, entry=entry, amount=amount,
                    sl=prot["sl_price"], tp=prot["tp_price"],
                    risk_usdt=round(amount * abs(entry - prot["sl_price"]), 4), reason=f"tetik={trigger}")
        else:
            log.error(f"{symbol} SL yok, pozisyon kapatiliyor!")
            if emergency_close(symbol, side, amount):
                if symbol in open_symbols:
                    open_symbols.remove(symbol)
            else:
                log.critical(f"🆘 {symbol} pozisyonu KORUMASIZ ve KAPATILAMADI! Sahiplenme (SL koyma) deneniyor. "
                             f"Gerekirse ELLE mudahale et!")
                try:
                    adopt_position(symbol, {"side": side, "contracts": amount, "entryPrice": entry,
                                            "leverage": lev})
                except Exception as e:
                    log.critical(f"🆘 {symbol} sahiplenme de basarisiz: {e} -> HEMEN ELLE KAPAT!")
                with trail_lock:
                    if symbol not in trail_state:
                        adopt_needed.set()          # trail_loop tekrar denesin
        return True
    finally:
        with open_lock:
            opening_symbols.discard(symbol)


# ---------------- ROTASYON (bayat pozisyonu onayli yeni trendle degistir) ----------------
rotate_times = []
rotate_lock = threading.Lock()


def position_pnl_margin_pct(st, price):
    """Pozisyonun marjine gore anlik kar/zarar yuzdesi (komisyon haric). Veri yoksa None."""
    try:
        entry = float(st.get("entry") or 0)
        if entry <= 0 or price <= 0:
            return None
        lev = float(st.get("lev") or LEVERAGE)
        move = (price - entry) / entry if st["side"] == "long" else (entry - price) / entry
        return move * lev * 100.0
    except (TypeError, ValueError, KeyError):
        return None


def own_trend_alive(symbol, side):
    """Pozisyondaki coinin kendi yonunde trendi suruyor mu? (EMA9/21 yonde + fiyat EMA50'nin dogru tarafinda).
    WS onbellekten, REST yok. Veri yoksa True doner (emin degilsek bayat saymayiz)."""
    try:
        if not is_fresh(to_raw(symbol)):
            return True
        df = get_df(symbol)
        if df is None or len(df) < 60:
            return True
        last = df.iloc[-1]
        if any(pd.isna(last[k]) for k in ("ema_fast", "ema_slow", "ema_trend")):
            return True
        ef, es, e50, close = (float(last["ema_fast"]), float(last["ema_slow"]),
                              float(last["ema_trend"]), float(last["close"]))
        if side == "long":
            return ef > es and close > e50
        return ef < es and close < e50
    except Exception:
        return True


def find_stale_positions():
    """Bayat pozisyonlar: yeterince eski + marjine gore kar/zarar bandin icinde + kendi yonunde trend kalmamis.
    REST istegi atmaz. Dondurur: [(coin, kar%, yas_dk)], en eskisi basta."""
    if not ws_healthy():
        return []
    now = time.time()
    with trail_lock:
        snap = {s: dict(st) for s, st in trail_state.items()}
    stale = []
    for symbol, st in snap.items():
        age = now - float(st.get("opened_at") or now)
        if age < ROTATE_MIN_AGE_SECONDS:
            continue
        with ws_lock:
            t = tickers.get(to_raw(symbol))
        if not t:
            continue
        pnl = position_pnl_margin_pct(st, t["price"])
        if pnl is None or not (ROTATE_PNL_MIN <= pnl <= ROTATE_PNL_MAX):
            continue
        if st.get("trigger") == "donus":      # donus islemi trendin tersine acilir: "bayat" sayilmaz
            continue
        if own_trend_alive(symbol, st["side"]):
            continue
        stale.append((symbol, pnl, age / 60.0))
    stale.sort(key=lambda x: -x[2])       # en eski once
    return stale


def close_position_for_rotation(symbol):
    return close_tracked_position(symbol, "rotasyon", "rotasyon (bayat pozisyon)")


def rotate_if_needed(open_symbols):
    """Pozisyon limiti doluyken calisir. Bayat pozisyon varsa ve en az ROTATE_MIN_CONFIRM onayli bir aday bulunursa
    bayati kapatip yeni coine girer. Rotasyon yapildiysa True doner."""
    if not ROTATE_ENABLED or not TRADING_ENABLED:
        return False
    now = time.time()
    with rotate_lock:
        rotate_times[:] = [t for t in rotate_times if now - t < 86400]
        if rotate_times and now - rotate_times[-1] < ROTATE_GAP_SECONDS:
            return False
        if len(rotate_times) >= ROTATE_MAX_PER_DAY:
            return False

    stale = find_stale_positions()
    if not stale:
        return False
    names = ", ".join(f"{s.split('/')[0]} (%{p:+.1f}, {a:.0f} dk)" for s, p, a in stale)
    log.info(f"💤 Bayat pozisyon(lar): {names}. Daha guclu aday araniyor...")

    if daily_loss_hit():
        return False

    with trail_lock:
        skip = set(open_symbols) | set(cooldown.keys())
    candidates = scan_market(skip)
    strong = [c for c in candidates                     # erken sinyal icin acik pozisyon kapatilmaz
              if c.get("trigger", "ema") == "ema" and c["confirm"] >= ROTATE_MIN_CONFIRM]
    if not strong:
        log.info(f"Rotasyon icin yeterince onayli aday yok (en az {ROTATE_MIN_CONFIRM} onay), bayat pozisyon korunuyor.")
        return False

    cand = strong[0]
    symbol, pnl, age_min = stale[0]
    log.info(f"🔄 ROTASYON: {symbol} (%{pnl:+.1f}, {age_min:.0f} dk) kapatilip "
             f"{cand['symbol']} {cand['side'].upper()} (onay {cand['confirm']}) acilacak.")
    if not close_position_for_rotation(symbol):
        log.warning(f"{symbol} rotasyon icin kapatilamadi, yeni islem acilmadi.")
        return False
    if symbol in open_symbols:
        open_symbols.remove(symbol)
    with rotate_lock:
        rotate_times.append(time.time())
    try:
        open_trade(cand, open_symbols)
    except Exception as e:
        log.error(f"{cand['symbol']} rotasyon sonrasi islem acma hatasi: {e}")
        note_ban(e)
    return True


def bot_loop():
    log.info("Bot arkaplan dongusu basladi.")
    if not TRADING_ENABLED:
        log.warning("📝 KAGIT MODU: bot yeni emir vermeyecek, sinyallerin sanal sonucunu sayacak (/paper).")
    open_symbols = []
    last_account = 0.0
    loss_hit = False
    while True:
        wait_if_banned()
        try:
            account_due = False
            if TRADING_ENABLED:
                account_due = time.time() - last_account >= ACCOUNT_CHECK_SECONDS
                if account_due:
                    try:
                        open_symbols = get_open_symbols()
                    except Exception as e:
                        log.error(f"Pozisyonlar cekilemedi: {e}")
                        note_ban(e)
                        time.sleep(30)
                        continue
                    last_account = time.time()

                if len(open_symbols) >= MAX_OPEN:
                    if account_due:
                        log.info(f"Pozisyon limiti dolu ({len(open_symbols)}/{MAX_OPEN}), yeni tarama yapilmadi.")
                        try:
                            rotate_if_needed(open_symbols)
                        except Exception as e:
                            log.error(f"Rotasyon hatasi: {e}")
                            note_ban(e)
                    time.sleep(LOOP_SECONDS)
                    continue

                if account_due:
                    loss_hit = daily_loss_hit()
                if loss_hit:
                    time.sleep(LOOP_SECONDS)
                    continue
            else:
                open_symbols = []

            now = time.time()
            with trail_lock:
                for s in [s for s, t in cooldown.items() if t <= now]:
                    cooldown.pop(s, None)
                skip = set(open_symbols) | set(cooldown.keys()) if TRADING_ENABLED else set()

            candidates = scan_market(skip)

            if candidates and TRADING_ENABLED:
                # emirden hemen once guncel pozisyon ve zarar limiti kontrolu
                open_symbols = get_open_symbols()
                last_account = time.time()
                if len(open_symbols) >= MAX_OPEN:
                    candidates = []
                else:
                    loss_hit = daily_loss_hit()
                    if loss_hit:
                        candidates = []

            for cand in candidates:
                if len(open_symbols) >= MAX_OPEN:
                    break
                symbol = cand["symbol"]
                with trail_lock:
                    cooling = TRADING_ENABLED and symbol in cooldown
                if symbol in open_symbols or cooling:
                    continue
                try:
                    opened = open_trade(cand, open_symbols)
                    if TRADING_ENABLED:
                        if not opened:
                            with trail_lock:
                                cooldown[symbol] = time.time() + FAILED_OPEN_COOLDOWN
                                save_state()
                        time.sleep(2)
                except Exception as inner_err:
                    log.error(f"{symbol} islem hatasi: {inner_err}")
                    # -4411: TradFi-Perps sozlesmesi imzalanmamis (hisse/emtia vadelisi): 24 saat atla
                    tradfi_err = "-4411" in str(inner_err)
                    with trail_lock:
                        cooldown[symbol] = time.time() + (24 * 3600 if tradfi_err else FAILED_OPEN_COOLDOWN)
                        save_state()
                    if tradfi_err:
                        log.warning(f"🚫 {symbol} TradFi-Perps sozlesmesi gerektiriyor, 24 saat atlanacak.")
                        continue
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
            _load_ban()
            paper_load()
            threading.Thread(target=paper_loop, daemon=True).start()
            threading.Thread(target=ws_thread, daemon=True).start()
            threading.Thread(target=bot_loop, daemon=True).start()
            threading.Thread(target=trail_loop, daemon=True).start()


start_bot_once()


@app.route("/")
def index():
    with ws_lock:
        funding_n = len(funding)
    return jsonify({
        "status": "running",
        "emir_veriyor": TRADING_ENABLED,
        "websocket": ws_healthy(),
        "websocket_bagli": ws_connected,
        "fiyat_yasi_sn": round(time.time() - tickers_ts, 1) if tickers_ts else None,
        "funding_yasi_sn": round(time.time() - funding_ts, 1) if funding_ts else None,
        "funding_coin_sayisi": funding_n,
        "izlenen_coin": len(subscribed),
        "takip_edilen": list(trail_state.keys()),
        "bekleyen_coin": len(cooldown),
    })


@app.route("/scan")
def scan_view():
    """Hareketli coinler + son tarama adaylari + acik pozisyonlarin durumu (kar %, yas, rotasyona aday mi)."""
    now = time.time()
    with trail_lock:
        snap = {s: dict(st) for s, st in trail_state.items()}
    stale_names = {s for s, _, _ in find_stale_positions()}
    pozisyonlar = []
    for s, st in snap.items():
        with ws_lock:
            t = tickers.get(to_raw(s))
        pnl = position_pnl_margin_pct(st, t["price"]) if t else None
        pozisyonlar.append({
            "coin": s,
            "yon": st.get("side"),
            "kar_marjin_yuzde": round(pnl, 2) if pnl is not None else None,
            "yas_dk": round((now - float(st.get("opened_at") or now)) / 60),
            "bayat": s in stale_names,
        })
    btc = btc_change_pct()
    return jsonify({
        "son_tarama_sn_once": round(now - last_scan["time"]) if last_scan["time"] else None,
        "tarama_kaynagi": last_scan["kaynak"],
        "btc_son_15dk_yuzde": round(btc, 2) if btc is not None else None,
        "hareketli": last_scan.get("hareketli", []),
        "adaylar": last_scan["adaylar"],
        "pozisyonlar": pozisyonlar,
        "rotasyon_acik": ROTATE_ENABLED,
        "bugunku_rotasyon": len([t for t in rotate_times if now - t < 86400]),
    })


@app.route("/paper")
def paper_view():
    """Kagit modu: tetik bazinda sanal sonuclar (komisyon dusulmus; gezen stop ve kismi kar-al hesaba katilmaz)."""
    return jsonify({"kagit_modu": not TRADING_ENABLED, "en_az_ornek": PAPER_MIN_SAMPLE,
                    "tetikler": paper_summary()})


@app.route("/journal")
def journal_view():
    """Kapanan islemlerin ozeti: islem sayisi, kazanma orani, net kar/zarar, islem basi beklenen deger + son kayitlar."""
    rows = []
    try:
        with open(JOURNAL_FILE, newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        pass
    nets = []
    for r in rows:
        if r.get("event") == "close" and r.get("net_usdt") not in (None, ""):
            try:
                nets.append(float(r["net_usdt"]))
            except ValueError:
                pass
    wins = [n for n in nets if n > 0]
    losses = [n for n in nets if n <= 0]
    return jsonify({
        "kapanan_islem": len(nets),
        "kazanan": len(wins),
        "kazanma_orani_yuzde": round(len(wins) / len(nets) * 100, 1) if nets else None,
        "net_toplam_usdt": round(sum(nets), 3) if nets else None,
        "islem_basi_net_usdt": round(sum(nets) / len(nets), 4) if nets else None,
        "ort_kazanc_usdt": round(sum(wins) / len(wins), 4) if wins else None,
        "ort_kayip_usdt": round(sum(losses) / len(losses), 4) if losses else None,
        "son_kayitlar": rows[-30:],
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
