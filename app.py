"""
Binance USDT-M Futures botu (7/24) - WEBSOCKET VERI + REST SADECE ISLEM
- Fiyat ve mum verisi WebSocket'ten gelir (REST'e istek atilmaz)
- REST sadece: al/sat, stop koy, gezen stop guncelle, pozisyon senkronu (60 sn'de 1 istek)
- Acilista her coinin gecmis mumlari REST'ten BIR KEZ cekilir (yavas, aralikli), sonrasi hep WebSocket
- WebSocket calismazsa: kontrollu REST yedek modu (az coin, yavas, ban korumali)
- 20M-800M hacim araligindaki TUM coinler taranir, 0-100 trend puani, LONG + SHORT
- ATR stop-loss, kismi kar al (MARJINE gore %20), KADEMELI ATR gezen stop, stop sonrasi bekleme, gunluk zarar limiti
- Render'da yeniden baslarsa acik pozisyonlari sahiplenir

Gerekli paketler (requirements.txt): ccxt, pandas, flask, gunicorn  (websocket icin EK PAKET GEREKMEZ)
"""
import os
import re
import sys
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
from urllib.parse import urlparse, unquote

import ccxt
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
SL_MAX_MARGIN_PCT = 10.0   # ...ama stop marjinde en fazla bu kadar zarar ettirecek yerde olur (volatil coinde stop cok uzaga dusmesin)
TP1_MARGIN_PCT = 20.0      # Ilk kar al: MARJINE gore %20 kar (fiyat hareketi = 20 / kaldirac %)
TP1_CLOSE_PCT = 0.50       # ilk kar alda pozisyonun %50'si kapanir, kalan gezen stopla gider

# --- Kademeli gezen stop (kar buyudukce daralir, ATR'ye baglidir) ---
# Kar (ATR cinsinden) -> takip carpani. Kar sinirin altindaysa o carpan kullanilir.
# Gezen stop islem acilir acilmaz fiyati izler: ilk kademede takip mesafesi SL ile ayni (1.5 ATR),
# yani fiyat yukseldikce stop hemen arkasindan gelir (3x gibi gevsek bir bekleme yok).
TRAIL_TIERS = ((2.0, SL_ATR),)           # kar < 2 ATR: 1.5x ATR takip
TRAIL_TIGHT_ATR = 1.2      # kar 2 ATR'yi gecince en dar takip (1.2 x ATR)
TRAIL_DIST_MARGIN_PCT = 3.0   # stop, fiyatin en fazla marjinde bu kadar gerisinde durur (fiyat hareketi = pct / kaldirac %). Pozisyon acildigi andan itibaren hep gecerli
BREAKEVEN_AT_ATR = 1.0     # kar bu kadar ATR olunca stop en az girise cekilir (zarar etmeme)

COOLDOWN_SECONDS = 2 * 3600   # bir coin kapaninca bu sure tekrar girilmez
DAILY_LOSS_LIMIT_PCT = 10.0   # gun basi bakiyeye gore bu kadar zarar olursa gun sonuna kadar yeni islem yok
ALLOW_LONG = True
ALLOW_SHORT = True
STATE_FILE = "bot_state.json"
TESTNET = False

# --- WebSocket / istek ayarlari ---
# Binance futures websocket adresleri yonlendirmeli: fiyat ve mum akislari /market altinda.
# Ikinci adres eski (legacy) adres, sadece yedek olarak denenir.
WS_URLS = ["wss://fstream.binance.com/market/stream", "wss://fstream.binance.com/stream"]
WS_IDLE_RECONNECT = 30      # WS bagli ama bu kadar sn hic mesaj gelmezse yeniden baglan
MAX_STREAMS = 300           # tek baglantida en fazla bu kadar coinin mum akisi istenir
KLINE_KEEP = 150            # her coin icin bellekte tutulan mum sayisi
SEED_LIMIT = 100            # acilista REST'ten cekilen gecmis mum sayisi
SEED_SLEEP = 0.5            # gecmis mum cekerken coinler arasi bekleme (sn)
WS_STALE_SECONDS = 45       # WS'ten bu kadar sn veri gelmezse "calismiyor" sayilir
KLINE_STALE_SECONDS = 600   # bir coinin mumu bu kadar sn guncellenmezse bayat sayilir
TRAIL_TICK_SECONDS = 2      # WS aciksa gezen stop kontrol sikligi (REST istegi atmaz)
TRAIL_CHECK_SECONDS = 20    # WS yoksa REST yedek kontrol sikligi
POSITION_SYNC_SECONDS = 60  # pozisyon kapandi mi kontrolu (tek REST istegi)
HTF_CACHE_SECONDS = 600     # 1s verisi bu sure onbellekte tutulur
FALLBACK_MAX_COINS = 30     # WS yoksa REST yedek modunda taranacak en fazla coin
FALLBACK_SLEEP = 0.5        # REST yedek modunda coinler arasi bekleme (sn)
REST_TICKER_MIN_GAP = 120   # WS yoksa coin listesi REST'ten en az bu aralikla cekilir

# ===================== TEKİL KOPYA KİLİDİ (LOCK) =====================
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
            log.error("⛔ KRİTİK: Botun başka bir kopyası zaten çalışıyor! Bu kopya kapatılıyor.")
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
        trail_state.update(trail)
        log.info(f"State yuklendi: {list(trail_state.keys())}")
    except Exception:
        pass


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
tickers_ts = 0.0      # son WS ticker mesaji zamani
first_tick_ts = 0.0   # ilk WS ticker mesaji zamani
klines = {}           # "BTCUSDT" -> [[t, o, h, l, c, v, kapandi_mi], ...]
kline_ts = {}         # "BTCUSDT" -> son guncelleme zamani
dirty = set()         # mum boslugu olan (yeniden yuklenmesi gereken) coinler
subscribed = set()    # mum akisi istenen coinler
ws_connected = False
rest_ticker_ts = 0.0
COLS = ["timestamp", "open", "high", "low", "close", "volume", "closed"]
STABLE_BASES = {"USDC", "BUSD", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "AEUR", "EURI", "USD1", "XUSD"}
START_TS = time.time()


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
    global tickers_ts, first_tick_ts
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
                    tickers[it["s"]] = {"price": float(it["c"]), "vol": float(it["q"])}
                except (KeyError, ValueError, TypeError):
                    continue
            tickers_ts = now
            if not first_tick_ts:
                first_tick_ts = now

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
                arr[-1][6] = True            # yeni mum geldiyse onceki kapanmistir
                arr.append(row)
                if len(arr) > KLINE_KEEP:
                    del arr[0]
            kline_ts[raw] = time.time()


def ws_build_url(base):
    """Binance 'birlesik akis' adresi: fiyat akisi + istenen coinlerin mum akislari."""
    with ws_lock:
        syms = sorted(subscribed)
    streams = ["!miniTicker@arr"] + [f"{r.lower()}@kline_{TIMEFRAME}" for r in syms]
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
            log.info(f"✅ WebSocket baglandi: {base} (1 fiyat akisi + {len(conn_syms)} mum akisi)")
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
                if missing and now - started > 15:
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
        if raw[:-4] in STABLE_BASES:       # 'in' ile parca aramak BNB, TRB gibi coinleri de eliyordu
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


# ---------------- Mum verisi ----------------
def is_fresh(raw):
    with ws_lock:
        arr = klines.get(raw)
        return bool(arr) and len(arr) >= 60 and raw not in dirty \
            and (time.time() - kline_ts.get(raw, 0)) < KLINE_STALE_SECONDS


def ensure_streams(symbols):
    """Yeni coinlerin mum akislarini kaydeder; websocket kendini yeniden baglayarak bunlari ekler."""
    with ws_lock:
        new = [to_raw(s) for s in symbols if to_raw(s) not in subscribed]
        new = new[:max(0, MAX_STREAMS - len(subscribed))]
        subscribed.update(new)
    if new:
        log.info(f"{len(new)} coin icin mum akisi isteniyor.")


def seed_symbol(symbol):
    """Gecmis mumlari REST'ten BIR KEZ yukler (sonrasi WS besler)."""
    raw = to_raw(symbol)
    ohlcv = ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=SEED_LIMIT)
    rows = [[int(r[0]), r[1], r[2], r[3], r[4], r[5], True] for r in ohlcv]
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
    wait_for_ws()
    symbols = get_dynamic_symbols()
    tradfi = {s for s in symbols if is_tradfi(s)}
    if tradfi:
        symbols = [s for s in symbols if s not in tradfi]
        names = ", ".join(sorted(s.split("/")[0] for s in tradfi)[:12])
        log.info(f"TradFi (hisse/emtia) vadeliler elendi: {len(tradfi)} adet ({names}...)")
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
def sl_distance(price, atr, lev):
    """Stop mesafesi: SL_ATR x ATR, ama marjinde SL_MAX_MARGIN_PCT zarari gecmeyecek kadar (hangisi yakinsa)."""
    cap = price * (SL_MAX_MARGIN_PCT / 100.0) / max(float(lev), 1.0)
    return min(SL_ATR * atr, cap)


def place_protection(symbol, entry, amount, atr, side="long", lev=1):
    """SL: giris -/+ sl_distance (en fazla 1.5 ATR, marjinde en fazla %SL_MAX_MARGIN_PCT zarar).
    TP1: marjine gore TP1_MARGIN_PCT kar (fiyat hareketi = pct / kaldirac)."""
    exit_side = "sell" if side == "long" else "buy"
    tp_move = (TP1_MARGIN_PCT / 100.0) / max(lev, 1)      # fiyatin yuzde kac oynamasi gerek
    sl_dist = sl_distance(entry, atr, lev)
    if side == "long":
        sl_raw = entry - sl_dist
        tp_raw = entry * (1 + tp_move)
    else:
        sl_raw = entry + sl_dist
        tp_raw = entry * (1 - tp_move)
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
            log.info(f"🎯 TP kondu (%{TP1_CLOSE_PCT*100:.0f} kapanir, marjinde %{TP1_MARGIN_PCT:.0f}): "
                     f"{symbol} @ {tp_price}")
        except Exception as e:
            log.error(f"{symbol} TP konamadi: {e}")
    return {"sl_id": sl_order.get("id"), "sl_price": sl_price}


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


def scan_exit_orders(symbol, exit_side):
    """Borsadaki bu yone ait cikis emirlerini TEK taramada okur: ([(id, fiyat) stop-loss], [(id, fiyat) kar-al])."""
    stops, tps = {}, {}
    for params in ({}, {"trigger": True}):
        try:
            orders = ex.fetch_open_orders(symbol, params=params)
        except Exception as e:
            log.warning(f"{symbol} acik emirler okunamadi {params}: {e}")
            continue
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
    return list(stops.items()), list(tps.items())


def find_stop_orders(symbol, exit_side):
    """Borsadaki bu yone ait STOP-LOSS emirlerini [(id, fiyat), ...] verir (kar al emirleri sayilmaz)."""
    return scan_exit_orders(symbol, exit_side)[0]


def dedupe_stops(symbol):
    """Borsada bu pozisyon icin birden fazla stop-loss varsa (zaman asimi, iki kopya vb. nedenle),
    en yakin (en siki) olani tutar, fazlalari iptal eder ve takip edilen stopu ona esitler. Kar-al emirlerine dokunmaz."""
    with trail_lock:
        st = trail_state.get(symbol)
        if not st:
            return
        exit_side = "sell" if st["side"] == "long" else "buy"
    stops, tps = scan_exit_orders(symbol, exit_side)      # REST, kilidin disinda (tek tarama)

    # Bot bir pozisyon icin en fazla 1 kar-al emri koyar; fazlasi eski/artik emirdir (iki kar-al = pozisyonun tamami kapanir)
    if len(tps) > 1:
        tps.sort(key=lambda x: str(x[0]).zfill(20))        # id'ler artar: en yeniyi tut
        removed_tp = sum(1 for oid, _ in tps[:-1] if cancel_safe(oid, symbol))
        log.info(f"🧹 {symbol}: {len(tps)} kar-al emri bulundu, {removed_tp} fazla kar-al iptal edildi.")

    if len(stops) <= 1:
        return
    keep_id, keep_price = (max if exit_side == "sell" else min)(stops, key=lambda x: x[1])
    removed = 0
    for oid, _ in stops:
        if oid != keep_id and cancel_safe(oid, symbol):
            removed += 1
    with trail_lock:
        cur = trail_state.get(symbol)
        if cur:
            cur["sl_id"] = keep_id
            cur["sl_price"] = keep_price
            save_state()
    log.info(f"🧹 {symbol}: {len(stops)} stop bulundu, {removed} fazla stop iptal edildi, kalan SL: {keep_price}")


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
    stops = find_stop_orders(symbol, exit_side)
    log.info(f"{symbol}: borsada {len(stops)} stop-loss emri bulundu")
    sl_id, sl_price = None, None
    if stops:
        # en yakin (en siki) stopu tut, fazlalari temizle
        sl_id, sl_price = (max if exit_side == "sell" else min)(stops, key=lambda x: x[1])
        extra = [oid for oid, _ in stops if oid != sl_id]
        for oid in extra:
            cancel_safe(oid, symbol)
        if extra:
            log.info(f"🧹 {symbol}: {len(extra)} fazla stop temizlendi")
    if sl_id is None:
        try:
            lev_pos = float(pos.get("leverage") or LEV_NORMAL)
        except (TypeError, ValueError):
            lev_pos = float(LEV_NORMAL)
        d = sl_distance(price, atr, lev_pos)
        raw = price - d if side == "long" else price + d
        sl_price = float(ex.price_to_precision(symbol, raw))
        amount = float(ex.amount_to_precision(symbol, size))
        order = ex.create_order(symbol, "STOP_MARKET", exit_side, amount, None,
                                {"stopPrice": sl_price, "reduceOnly": True})
        sl_id = order.get("id")
        log.warning(f"🛟 {symbol} stop'suz pozisyon bulundu, SL kondu @ {sl_price}")
    ext = max(entry, price) if side == "long" else min(entry, price)
    with trail_lock:
        try:
            lev = float(pos.get("leverage") or LEV_NORMAL)
        except (TypeError, ValueError):
            lev = float(LEV_NORMAL)
        trail_state[symbol] = {
            "side": side, "atr": atr, "ext": ext, "entry": entry, "sl_price": sl_price,
            "sl_id": sl_id, "amount": float(ex.amount_to_precision(symbol, size)), "lev": lev,
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

        lev = float(st.get("lev") or LEV_NORMAL)
        if is_long:
            if price <= st["ext"]:
                return
            st["ext"] = max(st["ext"], price)
            profit_atr = (st["ext"] - entry) / atr
            mult = trail_multiplier(profit_atr)
            dist = min(mult * atr, st["ext"] * TRAIL_DIST_MARGIN_PCT / 100.0 / lev)
            raw_sl = st["ext"] - dist
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
            dist = min(mult * atr, st["ext"] * TRAIL_DIST_MARGIN_PCT / 100.0 / lev)
            raw_sl = st["ext"] + dist
            if profit_atr >= BREAKEVEN_AT_ATR:
                raw_sl = min(raw_sl, entry)
            new_sl = float(ex.price_to_precision(symbol, raw_sl))
            move_ok = new_sl < st["sl_price"] - 0.1 * atr and new_sl > price

        if not move_ok:
            return
        old_sl_id = st["sl_id"]
        old_sl_price = st["sl_price"]
        amount_now = st.get("amount")

    # ---- kilit disinda: REST islemleri ----
    wait_if_banned()
    try:
        # Kismi kar alindiysa pozisyon kuculmustur: stopu GUNCEL miktarla koy (sadece stop tasirken 1 istek)
        try:
            for p in ex.fetch_positions([symbol]):
                if p.get("symbol") == symbol:
                    c = float(p.get("contracts") or 0)
                    if c > 0:
                        amount_now = float(ex.amount_to_precision(symbol, c))
                    break
        except Exception as e:
            log.warning(f"{symbol} guncel pozisyon miktari okunamadi, eski miktar kullanilacak: {e}")
            note_ban(e)

        new_order = ex.create_order(symbol, "STOP_MARKET", exit_side, amount_now, None,
                                    {"stopPrice": new_sl, "reduceOnly": True})
        cancel_safe(old_sl_id, symbol)
        log.info(f"📈 {symbol} gezen stop: {old_sl_price} -> {new_sl} (kar {profit_atr:.1f} ATR)")
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
                dedupe_stops(symbol)          # her dakika: fazladan stop kaldiysa temizle
            except Exception as e:
                log.warning(f"{symbol} fazla stop temizligi hatasi: {e}")
                note_ban(e)
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


def emergency_close(symbol, side, amount):
    """SL konamayan pozisyonu kapatir; 3 kez dener. Basarisizsa True/False doner."""
    close_side = "sell" if side == "long" else "buy"
    for attempt in range(1, 4):
        try:
            wait_if_banned()
            ex.create_order(symbol, "market", close_side, amount, None, {"reduceOnly": True})
            log.warning(f"🔒 {symbol} korumasiz pozisyon kapatildi (deneme {attempt}).")
            return True
        except Exception as e:
            log.error(f"{symbol} acil kapatma hatasi (deneme {attempt}/3): {e}")
            note_ban(e)
            time.sleep(2)
    return False


def open_trade(cand, open_symbols):
    symbol, side, atr = cand["symbol"], cand["side"], cand["atr"]
    lev = LEV_STRONG if cand["score"] >= STRONG_SCORE else LEV_NORMAL
    with open_lock:
        opening_symbols.add(symbol)
    try:
        # Bu coinde pozisyon yok (tarama bunu garanti eder): onceki islemden kalan artik stop/kar-al emirleri varsa temizle,
        # yoksa yeni pozisyona yanlislikla eski emirler (ornegin ikinci bir kar-al) baglanir.
        cancel_all_safe(symbol)
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

        prot = place_protection(symbol, entry, amount, atr, side, lev)
        if prot:
            with trail_lock:
                trail_state[symbol] = {
                    "side": side, "atr": atr, "ext": entry, "entry": entry,
                    "sl_price": prot["sl_price"], "sl_id": prot["sl_id"], "amount": amount,
                    "lev": lev,
                }
                save_state()
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
                    if "-4411" in str(inner_err):
                        # TradFi-Perps sozlesmesi imzalanmamis (hisse/emtia vadelisi): 24 saat atla
                        with trail_lock:
                            cooldown[cand["symbol"]] = time.time() + 24 * 3600
                            save_state()
                        log.warning(f"🚫 {cand['symbol']} TradFi-Perps sozlesmesi gerektiriyor, 24 saat atlanacak.")
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
            threading.Thread(target=ws_thread, daemon=True).start()
            threading.Thread(target=bot_loop, daemon=True).start()
            threading.Thread(target=trail_loop, daemon=True).start()


start_bot_once()


@app.route("/")
def index():
    return jsonify({
        "status": "running",
        "websocket": ws_healthy(),
        "websocket_bagli": ws_connected,
        "fiyat_yasi_sn": round(time.time() - tickers_ts, 1) if tickers_ts else None,
        "izlenen_coin": len(subscribed),
        "takip_edilen": list(trail_state.keys()),
        "bekleyen_coin": len(cooldown),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
