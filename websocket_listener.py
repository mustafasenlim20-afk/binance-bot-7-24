import json
import time
from websocket import WebSocketApp

# Binance Futures Tüm Piyasa Mini Ticker Akışı (HTTP isteklerine son verir, IP ban yemezsin)
SOCKET_URL = "wss://fstream.binance.com/ws/!miniTicker@arr"

def on_message(ws, message):
    data = json.loads(message)
    # Gelen canlı fiyat verilerini burada işleyebilirsin
    for ticker in data:
        symbol = ticker.get('s')
        price = ticker.get('c')
        # print(f"Canlı Fiyat - {symbol}: {price}")

def on_error(ws, error):
    print(f"WebSocket Hatası: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket bağlantısı kapandı, 5 saniye sonra yeniden bağlanılıyor...")
    time.sleep(5)
    start_websocket()

def on_open(ws):
    print("Binance WebSocket bağlantısı başarıyla kuruldu!")

def start_websocket():
    ws = WebSocketApp(
        SOCKET_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever(ping_interval=30, ping_timeout=10)

if __name__ == "__main__":
    start_websocket()
