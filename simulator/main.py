import os
import json
import logging
import signal
import time
from datetime import datetime, timezone

import websocket

from models import PriceTick
from producer import CryptoProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Konfigurasi ────────────────────────────────────────────────
BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_URL = os.getenv("SCHEMA_REGISTRY_URL",     "http://localhost:8081")

# Symbol Binance → symbol internal kita
SYMBOL_MAP = {
    "btcusdt": "BTC-USD",
    "ethusdt": "ETH-USD",
    "bnbusdt": "BNB-USD",
    "solusdt": "SOL-USD",
    "adausdt": "ADA-USD",
}

def build_ws_url(symbols: list) -> str:
    streams = "/".join([f"{s}@trade" for s in symbols])
    return f"wss://stream.binance.com:9443/stream?streams={streams}"


class BinanceSimulator:
    def __init__(self):
        self.producer   = CryptoProducer(BOOTSTRAP, SCHEMA_URL)
        self.running    = True
        self.tick_count = 0
        self.ws         = None

    def on_message(self, ws, message):
        try:
            raw  = json.loads(message)
            data = raw.get("data", raw)

            symbol_raw = data.get("s", "").lower()
            symbol     = SYMBOL_MAP.get(symbol_raw)
            if not symbol:
                return

            price  = float(data["p"])
            volume = float(data["q"])
            ts     = datetime.fromtimestamp(
                data["T"] / 1000, tz=timezone.utc
            ).isoformat()

            tick = PriceTick(
                timestamp = ts,
                symbol    = symbol,
                price     = price,
                bid       = price,
                ask       = price,
                volume    = volume,
                source    = "binance",
            )

            self.producer.send(tick.to_dict(), key=symbol)
            self.tick_count += 1

            if self.tick_count % 500 == 0:
                logger.info(
                    f"Published {self.tick_count} ticks | "
                    f"Last: {symbol} ${price:,.2f}"
                )

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"WebSocket closed: {close_status_code}")
        if self.running:
            logger.info("Reconnecting in 5 seconds...")
            time.sleep(5)
            self.start()

    def on_open(self, ws):
        logger.info("Connected to Binance WebSocket!")
        logger.info(f"Subscribed symbols: {list(SYMBOL_MAP.values())}")

    def start(self):
        url = build_ws_url(list(SYMBOL_MAP.keys()))
        logger.info(f"Connecting to Binance...")

        self.ws = websocket.WebSocketApp(
            url,
            on_open    = self.on_open,
            on_message = self.on_message,
            on_error   = self.on_error,
            on_close   = self.on_close,
        )
        self.ws.run_forever(
            ping_interval = 30,
            ping_timeout  = 10,
        )

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()
        self.producer.flush()
        logger.info(f"Stopped. Total ticks: {self.tick_count}")


def main():
    simulator = BinanceSimulator()

    def shutdown(sig, frame):
        logger.info("Shutting down...")
        simulator.stop()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT,  shutdown)

    logger.info("Starting Binance realtime data stream...")
    logger.info(f"Symbols: {list(SYMBOL_MAP.values())}")
    simulator.start()


if __name__ == "__main__":
    main()