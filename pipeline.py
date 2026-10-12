"""Stream real-time prices from Twelve Data into the crypto_ticks hypertable.

Based on https://www.tigerdata.com/docs/learn/tutorials/ingest-real-time-financial-data
Credentials come from the environment (see .env), not from this file.
"""
import os
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from twelvedata import TDClient


def load_env(path=".env"):
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())


class WebsocketPipeline:
    # name of the hypertable
    DB_TABLE = "crypto_ticks"

    # columns in the hypertable in the correct order
    DB_COLUMNS = ["time", "symbol", "price", "day_volume"]

    def __init__(self, conn, api_key):
        # batch size used to insert data in batches (the guide uses 100;
        # smaller batches make rows show up in the console sooner)
        self.MAX_BATCH_SIZE = int(os.environ.get("MAX_BATCH_SIZE", 10))
        self.conn = conn
        self.api_key = api_key
        self.current_batch = []
        self.insert_counter = 0

    def _insert_values(self, data):
        with self.conn.cursor() as cursor:
            sql = f"""
            INSERT INTO {self.DB_TABLE} ({','.join(self.DB_COLUMNS)})
            VALUES %s;"""
            execute_values(cursor, sql, data)
        self.conn.commit()

    def _on_event(self, event):
        if event["event"] == "subscribe-status":
            print(f"Subscribed: {[s['symbol'] for s in event.get('success') or []]}"
                  f" | failed: {event.get('fails')}")
        elif event["event"] == "price":
            timestamp = datetime.fromtimestamp(event["timestamp"], tz=timezone.utc)
            data = (timestamp, event["symbol"], event["price"], event.get("day_volume"))
            self.current_batch.append(data)
            print(f"{event['symbol']:8} {event['price']:>12}  (batch {len(self.current_batch)}/{self.MAX_BATCH_SIZE})")

            if len(self.current_batch) >= self.MAX_BATCH_SIZE:
                self._insert_values(self.current_batch)
                self.insert_counter += 1
                print(f"Batch insert #{self.insert_counter}")
                self.current_batch = []

    def start(self, symbols):
        td = TDClient(apikey=self.api_key)
        ws = td.websocket(on_event=self._on_event)
        ws.subscribe(symbols)
        ws.connect()
        try:
            while True:
                ws.heartbeat()
                time.sleep(10)
        except KeyboardInterrupt:
            if self.current_batch:
                self._insert_values(self.current_batch)
                print(f"Flushed final {len(self.current_batch)} rows")
            ws.disconnect()


if __name__ == "__main__":
    load_env()
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    symbols = os.environ.get("SYMBOLS", "BTC/USD,ETH/USD,MSFT,AAPL").split(",")
    WebsocketPipeline(conn, os.environ["TWELVEDATA_API_KEY"]).start(symbols=symbols)
