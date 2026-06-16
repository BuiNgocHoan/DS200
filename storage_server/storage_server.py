"""
SERVER 3 - STORAGE SERVER (Simplified - Local CSV Only)
  - Nghe kết quả từ Detection Server
  - Gom kết quả thành batch
  - Ghi CSV cục bộ

Port nghe: 6102
CSV output    : ./output/results_local.csv
"""

import socket
import json
import threading
import time
import os
import csv
from datetime import datetime
from collections import deque

class Config:
    LISTEN_HOST    = "localhost"
    LISTEN_PORT    = 6102

    # Local output
    LOCAL_OUTPUT   = os.path.join(os.path.dirname(__file__), "output")
    LOCAL_CSV      = os.path.join(LOCAL_OUTPUT, "results_local.csv")

    # Gom batch mỗi N giây rồi ghi
    BATCH_INTERVAL = 10     # giây
    BATCH_MIN_ROWS = 5      # ghi ngay nếu đủ N rows


def write_batch_to_csv(rows: list[dict]):
    os.makedirs(Config.LOCAL_OUTPUT, exist_ok=True)
    file_exists = os.path.isfile(Config.LOCAL_CSV)
    
    with open(Config.LOCAL_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["frame_id", "timestamp", "datetime_str",
                             "person_count", "process_ms", "boxes_json"])
        for r in rows:
            writer.writerow([
                r["frame_id"],
                r["timestamp"],
                datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M:%S"),
                r["person_count"],
                r.get("process_ms", 0),
                json.dumps(r.get("boxes", [])),
            ])
    
    print(f"[Storage] Đã ghi {len(rows)} bản ghi vào CSV: {Config.LOCAL_CSV}")


class BatchManager:
    def __init__(self):
        self.buffer : deque[dict] = deque()
        self.lock   = threading.Lock()
        self._start_flush_timer()

    def add(self, record: dict):
        with self.lock:
            self.buffer.append(record)
            if len(self.buffer) >= Config.BATCH_MIN_ROWS:
                self._flush_locked()

    def _flush_locked(self):
        if not self.buffer:
            return
        rows = list(self.buffer)
        self.buffer.clear()
        # Ghi bất đồng bộ để không block nhận frame
        threading.Thread(target=write_batch_to_csv, args=(rows,), daemon=True).start()

    def _start_flush_timer(self):
        def loop():
            while True:
                time.sleep(Config.BATCH_INTERVAL)
                with self.lock:
                    self._flush_locked()
        threading.Thread(target=loop, daemon=True).start()

def recv_line(conn: socket.socket) -> str | None:
    data = b""
    while not data.endswith(b"\n"):
        chunk = conn.recv(8192)
        if not chunk:
            return None
        data += chunk
    return data.decode()


def handle_detection(conn: socket.socket, addr, batch_mgr: BatchManager):
    print(f"[Storage] Detection Server kết nối từ {addr}")
    total = 0
    try:
        while True:
            raw = recv_line(conn)
            if raw is None:
                break
            record = json.loads(raw)
            batch_mgr.add(record)
            total += 1
            print(f"[Storage] Nhận frame {record['frame_id']:04d} | "
                  f"Người: {record['person_count']} | Tổng nhận: {total}")
    except Exception as e:
        print(f"[Storage] Lỗi: {e}")
    finally:
        conn.close()
        print(f"[Storage] Detection Server {addr} ngắt. Tổng frames nhận: {total}")

def run():
    batch_mgr = BatchManager()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((Config.LISTEN_HOST, Config.LISTEN_PORT))
    server.listen(5)
    print(f"[Storage] Đang lắng nghe tại "
          f"{Config.LISTEN_HOST}:{Config.LISTEN_PORT} ...")
    print(f"[Storage] CSV output: {Config.LOCAL_CSV}")
    print(f"[Storage] Batch interval: {Config.BATCH_INTERVAL}s")

    while True:
        conn, addr = server.accept()
        t = threading.Thread(
            target=handle_detection,
            args=(conn, addr, batch_mgr),
            daemon=True,
        )
        t.start()


if __name__ == "__main__":
    run()