"""
SERVER 2 - DETECTION SERVER
  - Nghe kết nối TCP từ Camera Server (cổng 6101)
  - Giải mã khung hình base64 → ảnh OpenCV
  - Chạy YOLOv8 để phát hiện người → trích xuất bounding boxes
  - Gửi bounding boxes về Camera Server qua TCP
  - Chuyển tiếp kết quả sang Storage Server (cổng 6102) qua TCP

Cổng nghe: 6101
Kết nối tới   : Storage Server tại localhost:6102
"""

import socket
import json
import base64
import time
import threading
import numpy as np
import cv2
from ultralytics import YOLO

class Config:
    LISTEN_HOST   = "localhost"
    LISTEN_PORT   = 6101
    STORAGE_HOST  = "localhost"
    STORAGE_PORT  = 6102
    YOLO_MODEL    = "yolov8n.pt"    # model nhẹ
    CONF_THRESH   = 0.40            # ngưỡng confidence tối thiểu
    PERSON_CLASS  = 0               # class index 0 = 'person' trong COCO

print("[Detection] Đang tải model YOLOv8...")
model = YOLO(Config.YOLO_MODEL)
print("[Detection] Model sẵn sàng.")

storage_sock: socket.socket | None = None
storage_lock = threading.Lock()


def get_storage_sock() -> socket.socket | None:
    """Trả về kết nối TCP tới Storage Server, tự kết nối lại nếu bị ngắt."""
    global storage_sock
    with storage_lock:
        if storage_sock is None:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3)
                s.connect((Config.STORAGE_HOST, Config.STORAGE_PORT))
                storage_sock = s
                print(f"[Detection] Đã kết nối tới Storage Server "
                      f"{Config.STORAGE_HOST}:{Config.STORAGE_PORT}")
            except Exception:
                print("[Detection] CẢNH BÁO: Chưa kết nối được Storage Server. "
                      "Kết quả sẽ không được lưu cho đến khi Storage khởi động.")
                return None
        return storage_sock


def send_to_storage(data: dict):
    """Gửi kết quả phát hiện sang Storage Server (fire-and-forget)."""
    global storage_sock
    sock = get_storage_sock()
    if sock is None:
        return
    try:
        message = (json.dumps(data) + "\n").encode()
        sock.sendall(message)
    except Exception as e:
        print(f"[Detection] Mất kết nối Storage: {e}. Sẽ kết nối lại lần sau.")
        with storage_lock:
            storage_sock = None


def decode_frame(image_b64: str) -> "cv2.Mat":
    """Chuyển base64 JPEG → numpy array (BGR)."""
    image_bytes = base64.b64decode(image_b64)
    np_arr = np.frombuffer(image_bytes, dtype=np.uint8)
    frame  = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    return frame


def detect_persons(frame: "cv2.Mat") -> list[dict]:
    """
    Chạy YOLOv8 trên frame, trả về list bounding boxes của người.
    Mỗi box: {"x1", "y1", "x2", "y2", "confidence"}
    """
    results = model(frame, conf=Config.CONF_THRESH, classes=[Config.PERSON_CLASS],
                    verbose=False)
    boxes = []
    for r in results:
        for box in r.boxes:
            x1, y1, x2, y2 = [int(v) for v in box.xyxy[0].tolist()]
            conf = float(box.conf[0])
            boxes.append({"x1": x1, "y1": y1, "x2": x2, "y2": y2,
                           "confidence": round(conf, 4)})
    return boxes


def recv_line(conn: socket.socket) -> str | None:
    """Đọc một dòng JSON kết thúc bằng '\\n' từ socket."""
    data = b""
    while not data.endswith(b"\n"):
        chunk = conn.recv(8192)
        if not chunk:
            return None
        data += chunk
    return data.decode()


def handle_camera(conn: socket.socket, addr):
    print(f"[Detection] Camera Server kết nối từ {addr}")
    try:
        while True:
            raw = recv_line(conn)
            if raw is None:
                break

            payload    = json.loads(raw)
            frame_id   = payload["frame_id"]
            timestamp  = payload["timestamp"]
            image_b64  = payload["image_b64"]

            frame = decode_frame(image_b64)

            t0    = time.time()
            boxes = detect_persons(frame)
            ms    = (time.time() - t0) * 1000

            count = len(boxes)

            response = {
                "frame_id"    : frame_id,
                "person_count": count,
                "boxes"       : boxes,
                "process_ms"  : round(ms, 2),
            }
            conn.sendall((json.dumps(response) + "\n").encode())

            storage_payload = {
                "frame_id"    : frame_id,
                "timestamp"   : timestamp,
                "person_count": count,
                "boxes"       : boxes,
                "process_ms"  : round(ms, 2),
            }
            threading.Thread(target=send_to_storage,
                             args=(storage_payload,), daemon=True).start()

            print(f"[Detection] Frame {frame_id:04d} → {count} người | {ms:.1f} ms")

    except Exception as e:
        print(f"[Detection] Lỗi xử lý frame: {e}")
    finally:
        conn.close()
        print(f"[Detection] Camera Server {addr} ngắt kết nối.")



def run():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((Config.LISTEN_HOST, Config.LISTEN_PORT))
    server.listen(5)
    print(f"[Detection] Đang lắng nghe tại "
          f"{Config.LISTEN_HOST}:{Config.LISTEN_PORT} ...")

    while True:
        conn, addr = server.accept()
        t = threading.Thread(target=handle_camera, args=(conn, addr), daemon=True)
        t.start()


if __name__ == "__main__":
    run()
