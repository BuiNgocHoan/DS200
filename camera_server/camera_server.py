"""
SERVER 1 - CAMERA SERVER
  - Bắt khung hình từ camera laptop
  - Gửi từng khung hình (dạng base64 JPEG) tới Detection Server qua TCP
  - Nhận phản hồi bounding boxes từ Detection Server, hiển thị lên màn hình

Cổng lắng nghe: 6100 (nhận lệnh bắt đầu từ client nếu cần)
Kết nối tới   : Detection Server tại localhost:6101
"""

import socket
import json
import base64
import time
import cv2

class Config:
    CAMERA_INDEX       = 0          # 0 = webcam mặc định của laptop
    DETECTION_HOST     = "localhost"
    DETECTION_PORT     = 6101
    FRAME_INTERVAL     = 0.1        # giây giữa 2 frame (10 FPS)
    JPEG_QUALITY       = 70         # chất lượng nén JPEG (0-100)
    DISPLAY_WINDOW     = True       # hiển thị cửa sổ video trực tiếp


def encode_frame(frame: "cv2.Mat") -> str:
    """Nén frame thành JPEG rồi mã hóa base64 để gửi qua TCP."""
    encode_params = [cv2.IMWRITE_JPEG_QUALITY, Config.JPEG_QUALITY]
    _, buffer = cv2.imencode(".jpg", frame, encode_params)
    return base64.b64encode(buffer).decode("utf-8")


def draw_boxes(frame: "cv2.Mat", boxes: list) -> "cv2.Mat":
    """Vẽ bounding boxes lên frame để hiển thị."""
    for box in boxes:
        x1, y1, x2, y2 = box["x1"], box["y1"], box["x2"], box["y2"]
        conf = box.get("confidence", 0)
        cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
        label = f"Person {conf:.0%}"
        cv2.putText(frame, label, (x1, y1 - 8),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 255, 0), 2)
    return frame


def connect_to_detection() -> socket.socket:
    """Kết nối TCP tới Detection Server, thử lại nếu thất bại."""
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((Config.DETECTION_HOST, Config.DETECTION_PORT))
            print(f"[Camera] Đã kết nối tới Detection Server "
                  f"{Config.DETECTION_HOST}:{Config.DETECTION_PORT}")
            return s
        except ConnectionRefusedError:
            print("[Camera] Chưa thể kết nối Detection Server, thử lại sau 2s...")
            time.sleep(2)


def send_recv(sock: socket.socket, payload: dict) -> dict | None:
    """Gửi một JSON payload và nhận phản hồi JSON trả về."""
    try:
        message = (json.dumps(payload) + "\n").encode()
        sock.sendall(message)

        # Nhận phản hồi (đọc cho đến khi gặp newline)
        response = b""
        while not response.endswith(b"\n"):
            chunk = sock.recv(4096)
            if not chunk:
                return None
            response += chunk
        return json.loads(response.decode())
    except Exception as e:
        print(f"[Camera] Lỗi giao tiếp TCP: {e}")
        return None


def run():
    cap = cv2.VideoCapture(Config.CAMERA_INDEX)
    if not cap.isOpened():
        raise RuntimeError(f"Không mở được camera index {Config.CAMERA_INDEX}")

    print("[Camera] Camera đã sẵn sàng. Nhấn 'q' trong cửa sổ để dừng.")

    detection_sock = connect_to_detection()
    frame_id = 0

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                print("[Camera] Không đọc được frame, bỏ qua.")
                continue

            frame_id += 1
            timestamp = time.time()
            encoded   = encode_frame(frame)

            payload = {
                "frame_id" : frame_id,
                "timestamp": timestamp,
                "image_b64": encoded,
            }

            result = send_recv(detection_sock, payload)

            if result:
                count = result.get("person_count", 0)
                boxes = result.get("boxes", [])
                print(f"[Camera] Frame {frame_id:04d} | Người: {count} | "
                      f"Thời gian xử lý: {result.get('process_ms', 0):.1f} ms")

                if Config.DISPLAY_WINDOW:
                    display = draw_boxes(frame.copy(), boxes)
                    cv2.putText(display, f"Nguoi: {count}", (10, 35),
                                cv2.FONT_HERSHEY_SIMPLEX, 1.2, (0, 0, 255), 3)
                    cv2.imshow("Person Counter - Camera Server", display)
                    if cv2.waitKey(1) & 0xFF == ord("q"):
                        print("[Camera] Người dùng nhấn 'q', dừng.")
                        break
            else:
                print("[Camera] Mất kết nối, kết nối lại...")
                detection_sock.close()
                detection_sock = connect_to_detection()

            time.sleep(Config.FRAME_INTERVAL)

    finally:
        cap.release()
        cv2.destroyAllWindows()
        detection_sock.close()
        print("[Camera] Đã dừng.")


if __name__ == "__main__":
    run()
