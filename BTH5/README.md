# Hệ Thống Đếm Người Qua Camera (Person Counter)

**Công nghệ:** YOLOv8 | Python | TCP Sockets | Batch Processing | Big Data
 
**Sinh viên:** Bùi Ngọc Hoàn

**Ngày hoàn thành:** 16-06-2026

---

## Tóm Tắt Dự Án

Dự án xây dựng **hệ thống đếm người qua camera** sử dụng **kiến trúc microservices** và **Big Data batch processing**. Hệ thống bao gồm 3 servers độc lập giao tiếp qua TCP, xử lý video realtime từ webcam laptop, nhận diện người bằng YOLOv8, và lưu kết quả dạng batch.

---

## Kiến Trúc Hệ Thống

```
┌─────────────────────────────────────┐
│    LAPTOP (Windows Local)           │
│                                     │
│   Webcam                            │
│      │                              │
│      ▼                              │
│  ┌─────────────────┐     TCP        │
│  │  SERVER 1       │ ────:6101──►   │
│  │ camera_server   │ ◄────────      │
│  │                 │                │
│  │ • Bắt frame     │  bounding      │
│  │ • Encode base64 │  boxes         │
│  │ • Hiển thị video│                │
│  └────────┬────────┘                │
│           │                         │
│           ▼                         │
│  ┌─────────────────┐                │
│  │  SERVER 2       │                │
│  │detection_server │     TCP        │
│  │                 │ ────:6102──►   │
│  │ • YOLOv8 detect │                │
│  │ • Trả bounding  │                │
│  │ • Forward result│                │
│  └────────┬────────┘                │
│           │                         │
│           ▼                         │
│  ┌─────────────────┐                │
│  │  SERVER 3       │                │
│  │ storage_server  │                │
│  │                 │                │
│  │ • Nhận kết quả  │                │
│  │ • Gom batch     │                │
│  │ • Lưu CSV       │                │
│  └─────────────────┘                │
│           │                         │
│           ▼                         │
│  storage_server/output/
│  └── results_local.csv
└─────────────────────────────────────┘
```

**Chi tiết:** Xem file `ARCHITECTURE.md`

## Cài Đặt & Chạy

### 1. Clone Repository

```bash
git clone https://github.com/[username]/person-counter.git
cd person-counter
```

### 2. Cài Thư Viện

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Chạy 3 Servers (3 Terminal Riêng Biệt)

**Terminal 1 – Storage Server:**
```bash
.venv\Scripts\activate
set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_xxx
cd storage_server
python storage_server.py
```

**Terminal 2 – Detection Server:**
```bash
.venv\Scripts\activate
set HADOOP_HOME=D:\hadoop\hadoop
cd detection_server
python detection_server.py
```

**Terminal 3 – Camera Server:**
```bash
.venv\Scripts\activate
cd camera_server
python camera_server.py
```

### 4. Dừng Hệ Thống

Nhấn **Q** trong cửa sổ video.

---

## Nội Dung File

### `camera_server/camera_server.py`
- **Tác dụng:** Bắt frame từ webcam, encode base64, gửi qua TCP
- **Cổng:** 6100 → 6101

### `detection_server/detection_server.py`
- **Tác dụng:** Nhận frame, chạy YOLOv8, trả bounding boxes
- **Model:** YOLOv8 nano (6MB)

### `storage_server/storage_server.py`
- **Tác dụng:** Nhận kết quả, gom batch, lưu CSV
- **Batch interval:** 10 giây

---

## Công Nghệ Sử Dụng

| Công Nghệ | Phiên Bản | Mục Đích |
|----------|---------|---------|
| **Python** | 3.8+ | Ngôn ngữ chính |
| **YOLOv8** | 8.0.0+ | Nhận diện người |
| **OpenCV** | 4.8.0+ | Xử lý ảnh |
| **Sockets** | Python stdlib | Giao tiếp TCP |
| **CSV** | Python stdlib | Lưu trữ |

---

## Performance

| Chỉ Số | Giá Trị |
|-------|--------|
| FPS (Frame Per Second) | 10 FPS |
| Thời gian xử lý/frame | 43-45 ms |
| Độ chính xác detect người | ~90% |
| Batch size | 5 frames |
| Batch interval | 10 giây |

---


## Cách Sử Dụng Kết Quả

### Đọc CSV

```python
import pandas as pd
df = pd.read_csv("storage_server/output/results_local.csv")
print(df)
```

### Phân Tích Thống Kê

```python
total_frames = len(df)
avg_people = df['person_count'].mean()
max_people = df['person_count'].max()
print(f"Tổng frames: {total_frames}")
print(f"Trung bình người/frame: {avg_people:.2f}")
print(f"Số người tối đa: {max_people}")
```

## Tài Liệu Tham Khảo

- [YOLOv8 Docs](https://docs.ultralytics.com/)
- [OpenCV Python](https://docs.opencv.org/master/d6/d00/tutorial_py_root.html)
- [Python Sockets](https://docs.python.org/3/library/socket.html)
- [Batch Processing Concepts](https://en.wikipedia.org/wiki/Batch_processing)

---

## License

MIT License - Tự do sử dụng & sửa đổi

---

## Tác Giả

- **Sinh viên:** Bùi Ngọc Hoàn
- **MSSV:** 23520510
- **Lớp:** DS200.Q21.1
