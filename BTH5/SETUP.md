# Hướng Dẫn Cài Đặt & Chạy Hệ Thống

## Yêu Cầu Hệ Thống

- **OS:** Windows 10/11
- **Python:** 3.8+
- **RAM:** 4GB+
- **Webcam:** Có (built-in hoặc USB)
- **Hadoop:** Tuỳ chọn (không cần cho version này)

---

## BƯỚC 1: Cài Đặt Python & Thư Viện

### 1.1. Kiểm Tra Python

```cmd
python --version
```

Nếu chưa cài, download từ: https://www.python.org/downloads/

### 1.2. Clone Repository

```cmd
git clone https://github.com/BuiNgocHoan/DS200.git
cd DS200/BTH5_person-counter
```

### 1.3. Tạo Virtual Environment

```cmd
python -m venv .venv
.venv\Scripts\activate
```

Bạn sẽ thấy:
```
(.venv) C:\...>
```

### 1.4. Cài Thư Viện

```cmd
pip install -r requirements.txt
```

Chờ 2-5 phút. Lần đầu sẽ tải YOLOv8 model (~6MB).

**Kiểm tra:**
```cmd
pip list
```

Bạn sẽ thấy:
- numpy
- opencv-python
- ultralytics
- pyspark (tuỳ chọn)

---

## BƯỚC 2: Chạy Hệ Thống

### Quan Trọng: Chạy 3 Terminals RIÊNG

### Terminal 1 – Storage Server

```cmd
.venv\Scripts\activate
cd storage_server
python storage_server.py
```

**Kết quả mong đợi:**
```
[Storage] Đang lắng nghe tại localhost:6102 ...
[Storage] CSV output: .../results/results_local.csv
[Storage] Batch interval: 10s
```

**Để terminal này chạy, không đóng**

---

### Terminal 2 – Detection Server

Mở terminal mới (Ctrl + Shift + `):

```cmd
.venv\Scripts\activate
cd detection_server
python detection_server.py
```

**Kết quả mong đợi (chờ 30-60 giây):**
```
[Detection] Đang tải model YOLOv8...
[Detection] Model sẵn sàng.
[Detection] Đang lắng nghe tại localhost:6101 ...
```

 **Để terminal này chạy, không đóng**

---

### Terminal 3 – Camera Server

Mở terminal mới (Ctrl + Shift + `):

```cmd
.venv\Scripts\activate
cd camera_server
python camera_server.py
```

**Kết quả mong đợi:**
```
[Camera] Camera đã sẵn sàng. Nhấn 'q' trong cửa sổ để dừng.
[Camera] Đã kết nối tới Detection Server localhost:6101
[Camera] Frame 0001 | Người: 2 | Thời gian xử lý: 45.3 ms
[Camera] Frame 0002 | Người: 2 | Thời gian xử lý: 43.1 ms
```

**Cửa sổ video sẽ hiện ra:**
- Ảnh từ webcam laptop
- Bounding boxes xanh quanh người
- Dòng chữ: "Nguoi: 2" (số lượng người)

---

## BƯỚC 3: Xem Kết Quả

### Cách 1: Xem CSV (Dễ Nhất)

```
storage_server/output/results_local.csv
```

Mở bằng Excel, bạn sẽ thấy:
```
frame_id | timestamp | datetime_str | person_count | process_ms | boxes_json
---------|-----------|--------------|--------------|-----------|----------
1        | 1718359... | 2024-06-14... | 2            | 45.3      | [...]
2        | 1718359... | 2024-06-14... | 2            | 43.1      | [...]
3        | 1718359... | 2024-06-14... | 1            | 44.8      | [...]
```

### Cách 2: Phân Tích CSV Bằng Python

```python
import pandas as pd

df = pd.read_csv("storage_server/output/results_local.csv")
print(f"Tổng frames: {len(df)}")
print(f"Trung bình người/frame: {df['person_count'].mean():.2f}")
print(f"Số người tối đa: {df['person_count'].max()}")
```

---

## BƯỚC 4: Dừng Hệ Thống

**Ở cửa sổ video, nhấn phím Q**

Tất cả 3 terminals sẽ tự động dừng.

---

## Xử Lý Lỗi

### "Camera không mở được"

**Nguyên nhân:** Camera index sai

**Fix:** Sửa file `camera_server/camera_server.py`

Tìm dòng:
```python
CAMERA_INDEX = 0
```

Thay thành:
```python
CAMERA_INDEX = 1   # hoặc 2, 3...
```

Lưu, chạy lại Terminal 3.

---

### "Failed to connect to Detection Server"

**Nguyên nhân:** Terminal 2 chưa chạy

**Fix:** Kiểm tra Terminal 2 đã hiển thị "Đang lắng nghe..." chưa

Nếu chưa, chạy lại Terminal 2.

---

### "ModuleNotFoundError: No module named 'ultralytics'"

**Nguyên nhân:** Thư viện chưa cài

**Fix:**
```cmd
pip install ultralytics
```

---

## Performance Tips

### 1. Tăng FPS

Sửa `camera_server/camera_server.py`:
```python
FRAME_INTERVAL = 0.033  # 30 FPS thay vì 10 FPS
```

### 2. Giảm Lag (Nếu Máy Yếu)

Sửa `camera_server/camera_server.py`:
```python
JPEG_QUALITY = 50  # Nén mạnh hơn (thay vì 70)
```

### 3. Dùng GPU (Nếu Có NVIDIA)

Cài CUDA:
```cmd
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
```

YOLOv8 sẽ tự detect GPU.

---

## Cấu Trúc Thư Mục

```
person-counter/
├── README.md                    # Tài liệu chính
├── ARCHITECTURE.md              # Kiến trúc chi tiết
├── SETUP.md                     # File này (hướng dẫn cài đặt)
├── requirements.txt             # pip install -r ...
├── .gitignore                   # Git config
├── camera_server/
│   └── camera_server.py         # Server 1: Bắt camera
├── detection_server/
│   └── detection_server.py      # Server 2: YOLOv8 detect
├── storage_server/
│   ├── storage_server.py        # Server 3: Lưu CSV
│   └── output/
│       └── results_local.csv    # Kết quả (tự tạo)
└── results/
    ├── results_sample.csv       # Sample data
    ├── screenshot_video.png     # Demo (tuỳ chọn)
    └── screenshot_terminal.png  # Demo (tuỳ chọn)
```
---

