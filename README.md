# Big Data Music Analytics (Kiến trúc Lambda)

Hệ thống phân tích thói quen và xu hướng nghe nhạc từ Last.fm theo kiến trúc **Lambda** (kết hợp **Batch Layer** và **Speed Layer** với tầng **Serving** thống nhất). Kiến trúc chi tiết và hợp đồng dữ liệu: `docs/architecture.md`.

```text
Last.fm ── producer batch (Celery, mỗi giờ) ──► Kafka events.bronze.lastfm ──► batch consumer → MinIO Bronze
                                                        │                                            │
                       replay (dữ liệu giả) ──► Kafka events.demo.lastfm                     Silver (Delta Lake)
                                                        │                                            │
                                Spark Structured Streaming (đọc cả hai topic)                Gold (Parquet trên MinIO)
                                                        │  process_events()                          │
              ┌─────────────────────────────────────────┴───────────────┐                            │
              ▼                                                         ▼                            ▼
   HBase: event thô, fact_listen,                        Redis (DB 2): khử trùng event_id,           │
   snapshot theo user/ngày                               thống kê user, top-K 5 phút → 24 giờ        │
                                                                                                     ▼
                                                                                          API HTTP Batch Gold
                                                                                                     │
                                                                                          Dashboard Streamlit
```

---

## 1. Chuẩn bị môi trường

Tất cả dịch vụ dùng chung **một file `.env` duy nhất** và **một entrypoint `docker-compose.yml` duy nhất** tại thư mục gốc:

```powershell
Copy-Item .env.example .env
# Điền LASTFM_API_KEY, REDIS_PASSWORD, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
```

* Mặc định `HOST_BIND_IP=127.0.0.1` khóa toàn bộ các cổng ở `localhost` để an toàn khi dùng mạng công cộng. Khi cần mở Dashboard (`8501`) hoặc API (`8001`) ra mạng LAN, đổi `HOST_BIND_IP=0.0.0.0` trong `.env`.
* Khi bật cả hệ thống (`--profile all`) hoặc luồng Stream (`--profile stream`), máy cần khoảng **8 GB RAM trống** cho Docker (HBase giới hạn `2g`, Spark `3g`, Redis `700m`). Chạy riêng tầng Gold chỉ tốn vài trăm MB RAM.

| Dịch vụ | Biến cổng (`.env`) | Mặc định trên Host | Phạm vi lắng nghe |
| :--- | :--- | :--- | :--- |
| Dashboard (Streamlit) | `DASHBOARD_PORT` | `8501` | `${HOST_BIND_IP:-127.0.0.1}` |
| HTTP API | `API_PORT` | `8001` → `8000` | `${HOST_BIND_IP:-127.0.0.1}` |
| MinIO API, Console | `MINIO_API_PORT`, `MINIO_CONSOLE_PORT` | `9000`, `9001` | `${HOST_BIND_IP:-127.0.0.1}` |
| Kafka External | `KAFKA_EXTERNAL_PORT` | `9092` | `${HOST_BIND_IP:-127.0.0.1}` |
| Redis | `REDIS_PORT` | `6379` (cần mật khẩu) | `${HOST_BIND_IP:-127.0.0.1}` |
| HBase Master, Thrift, REST | — | `16010`, `9090`, `8080` | `${HOST_BIND_IP:-127.0.0.1}` |

---

## 2. Khởi động bằng Docker Compose (theo Profile)

`docker-compose.yml` gốc nạp các file con trong `docker/compose/` (`infra.yml`, `batch.yml`, `stream.yml`, `serving.yml`) và chia rõ theo `--profile`:

### A. Chỉ bật tầng Gold + Dashboard (nhẹ, không bật Kafka / HBase / Spark)
Khi đã có dữ liệu Silver (trên MinIO hoặc tại `data/delta/silver/fact_listen`):
```powershell
docker compose --profile gold up -d --build
docker compose --profile gold logs --tail 20 gold-worker api
```
*(Để đọc Silver từ MinIO thay vì thư mục local `data/delta`, đặt `GOLD_SILVER_PATH=s3a://silver/fact_listen` hoặc `SILVER_BASE_PATH=s3://silver` trong `.env`.)*

### B. Chỉ bật luồng Stream (Speed Layer + API + Dashboard)
Khởi động `kafka`, `kafka-init`, `redis`, `hbase`, `stream-init`, `spark-speed-layer`, `api`, `dashboard`:
```powershell
docker compose --profile stream up -d --build
docker compose --profile stream logs -f spark-speed-layer api
```

### C. Chỉ bật luồng Batch (Ingestion → Bronze → Silver → Gold + Serving)
Khởi động `kafka`, `minio`, `redis`, `kafka-init`, `batch-consumer`, `celery-worker`, `celery-silver-worker`, `celery-beat`, `gold-worker`, `api`, `dashboard`:
```powershell
docker compose --profile batch up -d --build
```

### D. Bật toàn bộ hệ thống Lambda (Batch + Stream + Gold + Serving)
```powershell
docker compose --profile all up -d --build
```

### E. Phát lại dữ liệu giả (`replay`) để demo và đo tải luồng Stream
`replay` ghi vào `events.demo.lastfm` (Spark đọc, `batch-consumer` không đọc nên dữ liệu giả không lẫn vào tầng Bronze):
```powershell
python -m stream.tools.gen_sample_events --users 30 --events 3000 --days 14
docker compose --profile replay run --rm -e REPLAY_RATE=200 replay

# Muốn dời mốc thời gian về hiện tại để xem top 5m / 1h:
docker compose --profile replay run --rm replay python stream/producer/replay_scrobbles.py `
  --bootstrap-servers kafka:9094 --file /app/dataset/sample/scrobbles.jsonl --rate 200 --shift-to-now
```

*(Các dịch vụ cũ `scrobble-producer`, `metadata-worker`, `metadata-sink` được giữ dưới `--profile legacy`, mặc định không bật vì message của producer batch đã kèm sẵn metadata.)*

---

## 3. Truy cập Dashboard & HTTP API

* [Dashboard Batch Gold](http://localhost:8501) mở mục **Giờ và thứ trong tuần**, gồm 24 giờ UTC, bảy thứ và biểu đồ nhiệt 7 × 24. Mục **Ngày thường và cuối tuần** so sánh hai loại ngày. Bấm **Làm mới kết quả Gold** để đọc lần xuất hoàn tất mới nhất.
* **Endpoints của HTTP API (`http://localhost:8001`):**

```powershell
# Kiểm tra sức khỏe API
Invoke-RestMethod 'http://localhost:8001/health'

# Batch Gold: lấy manifest mới nhất và đọc bảng phân trang (limit 1..1000)
$gold = Invoke-RestMethod 'http://localhost:8001/batch/gold'
$page = Invoke-RestMethod "http://localhost:8001/batch/gold/weekday_hour_distribution?run_id=$($gold.run_id)&limit=168"
$page.rows
```

### Thể loại ưa thích

Chọn **Thể loại ưa thích** trên dashboard. Gold dùng `fact_listen.tags` của Silver (ưu tiên tag bài hát, sau đó dùng tag nghệ sĩ). Một lượt có ba tag hợp lệ đóng góp ⅓ lượt quy đổi cho mỗi tag. Dashboard hiển thị số lượt và số người có tag. Tỷ lệ thể loại của từng người dùng mẫu số là số lượt có tag của người đó. Các thống kê giờ và thứ ở màn hình chính vẫn tính toàn bộ lượt nghe.

| Bảng sau `/batch/gold/` | Cách đọc |
|---|---|
| `user_genre_top` | Top 10 tag của từng người. `pct` là phần trăm trên toàn bộ gu của người đó. `so_luot` đếm sự kiện mang tag, `luot_quy_doi` chia trọng số cho các tag. |
| `global_genre` | `share_pct_tb` là tỷ trọng trung bình trên tất cả người có tag, kể cả người có tỷ trọng 0 cho tag đang xét. Đọc cùng `so_user` để thấy có bao nhiêu người đóng góp. |
| `genre_diversity` | Entropy Shannon cơ số 2 trên toàn bộ tag, kèm số tag và tỷ lệ tag đứng đầu (0–1). Entropy bằng 0 khi chỉ nghe một tag. |
| `genre_by_hour`, `genre_by_weekday` | Tám tag đứng đầu toàn nhóm theo 24 giờ UTC hoặc bảy thứ UTC. Dashboard chuyển giữa lượt quy đổi và tỷ lệ trong tổng của tám tag ở từng hàng. Tỷ lệ để trống khi tổng hàng bằng 0. |
| `genre_similarity` | Cosine trên toàn bộ phân bố tag. Mỗi cặp có tag chung xuất hiện một lần. Hai người đều có hồ sơ nhưng không có tag chung có cosine bằng 0 và được lược khỏi bảng. Các tag phổ biến có thể làm điểm cao. |
| `genre_pairs` | Số sự kiện mang đồng thời hai tag khác nhau. Mỗi cạnh là một cặp tag không phân biệt thứ tự. |

Dùng `?user_id=...` để lọc thứ hạng, độ đa dạng hoặc các cặp của một người. Với `genre_similarity`, thêm `&peer=...` để xem một cặp cụ thể. Tất cả các bảng hỗ trợ `run_id`, `offset` và `limit`. Worker tự xuất lại khi nâng từ Gold phiên bản 2 lên 3, kể cả khi Silver chưa đổi.

Nếu muốn chạy API hoặc Dashboard trực tiếp bằng Python trên máy local:
```powershell
pip install -r requirements/api.txt -r requirements/ui.txt
python -m api --port 8001
python -m streamlit run ui/app.py
```

---

## 4. Cấu trúc dự án

```text
common/              Hợp đồng dữ liệu dùng chung: ids, bronze_event, fact_listen, nạp .env (env.py)
batch/               Pipeline Batch: bronze (producer, consumer), silver (Delta Lake), gold (DuckDB/Parquet),
                     datalake (MinIO client), scheduler (Celery Beat & Worker)
stream/              Pipeline Speed: processing (Spark & lõi pipeline), storage (Redis, HBase),
                     producer (replay & legacy), metadata (legacy), bootstrap, tools
api/                 Điểm khởi chạy API đọc kết quả Batch Gold từ MinIO
ui/                  Giao diện Streamlit Dashboard (ui/app.py)
docker/              Tập trung toàn bộ *.Dockerfile theo từng pipeline và các file docker/compose/*.yml
requirements/        Khai báo thư viện tách theo dịch vụ (common, batch, gold, stream, api, ui, dev)
docs/                Tài liệu kiến trúc (architecture.md, batch_pipeline.md, stream-flow.md, fields.csv...)
tests/               Bộ unit test và integration test cho cả Batch, Gold, Stream và Serving API
```

---

## 5. Kiểm thử

```powershell
pip install -r requirements/dev.txt
python -m unittest discover -s tests -t . -v

# Chạy thêm integration test Lua trên Redis thật (db 15):
docker compose --profile stream up -d redis
python -m unittest tests.test_integration_redis -v
```
