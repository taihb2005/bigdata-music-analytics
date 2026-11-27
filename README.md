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
              │                                                         │                            │
              └────────────────────────────┬────────────────────────────┴────────────────────────────┘
                                           ▼
                                  API HTTP (api/server.py)
                                           │
                                  Dashboard Streamlit (ui/app.py)
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

* **Dashboard Streamlit:** `http://localhost:8501` — chuyển đổi giữa **Trực tuyến** (Stream) và **Batch Gold** ở thanh bên.
* **Endpoints của HTTP API (`http://localhost:8001`):**

```powershell
# Kiểm tra sức khỏe API
Invoke-RestMethod 'http://localhost:8001/health'

# Batch Gold: lấy manifest mới nhất và đọc bảng phân trang (limit 1..1000)
$gold = Invoke-RestMethod 'http://localhost:8001/batch/gold'
$page = Invoke-RestMethod "http://localhost:8001/batch/gold/listening_sessions?run_id=$($gold.run_id)&offset=0&limit=100"
$page.rows

# Stream: thống kê user, xu hướng, lịch sử HBase và bảng xếp hạng toàn hệ thống
curl "http://localhost:8001/users/<username>/analytics?top=10&recent=10"
curl "http://localhost:8001/users/<username>/trend?n=20"
curl "http://localhost:8001/users/<username>/events?limit=20"
curl "http://localhost:8001/global/top?kind=tracks&window=1h&k=10"
python stream/tools/show_user_analytics.py <username>
```

Nếu muốn chạy API hoặc Dashboard trực tiếp bằng Python trên máy local:
```powershell
pip install -r requirements/api.txt -r requirements/ui.txt
python -m api
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
api/                 HTTP Serving API chỉ đọc (kết nối MinIO Gold + Redis + HBase)
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
