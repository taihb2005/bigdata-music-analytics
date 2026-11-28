# Chạy Gold với Spark

Worker `gold-worker` dùng Spark 3.5.6, Delta Lake 3.3.3 và Java 17 trong image `music-gold:local`. Worker đọc một phiên bản cố định của bảng Delta `fact_listen` bằng `versionAsOf`. Dữ liệu đầu ra nằm trong thư mục Gold riêng cho mỗi `run_id`.

Gold tổng hợp 12 bảng theo UTC và ghi Parquet vào MinIO. Mỗi bảng có thể gồm nhiều file. Worker kiểm tra các file đã tải lên trước khi công bố manifest v4. API hỗ trợ cả kết quả v4 và các bản Gold v2/v3 trước đó.

## Chạy bằng Compose

Đặt `GOLD_SILVER_PATH` và `GOLD_MINIO_ENDPOINT` theo nơi lưu dữ liệu hiện có. Ví dụ, với Silver trong MinIO của Compose, dùng `s3a://silver/fact_listen` và `minio:9000`. Các lệnh sau dùng cho stack khởi động từ Compose trong repo, khi MinIO đã chạy.

```powershell
docker compose --profile gold build gold-worker api
docker compose --profile gold run --rm --no-deps gold-worker python -m scripts.check_spark_migration
docker compose --profile gold up -d --no-deps api gold-worker
```

Lệnh kiểm tra đối chiếu số dòng và khóa sự kiện của cùng một phiên bản Silver bằng Spark và delta-rs. Worker định kỳ kiểm tra Silver sau mỗi `GOLD_INTERVAL_SECONDS` (mặc định 900 giây). Lần đầu dùng Spark sẽ xuất một bản v4 mới từ Silver hiện có.

Các biến `BATCH_ENGINE`, `BATCH_SPARK_MASTER` và `BATCH_SPARK_SHUFFLE_PARTITIONS` cấu hình riêng cho Gold. Mặc định tương ứng là `spark`, `local[2]` và `4`. Để dùng lại DuckDB ở Gold, đặt `BATCH_ENGINE=legacy` trong `.env` và tạo lại `gold-worker`.

## Kiểm thử Gold

Chạy từ thư mục gốc repo sau khi build image. Các bài kiểm thử dùng bảng Delta và Parquet trong thư mục tạm.

```powershell
docker run --rm --mount "type=bind,source=${PWD}/tests,target=/app/tests,readonly" music-gold:local python -m unittest tests.test_gold -q
docker run --rm --mount "type=bind,source=${PWD}/tests,target=/app/tests,readonly" -e RUN_BATCH_SPARK_TEST=1 music-gold:local python -m unittest tests.test_spark_batch -v
```
