# Big Data Music Analytics

Pipeline thu thap scrobble cua toan bo Last.fm user trong sheet Showoffs va xu
ly theo luong Kafka -> Spark Structured Streaming -> HBase + Redis. Metadata
thieu duoc lay bat dong bo qua cac topic Kafka rieng.

## Cai dat

```powershell
python -m pip install -r requirements.txt
docker compose up -d --build
```

File `.env` can co:

```text
LASTFM_API_KEY=your_api_key
```

Docker Compose khoi dong:

```text
Kafka
Redis
HBase + Thrift
Scrobble Producer
Spark Speed Layer
Metadata Worker
Metadata Sink
```

## Danh sach user Showoffs

Tao lai toan bo danh sach user trong sheet `Showoffs`:

```powershell
python scripts/extract_users.py
```

Ket qua mac dinh: `dataset/users/showoffs_all_users.json`.

## Phat scrobble vao Kafka

Mac dinh producer doc `dataset/users/showoffs_all_users.json`. Chay stream:

```powershell
python stream/producer/produce_scrobbles.py `
  --poll-seconds 60 `
  --limit 50
```

Producer chi ho tro file danh sach user. Event duoc phat ngay voi cac block
metadata rong dung schema, tranh goi Track/Artist/Album API cho tung scrobble.
`scrobble_event.image` chi giu URL anh co kich co `extralarge`; cac kich co
small, medium va large bi loai.

Mac dinh producer ket noi `localhost:9092`, gui vao topic `scrobble.raw`, va
luu checkpoint rieng tung username trong `dataset/state/` de tranh gui lai event.

Tuy chon Kafka co the dat qua bien moi truong:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=scrobble.raw
```

## Speed Layer

Ma nguon stream duoc tach theo muc dich:

```text
stream/
|-- producer/    # Lay recent tracks va gui scrobble.raw
|-- processing/  # Spark Streaming va cap nhat realtime
|-- metadata/    # Metadata worker va metadata sink
|-- storage/     # Adapter HBase va Redis
|-- bootstrap/   # Tao Kafka topics va HBase tables
|-- tools/       # Cong cu xem ket qua
`-- common/      # Last.fm client va message helpers dung chung
```

Spark Structured Streaming doc `scrobble.raw` va dung `event_id` de ghi
idempotent vao HBase, Redis. Event cu van duoc xu ly; event den tre hon nguong
duoc luu them vao `music_late_events` de kiem tra. Neu metadata
user/track/artist/album chua co, Spark gui request vao `metadata.request`.

```text
scrobble.raw
    -> Spark Structured Streaming
       -> HBase music_scrobble_events
       -> Redis analytics:user:*
       -> metadata.request
```

## Metadata enrichment

```text
metadata.request
    -> Metadata Worker
       -> Last.fm user/track/artist/album getInfo
       -> metadata.enriched hoac metadata.dead-letter

metadata.enriched
    -> Metadata Sink
       -> HBase metadata tables
       -> Redis metadata cache
```

Kafka topics:

```text
scrobble.raw
metadata.request
metadata.enriched
metadata.dead-letter
```

Khoi tao lai topics va HBase tables khi chay ngoai Docker:

```powershell
python stream/bootstrap/init_stream_resources.py
```

Chay Spark ngoai Docker can Spark 4.2.0 va Kafka connector:

```powershell
spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 `
  stream/processing/spark_speed_layer.py
```

Redis uu tien `REDIS_PASSWORD`; khi chay local script co the doc `requirepass`
tu `redis/redis.conf`. Mat khau khong duoc in ra log.

Xem ket qua cua mot user:

```powershell
python stream/tools/show_user_analytics.py --username kreisky
```

Moi user co summary, top tracks, top artists, top albums, lich su gan day va
thong ke theo ngay. Event duoc chong trung ben vung bang `event_id`. Khi
metadata track ve sau event, metadata sink cap nhat bu duration vao Redis.

HBase development UI: `http://localhost:16010`. HBase Thrift: `localhost:9090`.
Image HBase trong Compose chi phu hop development; production can HBase cluster
duoc van hanh rieng.
