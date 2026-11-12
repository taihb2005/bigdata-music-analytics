# Big Data Music Analytics: phần stream

Phân tích xu hướng nghe nhạc từ Last.fm theo kiến trúc Lambda. Tài liệu này nói về phần **speed layer** (`stream/`). Kiến trúc chung và hợp đồng dữ liệu giữa hai nhóm: `docs/architecture.md`.

```text
Last.fm ── producer batch (Celery, mỗi giờ) ──► Kafka events.bronze.lastfm ──► batch consumer → MinIO Bronze
                                                        │
                       replay (dữ liệu giả) ──► Kafka events.demo.lastfm
                                                        │
                                Spark Structured Streaming (đọc cả hai topic)
                                                        │  process_events()
              ┌─────────────────────────────────────────┴───────────────┐
              ▼                                                         ▼
   HBase: event thô, fact_listen,                        Redis: khử trùng event_id,
   snapshot theo user/ngày (tầng serving)                thống kê theo user, top-K 5 phút/1 giờ
                                                        │
                                               API HTTP (đọc Redis + HBase)
```

Message của producer batch đã kèm metadata (thời lượng bài, tag), nên speed **không gọi Last.fm** cho chúng. Metadata worker/sink chỉ còn phục vụ event định dạng cũ.

Chi tiết từng bước: `docs/stream-flow.md`. Các thay đổi so với bản trước: `docs/CHANGES.md`.

## Chạy

```bash
cp .env.example .env     # điền LASTFM_API_KEY, LASTFM_USERS, REDIS_PASSWORD, MINIO_* (bắt buộc)
docker compose up -d --build
docker compose logs -f celery-worker spark-speed-layer
```

Redis lấy mật khẩu từ `.env` nên **không còn file `redis.conf` phải tạo tay**. Máy cần khoảng 8 GB RAM trống cho Docker (giới hạn trong compose: HBase 2 GB, Spark 3 GB, Redis 0,7 GB; Kafka và MinIO chưa đặt giới hạn).

| Dịch vụ | Cổng | Nghe ở |
|---|---|---|
| Kafka | 9092 | **mọi địa chỉ** (`docker/kafka.yml`) |
| MinIO API, Console | 9000, 9001 | **mọi địa chỉ** (`batch/docker-compose.yml`) |
| Redis | 6379 (cần mật khẩu) | **mọi địa chỉ** (khai báo trong `batch/docker-compose.yml`) và `127.0.0.1` |
| HBase Master, Thrift, REST | 16010, 9090, 8080 | `127.0.0.1` |
| API | 8001 (host) → 8000 (container) | `127.0.0.1` |

Kafka, HBase Thrift và API không có xác thực. Kafka, MinIO và Redis đang mở ra mọi địa chỉ của máy, nên nếu máy nối mạng chung (wifi trường, quán cà phê) thì người khác có thể truy cập. Nên đổi về `127.0.0.1` (vấn đề 7 ở mục 6 của `docs/architecture.md`).

## Giới hạn tốc độ và dung lượng của Last.fm (đọc trước khi chạy 500 user)

- **Tổng** request/giây tới Last.fm (producer + metadata worker) do `LASTFM_MAX_RPS` quyết định (mặc định 1.0), chia sẻ qua Redis. Gặp lỗi 29, cả hệ thống nghỉ 60 giây; lỗi 26/10 (key bị khóa/sai) thì dừng.
- Poll một vòng tất cả user cần ít nhất `số user ÷ LASTFM_MAX_RPS` giây. Vì vậy producer **chỉ poll user trạng thái `A`** (đang hoạt động, khoảng 406/500 user) và giãn lịch của user im lặng (60 giây → tối đa 900 giây). Muốn poll thêm: `--statuses A,?` hoặc `all`.
- **Con số thật:** 406 user ở 1 request/giây nghĩa là mỗi user được poll **khoảng 7 phút một lần trở lên** (worker còn dùng chung giới hạn), không phải 60 giây. Producer in cảnh báo này khi khởi động. Muốn độ trễ thấp hơn thì giảm số user.
- Điều khoản API của Last.fm giới hạn dữ liệu được lưu ở **100 MB** (cần xin phép bằng văn bản nếu muốn nhiều hơn; khi dùng cho học thuật nên liên hệ trước). Ước tính thô 500 user × ~147 lượt nghe/ngày ≈ 50–70 MB event mỗi ngày trong HBase. Vì vậy: metadata dùng chung giữa các user, **không lưu** `wiki`, `bio`, danh sách bài của album và tên thật (thêm `--store-long-text`, `--keep-realname` cho `metadata-sink` nếu thật sự cần), và nên thu nhỏ danh sách user khi chạy dài ngày.

## Producer

Producer chính là **producer batch** (`batch/producer/`, hướng dẫn: `docs/producer.md`). Nó ghi vào `events.bronze.lastfm` theo schema `batch/producer/schema.py`.

**Producer cũ của speed (ngừng dùng).** Nó vẫn còn trong repo, chạy được bằng `docker compose --profile legacy up scrobble-producer`, nhưng ghi vào `scrobble.raw` mà Spark không còn đọc. Không bật song song với producer batch: cả hai dùng chung API key Last.fm. Mô tả cũ: `stream/producer/produce_scrobbles.py` poll theo lịch từng user, chỉ lấy scrobble mới bằng tham số `from` (không lấy lại 50 bài gần nhất mỗi lần), gọi `user.getRecentTracks` với `extended=1` (để có `loved`). Lần chạy đầu chỉ lấy scrobble trong `--backfill-minutes` (mặc định 10) phút gần nhất, không đẩy ~25.000 event cũ. Nếu user có nhiều hơn `--max-pages` trang bài mới (ví dụ producer tắt lâu), producer lấy các trang **cũ nhất** trước và đọc tiếp ở lần poll sau, nên không bỏ sót bài nào. Checkpoint (event đã gửi và `last_uts`) nằm ở `dataset/state/<user>_seen.json`.

```bash
python stream/producer/produce_scrobbles.py --once --statuses A --limit 50   # chạy thử trên máy
```

## Replay để demo và đo tải

Poll thật chỉ cho khoảng 1 event/giây hoặc ít hơn, không đủ để đo thông lượng. Dùng dữ liệu giả đúng định dạng thật:

```bash
python -m stream.tools.gen_sample_events --users 30 --events 3000 --days 14
docker compose --profile replay run --rm -e REPLAY_RATE=200 replay
# Muốn top "5 phút / 1 giờ gần nhất" như đang chạy thật: dời cả log về hiện tại
docker compose --profile replay run --rm replay python stream/producer/replay_scrobbles.py   --bootstrap-servers kafka:9094 --file /app/dataset/sample/scrobbles.jsonl --rate 200 --shift-to-now
```

- Replay ghi vào **`events.demo.lastfm`** (mặc định, không lấy `KAFKA_TOPIC` của `.env`). Spark đọc topic này, batch consumer thì không, nên dữ liệu giả không lẫn vào tầng Bronze.
- Mặc định giữ nguyên thời điểm nghe và `ingested_at` trong file (độ trễ giả lập 30–400 giây), nên event không bị coi là đến muộn. `--shift-to-now` dời cả log (giữ độ trễ từng event) và tính lại `event_id`.
- Replay đặt `source=replay`, và Spark chỉ gửi yêu cầu metadata cho event `source=lastfm` (`--metadata-sources`), nên dữ liệu giả **không tốn request Last.fm**. Phát lại log thật muốn có metadata thì thêm `--source lastfm`.
- Trên Git Bash (Windows), thêm `MSYS_NO_PATHCONV=1` trước lệnh, nếu không đường dẫn `/app/...` sẽ bị đổi thành `C:/Program Files/Git/app/...`.

## Speed layer

Spark **chỉ đọc Kafka** (`events.bronze.lastfm`, `events.demo.lastfm`) rồi gọi `stream/processing/pipeline.py::process_events` cho từng partition. Message batch không có trường `user`, nên Spark lấy username từ `user_profile.name`. Không có watermark hay cửa sổ stateful của Spark. Thay vào đó:

- **Metadata có sẵn:** với message batch (`schema_version` = `"1.0"`), thời lượng bài và tag lấy thẳng từ message (`track_metadata`, `artist_metadata`), không gửi `metadata.request`.
- **Khử trùng** `event_id` (do producer tính) bằng Lua trên Redis (khóa có TTL `--dedupe-days`, mặc định 7 ngày); chạy lại micro-batch hay replay hai lần không đếm dôi.
- **Mọi khóa Redis đều có TTL** (thống kê toàn thời gian của user: `--user-ttl-days` 90, gia hạn mỗi lần nghe) và Redis chạy `noeviction`: đầy bộ nhớ thì báo lỗi ghi chứ không lặng lẽ xóa khóa khử trùng.
- Khóa user **không phân biệt hoa thường** (`Alice` = `alice`), khớp `fact_listen.user_id`.
- Khi khởi động, Spark so checkpoint với offset Kafka; nếu checkpoint đi trước (topic bị tạo lại) thì dừng và báo lỗi, thay vì bỏ qua message mới.
- **Thống kê theo user** (tổng, theo ngày, top bài/nghệ sĩ/album, 50 lượt gần nhất). Thành viên của top bài và album có dạng `nghệ sĩ - tên`, nên hai bài cùng tên của hai nghệ sĩ không bị gộp.
- **Top-K toàn cục** theo cửa sổ 5 phút và 1 giờ, tính theo **thời gian nghe** (không phải thời gian xử lý).
- **HBase** (tầng serving): `music_scrobble_events` (event thô), `fact_listen` (dòng phẳng khớp bảng trường nhóm đã chốt: `docs/fields.csv`), snapshot theo user/ngày (ghi một lần mỗi user mỗi partition, không ghi cho từng event) và các bảng metadata.
- Event đến muộn (> `--late-after-seconds`, mặc định 1800) được ghi thêm vào `music_late_events`. Với message batch, độ trễ tính từ `trigger_time` (lúc cửa sổ producer đóng), vì producer lấy trọn giờ trước nên tính từ lúc nghe thì event nào cũng trễ tới 1 giờ.

`event_id` lấy nguyên từ message của producer, không tính lại. Khóa thực thể (`t:`, `a:`, `al:`) tính bằng `stream/common/ids.py`, dựa trên tên chuẩn hóa chứ không dựa MBID, để batch và stream ghép được theo cùng khóa (`docs/architecture.md` mục 3.3).

## Metadata

Phần này chỉ áp dụng cho event định dạng cũ có `source=lastfm` (message batch đã kèm metadata). Mặc định artist, track, album là yêu cầu **dùng chung** (không truyền `username` cho Last.fm): cùng một bài do nhiều user nghe chỉ tốn một request. `userplaycount` theo từng user chỉ lấy khi chạy Spark với `--per-user-stats` (mỗi cặp user × mục một request, rất tốn). Lỗi tạm thời được thử lại có backoff, lỗi vĩnh viễn không thử lại, và lỗi 29 không bị đưa vào dead-letter. Yêu cầu vào dead-letter được ghi `failed` và **không bị gửi lại trong `--failure-backoff`** (mặc định 6 giờ) dù bài đó được nghe lại.

## API

```bash
curl "localhost:8001/users/<username>/analytics?top=10&recent=10"
curl "localhost:8001/users/<username>/trend?n=20"            # xu hướng N lượt nghe gần nhất
curl "localhost:8001/users/<username>/events?limit=20"       # từ HBase
curl "localhost:8001/global/top?kind=tracks&window=1h&k=10"  # kind: tracks, artists, albums; window: 5m, 15m, 1h, 6h, 24h
python stream/tools/show_user_analytics.py <username>        # bản dòng lệnh
```

Không truyền `end`, "bây giờ" là bucket mới nhất có dữ liệu (đúng khi replay dữ liệu cũ).

## Giao diện Streamlit tạm thời

Khởi động API bằng Docker Compose, sau đó mở PowerShell tại thư mục dự án:

```powershell
python -m pip install -r requirements-ui.txt
python -m streamlit run streamlit_app.py
```

Mở địa chỉ Streamlit in ra trong terminal (thường là `http://localhost:8501`), nhập username và bấm **Tải / làm mới dữ liệu**. Mặc định giao diện gọi API ở `http://localhost:8001`; có thể sửa địa chỉ này ở thanh bên. Giao diện hiển thị thống kê, xu hướng, lượt nghe từ HBase và top bài hát/nghệ sĩ/album toàn hệ thống. Top từng loại có cửa sổ thời gian riêng.

Top album toàn cục cần chạy phiên bản API và Spark mới; dữ liệu album được tích lũy từ lúc phiên bản này chạy, không hồi tố các event cũ.

## Cấu trúc

```text
stream/common/       bronze_event (đọc message batch), ids, fact_listen, giới hạn tốc độ, client Last.fm, yêu cầu metadata
stream/producer/     replay (replay_scrobbles); produce_scrobbles là producer cũ, ngừng dùng
stream/processing/   pipeline (lõi), realtime_store (Redis), spark_speed_layer (đọc Kafka)
stream/metadata/     worker (gọi getInfo) và sink (ghi HBase, Redis)
stream/storage/      hbase_store, redis_reader, redis_utils
stream/api/          API HTTP chỉ đọc
stream/tools/        gen_sample_events, show_user_analytics
docs/                architecture.md (kiến trúc chung), stream-flow.md, CHANGES.md, fields.csv (bảng trường của nhóm)
tests/               test_bronze_event.py (message batch), test_pipeline.py, test_fixes.py, test_review_fixes.py,
                     test_integration_redis.py (Redis thật), fakes.py; test_producer.py, test_batch_consumer.py của batch
```

## Kiểm thử

```bash
pip install -r batch/requirements.txt                     # test của batch cần celery, jsonschema, minio, ...
python -m unittest discover -s tests -t .                 # không cần Docker; test Spark tự bỏ qua nếu thiếu pyspark
docker compose up -d redis
python -m unittest tests.test_integration_redis -v        # chạy CHÍNH đoạn Lua trên Redis thật (db 15, bị xóa sạch)
```

Không có Redis thì `test_integration_redis` tự bỏ qua.

**Đã chạy thử trên Docker:**
- Replay 3000 event hai lần: 6000 event, 3000 mới, 3000 trùng, 0 đến muộn, 0 request metadata.
- Tạo lại container Kafka và HBase: offset và dữ liệu còn nguyên; Spark và API tự nối lại.
- Producer `--once` với 3 user thật: 9 scrobble, 28 yêu cầu metadata, đều `available`; duration được cộng vào `listening_ms`.

**Chưa chạy:** toàn hệ thống trên Docker với producer batch thật; producer chạy đủ 406 user trong thời gian dài. Các kết quả ở trên là của producer cũ.

## Nâng cấp từ bản cũ

- Checkpoint Spark mới là `spark-speed-layer-v5` (đổi topic sang `events.bronze.lastfm`, `events.demo.lastfm`). Checkpoint `v4` thuộc topic `scrobble.raw`, không dùng nữa.
- Replay ghi vào `events.demo.lastfm` thay cho `scrobble.raw`.

- Xóa khóa Redis cũ `analytics:user:*` (khóa mới có tiền tố `analytics:v2`, thành viên top đã đổi sang `nghệ sĩ - tên`).
- Khóa `analytics:v2:user:*` tạo bởi bản trước đợt 2 **không có TTL** và có thể viết hoa (`RU-SHUTNICK`), trong khi bản mới dùng chữ thường. Chúng không được đọc nữa và không tự hết hạn; nếu không cần thì xóa tay.
- (Bản trước) checkpoint `spark-speed-layer-v4`: Kafka bản trước nữa ghi log vào `/tmp` trong container (mất khi tạo lại), nên checkpoint v3 không còn khớp. Nay Kafka ghi vào volume (`KAFKA_LOG_DIRS`).
- HBase dùng volume mới `hbase_data_v2` cùng volume ZooKeeper `hbase_zk`. Volume cũ `hbase_data` bị kẹt "Master is initializing" nên được giữ nguyên, không dùng nữa (bản sao lưu: `dataset/state/backup/`). Không cần thì xóa: `docker volume rm bigdata-music-analytics_hbase_data`.
- Row key HBase theo user đổi sang chữ thường; id thực thể metadata đổi từ MBID sang tên chuẩn hóa, nên các dòng metadata cũ không còn được dùng.
- Bảng HBase `fact_listen` được `stream-init` và Spark tự tạo.
- Tạo `.env` từ `.env.example`; `redis/redis.conf` không còn cần cho Docker.
