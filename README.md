# Big Data Music Analytics: phần stream

Phân tích xu hướng nghe nhạc từ Last.fm theo kiến trúc Lambda. Thư mục này là **speed layer**:

```text
Last.fm ── producer (poll theo lịch, có giới hạn tốc độ) ──► Kafka scrobble.raw
                                                                  │
                                            Spark Structured Streaming (đọc Kafka)
                                                                  │  process_events()
              ┌───────────────────────────────┬───────────────────┴──────────────────┐
              ▼                               ▼                                      ▼
   HBase: event thô, fact_listen,     Redis: khử trùng event_id,          Kafka metadata.request
   snapshot theo user/ngày            thống kê theo user, top-K toàn      → worker → metadata.enriched
   (tầng serving)                     cục theo cửa sổ 5 phút/1 giờ        / metadata.dead-letter → sink → HBase + Redis
                                                                  │
                                                         API HTTP (đọc Redis + HBase)
```

Chi tiết từng bước: `docs/stream-flow.md`. Các thay đổi so với bản trước: `docs/CHANGES.md`.

## Chạy

```bash
cp .env.example .env     # điền LASTFM_API_KEY và REDIS_PASSWORD (bắt buộc)
docker compose up -d --build
docker compose logs -f scrobble-producer spark-speed-layer
```

Mọi cổng chỉ nghe ở `127.0.0.1`. Redis lấy mật khẩu từ `.env` nên **không còn file `redis.conf` phải tạo tay**. Máy cần khoảng 8 GB RAM trống cho Docker (giới hạn trong compose: Kafka 1 GB, HBase 2 GB, Spark 3 GB, Redis 0,7 GB).

| Dịch vụ | Cổng (localhost) |
|---|---|
| Kafka | 9092 |
| HBase Master, Thrift, REST | 16010, 9090, 8080 |
| Redis | 6379 (cần mật khẩu) |
| API | 8000 |
| MinIO (chỉ khi `--profile batch`) | 9000, 9001 |

## Giới hạn tốc độ và dung lượng của Last.fm (đọc trước khi chạy 500 user)

- **Tổng** request/giây tới Last.fm (producer + metadata worker) do `LASTFM_MAX_RPS` quyết định (mặc định 1.0), chia sẻ qua Redis. Gặp lỗi 29, cả hệ thống nghỉ 60 giây; lỗi 26/10 (key bị khóa/sai) thì dừng.
- Poll một vòng tất cả user cần ít nhất `số user ÷ LASTFM_MAX_RPS` giây. Vì vậy producer **chỉ poll user trạng thái `A`** (đang hoạt động, khoảng 406/500 user) và giãn lịch của user im lặng (60 giây → tối đa 900 giây). Muốn poll thêm: `--statuses A,?` hoặc `all`.
- **Con số thật:** 406 user ở 1 request/giây nghĩa là mỗi user được poll **khoảng 7 phút một lần trở lên** (worker còn dùng chung giới hạn), không phải 60 giây. Producer in cảnh báo này khi khởi động. Muốn độ trễ thấp hơn thì giảm số user.
- Điều khoản API của Last.fm giới hạn dữ liệu được lưu ở **100 MB** (cần xin phép bằng văn bản nếu muốn nhiều hơn; khi dùng cho học thuật nên liên hệ trước). Ước tính thô 500 user × ~147 lượt nghe/ngày ≈ 50–70 MB event mỗi ngày trong HBase. Vì vậy: metadata dùng chung giữa các user, **không lưu** `wiki`, `bio`, danh sách bài của album và tên thật (thêm `--store-long-text`, `--keep-realname` cho `metadata-sink` nếu thật sự cần), và nên thu nhỏ danh sách user khi chạy dài ngày.

## Producer

`stream/producer/produce_scrobbles.py` poll theo lịch từng user, chỉ lấy scrobble mới bằng tham số `from` (không lấy lại 50 bài gần nhất mỗi lần), gọi `user.getRecentTracks` với `extended=1` (để có `loved`). Lần chạy đầu chỉ lấy scrobble trong `--backfill-minutes` (mặc định 10) phút gần nhất, không đẩy ~25.000 event cũ. Nếu user có nhiều hơn `--max-pages` trang bài mới (ví dụ producer tắt lâu), producer lấy các trang **cũ nhất** trước và đọc tiếp ở lần poll sau, nên không bỏ sót bài nào. Checkpoint (event đã gửi và `last_uts`) nằm ở `dataset/state/<user>_seen.json`.

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

- Mặc định giữ nguyên thời điểm nghe và `ingested_at` trong file (độ trễ giả lập 30–400 giây), nên event không bị coi là đến muộn. `--shift-to-now` dời cả log (giữ độ trễ từng event) và tính lại `event_id`.
- Replay đặt `source=replay`, và Spark chỉ gửi yêu cầu metadata cho event `source=lastfm` (`--metadata-sources`), nên dữ liệu giả **không tốn request Last.fm**. Phát lại log thật muốn có metadata thì thêm `--source lastfm`.
- Trên Git Bash (Windows), thêm `MSYS_NO_PATHCONV=1` trước lệnh, nếu không đường dẫn `/app/...` sẽ bị đổi thành `C:/Program Files/Git/app/...`.

## Speed layer

Spark **chỉ đọc Kafka** rồi gọi `stream/processing/pipeline.py::process_events` cho từng partition. Không có watermark hay cửa sổ stateful của Spark. Thay vào đó:

- **Khử trùng** `event_id` bằng Lua trên Redis (khóa có TTL `--dedupe-days`, mặc định 7 ngày); chạy lại micro-batch hay replay hai lần không đếm dôi.
- **Mọi khóa Redis đều có TTL** (thống kê toàn thời gian của user: `--user-ttl-days` 90, gia hạn mỗi lần nghe) và Redis chạy `noeviction`: đầy bộ nhớ thì báo lỗi ghi chứ không lặng lẽ xóa khóa khử trùng.
- Khóa user **không phân biệt hoa thường** (`Alice` = `alice`), khớp `fact_listen.user_id`.
- Khi khởi động, Spark so checkpoint với offset Kafka; nếu checkpoint đi trước (topic bị tạo lại) thì dừng và báo lỗi, thay vì bỏ qua message mới.
- **Thống kê theo user** (tổng, theo ngày, top bài/nghệ sĩ/album, 50 lượt gần nhất). Thành viên của top bài và album có dạng `nghệ sĩ - tên`, nên hai bài cùng tên của hai nghệ sĩ không bị gộp.
- **Top-K toàn cục** theo cửa sổ 5 phút và 1 giờ, tính theo **thời gian nghe** (không phải thời gian xử lý).
- **HBase** (tầng serving): `music_scrobble_events` (event thô), `fact_listen` (dòng phẳng khớp bảng trường nhóm đã chốt: `docs/fields.csv`), snapshot theo user/ngày (ghi một lần mỗi user mỗi partition, không ghi cho từng event) và các bảng metadata.
- Event đến muộn (> `--late-after-seconds`, mặc định 1800) được ghi thêm vào `music_late_events`.

Khóa thực thể (`t:`, `a:`, `al:`) và `event_id` dùng **cùng công thức với bảng trường của nhóm** (`stream/common/ids.py`) và dựa trên tên chuẩn hóa, không dựa MBID, nên batch và stream ghép được theo cùng khóa.

## Metadata

Mặc định artist, track, album là yêu cầu **dùng chung** (không truyền `username` cho Last.fm): cùng một bài do nhiều user nghe chỉ tốn một request. `userplaycount` theo từng user chỉ lấy khi chạy Spark với `--per-user-stats` (mỗi cặp user × mục một request, rất tốn). Lỗi tạm thời được thử lại có backoff, lỗi vĩnh viễn không thử lại, và lỗi 29 không bị đưa vào dead-letter. Yêu cầu vào dead-letter được ghi `failed` và **không bị gửi lại trong `--failure-backoff`** (mặc định 6 giờ) dù bài đó được nghe lại.

## API

```bash
curl "localhost:8000/users/<username>/analytics?top=10&recent=10"
curl "localhost:8000/users/<username>/trend?n=20"            # xu hướng N lượt nghe gần nhất
curl "localhost:8000/users/<username>/events?limit=20"       # từ HBase
curl "localhost:8000/global/top?kind=tracks&window=1h&k=10"  # window: 5m, 15m, 1h, 6h, 24h
python stream/tools/show_user_analytics.py <username>        # bản dòng lệnh
```

Không truyền `end`, "bây giờ" là bucket mới nhất có dữ liệu (đúng khi replay dữ liệu cũ).

## Cấu trúc

```text
stream/common/       ids, giới hạn tốc độ, client Last.fm, fact_listen, yêu cầu metadata, kafka_utils
stream/producer/     poll thật (produce_scrobbles) và replay (replay_scrobbles)
stream/processing/   pipeline (lõi), realtime_store (Redis), spark_speed_layer (đọc Kafka)
stream/metadata/     worker (gọi getInfo) và sink (ghi HBase, Redis)
stream/storage/      hbase_store, redis_reader, redis_utils
stream/api/          API HTTP chỉ đọc
stream/tools/        gen_sample_events, show_user_analytics
docs/                stream-flow.md, CHANGES.md, fields.csv (bảng trường của nhóm)
tests/               test_pipeline.py (bản gốc), test_fixes.py, test_review_fixes.py, test_integration_redis.py (Redis thật), fakes.py
```

## Kiểm thử

```bash
python -m unittest discover -s tests -t .                 # 79 test; không cần Docker
docker compose up -d redis
python -m unittest tests.test_integration_redis -v        # chạy CHÍNH đoạn Lua trên Redis thật (db 15, bị xóa sạch)
```

Không có Redis thì `test_integration_redis` tự bỏ qua.

**Đã chạy thử trên Docker:**
- Replay 3000 event hai lần: 6000 event, 3000 mới, 3000 trùng, 0 đến muộn, 0 request metadata.
- Tạo lại container Kafka và HBase: offset và dữ liệu còn nguyên; Spark và API tự nối lại.
- Producer `--once` với 3 user thật: 9 scrobble, 28 yêu cầu metadata, đều `available`; duration được cộng vào `listening_ms`.

**Chưa chạy:** producer poll đủ 406 user trong thời gian dài.

## Nâng cấp từ bản cũ

- Xóa khóa Redis cũ `analytics:user:*` (khóa mới có tiền tố `analytics:v2`, thành viên top đã đổi sang `nghệ sĩ - tên`).
- Khóa `analytics:v2:user:*` tạo bởi bản trước đợt 2 **không có TTL** và có thể viết hoa (`RU-SHUTNICK`), trong khi bản mới dùng chữ thường. Chúng không được đọc nữa và không tự hết hạn; nếu không cần thì xóa tay.
- Checkpoint Spark mới là `spark-speed-layer-v4`: Kafka bản trước ghi log vào `/tmp` trong container (mất khi tạo lại), nên checkpoint v3 không còn khớp. Nay Kafka ghi vào volume (`KAFKA_LOG_DIRS`).
- HBase dùng volume mới `hbase_data_v2` cùng volume ZooKeeper `hbase_zk`. Volume cũ `hbase_data` bị kẹt "Master is initializing" nên được giữ nguyên, không dùng nữa (bản sao lưu: `dataset/state/backup/`). Không cần thì xóa: `docker volume rm bigdata-music-analytics_hbase_data`.
- Row key HBase theo user đổi sang chữ thường; id thực thể metadata đổi từ MBID sang tên chuẩn hóa, nên các dòng metadata cũ không còn được dùng.
- Bảng HBase `fact_listen` được `stream-init` và Spark tự tạo.
- Tạo `.env` từ `.env.example`; `redis/redis.conf` không còn cần cho Docker.
