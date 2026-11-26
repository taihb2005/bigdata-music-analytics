# Thay đổi của phần stream so với commit `aa925e7`

So sánh với commit gần nhất trên nhánh `realtime-streaming`: `aa925e7` (2026-11-02, "update luong stream"). Ở commit đó đã có khung speed layer: producer, Spark job, metadata worker/sink, lưu HBase/Redis và `tests/test_pipeline.py`. Mọi thay đổi dưới đây **chưa được commit**.

Cột "Kiểm chứng" ghi test tự động tương ứng (trong `tests/`), hoặc "chạy thật" nếu lỗi chỉ kiểm tra được trên Docker.

## 1. Đã sửa

### Producer (cào dữ liệu Last.fm)

| Vấn đề ở `aa925e7` | Cách sửa | Kiểm chứng |
|---|---|---|
| Không giới hạn tốc độ: cào 500 user tuần tự, không xử lý lỗi 29 (vượt giới hạn); một vòng thật dài hơn nhiều so với 60 giây | Giới hạn **chung** cho producer và worker qua Redis (`LASTFM_MAX_RPS`, mặc định 1 request/giây); lỗi 29 thì cả hệ thống nghỉ 60 giây; lỗi 26/10 (key bị khóa/sai) thì dừng; user ẩn lịch sử hoặc không tồn tại (17/6) bị tạm bỏ 24 giờ | `RateLimitTests`, `ClientErrorTests` |
| Poll mọi user với cùng nhịp | Lịch poll riêng cho từng user (60 giây → tối đa 900 giây nếu im lặng); mặc định chỉ poll user trạng thái `A` (406/500) | `ProducerTests` |
| Mỗi lần poll lấy lại 50 bài gần nhất; lần chạy đầu đẩy ~25.000 event cũ | Chỉ lấy bài mới bằng tham số `from`; lần đầu chỉ lấy `--backfill-minutes` (10) phút gần nhất | `test_poll_once_*` |
| Mất bài khi có nhiều hơn `max_pages` trang mới (Last.fm trả trang mới nhất trước, producer lấy các trang đó rồi đẩy checkpoint qua luôn) | Lấy các trang **cũ nhất** trước, lần poll sau đọc tiếp; cố định `to` trong một lần poll | `ProducerPagingTests` |
| `loved` luôn bằng 0 | Gọi `getRecentTracks` với `extended=1` | `test_recent_tracks_uses_extended_and_from`; chạy thật |
| Không biết chu kỳ poll thật | In cảnh báo khi `--min-interval` không đạt được (406 user ở 1 request/giây ≈ 7 phút một vòng) | không |

### Khóa và schema (dùng chung với batch)

| Vấn đề ở `aa925e7` | Cách sửa | Kiểm chứng |
|---|---|---|
| `entity_id` ưu tiên MBID, mà MBID lúc có lúc không, nên một bài bị tách thành nhiều id | Khóa dựa trên tên chuẩn hóa (`stream/common/ids.py`), đúng công thức của bảng trường nhóm (`t:`/`a:`/`al:`, `event_id`) | `IdsAndFactListenTests` |
| Không có dạng dữ liệu phẳng chung với batch | Bảng HBase `fact_listen` theo `docs/fields.csv` | `test_to_fact_listen_matches_team_field_table` |
| Khóa user phân biệt hoa thường (`Alice` ≠ `alice`), lệch với `fact_listen.user_id` | `ids.user_key()` dùng ở mọi nơi (Redis, row key HBase, API) | `test_user_keys_are_case_insensitive` |

### Xử lý và Redis

| Vấn đề ở `aa925e7` | Cách sửa | Kiểm chứng |
|---|---|---|
| Top bài/album gộp nhầm các bài trùng tên của nghệ sĩ khác nhau | Thành viên ZSET dạng `nghệ sĩ - tên`; tiền tố khóa mới `analytics:v2` | `test_same_title_by_different_artists_is_not_merged` |
| Redis không giới hạn bộ nhớ; nhiều khóa không có TTL | Mọi khóa có TTL (khử trùng 7 ngày, theo ngày 30 ngày, toàn thời gian của user 90 ngày kể từ lần nghe cuối, `duration-applied` 7 ngày); `maxmemory 512mb` + `noeviction` (đầy thì báo lỗi ghi, không lặng lẽ xóa khóa khử trùng rồi đếm trùng) | `RedisTtlTests`; Lua trên Redis thật |
| Tài liệu nói có watermark nhưng code không có | Sửa tài liệu; thêm top-K toàn cục theo cửa sổ 5 phút → 24 giờ, tính theo thời gian nghe | `test_global_windows_*` |
| (top-K, đợt 2) chỉ xấp xỉ, và API phải SCAN toàn bộ Redis | `ZUNIONSTORE` (kết quả chính xác); khóa đánh dấu bucket mới nhất thay cho SCAN | `GlobalTopTests` |
| Gọi HBase/Redis cho từng event | Snapshot ghi một lần mỗi (user, ngày) trong mỗi partition; tạo bảng HBase một lần lúc khởi động; `maxOffsetsPerTrigger` | `test_snapshot_written_once_per_user_and_day` |
| Ngưỡng "đến muộn" quá thấp, scrobble bình thường cũng bị đánh dấu | Ngưỡng 1800 giây | `test_late_events_*` |
| Race: metadata về đúng lúc event vừa vào danh sách chờ thì `listening_ms` không bao giờ được cộng | Pipeline đọc lại cache sau `rpush`; khóa `duration-applied` bảo đảm chỉ cộng một lần | `DurationRaceTests` |

### Metadata

| Vấn đề ở `aa925e7` | Cách sửa | Kiểm chứng |
|---|---|---|
| Metadata gọi theo từng cặp (user, bài), tốn hàng chục nghìn request | Artist/track/album là yêu cầu **dùng chung** (không truyền username); `userplaycount` chỉ lấy khi bật `--per-user-stats` | `MetadataRequestTests` |
| Lưu `wiki`, `bio`, danh sách bài của album, tên thật: tốn dung lượng và dễ chạm giới hạn 100 MB của Last.fm | Mặc định không lưu; bật bằng `--store-long-text`, `--keep-realname` | `test_long_text_and_realname_dropped_by_default` |
| Lỗi 29 bị tính là một lần thử và bị đưa vào dead-letter | Lỗi 29 không tính lần thử; lỗi vĩnh viễn thì không thử lại | `WorkerTests` |
| Yêu cầu đã vào dead-letter bị gửi lại mỗi lần bài được nghe lại | Trạng thái `failed` có thời gian chờ `--failure-backoff` (6 giờ) | `MetadataFailureTests` |
| Worker/sink crash vì `CommitFailedError` khi broker khởi động lại | `safe_commit`: ghi log rồi chạy tiếp; message được giao lại và xử lý lại an toàn | `SafeCommitTests`; chạy thật |

### Hạ tầng (Docker)

| Vấn đề ở `aa925e7` | Cách sửa | Kiểm chứng |
|---|---|---|
| **Kafka không ghi vào volume** (image ghi log vào `/tmp` trong container): tạo lại container là mất hết, offset quay về 0, và Spark **lặng lẽ bỏ qua** message mới (lúc chạy thử mất 349/3000 event) | `KAFKA_LOG_DIRS: /var/lib/kafka/data`; checkpoint mới `spark-speed-layer-v4`; Spark dừng và báo lỗi nếu checkpoint đi trước offset Kafka | `CheckpointGuardTests`; chạy thật |
| **HBase kẹt "Master is initializing"** sau khi tạo lại container (ZooKeeper không được lưu), trong khi healthcheck chỉ kiểm tra cổng nên vẫn báo healthy | Thêm volume `hbase_zk`; healthcheck chạy lệnh `list` thật; dùng volume mới `hbase_data_v2` | chạy thật |
| Redis phải tạo `redis.conf` bằng tay; mọi cổng mở ra mọi địa chỉ | Mật khẩu lấy từ `.env`; mọi cổng chỉ nghe ở `127.0.0.1` | không |
| `kafka:latest` và image MinIO cá nhân không rõ nguồn | Ghim `apache/kafka:3.8.0`; dùng image chính thức `minio/minio` (chỉ bật khi chạy `--profile batch`) | không |
| Chưa có healthcheck, giới hạn bộ nhớ, `.env.example` | Đã thêm | không |
| Rủi ro build `happybase` trên Python 3.13 | Image Python 3.12, cài gcc tạm thời khi build | chạy thật |
| Trên Windows, `print` tiếng Việt làm chết tiến trình (console cp1252) | `stream/__init__.py` chuyển stdout/stderr sang UTF-8 | chạy thật |

## 2. Đã thêm

| Hạng mục | File |
|---|---|
| Lõi xử lý tách khỏi Spark, test được không cần Spark | `stream/processing/pipeline.py` |
| API HTTP chỉ đọc: thống kê user, xu hướng N lượt gần nhất, event từ HBase, top-K toàn cục; pool kết nối HBase | `stream/api/server.py`, `stream/storage/redis_reader.py` |
| Replay và dữ liệu giả để demo/đo tải, không tốn request Last.fm (`source=replay`, `--shift-to-now`) | `stream/producer/replay_scrobbles.py`, `stream/tools/gen_sample_events.py` |
| Giới hạn tốc độ dùng chung qua Redis | `stream/common/rate_limit.py` |
| Bảng trường của nhóm và ánh xạ `fact_listen` | `docs/fields.csv`, `stream/common/fact_listen.py`, `stream/common/ids.py` |
| Test: 79 test tự động (Redis/HBase/Kafka giả), trong đó 3 test chạy đoạn Lua trên Redis thật | `tests/test_fixes.py`, `tests/test_review_fixes.py`, `tests/test_integration_redis.py`, `tests/fakes.py` |
| Cấu hình | `.env.example`, `.gitattributes`; thêm `dataset/sample/` và `.vscode/` vào `.gitignore` |

## 3. Đã chạy thử trên Docker

- Phát lại 3000 event mẫu hai lần: 6000 event = 3000 mới + 3000 trùng; 0 event đến muộn; 0 request Last.fm.
- Tạo lại container Kafka và HBase: offset và 5651 dòng `fact_listen` còn nguyên; Spark và API tự nối lại.
- Producer `--once` với 3 user thật: 9 scrobble; 28 yêu cầu metadata đều `available`; `listening_ms` được cộng; `loved` đúng.

## 4. Còn thiếu

**Tính năng chính của stream:**
- **Gợi ý bài tương tự theo thời gian thực** (user đang nghe bài gì → gợi ý bài có thể nghe tiếp): **chưa làm.** Dự kiến:
  1. Lấy `track.getSimilar`, cache dùng chung theo bài; nếu bài không có dữ liệu thì dùng nghệ sĩ tương tự đã lưu trong HBase.
  2. Loại các bài user vừa nghe, rồi xếp hạng theo sở thích của user.
  3. Lưu kết quả vào Redis, thêm API `GET /users/<user>/recommendations`.

**Kết nối với batch (cần thống nhất với nhóm batch):**
- Chưa có đường Kafka → MinIO để lưu dữ liệu thô cho batch.
- `tags` và `duration_ms` trong `fact_listen` chưa được điền từ metadata.
- Chưa có phần gộp kết quả batch với kết quả stream (tầng serving).

**Chưa kiểm chứng hoặc còn hạn chế:**
- Producer chưa chạy liên tục với đủ 406 user.
- Độ trễ thật khoảng 7 phút trở lên: Last.fm chỉ ghi nhận scrobble khi đã nghe được nửa bài, và 406 user ở mức 1 request/giây thì mỗi user khoảng 7 phút mới được poll một lần. Để demo nên dùng nhóm 20–30 user.
- Spark vẫn xử lý từng event bằng Python trong `foreachPartition`; nếu cần thông lượng cao hơn nhiều thì nên chuyển phần đếm sang cửa sổ stateful của Spark.
- Chưa có dashboard và chưa có giám sát (consumer lag, số lỗi 29, dung lượng Redis/HBase).

## 5. Khi nâng cấp từ `aa925e7`

- Tạo `.env` từ `.env.example`; không cần `redis/redis.conf` nữa.
- Khóa Redis cũ (`analytics:user:*`, và `analytics:v2:user:*` viết hoa hoặc không có TTL) không còn được đọc; không cần thì xóa tay.
- Checkpoint Spark mới là `spark-speed-layer-v4`.
- HBase dùng volume mới `hbase_data_v2` cùng `hbase_zk`. Volume cũ `hbase_data` được giữ nguyên (bản sao lưu ở `dataset/state/backup/`); không cần thì xóa: `docker volume rm bigdata-music-analytics_hbase_data`.
- Row key HBase theo user đổi sang chữ thường, và id metadata đổi từ MBID sang tên chuẩn hóa, nên các dòng HBase cũ không còn được dùng.
