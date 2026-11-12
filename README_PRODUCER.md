# Producer Last.fm: làm xong bản demo trong 60 phút

Mục tiêu: một job lấy đủ các lượt nghe trong một giờ, ghép 5 nhóm dữ liệu của
biên bản ngày 25/10/2026, gửi Kafka có xác nhận; Celery Beat chạy mỗi giờ.
Mốc 60 phút dành cho bản demo với một user, khi Docker đã chạy và đã có API key.
Thời gian tải image, số lượt nghe và phản hồi Last.fm có thể làm thời gian dài hơn.

```text
Celery Beat → Redis → Celery Worker → Last.fm API → Chuẩn hóa → Kafka
                                                           events.bronze.lastfm
```

Mã đã được tạo sẵn trong thư mục này. `crawl.py` và dữ liệu crawler cũ giữ nguyên.
Các file trống cũ (`exactor.py`, `lastfm_client.py`, `normalize.py`, `kafka_writer.py`,
`scheduler.py`) đã được xóa; bản này dùng các file trong bảng ở cuối.

## 0–5 phút: điền cấu hình

Mở Docker Desktop và đợi Docker Engine chạy. Dùng PowerShell trong VS Code:

```powershell
Set-Location D:\Bigdata
function dc { docker compose -f compose.producer.yml @args }
docker info --format '{{.OSType}}'
if (-not (Test-Path -LiteralPath '.env')) { Copy-Item -LiteralPath '.env.example' -Destination '.env' }
```

Kết quả Docker phải là `linux`. Hàm `dc` chỉ là cách viết ngắn của lệnh Compose;
khi mở terminal mới, chạy lại dòng định nghĩa hàm.

Mở `.env` bằng VS Code và điền hai dòng đầu:

```dotenv
LASTFM_API_KEY=API_KEY_CUA_BAN
LASTFM_USERS=USERNAME_LASTFM_CUA_BAN
```

Username là phần sau `https://www.last.fm/user/` trong địa chỉ trang cá nhân,
không phải API key, tên ứng dụng API hoặc tên hiển thị. Bắt đầu với **một user
đã có lịch sử nghe**. Sau khi chạy được, có thể dùng `user1,user2`.
Lưu file; giữ các cấu hình còn lại như mẫu. `.env` đã được loại khỏi Git và image.

Không cần cài Celery, Kafka hay Java trực tiếp trên Windows.

## 5–15 phút: build và khởi động

```powershell
dc build app
dc up -d kafka redis kafka-init worker
dc ps -a
```

Kết quả cần thấy:

| Dịch vụ | Trạng thái |
|---|---|
| `kafka-volume-init` | `Exited (0)` — đã cấp quyền ghi cho volume |
| `kafka` | `Up`, `healthy` |
| `redis` | `Up`, `healthy` |
| `kafka-init` | `Exited (0)` — đã tạo topic |
| `worker` | `Up` |

`Exited (0)` ở hai dịch vụ khởi tạo là bình thường. Chưa cần bật Beat ở bước này.
Image và volume dùng riêng project `bigdata-producer`.

Kiểm tra cấu hình và các bài test:

```powershell
dc run --rm --no-deps app python -m producer check-config
dc run --rm --no-deps app python -m unittest discover -s tests -v
```

`api_key_present` phải là `true`, `users` có username của bạn; lệnh không in API key.
Các bài test cần kết thúc bằng `OK`.

## 15–25 phút: thử luồng bằng dữ liệu mẫu

```powershell
dc run --rm --no-deps app python -m producer enqueue --demo --wait
dc run --rm --no-deps app python -m producer inspect --demo --count 2
```

Lệnh đầu đi qua **Redis → Worker → Kafka**, không cần Last.fm API.
Kết quả mong đợi:

```json
{
  "events": 2,
  "rejected": 0,
  "status": "published",
  "delivered": 2,
  "topic": "events.demo.lastfm"
}
```

Lệnh đọc lại phải hiển thị hai dòng có `schema: VALID`, tên bài `Demo Track A/B`.
Dữ liệu này là giả lập, có `source: demo`, đi vào topic demo riêng.
Chạy lại sẽ thêm message vào topic: Kafka không tự loại task chạy lại theo `event_id`.

## 25–40 phút: lấy dữ liệu thật

Chạy một lần chỉ lấy và lưu JSONL, chưa gửi Kafka:

```powershell
dc run --rm --no-deps app python -m producer run --latest --dry-run
```

`--latest` tìm một **giờ đã kết thúc có lượt nghe gần đây** của user, rồi lấy toàn bộ
lượt nghe trong giờ đó. Đây là cách tránh demo ra 0 event khi user không nghe nhạc
trong giờ vừa qua. Lịch tự động vẫn lấy đúng giờ liền trước thời điểm kích hoạt.

Kết quả chứa `events`, `rejected`, `metadata_incomplete`, `trigger_time`, `run_dir`.
Ví dụ đường dẫn container `/app/output/runs/1792890000_abcd1234` tương ứng
`D:\Bigdata\data\producer\runs\1792890000_abcd1234` trên Windows.

Mỗi lần chạy có ba file:

| File | Dùng để làm gì |
|---|---|
| `events.jsonl` | Mỗi dòng là một event đã chuẩn hóa và kiểm tra schema |
| `dead_letter.jsonl` | Bản ghi lỗi kèm lý do; không bỏ âm thầm |
| `summary.json` | Số lượng, khoảng giờ và trạng thái job |

Xem dòng đầu của lần chạy gần nhất:

```powershell
$producerRun = Get-ChildItem -LiteralPath '.\data\producer\runs' -Directory | Sort-Object LastWriteTime -Descending | Select-Object -First 1
Get-Content -LiteralPath (Join-Path $producerRun.FullName 'summary.json')
Get-Content -LiteralPath (Join-Path $producerRun.FullName 'events.jsonl') -TotalCount 1
```

Phải có đủ `scrobble_event`, `user_profile`, `track_metadata`, `artist_metadata`,
`album_metadata`. Các trường không được API cung cấp được giữ `null` hoặc danh sách
rỗng theo schema; `metadata_errors` ghi nhận metadata không tìm thấy. Không suy ra
số lượt nghe bằng 0 từ một trường bị thiếu.

Sau khi kiểm tra JSON, gửi dữ liệu thật bằng Celery:

```powershell
dc run --rm --no-deps app python -m producer enqueue --latest --wait
dc run --rm --no-deps app python -m producer inspect --count 1
```

Kết quả task cần có `status: published`, `events > 0`. Nếu `rejected = 0` thì
`delivered` phải bằng `events`. Nếu có bản ghi bị loại, `delivered` còn bao gồm
các bản ghi đã gửi thành công sang DLQ; cần xem file lỗi trước khi nghiệm thu.

`--wait` chờ tối đa 60 giây. Nếu hết thời gian, task vẫn có thể đang chạy: xem log
Worker và `summary.json`, tránh gửi thêm task chỉ vì lệnh chờ hết hạn.

```powershell
dc logs --tail 80 worker
```

Muốn chạy một khoảng giờ cụ thể:

```powershell
dc run --rm --no-deps app python -m producer run --trigger "2026-10-25T08:00:00+07:00"
```

Lệnh này lấy khoảng `[07:00, 08:00)` ngày 25/10 theo múi giờ `+07:00` rồi gửi Kafka.
Chỉ dùng ngày/giờ mà user thực sự có lịch sử nghe. Bỏ `--trigger` và `--latest`
sẽ lấy giờ liền trước mốc giờ hiện tại.

## 40–50 phút: bật lịch mỗi giờ

```powershell
dc up -d worker beat
dc logs --tail 50 beat worker
```

Beat chạy ở phút 0 của mỗi giờ. `trigger_time` được tính ở tiến trình Beat và
đưa vào task dưới dạng Unix timestamp; Worker nhận trễ hay retry vẫn dùng mốc cũ.
Chỉ chạy một Beat cho lịch này. UTC và giờ Việt Nam đều đổi giờ tại cùng phút 0;
các timestamp lưu trong dữ liệu dùng UTC.

Muốn kiểm tra ngay, dùng `enqueue --latest --wait` ở trên; không cần đợi đủ một giờ.
Sau khi sửa `.env`, dùng `dc up -d worker beat` để Compose cập nhật container.
Chỉ `restart` không nạp lại biến môi trường từ file mới.

## 50–60 phút: kiểm tra và bàn giao

Xuất hợp đồng dữ liệu:

```powershell
dc run --rm --no-deps app python -m producer schema
```

Gửi cho bạn làm Consumer:

- `data/producer/lastfm-event-v1.schema.json` và một dòng JSON mẫu đã bỏ thông tin riêng nếu cần.
- Topic thật `events.bronze.lastfm`; topic lỗi `events.bronze.lastfm.dlq`.
- Kafka key là username; mỗi message là một event, JSON UTF-8.
- Consumer trong cùng Docker network dùng `kafka:9092`.
- Consumer chạy trực tiếp trên **cùng máy Windows** dùng `localhost:19092`.
- `event_id` là khóa đề xuất để khử trùng khi retry/chạy lại; payload metadata có thể thay đổi.

Cổng Kafka hiện chỉ bind vào loopback để chạy local. Bạn kia chạy trên máy khác
không dùng được `localhost:19092` của bạn. Hai người có thể dùng cùng bộ Compose
trên từng máy để phát triển độc lập, hoặc thống nhất broker dùng chung rồi cấu hình
listener, địa chỉ truy cập và quyền mạng cho broker đó.

Được coi là hoàn thành bản demo khi:

- Test báo `OK`.
- Task demo đi qua Worker và đọc lại được trong Kafka.
- Task dữ liệu thật có `events > 0`, `status: published`; schema hợp lệ khi đọc lại.
- Bản ghi bị loại có lý do, không bị mất âm thầm.
- Beat đang chạy với username đã cấu hình.
- Bạn làm Consumer nhận được schema, message mẫu và thông tin kết nối.

## Xử lý lỗi thường gặp

| Dấu hiệu | Cách xử lý |
|---|---|
| Docker daemon không truy cập được | Mở Docker Desktop, đợi Linux Engine chạy rồi thử lại |
| Thiếu `LASTFM_USERS` | Điền username từ URL trang cá nhân vào `.env`, không dùng email/API key |
| API error `10` hoặc `26` | Kiểm tra API key hợp lệ hoặc trạng thái ứng dụng Last.fm |
| API error `6` ở user | Kiểm tra chính tả username; lỗi metadata riêng được ghi nhận ở `metadata_errors` |
| `events: 0` | User không có lượt nghe trong khoảng đó; thử `--latest` hoặc user có lịch sử |
| Timeout khi `enqueue --wait` | Xem `dc logs --tail 80 worker`; task có thể vẫn đang chạy |
| Không đọc được topic thật | Phải chạy dữ liệu thật trước; demo nằm ở topic khác |
| Kafka port 19092 đang bận | Dừng đúng dịch vụ đang dùng cổng đó, hoặc sửa cả port mapping và `EXTERNAL` listener |
| Kafka báo không ghi được volume | Xem `dc logs kafka-volume-init kafka`; Compose có bước cấp quyền riêng |
| Sửa Python nhưng container vẫn chạy code cũ | `dc build app`, sau đó `dc up -d worker beat` |
| `publish_failed` | Sửa kết nối Kafka, rồi dùng lệnh replay bên dưới |

Gửi lại đúng payload đã lưu sau khi Kafka phục hồi:

```powershell
dc run --rm --no-deps app python -m producer publish --run-dir /app/output/runs/TEN_THU_MUC_RUN
```

Thay `TEN_THU_MUC_RUN` bằng thư mục in trong kết quả trước đó. Chỉ replay lần đã
`extracted`, `publish_failed` hoặc `published`. Nếu `extract_failed`, chạy lại job
với đúng username và `trigger_time`; không publish phần dữ liệu lấy dở.
Replay có thể tạo bản ghi trùng, Consumer/Silver cần khử trùng theo thỏa thuận.

Dừng sau buổi demo, giữ volume dữ liệu:

```powershell
dc stop
```

## Đọc code theo thứ tự này

| File | Trách nhiệm |
|---|---|
| `producer/window.py` | Parse trigger có timezone, xác định `[start, end)` |
| `producer/lastfm.py` | Gọi API, phân trang, retry, cache theo user trong một job |
| `producer/schema.py` | Năm nhóm dữ liệu, chuẩn hóa và kiểm tra JSON Schema, tạo event ID |
| `producer/pipeline.py` | Lấy dữ liệu, ghép metadata, lưu run và DLQ, gọi Kafka |
| `producer/kafka.py` | Topic, idempotent producer, delivery callback, công cụ đọc kiểm tra |
| `producer/tasks.py` | Celery task; retry giữ nguyên trigger |
| `producer/celery_app.py` | Redis, Beat mỗi giờ, cấu hình Worker |
| `producer/__main__.py` | Các lệnh bạn vừa chạy |

`schema_version`, `event_id`, `source`, `trigger_time`, `ingested_at`,
`metadata_errors` và quy ước nullable là phần bổ sung triển khai, cần thống nhất
với Consumer. Tên và nhóm trường nghiệp vụ bám theo biên bản.

## Giới hạn của bản demo

Đây là một broker local với replication factor 1, một Worker, xử lý tuần tự.
Chưa chứng minh mục tiêu mất dữ liệu 0% hay exactly-once toàn pipeline. Idempotence
Kafka xử lý retry của producer client; chạy lại task vẫn có thể tạo duplicate.
Hai scrobble giống hệt tên, user, album và cùng giây dùng chung ID theo quy tắc hiện tại.

Beat không tự chạy bù mọi giờ bị bỏ qua khi máy tắt. Last.fm có thể nhận scrobble
muộn, và các trang có thể thay đổi khi đang đọc. Muốn vận hành lâu dài cần lưu sổ
job, chạy bù/đọc chồng cửa sổ có kiểm soát và khử trùng, kiểm thử crash/recovery,
quản lý giới hạn request chung khi tăng số Worker, cùng cơ chế giám sát.
Các bộ đếm metadata phản ánh lúc lấy dữ liệu, không phải trạng thái lịch sử tại lúc nghe.

Tài liệu tham chiếu:

- [Last.fm recent tracks](https://www.last.fm/api/show/user.getRecentTracks): phân trang, Unix seconds, `extended`.
- [Celery periodic tasks](https://docs.celeryq.dev/en/stable/userguide/periodic-tasks.html): Beat và lịch crontab.
- [Kafka Python client](https://docs.confluent.io/kafka-clients/python/current/overview.html): delivery callback, `poll`, `flush`.
- [Apache Kafka quickstart](https://kafka.apache.org/quickstart/): image và topic Kafka.
