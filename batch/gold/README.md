# Batch Gold · Thói quen nghe nhạc theo ngày

Chạy các lệnh dưới đây từ thư mục gốc dự án với cấu hình MinIO trong `.env`. Cổng mặc định là 8001 cho API và 8501 cho dashboard. API stream trong Compose gốc cũng dùng cổng 8001, nên chỉ chạy một trong hai API trên cổng này tại một thời điểm.

Nếu Gold đã có trên MinIO, chạy lệnh sau từ thư mục dự án để xem kết quả đã phân tích:

```powershell
docker compose -f compose.gold.yml up -d --build api dashboard
```

Mở [dashboard](http://localhost:8501). API `batch.gold.api` chỉ đọc bản xuất Gold đã hoàn tất. Để tự động tạo Gold khi Silver có dữ liệu mới, khởi động cả worker:

```powershell
docker compose -f compose.gold.yml up -d --build
docker compose -f compose.gold.yml logs --tail 20 gold-worker
```

Worker dùng endpoint và thông tin đăng nhập MinIO trong `.env`, đọc bảng Delta `s3a://silver/fact_listen` và xuất Parquet vào `s3a://gold/batch_analysis/<run_id>/`. Nó kiểm tra Silver ngay khi khởi động và sau mỗi 900 giây. Chỉnh `GOLD_INTERVAL_SECONDS` để đổi chu kỳ. Khi phiên bản Silver đã được xử lý, worker bỏ qua lần xuất. Nếu ghi bảng bị gián đoạn, API tiếp tục phục vụ lần xuất hoàn tất trước đó và worker thử lại ở chu kỳ sau.

Trong [dashboard](http://localhost:8501), mục **Giờ và thứ trong tuần** hiển thị phân bố theo 24 giờ UTC, bảy thứ và biểu đồ nhiệt 7 × 24. Mỗi ô tính tỷ lệ lượt nghe của từng người rơi vào đúng thứ và giờ đó, rồi lấy trung bình trên tất cả người nghe. Các khung giờ trống có giá trị 0. Mở **Xem số liệu theo giờ và thứ** để xem toàn bộ dòng.

Mục **Ngày thường và cuối tuần** so sánh thứ Hai–thứ Sáu với thứ Bảy–Chủ nhật theo UTC. Bên cạnh phân bố chung, báo cáo tính số lượt trên mỗi ngày có nghe và so sánh hai loại ngày cho những người hoạt động ở cả hai nhóm. Nút **Làm mới kết quả Gold** tải lần xuất hoàn tất mới nhất.

API trả danh sách năm bảng, số dòng, thời gian hoàn tất và phiên bản Silver tại [bản xuất Gold mới nhất](http://localhost:8001/batch/gold). Các bảng là `hour_distribution`, `weekday_distribution`, `weekday_hour_distribution`, `day_type_distribution` và `day_comparison`. Ví dụ sau đọc đủ 168 ô của bảng giờ theo thứ. Tham số `run_id` cũng hỗ trợ đọc các bản xuất lịch sử theo đúng danh sách bảng của bản đó.

```powershell
$gold = Invoke-RestMethod 'http://localhost:8001/batch/gold'
$page = Invoke-RestMethod "http://localhost:8001/batch/gold/weekday_hour_distribution?run_id=$($gold.run_id)&limit=168"
$page.rows
```

## Chạy riêng giao diện Streamlit trên Windows

Nếu đang chạy riêng dịch vụ `api` trong Docker, có thể mở dashboard bằng PowerShell tại thư mục dự án.

```powershell
python -m pip install -r batch/gold/requirements-ui.txt
python -m streamlit run batch/gold/dashboard.py
```

Mặc định giao diện gọi API ở `http://localhost:8001`. Biến môi trường `MUSIC_API_URL` cho phép trỏ tới một máy khác đang phục vụ Batch Gold.
