"""Speed layer của dự án phân tích nghe nhạc Last.fm."""
import sys

# Log có tiếng Việt. Console Windows mặc định (cp1252) không mã hóa được, và print sẽ ném
# UnicodeEncodeError làm chết producer/worker chỉ vì một dòng log. Chuyển stdout/stderr sang UTF-8;
# ký tự nào vẫn không in được thì thay thế thay vì ném lỗi. Trong Docker (UTF-8) không có gì thay đổi.
for _stream in (sys.stdout, sys.stderr):
    try:
        if _stream is not None and (getattr(_stream, "encoding", "") or "").lower().replace("-", "") != "utf8":
            _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):  # luồng đã bị thay (ví dụ trong test runner) thì bỏ qua
        pass
