"""Tiện ích Kafka dùng chung cho các consumer (metadata worker, metadata sink)."""
from __future__ import annotations


def safe_commit(consumer) -> bool:
    """Commit offset; nếu group vừa rebalance (broker khởi động lại, consumer bị coi là đã rời group) thì
    KHÔNG làm chết tiến trình.

    Khi đó commit bị từ chối (CommitFailedError / IllegalGeneration). Lần poll kế tiếp consumer tự tham gia
    lại group và nhận lại message chưa commit; xử lý của worker và sink là idempotent nên an toàn.
    Trả về True nếu commit thành công.
    """
    from kafka.errors import CommitFailedError, IllegalGenerationError, RebalanceInProgressError

    try:
        consumer.commit()
        return True
    except (CommitFailedError, IllegalGenerationError, RebalanceInProgressError) as exc:
        print(f"Commit bị từ chối do rebalance ({exc}); message sẽ được giao lại và xử lý lại.", flush=True)
        return False
