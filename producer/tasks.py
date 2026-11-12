from .celery_app import app
from .kafka import KafkaDeliveryError
from .lastfm import TemporaryAPIError
from .pipeline import run_window


@app.task(name="producer.ingest_user_window", bind=True,
          autoretry_for=(TemporaryAPIError, KafkaDeliveryError),
          retry_backoff=True, retry_backoff_max=60, retry_jitter=True,
          retry_kwargs={"max_retries": 5}, acks_late=True, reject_on_worker_lost=True)
def ingest_user_window(self, username, trigger_time, demo=False):
    # Retry reuses these serialized arguments. Do not calculate 'now' here.
    return run_window(username, trigger_time, publish=True, demo=demo)
