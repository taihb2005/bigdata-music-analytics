import json
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .schema import VALIDATOR


class KafkaDeliveryError(RuntimeError):
    pass


def init_topics(settings):
    admin = AdminClient({"bootstrap.servers": settings.bootstrap})
    names = (settings.topic, settings.dlq, settings.demo_topic, settings.demo_topic + ".dlq")
    futures = admin.create_topics([NewTopic(n, num_partitions=3, replication_factor=1) for n in names], request_timeout=30)
    for name, future in futures.items():
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        print(f"Topic ready: {name}", flush=True)


class KafkaWriter:
    def __init__(self, settings, producer=None):
        self.producer = producer if producer is not None else Producer({
            "bootstrap.servers": settings.bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "max.in.flight.requests.per.connection": 5,
            "retries": 2147483647,
            "queue.buffering.max.kbytes": 65536,
            "retry.backoff.ms": 250,
            "retry.backoff.max.ms": 10000,
            "delivery.timeout.ms": 30000,
        })
        self.delivered = 0
        self.failed = 0

    def on_delivery(self, error, message):
        if error is not None:
            self.failed += 1
        else:
            self.delivered += 1

    def publish_file(self, topic, path):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                value = json.loads(line)
                key = value.get("user_profile", {}).get("name", value.get("username", "unknown"))
                deadline = time.monotonic() + 35
                while True:
                    try:
                        self.producer.produce(topic, key=key.encode(), value=line.rstrip("\n").encode(), callback=self.on_delivery)
                        self.producer.poll(0)
                        break
                    except BufferError:
                        if time.monotonic() >= deadline:
                            raise KafkaDeliveryError("Kafka local buffer stayed full; replay the saved run") from None
                        self.producer.poll(0.5)
                    except KafkaException:
                        raise KafkaDeliveryError("Kafka enqueue failed; replay the saved run") from None
                if self.failed:
                    raise KafkaDeliveryError("Kafka delivery failed; replay the saved run")

    def finish(self):
        remaining = self.producer.flush(35)
        if remaining or self.failed:
            raise KafkaDeliveryError(f"Kafka delivery incomplete: pending={remaining}, failed={self.failed}")
        return self.delivered


def inspect_events(settings, demo=False, count=2, timeout=20):
    topic = settings.demo_topic if demo else settings.topic
    consumer = Consumer({"bootstrap.servers": settings.bootstrap,
                         "group.id": "producer-inspect-" + uuid.uuid4().hex,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    seen = 0
    try:
        consumer.subscribe([topic])
        deadline = time.monotonic() + timeout
        while seen < count and time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaDeliveryError(str(message.error()))
            event = json.loads(message.value())
            VALIDATOR.validate(event)
            print(json.dumps({"topic": topic, "partition": message.partition(), "offset": message.offset(),
                              "event_id": event["event_id"], "source": event["source"],
                              "username": event["user_profile"]["name"], "track": event["scrobble_event"]["name"],
                              "schema": "VALID"}, ensure_ascii=False), flush=True)
            seen += 1
    finally:
        consumer.close()
    if seen < count:
        raise KafkaDeliveryError(f"Read {seen}/{count} messages in {timeout}s; check topic and run results")
