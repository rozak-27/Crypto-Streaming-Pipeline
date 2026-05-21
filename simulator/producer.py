from confluent_kafka import Producer
import logging
import json

logger = logging.getLogger(__name__)

class CryptoProducer:
    def __init__(self, bootstrap_servers: str, schema_registry_url: str = None,
                 topic: str = "crypto-prices"):
        self.topic = topic

        # Setup Kafka Producer - kirim JSON biasa tanpa Avro
        self.producer = Producer({
            "bootstrap.servers":  bootstrap_servers,
            "acks":               "1",
            "linger.ms":          5,
            "compression.type":   "none",
            "message.max.bytes":  1048576,
        })

    def send(self, tick: dict, key: str = None):
        """Kirim satu tick ke Kafka sebagai JSON."""
        try:
            self.producer.produce(
                topic    = self.topic,
                key      = (key or tick.get("symbol", "")).encode("utf-8"),
                value    = json.dumps(tick).encode("utf-8"),
                on_delivery = self._delivery_report,
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to produce message: {e}")

    def _delivery_report(self, err, msg):
        if err:
            logger.error(f"Message delivery failed: {err}")

    def flush(self):
        self.producer.flush()