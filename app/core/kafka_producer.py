import asyncio
import logging
from typing import List, Dict, Any

try:
    from IoTKafka.producer import IoTKafkaProducer, IoTProducerException
    from IoTKafka.producer_singleton import KafkaProducerManager
    from IoTKafka.kafka_configs import KafkaTopics
except ImportError:
    IoTKafkaProducer = None
    IoTProducerException = Exception
    KafkaProducerManager = None
    KafkaTopics = None

logger = logging.getLogger(__name__)


class KafkaManager:
    def __init__(self):
        self.producer: IoTKafkaProducer | None = None
        self.topic = (
            KafkaTopics.telemetry_raw if KafkaTopics else "telemetry.raw"
        )

    async def start(self):
        """Initialize producer on stratup FastAPI."""
        if IoTKafkaProducer:
            self.producer = IoTKafkaProducer()
            logger.info("IoTKafkaProducer successfully initialized.")
        else:
            logger.error(
                "IoTKafkaProducer not found. Check umbrella repo imports."
            )

    async def stop(self):
        """Graceful Shutdown: safe close all connection."""
        if KafkaProducerManager:
            logger.info("Flushing all Kafka producers before shutdown...")
            await asyncio.to_thread(KafkaProducerManager.close_all)
            logger.info("Kafka producers successfully closed.")

    def _sync_send_batch(self, events: List[Dict[str, Any]]) -> str:
        """
        Sync function for iteration through the batch
        Running izolated in background threads pool.
        """
        if not self.producer:
            raise RuntimeError("Kafka producer is not initialized")

        for event in events:
            device_key = event.get("serial_number", "unknown_device")
            try:
                self.producer.produce(
                    topic=self.topic, key=device_key, value=event
                )
            except IoTProducerException as exc:
                logger.error(
                    f"Failed to enqueue message to Kafka buffer: {exc}"
                )
                raise

        return self.topic

    async def send_batch(self, events: List[Dict[str, Any]]) -> str:
        """This methods call FastAPI (didn't lock Event Loop)."""
        try:
            return await asyncio.to_thread(self._sync_send_batch, events)
        except Exception as exc:
            raise Exception(f"Failed to publish to Kafka: {exc}")


kafka_manager = KafkaManager()
