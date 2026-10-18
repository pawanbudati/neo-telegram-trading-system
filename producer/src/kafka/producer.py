import json
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from aiokafka import AIOKafkaProducer

from config.settings import settings
from src.logger import logger
from src.models import ParsedSignal

class KafkaSignalProducer:
    """
    Kafka Signal Producer.
    
    Publishes parsed trading signals from Telegram extraction to an Apache Kafka topic.
    Decouples the Telegram listener from any broker/account executors.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_plain_username: Optional[str] = None,
        sasl_plain_password: Optional[str] = None,
    ):
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_topic_signals
        self.security_protocol = security_protocol or settings.kafka_security_protocol
        self.sasl_mechanism = sasl_mechanism or settings.kafka_sasl_mechanism
        self.sasl_plain_username = sasl_plain_username or settings.kafka_sasl_username
        self.sasl_plain_password = sasl_plain_password or settings.kafka_sasl_password

        self.producer: Optional[AIOKafkaProducer] = None
        self._is_started = False

    async def start(self):
        """Initializes and connects the Kafka producer."""
        if self._is_started and self.producer:
            return

        logger.info(
            f"[cyan]Connecting Kafka Signal Producer to brokers: {self.bootstrap_servers} "
            f"(Topic: {self.topic})...[/cyan]"
        )

        kwargs: Dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol,
            "value_serializer": lambda v: json.dumps(v, default=str).encode("utf-8"),
            "key_serializer": lambda k: str(k).encode("utf-8") if k is not None else None,
            "acks": "all",  # Strongest durability guarantee
            "enable_idempotence": True,
        }

        if self.sasl_mechanism:
            kwargs["sasl_mechanism"] = self.sasl_mechanism
            kwargs["sasl_plain_username"] = self.sasl_plain_username
            kwargs["sasl_plain_password"] = self.sasl_plain_password

        self.producer = AIOKafkaProducer(**kwargs)
        await self.producer.start()
        self._is_started = True
        logger.info(f"[bold green]Kafka Signal Producer connected and ready to publish on topic '{self.topic}'.[/bold green]")

    async def stop(self):
        """Flushes and stops the Kafka producer."""
        if self.producer and self._is_started:
            logger.info("[cyan]Stopping Kafka Signal Producer...[/cyan]")
            await self.producer.stop()
            self._is_started = False
            logger.info("[bold green]Kafka Signal Producer stopped cleanly.[/bold green]")

    async def publish(self, signal: ParsedSignal) -> Dict[str, Any]:
        """
        Publishes a ParsedSignal to the Kafka topic.
        Uses contract_key as the partition key to ensure FIFO order per instrument contract.
        """
        if not self._is_started or not self.producer:
            await self.start()

        key = signal.contract_key()
        event_id = str(uuid.uuid4())
        payload = signal.model_dump()
        payload["_meta"] = {
            "event_id": event_id,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "source": "telegram_gemini_extractor",
        }

        try:
            logger.info(
                f"[bold cyan]Kafka Producer: Publishing signal[/bold cyan] event_id={event_id} | "
                f"Key={key} | Type={signal.signal_type.value} -> Topic='{self.topic}'"
            )
            record_metadata = await self.producer.send_and_wait(
                self.topic,
                key=key,
                value=payload
            )

            result = {
                "success": True,
                "event_id": event_id,
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
            }
            logger.info(
                f"[bold green]Kafka Producer: Published successfully[/bold green] "
                f"[partition={record_metadata.partition}, offset={record_metadata.offset}]"
            )
            return result
        except Exception as e:
            logger.error(f"[bold red]Kafka Producer: Failed to publish signal to {self.topic}:[/bold red] {e}", exc_info=True)
            return {"success": False, "error": str(e), "event_id": event_id}
