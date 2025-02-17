from datetime import datetime, timezone
from typing import Dict
from logging import Logger
from lib.kafka_connect import KafkaConsumer
from postgresql_loader.repository import PostgresqlRepository, MessageBuilder


class KafkaPostgresqlMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer, 
                 postgresql_repository: PostgresqlRepository,
                 logger: Logger,
                 batch_size: int = 10) -> None:
        self._consumer = consumer
        self._postgresql_repository = postgresql_repository
        self._logger = logger
        self._batch_size = batch_size

    def _validate_message(self, message: Dict) -> bool:
        required_fields = ('object_id', 'object_type', 'sent_dttm', 'payload')
        for field in required_fields:
            if field not in message:
                self._logger.error(f"{datetime.now(timezone.utc)}: Missing required field '{field}' in message")
                return False
        return True

    def run(self) -> None:
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        for _ in range(self._batch_size):
            message_in = self._consumer.consume()
            if not message_in:
                self._logger.info(f"{datetime.now(timezone.utc)}: NO MORE MESSAGES, quitting...")
                break

            if not self._validate_message(message_in):
                continue

            self._logger.info(f"{datetime.now(timezone.utc)}: Processing message: object_id: {message_in.get('object_id')}, sent_dttm: {message_in.get('sent_dttm')}, object_type: {message_in.get('object_type')}")
            object = MessageBuilder(message_in)
            kafka_message = object.kafka_message()
            self._postgresql_repository.message_insert(kafka_message)

        self._logger.info(f"{datetime.now(timezone.utc)}: FINISH")
