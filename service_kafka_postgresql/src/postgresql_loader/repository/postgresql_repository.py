import json

from uuid import UUID
from datetime import datetime, timezone
from typing import Dict
from lib.pg import PgConnect
from pydantic import BaseModel
from logging import Logger


class KafkaMessage(BaseModel):
    object_id: UUID
    object_type: str
    sent_dttm: datetime
    payload: Dict
    load_src: str


class MessageBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = 'kafka-transaction-service-input'

    def kafka_message(self) -> KafkaMessage:
        return KafkaMessage(
            object_id=UUID(self._dict['object_id']),
            object_type=self._dict['object_type'],
            sent_dttm=datetime.strptime(self._dict['sent_dttm'], '%Y-%m-%dT%H:%M:%S'),
            payload=self._dict['payload'],
            load_src=self.source_system
        )


class PostgresqlRepository:
    def __init__(self, db: PgConnect, logger: Logger) -> None:
        self._db = db
        self._logger = logger

    def message_insert(self, obj: KafkaMessage) -> None:
        try:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO stv202408062__stg.kafka_outbox(
                                object_id,
                                object_type,
                                sent_dttm,
                                payload,
                                load_src
                            )
                            VALUES(
                                %(object_id)s,
                                %(object_type)s,
                                %(sent_dttm)s,
                                %(payload)s,
                                %(load_src)s
                            )
                            ON CONFLICT (object_id) DO NOTHING;
                        """,
                        {
                            'object_id': obj.object_id,
                            'object_type': obj.object_type,
                            'sent_dttm': obj.sent_dttm,
                            'payload': json.dumps(obj.payload),
                            'load_src': obj.load_src
                        }
                    )
                conn.commit()
        except Exception as e:
            self._logger.error(f"{datetime.now(timezone.utc)}: Failed to insert message {obj.object_id} ({obj.object_type}) - Unexpected error: {e}")
            raise
