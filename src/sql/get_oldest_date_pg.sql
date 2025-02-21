SELECT sent_dttm::DATE
FROM stv202408062__stg.kafka_outbox 
ORDER BY sent_dttm ASC
LIMIT 1;