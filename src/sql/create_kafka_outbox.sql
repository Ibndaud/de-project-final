DROP SCHEMA IF EXISTS stv202408062__stg;
CREATE SCHEMA IF NOT EXISTS stv202408062__stg;

DROP TABLE IF EXISTS stv202408062__stg.kafka_outbox;
CREATE TABLE IF NOT EXISTS stv202408062__stg.kafka_outbox (
    id INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id UUID NOT NULL UNIQUE,
    object_type VARCHAR NOT NULL,
	sent_dttm TIMESTAMP NOT NULL,
    payload JSON NOT NULL,
    load_src VARCHAR NOT NULL
);
CREATE INDEX idx_kafka_outbox_sent_dttm ON stv202408062__stg.kafka_outbox USING btree (sent_dttm);
