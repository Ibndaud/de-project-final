SELECT (payload::json->>'date_update')::TIMESTAMP AS date_update,
       (payload::json->>'currency_code')::INT AS currency_code,
       (payload::json->>'currency_code_with')::INT AS currency_code_with,
       (payload::json->>'currency_with_div')::FLOAT AS currency_with_div
FROM stv202408062__stg.kafka_outbox 
WHERE object_type = 'CURRENCY' AND sent_dttm::DATE = %(threshold)s::DATE;