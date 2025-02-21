SELECT payload::json->>'operation_id' AS operation_id,
       (payload::json->>'account_number_from')::INT AS account_number_from,
       (payload::json->>'account_number_to')::INT AS account_number_to,
       (payload::json->>'currency_code')::INT AS currency_code,
       payload::json->>'country' AS country,
       payload::json->>'status' AS status,
       payload::json->>'transaction_type' AS transaction_type,
       (payload::json->>'amount')::INT AS amount,
       (payload::json->>'transaction_dt')::TIMESTAMP AS transaction_dt  
FROM stv202408062__stg.kafka_outbox 
WHERE object_type = 'TRANSACTION' AND sent_dttm::DATE = %(threshold)s::DATE;