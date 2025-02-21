COPY STV202408062__STAGING.transactions (
    operation_id, account_number_from, account_number_to, currency_code,
    country, status, transaction_type, amount, transaction_dt
)
FROM STDIN DELIMITER ',' NULL AS 'NULL' DIRECT;