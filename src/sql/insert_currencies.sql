COPY STV202408062__STAGING.currencies (
    date_update, currency_code, currency_code_with, currency_with_div
)
FROM STDIN DELIMITER ',' NULL AS 'NULL' DIRECT;