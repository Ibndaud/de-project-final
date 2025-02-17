DROP TABLE IF EXISTS STV202408062__STAGING.currencies;
CREATE TABLE IF NOT EXISTS STV202408062__STAGING.currencies (
	date_update TIMESTAMP NOT NULL,
	currency_code INT NOT NULL,
	currency_code_with INT NOT NULL,
	currency_with_div FLOAT NOT NULL,
	CONSTRAINT date_update_currency_code_currency_code_with_pkey PRIMARY KEY (date_update, currency_code, currency_code_with)
)
ORDER BY date_update, currency_code
SEGMENTED BY HASH(date_update) ALL NODES
PARTITION BY date_update::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(date_update::DATE, 2, 2);
