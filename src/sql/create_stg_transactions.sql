DROP TABLE IF EXISTS STV202408062__STAGING.transactions;
CREATE TABLE IF NOT EXISTS STV202408062__STAGING.transactions (
	operation_id UUID NOT NULL,
	account_number_from INT NOT NULL,
	account_number_to INT NOT NULL,
	currency_code INT NOT NULL,
	country VARCHAR(100) NOT NULL,
	status VARCHAR(30) NOT NULL,
	transaction_type VARCHAR(30) NOT NULL,
	amount INT NOT NULL,
	transaction_dt TIMESTAMP NOT NULL,
	CONSTRAINT operation_id_status PRIMARY KEY (operation_id, status)
)
ORDER BY transaction_dt
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES
PARTITION BY transaction_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(transaction_dt::DATE, 2, 2);
