DROP TABLE IF EXISTS STV202408062__DWH.global_metrics;
CREATE TABLE IF NOT EXISTS STV202408062__DWH.global_metrics (
	id INT NOT NULL PRIMARY KEY,
	date_update TIMESTAMP NOT NULL,
	currency_from INT NOT NULL,
	amount_total INT NOT NULL,
	cnt_transactions INT NOT NULL,
	avg_transactions_per_account FLOAT NOT NULL,
	cnt_accounts_make_transactions INT NOT NULL
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES
PARTITION BY date_update::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(date_update::DATE, 2, 2);
