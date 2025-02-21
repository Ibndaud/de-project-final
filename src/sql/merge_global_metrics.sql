MERGE INTO STV202408062__DWH.global_metrics AS tgt
USING (WITH tmp_transactions AS (SELECT t.operation_id,
									     t.account_number_from,
									     t.account_number_to,
									     t.currency_code,
									     t.status,
									     t.amount,
									     t.transaction_dt::DATE,
									     ROW_NUMBER() OVER (PARTITION BY t.operation_id, t.account_number_from, t.account_number_to ORDER BY t.transaction_dt DESC) AS rn
								  FROM STV202408062__STAGING.transactions AS t
								  WHERE t.account_number_from <> -1),
		done_transactions AS (SELECT tt.account_number_from,
								     tt.account_number_to,
								     tt.currency_code,
								     tt.amount,
								     tt.transaction_dt
							  FROM tmp_transactions AS tt
							  WHERE rn = 1 AND tt.status = 'done' AND tt.transaction_dt = %(threshold)s),
		tmp_currencies AS (SELECT c.currency_code,
								  c.currency_code_with,
								  c.currency_with_div,
								  c.date_update::DATE
						   FROM STV202408062__STAGING.currencies AS c 
						   WHERE c.date_update::DATE = %(threshold)s),
		done_transactions_with_us AS(SELECT dt.account_number_from,
											dt.account_number_to,
											dt.amount,
											dt.currency_code,
											tc.currency_with_div,
											dt.transaction_dt 
									 FROM done_transactions AS dt
									 LEFT JOIN tmp_currencies  AS tc ON dt.currency_code = tc.currency_code 
									 WHERE tc.currency_code_with = 420),
		us_done_transactions AS (SELECT dt.account_number_from,
										dt.account_number_to,
										dt.amount,
										dt.currency_code,
										1 AS currency_with_div,
										dt.transaction_dt
								 FROM done_transactions AS dt
								 WHERE dt.currency_code = 420),
		all_transactions AS (SELECT *
							 FROM done_transactions_with_us
							 UNION ALL
							 SELECT *
							 FROM us_done_transactions)
		SELECT at.transaction_dt AS date_update,
			   at.currency_code AS currency_from,
			   SUM(at.amount * at.currency_with_div / 100) AS amount_total,
			   COUNT(*) AS cnt_transactions,
			   COUNT(*) / COUNT(DISTINCT at.account_number_from) AS avg_transactions_per_account,
			   COUNT(DISTINCT at.account_number_from) AS cnt_accounts_make_transactions
		FROM all_transactions AS at
		GROUP BY at.transaction_dt, at.currency_code) AS src
ON tgt.date_update = src.date_update AND tgt.currency_from = src.currency_from
WHEN MATCHED AND src.amount_total <=> tgt.amount_total OR src.cnt_transactions <=> tgt.cnt_transactions OR src.avg_transactions_per_account <=> tgt.avg_transactions_per_account OR src.cnt_accounts_make_transactions <=> tgt.cnt_accounts_make_transactions
THEN UPDATE SET amount_total = src.amount_total, 
				cnt_transactions = src.cnt_transactions, 
				avg_transactions_per_account = src.avg_transactions_per_account,
				cnt_accounts_make_transactions = src.cnt_accounts_make_transactions
WHEN NOT MATCHED 
THEN INSERT (date_update, currency_from, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
VALUES (src.date_update, src.currency_from, src.cnt_transactions, src.avg_transactions_per_account, src.cnt_accounts_make_transactions);

