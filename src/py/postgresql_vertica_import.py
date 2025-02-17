import csv
import json

from io import StringIO
from uuid import UUID
from logging import Logger 
from typing import List 
 
from .etl_settings_repository import EtlSetting, EtlSettingsRepository 
from .lib.pg_connect import PgConnect
from .lib.vertica_connect import VerticaConnect
from psycopg.rows import class_row 
from pydantic import BaseModel 
from datetime import datetime, timedelta, date

#from yc_postgresql_config import DB_CONFIG
#from vertica_config import V_CONFIG


class CurrencyObj(BaseModel):
	date_update: datetime
	currency_code: int
	currency_code_with: int
	currency_with_div: float


class TransactionObj(BaseModel):
	operation_id: UUID
	account_number_from: int
	account_number_to: int
	currency_code: int
	country: str
	status: str
	transaction_type: str
	amount: int
	transaction_dt: datetime


class PostgresqlRepository: 
    def __init__(self, pg: PgConnect) -> None: 
        self._db = pg 
 
    def list_transactions(self, sent_dttm: date) -> List[TransactionObj]: 
        with self._db.client().cursor(row_factory=class_row(TransactionObj)) as cur: 
            cur.execute( 
                """ 
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
                """, 
                { 
                    "threshold": sent_dttm
                }, 
            ) 
            objs = cur.fetchall() 
        return objs 

    def list_currencies(self, sent_dttm: date) -> List[CurrencyObj]: 
        with self._db.client().cursor(row_factory=class_row(CurrencyObj)) as cur: 
            cur.execute( 
                """ 
                    SELECT (payload::json->>'date_update')::TIMESTAMP AS date_update,
                           (payload::json->>'currency_code')::INT AS currency_code,
                           (payload::json->>'currency_code_with')::INT AS currency_code_with,
                           (payload::json->>'currency_with_div')::FLOAT AS currency_with_div
                    FROM stv202408062__stg.kafka_outbox 
                    WHERE object_type = 'CURRENCY' AND sent_dttm::DATE = %(threshold)s::DATE;
                """, 
                { 
                    "threshold": sent_dttm
                }, 
            ) 
            objs = cur.fetchall() 
        return objs 

    def get_oldest_date(self) -> date: 
        with self._db.client().cursor() as cur: 
            cur.execute( 
                """ 
                    SELECT sent_dttm::DATE
                    FROM stv202408062__stg.kafka_outbox 
                    ORDER BY sent_dttm ASC
                    LIMIT 1;
                """
                ) 
            objs = cur.fetchone() 
        return objs 


class VerticaRepository: 
    def __init__(self, vertica: VerticaConnect) -> None: 
        self._db = vertica 
 
    def insert_transactions(self, data: str) -> None:
         with self._db.client().cursor() as cur:
            csv_data = StringIO(data)
            cur.copy(
                """ 
                    COPY STV202408062__STAGING.transactions (
                        operation_id, account_number_from, account_number_to, currency_code,
                        country, status, transaction_type, amount, transaction_dt
                    )
                    FROM STDIN DELIMITER ',' NULL AS 'NULL' DIRECT; 
                """, csv_data
                ) 

    def insert_currencies(self, data: str) -> None:
         with self._db.client().cursor() as cur:
            csv_data = StringIO(data)
            cur.copy(
                """ 
                    COPY STV202408062__STAGING.currencies (
                        date_update, currency_code, currency_code_with, currency_with_div
                    )
                    FROM STDIN DELIMITER ',' NULL AS 'NULL' DIRECT; 
                """, csv_data
                ) 


class TransactionsCurrenciesLoader: 
    WF_KEY = "transactions_and_currencies_postgresql_to_vertica_workflow" 
    DATE_TO_LOAD = "date_to_load"
 
    def __init__(self, pg_conn: PgConnect, vertica_conn: VerticaConnect, log: Logger) -> None: 
        self.pg_dest = pg_conn
        self.vertica_dest = vertica_conn 
        self.pg_rep = PostgresqlRepository(pg_conn) 
        self.vertica_rep = VerticaRepository(vertica_conn) 
        self.settings_repository = EtlSettingsRepository() 
        self.log = log  

    def _transactions_to_csv(self, transactions: List[TransactionObj]) -> list:
        buffer = StringIO()
        writer = csv.writer(buffer, delimiter=',', quoting=csv.QUOTE_MINIMAL)

        for transaction in transactions:
            writer.writerow([
                str(transaction.operation_id),
                transaction.account_number_from,
                transaction.account_number_to,
                transaction.currency_code,
                transaction.country,
                transaction.status,
                transaction.transaction_type,
                transaction.amount,
                transaction.transaction_dt.strftime('%Y-%m-%d %H:%M:%S')
            ])
        
        buffer.seek(0)
        return buffer.getvalue() #.splitlines()

    def _currencies_to_csv(self, currencies: List[CurrencyObj]) -> list:
        buffer = StringIO()
        writer = csv.writer(buffer, delimiter=',', quoting=csv.QUOTE_MINIMAL)

        for currency in currencies:
            writer.writerow([
                currency.date_update.strftime('%Y-%m-%d %H:%M:%S'),
                currency.currency_code,
                currency.currency_code_with,
                currency.currency_with_div
            ])
        
        buffer.seek(0)
        return buffer.getvalue() #.splitlines()

    def load_transactions_currencies(self): 
        with self.pg_dest.connection() as conn: 
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                oldest_date = self.pg_rep.get_oldest_date() 
                self.log.info(f"!!!!! --- {oldest_date} --- {type(oldest_date)}.") ### !!!
                if not oldest_date:
                    self.log.info("Quitting... No data in stv202408062__stg.kafka_outbox.")
                    return
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.DATE_TO_LOAD: oldest_date})
                date_to_load = wf_setting.workflow_settings[self.DATE_TO_LOAD]
            else:
                date_to_load = (date.fromisoformat(wf_setting.workflow_settings[self.DATE_TO_LOAD]),)

            self.log.info(f"!!!!! --- {date_to_load} --- {type(date_to_load)}.") ### !!!
            transactions = self.pg_rep.list_transactions(date_to_load)
            currencies = self.pg_rep.list_currencies(date_to_load)
 
            if not transactions or not currencies: 
                self.log.info("Quitting... No transactions or currencies data.") 
                self.log.info(f"Found {len(transactions)} transactions rows and {len(currencies)} currencies rows to load.") ### !!!
                return 
 
            self.log.info(f"Found {len(transactions)} transactions rows and {len(currencies)} currencies rows to load.")
            parsed_transactions = self._transactions_to_csv(transactions)
            self.vertica_rep.insert_transactions(parsed_transactions)
            self.log.info("Parsed transactions data inserted in to Vertica.")
            parsed_currencies = self._currencies_to_csv(currencies)
            self.vertica_rep.insert_currencies(parsed_currencies)
            self.log.info("Parsed currencies data inserted in to Vertica.")

            next_date_to_load = date_to_load[0] + timedelta(days=1)
            wf_setting.workflow_settings[self.DATE_TO_LOAD] = next_date_to_load.isoformat()
            self.log.info(f"!!!!! --- {wf_setting.workflow_settings} --- {type(wf_setting.workflow_settings)}.") ### !!!
            wf_setting_json = json.dumps(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД. 
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json) 
            self.log.info(f"Load finished for {date_to_load}, next date: {wf_setting.workflow_settings[self.DATE_TO_LOAD]}") 
