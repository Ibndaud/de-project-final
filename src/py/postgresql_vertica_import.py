import csv
import json

from pathlib import Path
from io import StringIO
from uuid import UUID
from logging import Logger 
from typing import List
from psycopg.rows import class_row 
from pydantic import BaseModel 
from datetime import datetime, timedelta, date

from etl_settings_repository import EtlSetting, EtlSettingsRepository 
from lib.pg_connect import PgConnect
from lib.vertica_connect import VerticaConnect


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
        self._sql_dir = Path(__file__).parent.parent / "sql"

    def _load_sql_query(self, filename: str) -> str:
        sql_file_path = self._sql_dir / filename
        try:
            with open(sql_file_path, "r") as query:
                return query.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"SQL query ({filename}) not found in: {sql_file_path}")
        except IOError as e:
            raise IOError(f"Error reading {filename} in {sql_file_path}: {e}")
    
    def list_transactions(self, sent_dttm: date) -> List[TransactionObj]:
        query = self._load_sql_query("list_transactions.sql")

        with self._db.client().cursor(row_factory=class_row(TransactionObj)) as cur: 
            cur.execute(
                query, 
                { 
                    "threshold": sent_dttm
                }, 
            ) 
            objs = cur.fetchall() 
        return objs 

    def list_currencies(self, sent_dttm: date) -> List[CurrencyObj]:
        query = self._load_sql_query("list_currencies.sql")

        with self._db.client().cursor(row_factory=class_row(CurrencyObj)) as cur: 
            cur.execute(
                query, 
                { 
                    "threshold": sent_dttm
                }, 
            ) 
            objs = cur.fetchall() 
        return objs 

    def get_oldest_date(self) -> date:
        query = self._load_sql_query("get_oldest_date_pg.sql")

        with self._db.client().cursor() as cur: 
            cur.execute(
                query
                ) 
            obj = cur.fetchone() 
        return obj[0]


class VerticaRepository: 
    def __init__(self, vertica: VerticaConnect) -> None: 
        self._db = vertica
        self._sql_dir = Path(__file__).parent.parent / "sql"

    def _load_sql_query(self, filename: str) -> str:
        sql_file_path = self._sql_dir / filename
        try:
            with open(sql_file_path, "r") as query:
                return query.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"SQL query ({filename}) not found in: {sql_file_path}")
        except IOError as e:
            raise IOError(f"Error reading {filename} in {sql_file_path}: {e}")

    def insert_transactions(self, data: str) -> None:
        query = self._load_sql_query("insert_transactions.sql")
         
        with self._db.client().cursor() as cur:
            csv_data = StringIO(data)
            cur.copy(
                query, csv_data
                ) 

    def insert_currencies(self, data: str) -> None:
        query = self._load_sql_query("insert_currencies.sql")

        with self._db.client().cursor() as cur:
            csv_data = StringIO(data)
            cur.copy(
                query, csv_data
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
        return buffer.getvalue()

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
        return buffer.getvalue()

    def _next_date(self, conn: PgConnect, wf_setting: EtlSetting, date: datetime) -> None:
        next_date = date + timedelta(days=1)
        wf_setting.workflow_settings[self.DATE_TO_LOAD] = next_date.isoformat()
        wf_setting_json = json.dumps(wf_setting.workflow_settings)
        self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
        self.log.info(f"Load finished for {date}, next date: {wf_setting.workflow_settings[self.DATE_TO_LOAD]}")
        
    def load_transactions_currencies(self): 
        with self.pg_dest.connection() as conn: 
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                oldest_date = self.pg_rep.get_oldest_date()
                if not oldest_date:
                    self.log.info("Quitting... No data in stv202408062__stg.kafka_outbox.")
                    return
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.DATE_TO_LOAD: oldest_date.isoformat()})
                date_to_load = wf_setting.workflow_settings[self.DATE_TO_LOAD]

            date_to_load = datetime.strptime(wf_setting.workflow_settings[self.DATE_TO_LOAD], '%Y-%m-%d').date()
            transactions = self.pg_rep.list_transactions(date_to_load)
            currencies = self.pg_rep.list_currencies(date_to_load)
 
            if not transactions or not currencies: 
                self.log.info("Quitting... No transactions or currencies data.") 
                self.log.info(f"Found {len(transactions)} transactions rows and {len(currencies)} currencies rows to load.")
                self._next_date(conn, wf_setting, date_to_load)
                return 
 
            self.log.info(f"Found {len(transactions)} transactions rows and {len(currencies)} currencies rows to load.")
            parsed_transactions = self._transactions_to_csv(transactions)
            self.vertica_rep.insert_transactions(parsed_transactions)
            self.log.info("Parsed transactions data inserted in to Vertica.")

            parsed_currencies = self._currencies_to_csv(currencies)
            self.vertica_rep.insert_currencies(parsed_currencies)
            self.log.info("Parsed currencies data inserted in to Vertica.")

            self._next_date(conn, wf_setting, date_to_load)
