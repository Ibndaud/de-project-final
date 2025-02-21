import json

from pathlib import Path
from logging import Logger
from datetime import datetime, timedelta, date

from etl_settings_repository import EtlSetting, EtlSettingsRepository 
from lib.pg_connect import PgConnect
from lib.vertica_connect import VerticaConnect


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
 
    def merge_global_metrics(self, sent_dttm: date) -> None:
        query = self._load_sql_query("merge_global_metrics.sql")

        with self._db.client().cursor() as cur: 
            cur.execute(
                query, 
                { 
                    "threshold": sent_dttm
                }, 
            ) 

    def get_oldest_date(self) -> date:
        query = self._load_sql_query("get_oldest_date_vertica.sql")

        with self._db.client().cursor() as cur: 
            cur.execute(
                query
                ) 
            obj = cur.fetchone() 
        return obj[0]


class DatamartUpdater:
    WF_KEY = "staging_to_dwh_vertica_workflow"
    DATE_TO_LOAD = "date_to_load"
 
    def __init__(self, pg_conn: PgConnect, vertica_conn: VerticaConnect, log: Logger) -> None:
        self.pg_dest = pg_conn
        self.vertica_dest = vertica_conn
        self.vertica_rep = VerticaRepository(vertica_conn)
        self.settings_repository = EtlSettingsRepository()
        self.log = log

    def _next_date(self, conn: PgConnect, wf_setting: EtlSetting, date: datetime) -> None:
        next_date = date + timedelta(days=1)
        self.log.info(f"!!!!! 1 next_date_to_load --- {next_date} --- {type(next_date)}.") ### !!!
        wf_setting.workflow_settings[self.DATE_TO_LOAD] = next_date.isoformat()
        self.log.info(f"!!!!! 2 wf_setting.workflow_settings --- {wf_setting.workflow_settings} --- {type(wf_setting.workflow_settings)}.") ### !!!
        wf_setting_json = json.dumps(wf_setting.workflow_settings)
        self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
        self.log.info(f"Load finished for {date}, next date: {wf_setting.workflow_settings[self.DATE_TO_LOAD]}")

    def update_datamart(self):
        with self.pg_dest.connection() as conn: 
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                oldest_date = self.vertica_rep.get_oldest_date()
                self.log.info(f"!!!!! 3 oldest_date --- {oldest_date} --- {type(oldest_date)}.") ### !!!
                if not oldest_date:
                    self.log.info("Quitting... No data in stv202408062__stg.kafka_outbox.")
                    return
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.DATE_TO_LOAD: oldest_date.isoformat()})
                date_to_load = wf_setting.workflow_settings[self.DATE_TO_LOAD]
                self.log.info(f"!!!!! 4 date_to_load --- {date_to_load} --- {type(date_to_load)}.") ### !!!

            date_to_load = datetime.strptime(wf_setting.workflow_settings[self.DATE_TO_LOAD], '%Y-%m-%d').date()
            self.log.info(f"!!!!! 5 date_to_load --- {date_to_load} --- {type(date_to_load)}.") ### !!!

            self.vertica_rep.merge_global_metrics(date_to_load)
            self.log.info(f"Finished for {date_to_load}.")

            self._next_date(conn, wf_setting, date_to_load)
