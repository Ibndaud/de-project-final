import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging 
import pendulum 
 
from airflow.decorators import dag, task 
from airflow.operators.empty import EmptyOperator

from py.lib.pg_connect import PgConnectionBuilder
from py.lib.vertica_connect import VerticaConnectionBuilder
from py.vertica_datamart_update import DatamartUpdater

 
log = logging.getLogger(__name__) 
 
 
@dag( 
    schedule='0 2 * * *',  # Задаем расписание выполнения дага - 02:00 UTC. 
    start_date=pendulum.datetime(2022, 9, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня. 
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно). 
    tags=['final-project', 'vertica', 'datamart', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow. 
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен. 
) 
def final_project_vertica_datamart_updater_dag(): 
    # Создаем подключение к базе dwh.
    dwh_pg_connect = PgConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    dwh_vertica_connect = VerticaConnectionBuilder.vertica_conn("VERTICA_WAREHOUSE_CONNECTION")
 
    # Объявляем таск, который загружает данные. 
    @task(task_id="datamart_update") 
    def update_datamart(): 
        # создаем экземпляр класса, в котором реализована логика. 
        datamart_updater = DatamartUpdater(dwh_pg_connect, dwh_vertica_connect, log) 
        datamart_updater.update_datamart()  # Вызываем функцию, которая перельет данные. 

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Инициализируем объявленные таски. 
    update_task = update_datamart() 
 
    # Далее задаем последовательность выполнения тасков. 
    start >> update_task >> end
 
datamart_update_dag = final_project_vertica_datamart_updater_dag()  # noqa 