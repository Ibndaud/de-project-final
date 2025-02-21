import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from postgresql_loader.repository import PostgresqlRepository
from postgresql_loader.kafka_postgresql_message_processor_job import KafkaPostgresqlMessageProcessor

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("service_kafka_postgres_app")

config = AppConfig()
postgresql_repository = PostgresqlRepository(config.pg_warehouse_db(), logger)
kafka_consumer = config.kafka_consumer()

@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    proc = KafkaPostgresqlMessageProcessor(kafka_consumer, postgresql_repository, app.logger)

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=20)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
