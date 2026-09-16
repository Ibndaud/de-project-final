# FinTech Data Platform: ETL, DWH (Vertica)

![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Vertica](https://img.shields.io/badge/Vertica-DWH-blue)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Helm](https://img.shields.io/badge/Helm-0F1689?logo=helm&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

## Описание проекта

В рамках проекта реализована полноценная аналитическая платформа для финтех-стартапа, предоставляющего международные банковские переводы.  

Цель решения — объединить транзакционные данные из различных источников, загрузить их в хранилище данных (Vertica), построить витрину с агрегированной финансовой аналитикой для бизнеса.

Проект охватывает полный цикл работы с данными:
- загрузку из источников,
- построение staging-слоя,
- инкрементальное обновление DWH,
- формирование витрины.

---

## Архитектура решения

### Источники данных
- Kafka / PostgreSQL
- Данные:
  - `transactions` — транзакционная активность пользователей
  - `currencies` — курсы валют

### Слои хранилища (Vertica)

- `*_STAGING` — слой сырых данных
- `*_DWH` — слой витрин
- `global_metrics` — агрегированная витрина для аналитики

### Оркестрация
- Apache Airflow (DAG-и):
  - `1_data_import_dag.py` — загрузка данных в staging
  - `2_datamart_update_dag.py` — обновление витрины

### Дополнительные сервисы
- Kafka → PostgreSQL сервис (outbox pattern)
- Docker-инфраструктура
- Helm-конфигурация для деплоя сервиса

```mermaid
flowchart LR
    K[("Kafka<br/>транзакции + курсы валют")] --> SVC["kafka_postgresql_service<br/>(Flask, Docker, Helm)"]
    SVC -->|"outbox pattern"| PG[("PostgreSQL<br/>база-источник")]
    PG -->|"DAG 1: data_import<br/>01:00 UTC, инкрементально"| STG[("Vertica<br/>STAGING: transactions, currencies")]
    STG -->|"DAG 2: datamart_update<br/>02:00 UTC"| DM[("Vertica<br/>DWH: global_metrics")]
    DM --> BI["Аналитика: динамика оборота<br/>по валютам и пользователям"]
```
---
## Структура проекта

```text
.
├── src/
│   ├── dags/                            # DAG-и Airflow
│   │   ├── 1_data_import_dag.py         #   PostgreSQL → Vertica STAGING (01:00 UTC)
│   │   └── 2_datamart_update_dag.py     #   STAGING → DWH.global_metrics (02:00 UTC)
│   ├── py/                              # ETL-логика
│   │   ├── postgresql_vertica_import.py #   загрузка transactions/currencies в staging
│   │   ├── vertica_datamart_update.py   #   инкрементальное обновление витрины
│   │   ├── etl_settings_repository.py   #   workflow-настройки (инкрементальность)
│   │   └── lib/                         #   коннекторы к PostgreSQL и Vertica
│   ├── sql/                             # DDL/DML: staging, DWH, outbox, merge
│   └── img/
├── service_kafka_postgresql/            # сервис Kafka → PostgreSQL (outbox pattern)
│   ├── src/                             #   Flask-приложение и процессор сообщений
│   ├── app/                             #   Helm-чарт для деплоя в Kubernetes
│   ├── dockerfile
│   └── requirements.txt
├── docker-compose.yaml                  # запуск сервиса
└── README.md
```
---

## Реализованный пайплайн

### 1️⃣ Загрузка данных в STAGING

- Ежедневная инкрементальная загрузка данных
- Поддержка параметра даты
- Автоматический запуск за выбранный период (октябрь 2022)
- Хранение данных в Vertica с проекциями, сегментацией и сортировкой

---

### 2️⃣ Обновление витрины `global_metrics`

Инкрементальное обновление витрины по дням (за вчера).

Дополнительно реализовано:
- Очистка тестовых аккаунтов (`account_number < 0`)
- Конвертация оборота в единую валюту
- Расчёт бизнес-метрик

---
## Как запустить

### 1. Подготовить базы

Выполнить SQL-скрипты из `src/sql/` в соответствующих базах (PostgreSQL — источник и outbox,
Vertica — схемы `STAGING` и `DWH`, таблица workflow-настроек).

### 2. Запустить сервис Kafka → PostgreSQL

Сервис читает сообщения из Kafka и пишет их в PostgreSQL (outbox pattern).
Все параметры передаются через переменные окружения (см. `docker-compose.yaml`):

```bash
# создать .env рядом с docker-compose.yaml
cat > .env <<EOF
KAFKA_HOST=<хост>
KAFKA_PORT=<порт>
KAFKA_CONSUMER_USERNAME=<логин>
KAFKA_CONSUMER_PASSWORD=<пароль>
KAFKA_CONSUMER_GROUP=<группа>
KAFKA_SOURCE_TOPIC=<топик>
PG_WAREHOUSE_HOST=<хост>
PG_WAREHOUSE_PORT=<порт>
PG_WAREHOUSE_DBNAME=<база>
PG_WAREHOUSE_USER=<логин>
PG_WAREHOUSE_PASSWORD=<пароль>
EOF

docker-compose up -d
```

### 3. Деплой DAG-ов в Airflow

1. Скопировать `src/dags/` и `src/py/` в каталог DAG-ов Airflow (`AIRFLOW_HOME/dags/`).
2. Создать подключения: `PG_WAREHOUSE_CONNECTION` (PostgreSQL) и
   `VERTICA_WAREHOUSE_CONNECTION` (Vertica) — Admin → Connections.
3. Включить DAG-и в UI: `final_project_postgresql_to_vertica_data_transfer_dag`
   и `final_project_vertica_datamart_updater_dag`.
---

## Технологии

- Python
- Apache Airflow
- Apache Kafka
- PostgreSQL
- Vertica (DWH)
- Docker
- Helm

---

## Результат

В результате создана масштабируемая аналитическая платформа, которая:

- агрегирует финансовые данные из распределённых источников;
- обеспечивает ежедневное инкрементальное обновление витрины;
- предоставляет бизнесу прозрачную картину динамики оборота;
- позволяет анализировать транзакционную активность по валютам и пользователям.

Проект демонстрирует навыки построения DWH, проектирования ETL-пайплайнов, работы с потоковыми и batch-данными на промышленном стеке.

---
## Лицензия

Проект распространяется по лицензии [MIT](LICENSE.txt).
