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
