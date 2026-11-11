# Мониторинг сервиса скоринга

## Запуск

Нужны Docker с Compose v2. Все команды выполняются из `hw5`:

```bash
docker compose up -d --build --wait --wait-timeout 600
```

- Grafana: http://localhost:3000, `admin` / `admin`.
- Prometheus: http://localhost:9090, источники: http://localhost:9090/targets.
- Метрики скоринга: http://localhost:9100/metrics, записи: http://localhost:9101/metrics.
- Airflow: http://localhost:8087, `admin` / `admin`.
- Приложение: http://localhost:8505, Kafka UI: http://localhost:8086.
- Отчеты DQ: http://localhost:8088, Kafka с хоста: `localhost:9098`.

Симулятор непрерывно отправляет около 10 транзакций/с с новыми UUID и текущим временем. В `data/demo_train.csv` включен пример из ДЗ 3 с увеличенной долей фрода. `target` в скоринг не передается, порог фрода: `score > 0.5`.

Для полного датасета можно положить свой train.csv в `data` и при первом запуске выполнить:

```bash
TRAIN_DATA_CSV=/data/train.csv STREAM_DATA_CSV=/data/train.csv docker compose up -d --build --wait --wait-timeout 600
```

Настройки в `.env.example`, модели в `artifacts/models`, отчеты в `artifacts/reports`.

## Дашборды

Prometheus и PostgreSQL подключаются автоматически, дашборды находятся в папке `Fraud monitoring`. Сбор метрик раз в секунду, обновление графиков раз в 5 секунд.

- [01 Скоринг транзакций](http://localhost:3000/d/hw5-scoring): TPS, P50/P95, доля фрода, распределение скоров, задержка Kafka, смена модели и SQL-срез по версиям.
- [02 Запись результатов](http://localhost:3000/d/hw5-writer): скорость и P95 записи, число скоров и фродов, распределение скоров.
- [03 Системные метрики](http://localhost:3000/d/hw5-system): CPU, память, сеть, диск и доступность источников. На macOS это метрики Linux VM Docker.
- [04 Бизнес и сегменты](http://localhost:3000/d/hw5-business): плотность скоров и TPS с фильтрами по `us_state` и `merch`, доля фрода по `cat_id`, суммы подозрительных операций и топ мерчантов.

Barplot по категориям использует последние 1000 записей всего потока независимо от фильтров и времени. Доля фрода считается по `fraud_flag = 1`.

## Метрики

- `fraud_transactions_total`, `fraud_flagged_total`: число обработанных транзакций и фродов
- `fraud_processing_seconds`, `fraud_score`, `fraud_ratio`: время обработки, распределение скоров и доля фрода с запуска
- `fraud_queue_seconds`: задержка до начала обработки, отсутствует в семинаре. График "Задержка в Kafka P95" помогает заметить отставание от потока
- `fraud_last_processed_timestamp_seconds`: время последнего результата для алерта остановки потока
- `fraud_model_updates_total{version}`, `fraud_model_loaded_timestamp_seconds{version}`: счетчик и время смены модели без учета начальной загрузки
- `writer_scores_total`, `writer_fraud_total`, `writer_db_seconds`, `writer_score`: число записанных скоров и фродов, время записи и распределение скоров.
- `node_cpu_seconds_total`, `node_memory_*`, `node_network_*`, `node_disk_*`, `node_filesystem_*`: системные метрики

У гистограмм есть `_bucket`, `_count`, `_sum`. Счетчики сбрасываются при перезапуске, графики используют `rate` и `increase`. Повторная доставка добавляет строку в PostgreSQL и учитывается в метриках.

## Проверка потока и модели

1. Подождите минуту: все три target в Prometheus должны быть `UP`, счетчики на страницах `/metrics` должны расти.
2. Запустите `retrain_catboost` через Trigger DAG в Airflow или командой ниже. Расписание: раз в 5 минут.
3. После публикации изменится `latest.json`, скорер подхватит модель без перезапуска. На дашборде скоринга появится отметка с версией примерно на 30 секунд и SQL-срез новой версии: число скоров и доля фрода.

```bash
docker compose exec airflow airflow dags trigger retrain_catboost
cat artifacts/models/latest.json
docker compose exec postgres psql -U fraud -d fraud -c 'SELECT model_version, count(*), avg(fraud_flag) AS fraud_rate FROM transaction_scores GROUP BY model_version;'
docker compose logs --tail=20 fraud_detector score_writer
docker compose ps
```

Кандидат публикуется, если PR-AUC, ROC-AUC и F1 не хуже текущей модели. При отклонении или отсутствии следующего месяца запустите `supply_labeled_batch`, затем повторите переобучение.

## Алерты

Правила из `grafana/provisioning/alerting/rules.json` появляются в [Alerting](http://localhost:3000/alerting/list) автоматически и проверяются каждые 10 секунд.

"Остановился поток": более 30 секунд без результата и еще 15 секунд в Pending. Остановите симулятор:

```bash
docker compose stop transaction_simulator
```

После опустошения очереди подождите около минуты до Firing. Вернуть поток:

```bash
docker compose start transaction_simulator
```

"Высокая доля фрода": доля за последние 2 минуты выше 35% в течение 15 секунд. Для проверки отправляйте реальные строки с `target = 1`:

```bash
STREAM_MODE=fraud docker compose up -d --no-deps transaction_simulator
```

Подождите до Firing, затем верните обычный поток:

```bash
STREAM_MODE=mixed docker compose up -d --no-deps transaction_simulator
```

После обновления окна правило вернется в Normal.

Результаты и примеры срабатываний находятся в `results`.