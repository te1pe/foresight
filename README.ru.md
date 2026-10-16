# foresight

foresight собирает метрики и логи сервисов, ищет аномалии и **прогнозирует исчерпание ресурсов до сбоя**: «диск заполнится через 3 ч 20 мин», «пул соединений закончится через 4 ч».

- `ingest` опрашивает цели из таблицы `targets` (Postgres), разбирает Prometheus text format собственным парсером, принимает логи через `POST /v1/logs` и публикует всё в Redpanda;
- `processor` читает их в consumer group и идемпотентно пишет в ClickHouse. Окна 1м/5м считают materialized views при вставке;
- раз в минуту `processor` прогоняет детекторы по окнам: EWMA z-score на скорость изменения, прогноз исчерпания (диск) и устойчивый рост (утечка горутин, журнал CRDT без компакции);
- `query` отдаёт ряды и вердикты детекторов по HTTP: «`/` заполнится через 1d21h»;
- `alerter` превращает срабатывания в инциденты (PostgreSQL) и шлёт уведомления в Telegram без дублей (Redis).

Доставка at-least-once, хранение без дублей: повторная доставка не меняет ни сырые точки, ни агрегаты. foresight наблюдает сам за собой: у каждого сервиса есть `/metrics` и pprof, и он опрашивает сам себя. Цифры нагрузочного теста — в разделе [«Нагрузка»](#нагрузка).

Интеграция с [Cloute](https://github.com/te1pe/cloute) сделана на стороне foresight (цели, приём логов, правило журнала CRDT); сам Cloute метрику журнала пока не отдаёт.

## Архитектура

Зелёным отмечено то, что уже работает, серым — то, что ещё не подключено.

```mermaid
flowchart TD
    NE[node_exporter<br/>localhost:9100]
    CL[Cloute /metrics]
    LG[POST /v1/logs]
    IN[cmd/ingest<br/>scrape + приём логов]
    RP[[Redpanda<br/>topic foresight.samples]]
    PR[cmd/processor<br/>consumer group,<br/>идемпотентная запись]
    CH[(ClickHouse<br/>samples → MV → samples_1m / samples_5m<br/>detections)]
    QY[cmd/query<br/>/v1/series, /v1/detections]
    DT[processor: правила раз в минуту<br/>EWMA z-score, МНК-прогноз]
    AL[cmd/alerter<br/>инциденты + outbox]
    PG[(PostgreSQL<br/>incidents, incident_events)]
    RD[(Redis)]
    TG[Telegram]

    NE --> IN
    CL -.-> IN
    LG --> IN
    IN --> RP
    RP --> PR
    PR --> CH
    CH --> QY
    CH --> DT
    DT --> CH
    CH --> AL
    AL --> PG
    AL --> RD
    AL --> TG

    classDef done fill:#d4edda,stroke:#28a745,color:#000
    classDef todo fill:#f1f1f1,stroke:#999,color:#666,stroke-dasharray: 4 3
    class NE,IN,RP,PR,CH,QY,DT,AL,PG,RD,TG done
    class CL todo
```

PostgreSQL хранит метаданные: инциденты, очередь их уведомлений и таблицу целей.

## Как это устроено

### ingest

- Цели лежат в таблице `targets` и перечитываются раз в 30 с. Добавление и выключение работают без рестарта:
  ```sql
  INSERT INTO targets (target) VALUES ('localhost:8080');          -- например, Cloute
  UPDATE targets SET enabled = false WHERE target = 'localhost:8080';
  ```
  Если Postgres недоступен, опрашивается последний известный список.
- По одной горутине на цель. Опрос идёт сразу при старте, затем раз в `FORESIGHT_SCRAPE_INTERVAL`, на каждый запрос даётся 10 с. Все сэмплы одного опроса получают одну метку времени. Синтетические `up` и `scrape_duration_seconds` добавляются, как в Prometheus.
- Один опрос одной цели — одна запись в Kafka (JSON, значения строками) с ключом `target`.
- Producer идемпотентный, `acks=all`, буфер ограничен 64 МиБ. Если брокер недоступен, scraper'ы притормаживают (backpressure), данные не теряются.
- `GET /healthz`, `GET /readyz` (проверяет брокер), `GET /v1/targets` (состояние целей).
- `POST /v1/logs?service=NAME` принимает NDJSON, например вывод `slog.NewJSONHandler` как есть:
  ```bash
  ./my-service 2>&1 | curl --data-binary @- 'localhost:8080/v1/logs?service=my-service'
  ```
  Ответ `202 {"accepted":N,"rejected":M}` приходит только после подтверждения брокера; `503` означает «повтори». Битые строки отклоняются поштучно. Ограничения: тело до 256 КиБ (иначе 413), строка до 64 КиБ. В ClickHouse логи лежат в таблице `logs`; дубли (повтор запроса, повторная доставка) схлопываются по `(service, ts, хеш строки)`, поэтому одинаковые строки без собственного времени сливаются в одну.

### processor

- Consumer group `foresight-processor`. Offset'ы коммитятся только после успешной записи. Временные ошибки ретраятся с паузой 0,5–10 с без коммита.
- **Одна вставка в ClickHouse на один опрос**, с `insert_deduplication_token = target|время опроса`. Повторно доставленный опрос несёт тот же токен, и ClickHouse его отбрасывает. Неважно, как consumer сгруппировал записи в poll.
- `deduplicate_blocks_in_dependent_materialized_views=1` обязателен: без него ClickHouse 26.3 отбрасывает дубликат в сырой таблице, но всё равно передаёт его в materialized views, и `count`/`avg` в окнах завышаются. Это проверено на сервере и закреплено интеграционным тестом.

### ClickHouse

| Таблица | Движок | Ключ сортировки | Хранение |
|---|---|---|---|
| `samples` | ReplacingMergeTree | `(target, name, series_id, ts)` | 14 дней |
| `samples_1m` | AggregatingMergeTree, наполняется MV | `(target, name, series_id, window_start)` | 90 дней |
| `samples_5m` | AggregatingMergeTree, наполняется MV | `(target, name, series_id, window_start)` | 365 дней |

- `series_id` — хеш FNV-1a от `(target, name, отсортированные labels)`: `Map` не может входить в ключ сортировки.
- В окнах хранятся `min`, `max`, `count` (простые агрегаты) и состояния `avg` и `last = argMax(value, ts)`, которые сливаются при чтении через `-Merge`.
- Дедупликация двухуровневая. Токен защищает от повторов в пределах последних 10 000 вставок. ReplacingMergeTree и чтение сырых точек через `FINAL` — страховка для сырых данных при более старых повторах.

### Детекторы

Раз в минуту, через 20 с после границы (чтобы последнее окно успело дописаться), processor оценивает правила по закрытым 1m-окнам за последний час. Каждый результат, сработавший или нет, пишется в `detections`.

| Правило | Что делает | Срабатывает |
|---|---|---|
| `disk_fill_forecast` | МНК по `node_filesystem_avail_bytes` → время до нуля | значимый спад (t ≤ −3) и ETA ≤ 24 ч |
| `disk_usage_rate_anomaly` | EWMA z-score скорости изменения свободного места (α=0.1, \|z\| ≥ 4) | резкий выброс скорости |
| `goroutine_leak` | устойчивый рост `go_goroutines` | t ≥ 3, рост ≥ 30 мин, +20% и +50 |
| `crdt_journal_growth` | устойчивый рост `cloute_crdt_journal_size` без компакции | t ≥ 3, ≥ 45 мин без скачка вниз, +50% и +1000 |

`go_goroutines` есть у любого Go-сервиса с клиентом Prometheus. `cloute_crdt_journal_size` — контракт с Cloute: gauge с числом операций в журнале документа. Компакция (резкое падение) сбрасывает отсчёт роста, поэтому нормальная «пила» не срабатывает (есть тест).

- **Прогноз смотрит только на точки после последнего скачка скорости.** Разовое копирование 50 ГБ — это ступенька, а не тренд. Без этого регрессия выдала бы «заполнится через 2 часа» (есть контрольный тест). Тот же приём находит начало заполнения посреди окна.
- **Значимость тренда проверяется t-статистикой**, а не R²: на чистом шуме 0 ложных трендов из 300 прогонов.
- **Ограничение — сезонность.** На окне в пару периодов синусоида даёт ложный наклон (на синтетике t = −4.6). Спасает величина: ETA ≈ 9 дней, за горизонтом. Суточную сезонность часовое окно не видит.
- Оценка — чистая функция от данных и минуты. Повтор или второй processor дают те же ключи, ReplacingMergeTree их схлопывает.

Детекторы проверены на синтетике: тренд, шум, ступенька, спайк, сезонность и их сочетания (`internal/detect`).

### alerter

Раз в 15 с alerter читает свежие оценки из `detections` и ведёт инциденты:

```
firing 2 раунда подряд ──► инцидент открыт ──► событие "opened" в outbox
тихо 3 раунда подряд   ──► инцидент закрыт ──► событие "resolved"
ряд не оценивается 10 мин (а остальные оцениваются) ──► закрыт как no_data
```

- **Гистерезис** (2 на открытие, 3 на закрытие) не даёт инциденту дребезжать у порога.
- **Один открытый инцидент на ряд** гарантирует частичный уникальный индекс Postgres, а не код. Гонку двух alerter'ов выигрывает ровно один.
- **Transactional outbox:** уведомление пишется в той же транзакции, что и переход. Падение между ними ничего не теряет.
- **Redis-захват перед отправкой:** `pending:<token>` на 2 мин (SET NX), после успеха `sent` на 7 дней. Повторы и параллельные alerter'ы не шлют одно и то же дважды. Освобождение атомарно сверяет токен (Lua), чтобы не удалить чужой захват.
- Переходы одного раунда уходят **одним сообщением**. Ответ 429 от Telegram ставит доставку на паузу на `retry_after`.
- Без `FORESIGHT_TELEGRAM_TOKEN` уведомления пишутся в лог: весь конвейер работает локально без бота.

Пример уведомления:

```
🔴 Диск заполняется
localhost:9100 mountpoint=/
заполнится через 3 ч 20 мин (свободно 12.0 ГиБ, убывает на 60.0 МиБ/мин)
```

### query

```
GET /v1/series?target=localhost:9100&name=node_filesystem_avail_bytes
              &step=raw|1m|5m&from=RFC3339&to=RFC3339&label=mountpoint=/
```

- По умолчанию `step=raw` и последний час. `label` можно повторять, это точное совпадение.
- Значения отдаются строками (`"NaN"`, `"+Inf"` допустимы), `id` ряда — тоже строкой: uint64 не помещается в число JavaScript.
- Больше 50 000 точек — ответ 422 с подсказкой сузить запрос. Обрезанный ряд выглядел бы как настоящие данные.

```
GET /v1/detections[?target=localhost:9100][&firing=true]
```

Последний вердикт каждого правила по каждому ряду за 15 минут: `status`, `firing`, `score` (t или z), `eta` («1d21h34m») и `eta_seconds`.
- Отдельный сервис, чтобы тяжёлый запрос не тормозил приём и обработку.

## Самонаблюдение

У каждого сервиса есть admin-сервер только на loopback, с `/metrics` (client_golang) и `/debug/pprof/*`:

| Сервис | Адрес |
|---|---|
| ingest | `127.0.0.1:6060` |
| processor | `127.0.0.1:6061` |
| query | `127.0.0.1:6062` |
| alerter | `127.0.0.1:6063` |

pprof отдаёт стеки и кучу, поэтому он вынесен с публичных портов (есть тест). Миграция `00005` заводит эти адреса в `targets`: foresight опрашивает сам себя, и его правило `goroutine_leak` следит за его же процессами.

Ключевые метрики:

| Метрика | Что показывает |
|---|---|
| `foresight_pipeline_latency_seconds{kind}` | время от опроса (или приёма лога) до записи в ClickHouse; главная цифра под нагрузкой |
| `foresight_scrape_duration_seconds{target}`, `_samples_total`, `_failures_total` | опрос целей |
| `foresight_queue_handle_duration_seconds`, `_handle_retries_total`, `_dropped_total` | consumer: обработка, повторы, отброшенное |
| `foresight_clickhouse_insert_duration_seconds{table}`, `_inserted_rows_total` | запись |
| `foresight_rules_firing{rule}`, `foresight_alert_open_incidents` | детекторы и инциденты |
| `foresight_http_request_duration_seconds{handler}` | API; метка — шаблон маршрута, а не сырой путь |

```bash
go tool pprof -top 'http://127.0.0.1:6061/debug/pprof/profile?seconds=30'
```

## Нагрузка

`make load` (`cmd/loadgen`) поднимает фальшивый экспортёр, который отдаёт реальную фикстуру node_exporter (540 сэмплов) на путях `/t/{i}/metrics`. Он заводит N таких целей в `targets` (и удаляет их при выходе), может слать логи, а в конце снимает `/metrics` foresight нашим же парсером и считает квантили по бакетам гистограмм, так же, как `histogram_quantile`. Параметры: `FORESIGHT_LOADGEN_TARGETS`, `_DURATION`, `_WARMUP`, `_LOGS_RPS`.

Стенд: MacBook Air M3, 8 ядер, 16 ГБ; Postgres, Redpanda, ClickHouse и Redis в OrbStack на той же машине. Интервал опроса 15 с, окно измерения 90 с после прогрева.

| Цели | Сэмплов/с | Записано строк/с | Задержка p50 / p90 / p99 | CPU ingest / processor | RSS ingest / processor |
|---|---|---|---|---|---|
| 100 | 3 628 | 3 635 | 113 мс / 239 мс / 660 мс | 0.03 / 0.03 ядра | 27 / 27 МиБ |
| 150 | 5 415 | 5 435 | 147 мс / 452 мс / 876 мс | 0.04 / 0.04 | 26 / 33 МиБ |
| 200 | 7 208 | 7 235 | 322 мс / 854 мс / 1.66 с | 0.05 / 0.05 | 26 / 32 МиБ |
| **300** | 10 799 | **7 720** | 20 с / 39 с / 44 с, растёт | 0.07 / 0.04 | 28 / 128 МиБ |
| 150 + 2000 строк лога/с | 5 420 | 5 435 + 2 000 логов | 142 мс / 454 мс / 877 мс; логи p99 128 мс | 0.08 / 0.07 | 25 / 31 МиБ |

**Потолок — около 7,7 тыс. строк/с, то есть примерно 14 вставок/с или ~210 целей такого размера при интервале 15 с.** На 300 целях processor отстаёт: lag дорос до 872 записей и разобрался за 91 с после прогона, ничего не потерялось. Узкое место — не CPU (оба сервиса тратят меньше 0,1 ядра, CPU-профили под нагрузкой показывают ~6% одного ядра), а **последовательные вставки «одна на опрос»**: около 70 мс каждая на ожидание ClickHouse.

Что нагрузочный тест нашёл и что изменилось:

| Находка | До | После |
|---|---|---|
| Все цели, добавленные одной сверкой, опрашиваются в одну секунду, и в processor каждые 15 с приходит залп вставок | p99 11.6 с при p99 вставки 140 мс | детерминированный сдвиг фазы по хешу цели, как в Prometheus: **p99 660 мс** |
| У `http.Transport` по умолчанию 2 keep-alive соединения на `host:port`: экспортёры за одним адресом переподключались на каждый опрос | p99 опроса 3.2 с, ошибки | пул на 256 соединений: p99 опроса 5–20 мс |
| `consumer.Close()` при недоступном брокере вечно ждал выхода из группы, мимо таймаута остановки | процесс не завершался по SIGTERM | выход из группы ограничен тем же таймаутом |
| Попытка распараллелить вставки (8 потоков) | — | **откачена**: под нагрузкой на живой базе ClickHouse 26.3 падал на одновременных вставках в 1m-MV с `LOGICAL_ERROR: Invalid partition key size: 0`, а те же записи последовательно проходили. В чистом контейнере ошибка не воспроизвелась. Ошибка сервера, зависящая от состояния, которая подвешивает партицию, хуже известного потолка. |

Как поднять потолок: меньше и крупнее вставок. Для этого нужен ключ дедупликации, который переживает перегруппировку опросов по вставкам (сейчас токен — один опрос). Например, детерминированная нарезка по `(партиция Kafka, диапазон offset'ов)` или дедупликация до MV. Это изменение гарантий доставки, отдельная задача.

Собственный парсер text format: **0,2 мс** на реальный вывод node_exporter (752 строки, 538 сэмплов), около 258 МБ/с, 3125 аллокаций (`go test -bench ParseNodeExporter ./internal/scrape/`).

## Конфигурация

| Переменная | По умолчанию | Кто читает |
|---|---|---|
| `FORESIGHT_SCRAPE_INTERVAL` | `15s` | ingest |
| `FORESIGHT_HTTP_ADDR` | `:8080` | ingest |
| `FORESIGHT_KAFKA_BROKERS` | `localhost:19092` | ingest, processor |
| `FORESIGHT_KAFKA_TOPIC` | `foresight.samples` | ingest, processor |
| `FORESIGHT_KAFKA_LOGS_TOPIC` | `foresight.logs` | ingest, processor |
| `FORESIGHT_KAFKA_GROUP` | `foresight-processor` | processor |
| `FORESIGHT_CLICKHOUSE_DSN` | `clickhouse://foresight:foresight@localhost:9000/foresight` | processor, query |
| `FORESIGHT_QUERY_HTTP_ADDR` | `:8081` | query |
| `FORESIGHT_DB_DSN` | `postgres://foresight:foresight@localhost:5433/foresight?sslmode=disable` | ingest (цели), alerter |
| `FORESIGHT_REDIS_ADDR` | `localhost:6379` | alerter |
| `FORESIGHT_TELEGRAM_TOKEN` | — (пусто: уведомления в лог) | alerter |
| `FORESIGHT_TELEGRAM_CHAT_ID` | — (обязателен вместе с токеном) | alerter |
| `FORESIGHT_TELEGRAM_API_URL` | `https://api.telegram.org` | alerter |
| `FORESIGHT_ADMIN_ADDR` | свой у каждого сервиса, `127.0.0.1:6060`–`6063` | все |

## Запуск на macOS

Понадобятся Go (последняя стабильная версия), Docker (Docker Desktop или OrbStack) и Homebrew.

1. Запустите `node_exporter` **нативно**, не в Docker. Контейнеры на macOS работают внутри Linux-VM, поэтому из Docker вы получите метрики VM, а не Mac.
   ```bash
   brew install node_exporter
   brew services start node_exporter
   ```
2. Поднимите PostgreSQL (5433), Redpanda (19092), ClickHouse (9000 native, 8123 HTTP) и Redis (6379). Команда сама создаёт топик.
   ```bash
   make compose-up
   ```
3. Примените миграции обеих БД. goose запускается через `go run` с закреплённой версией.
   ```bash
   make migrate-up
   ```
4. В четырёх терминалах:
   ```bash
   make run-processor
   ```
   ```bash
   make run-query
   ```
   ```bash
   make run-alerter
   ```
   ```bash
   make run
   ```
   Для Telegram создайте бота у @BotFather, узнайте `chat_id` и перед `make run-alerter` выполните `export FORESIGHT_TELEGRAM_TOKEN=... FORESIGHT_TELEGRAM_CHAT_ID=...`. Токен держите в окружении, в репозиторий его не коммитьте.
5. Проверьте (прогнозу нужно минимум 10 минут данных):
   ```bash
   curl -s 'localhost:8081/v1/series?target=localhost:9100&name=node_filesystem_avail_bytes&step=1m&label=mountpoint=/'
   curl -s 'localhost:8081/v1/detections?target=localhost:9100'
   ```

Остановка: `Ctrl+C` или `kill -TERM <pid>`. Инфраструктура гасится через `make compose-down`, данные остаются в volume.

Так как Kafka хранит историю, новое хранилище можно наполнить из топика: остановите processor, выполните `rpk group seek foresight-processor --to start` и запустите его снова. Уже записанное отбросится дедупликацией.

## Разработка

```bash
make lint   # go vet + gofmt, при успехе ничего не печатает
make test   # все тесты с -race; интеграционным нужен запущенный Docker
```

Интеграционные тесты поднимают PostgreSQL, Redpanda, ClickHouse и Redis через testcontainers-go и применяют настоящие миграции. С `go test -short ./...` они пропускаются.

## Структура

```
cmd/ingest/                  # scrape и приём логов → Redpanda, /v1/targets, /v1/logs
cmd/processor/               # Redpanda → ClickHouse (сэмплы и логи), ежеминутная оценка правил
cmd/query/                   # GET /v1/series, /v1/detections
cmd/alerter/                 # detections → инциденты → Telegram
cmd/loadgen/                 # нагрузочный тест (make load)
internal/config/             # чтение FORESIGHT_* из env
internal/scrape/             # парсер Prometheus text format, scraper, наблюдатель за targets
internal/logs/               # разбор NDJSON-логов
internal/queue/              # формат сообщений, producer, consumer с at-least-once
internal/detect/             # EWMA z-score, МНК-прогноз, устойчивый рост (чистая математика)
internal/rules/              # правила для диска и Go-сервисов, цикл оценки
internal/alert/              # жизненный цикл инцидентов, доставка из outbox
internal/notify/             # Telegram и log-уведомитель
internal/storage/redis/      # захваты для дедупликации отправок
internal/storage/clickhouse/ # сэмплы, окна, detections, logs
internal/storage/postgres/   # targets, инциденты и outbox; первоначальная запись сэмплов в PostgreSQL (оставлена, не используется)
internal/httpapi/            # хендлеры ingest и query, HTTP-метрики
internal/admin/              # /metrics и pprof на loopback
internal/loadgen/            # фальшивый экспортёр, чтение /metrics, отчёт
migrations/                  # goose SQL для PostgreSQL
migrations/clickhouse/       # goose SQL для ClickHouse
deploy/docker-compose.yml    # PostgreSQL 18, Redpanda 26.2, ClickHouse 26.3, Redis 8.8
testdata/                    # реальный вывод node_exporter
```
