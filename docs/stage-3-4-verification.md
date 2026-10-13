# Проверка этапов 3–4: распределение и Kafka

Дата: 13 октября 2026. Рабочая ветка: `dev`, база изменений — `804dbdd`.
Сообщение коммита этапов 3–4 согласовано с пользователем.

## Окружение

- macOS arm64, Go 1.25.14; тесты Go с детектором гонок.
- Docker Engine 28.3.2, Compose 2.38.2.
- Реальная PostgreSQL 16.15 (`postgres:16-alpine`), localhost:55432.
- Реальная Apache Kafka (`apache/kafka:3.9.1`), KRaft, localhost:29092.
- Один брокер, replication factor 1. Топик `segment-distributions`: 4 партиции.
- Один worker, внутри четыре независимых Kafka Reader одной группы `segment-service`.
- Тесты создают изолированные схемы PostgreSQL и отдельные топики Kafka.
  Таблицы основного приложения не очищаются. Отсутствие переменных тестового окружения
  приводит к ошибке, а не к пропуску интеграционных тестов.

## Команды и результаты

```sh
make check
make build
make coverage
GIN_MODE=release \
TEST_DATABASE_URL='postgres://segments:segments_local@localhost:55432/segments?sslmode=disable' \
TEST_KAFKA_BROKERS=localhost:29092 \
go test -race -tags=integration,kafka -count=1 -timeout=300s -json ./...
docker compose up -d --build --wait
```

Все команды завершились с кодом 0. `make check` включает gofmt и go vet.
Собраны API, migrate и worker; API, worker, PostgreSQL и Kafka — healthy.
Локально дополнительно заданы GOCACHE и GOMODCACHE в /private/tmp, что не меняет
состав проверок. Для общего прогона использована копия исходников; байты Go/SQL-файлов
сверены с рабочим репозиторием. Финальные make check/build/coverage выполнены в репозитории.

- **84 тестовые функции**, из них 36 модульных и 48 интеграционных.
- **216 конечных сценариев**, если считать отдельные subtest и не считать их родителей дважды.
- 0 ошибок, 0 пропущенных тестов, race detector не обнаружил гонок.
- Интеграционный пакет: 71,476 секунды в итоговом прогоне.
- Из 48 интеграционных функций 10 используют одновременно реальные Kafka и PostgreSQL.

[Вывод тестов](verification-stage-3-4/tests.txt) содержит названия сценариев и их результаты.
[Проверка, сборка и покрытие](verification-stage-3-4/check-build-coverage.txt) — исходный вывод make.

## Покрытие бизнес-логики

```sh
go test -race -coverprofile=coverage.out ./internal/service ./internal/distribution
go tool cover -func=coverage.out
```

| Область | Statements coverage |
|---|---:|
| internal/service: валидация и операции сервиса | 100,0% |
| internal/distribution: проценты, выбор, батчи и валидация сообщений | 98,4% |
| Вместе | **99,1%** |

Это покрытие двух указанных пакетов бизнес-логики, а не всего приложения.
HTTP, конфигурация, pgx и Kafka проверяются отдельными модульными и интеграционными тестами.
Непокрытая ветка сравнения равных SHA-256 не подменялась искусственными хешами ради процента.

## Что подтверждено тестами

- CRUD, ошибки, строгий JSON, получение сегментов пользователя, ограничения и миграции MVP.
- Точный процент без float64, округление вниз, 0/100%, дроби, пустая база, большие int64.
- Воспроизводимость хеширования, сохранение существующих назначений и конфликт превышения цели.
- Границы 4999/5000/5001/10000/10001, отсутствие повторных кандидатов.
- Согласованность снимка при создании пользователя после чтения списка, до коммита задания.
- Конкурентный запуск: одно активное задание на сегмент, ручные операции и удаление получают 409.
- Атомарность задания/outbox при ошибке вставки; параллельные claims через SKIP LOCKED.
- Атомарность назначений/processed_batches/счётчиков при ошибке БД, повтор одного батча.
- Реальная группа Kafka из четырёх участников, по одной партиции каждому.
- Реальная параллельность: четыре батча по 5000, BEFORE INSERT trigger задерживает каждый
  на 2 секунды; разброс начала четырёх транзакций **0,029372 секунды** в итоговом прогоне.
  Проверяются четыре различных consumer_id и kafka_partition. Это проверка перекрытия
  транзакций с искусственной задержкой, не оценка пропускной способности.
- Пока транзакция PostgreSQL заблокирована advisory lock, offset Kafka не продвинут;
  после коммита offset подтверждается.
- Повторная доставка, перезапуск после коммита БД до offset, временная ошибка БД.
- Kafka подтвердила публикацию, а UPDATE outbox упал: повторная отправка не удваивает назначения.
- Недоступный адрес брокера: outbox и retrying сохраняются; после восстановления отправка завершается.
- Повреждённый JSON сохраняется в kafka_dead_letters; следующее сообщение той же партиции обрабатывается.
- Ошибка ограничения БД переводит job в failed; частично завершённые предыдущие батчи сохраняются.
- Топик с одной партицией отклоняется при проверке конфигурации worker.

## Проверка через запущенный HTTP API

В локальную базу добавлены пользователи 1..70000 через generate_series с ON CONFLICT DO NOTHING.
До проверки в базе уже был пользователь 42. Через POST /segments создан MAIL_GPT (id=2),
POST /segments/2/distributions с `{"percent":30}` вернул 202.

[Ответ задания](verification-stage-3-4/demo.json):

- job_id: `94bdd047-0db3-47c2-8dac-42dded333f80`;
- snapshot_users=70000, target_users=21000, existing_users=0;
- assigned_users=21000, processed_batches=batch_count=5;
- status=completed, retry_count=0, last_error пуст;
- ожидание завершения после ответа POST: 0,780 секунды при опросе каждые 250 ms;
- created_at → completed_at: около 0,550 секунды. Это один локальный запуск, не нагрузочный бенчмарк.

Сверка PostgreSQL и Kafka:

| batch_number | inserted_users | consumer_id | partition |
|---:|---:|---|---:|
| 0 | 5000 | 5d55bf9dc304/3 | 0 |
| 1 | 5000 | 5d55bf9dc304/2 | 1 |
| 2 | 5000 | 5d55bf9dc304/1 | 2 |
| 3 | 5000 | 5d55bf9dc304/0 | 3 |
| 4 | 1000 | 5d55bf9dc304/3 | 0 |

COUNT(*) назначений MAIL_GPT = 21000; неопубликованных строк outbox = 0.
Группа `segment-service` имеет четыре участника; offsets партиций 0..3: 2,1,1,1;
lag в каждой партиции = 0. GET /users/6/segments вернул MAIL_GPT,
GET /users/1/segments вернул пустой массив.

Для повторения на локальной демонстрационной базе:

```sh
docker compose exec -T postgres psql -U segments -d segments \
  -c 'INSERT INTO users(id) SELECT generate_series(1,70000) ON CONFLICT DO NOTHING;'
curl -X POST http://localhost:18080/segments -H 'Content-Type: application/json' \
  -d '{"code":"MAIL_GPT","name":"GPT в письмах"}'
curl -X POST http://localhost:18080/segments/2/distributions -H 'Content-Type: application/json' \
  -d '{"percent":30}'
curl http://localhost:18080/distributions/JOB_ID
docker compose exec -T postgres psql -U segments -d segments \
  -c "SELECT count(*) FROM user_segments us JOIN segments s ON s.id=us.segment_id WHERE s.code='MAIL_GPT';"
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group segment-service
```

ID сегмента и JOB_ID нужно взять из ответов; здесь 2 — ID конкретного проверенного запуска.
При повторении на той же базе POST сегмента вернёт 409; используйте существующий сегмент.
Повторное распределение без изменений сразу completed, так как цель уже достигнута.

## Ограничения текущих этапов

- Планирование O(N) памяти/O(N log N) сортировки выполняется до ответа 202; массовая запись асинхронна.
- Активный job блокирует ручные назначения этого сегмента; автоматической отмены нет.
- При failed предыдущие успешно обработанные батчи сохраняются. Прямые изменения таблиц
  оператором не входят в гарантии согласованности API.
- Kafka в Compose без высокой доступности, TLS/SASL и резервного копирования; история/outbox/DLQ
  пока не очищаются автоматически. Это локальное окружение для разработки.
- EXPLAIN ANALYZE и измерения ускорения SQL относятся к следующему этапу и ещё не выполнены.
  Генератор пользователей и автоматизированный демонстрационный скрипт также остаются следующим этапом.
