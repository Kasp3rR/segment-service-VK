# Segment Service

Сервис управления пользовательскими сегментами для A/B-тестов.
Репозиторий: `Kasp3rR/segment-service-VK`. Разработка ведётся в ветке `dev`.

На втором этапе реализованы HTTP API на **Gin**, работа с PostgreSQL через **pgx/pgxpool**,
бизнес-валидация, SQL-миграции и структурированные JSON-логи через **zap**.
Асинхронное распределение, Kafka и SQL-замеры относятся к следующим этапам.

## Запуск

Нужны Docker Engine и Docker Compose. Для локальной Go-разработки — Go 1.25+.

```sh
cp .env.example .env
docker compose up -d --build --wait
curl http://localhost:18080/readyz
```

API: `http://localhost:18080`. PostgreSQL: `localhost:55432`.
Порты привязаны к loopback; `API_PORT` и `POSTGRES_PORT` можно изменить в `.env`.
Данные PostgreSQL сохраняются в Docker volume. `docker compose down` сохраняет этот volume.
При старте API автоматически применяет недостающие миграции.

```sh
docker compose logs -f api
docker compose down
```

Локальный запуск Go с базой в Docker:

```sh
docker compose up -d --wait postgres
set -a
. ./.env
set +a
make build
go run ./cmd/api
```

Go-программы читают переменные окружения, `.env` автоматически загружает только Compose.

## Правила данных

- `user_id` задаёт клиент: целое число от 1 до 9223372036854775807. Повторное создание — `409`.
- `segment.id` генерирует PostgreSQL. `code` уникален, соответствует `[A-Z][A-Z0-9_]{0,63}`
  и неизменяем как через HTTP API, так и прямым SQL UPDATE.
- `name` содержит 1–200 Unicode-символов после удаления пробелов по краям;
  `description` — до 4000 символов, может быть пустым.
- PATCH меняет только переданные `name` и `description`. Пустой объект, `null`, изменение
  `code` и неизвестные поля отклоняются. Пустая строка очищает описание.
- Повторные PUT и DELETE участия возвращают `204`. Оба ресурса должны существовать;
  иначе возвращается `404`.
- Удаление сегмента каскадно удаляет его назначения. Повторное удаление сегмента — `404`.
- Список сегментов существующего пользователя без назначений — `[]`; отсутствующий пользователь — `404`.
- `(user_id, segment_id)` — уникальный первичный ключ и составной B-tree индекс.
  Отдельный индекс `segment_id` ускоряет каскадное удаление.

## REST API

POST и PATCH требуют `Content-Type: application/json`; максимальный размер тела — 64 KiB.
JSON должен содержать ровно один объект. Неизвестные поля отклоняются.

| Метод | URL | Успех |
|---|---|---|
| POST | `/segments` | 201, созданный сегмент и Location |
| GET | `/segments?limit=100&after_id=0` | 200, массив сегментов |
| GET | `/segments/{id}` | 200, сегмент |
| PATCH | `/segments/{id}` | 200, обновлённый сегмент |
| DELETE | `/segments/{id}` | 204 |
| POST | `/users` | 201, пользователь |
| PUT | `/users/{user_id}/segments/{segment_id}` | 204 |
| DELETE | `/users/{user_id}/segments/{segment_id}` | 204 |
| GET | `/users/{user_id}/segments` | 200, массив сегментов |
| GET | `/healthz` | 200, процесс работает |
| GET | `/readyz` | 200, PostgreSQL доступна; иначе 503 |

Список сегментов сортируется по `id`, `limit` от 1 до 1000 (по умолчанию 100).
Для следующей страницы передайте последний полученный `id` как `after_id`.
Сегменты пользователя тоже сортируются по `id`.

Ошибки имеют вид:

```json
{"error":{"code":"not_found","message":"resource not found"}}
```

Коды: 400 — неверные данные; 404 — ресурс отсутствует; 409 — конфликт уникальности;
413 — слишком большое тело; 415 — неверный Content-Type; 504 — истёк срок выполнения.
Внутренние ошибки базы клиенту не раскрываются.

## Примеры curl

```sh
BASE=http://localhost:18080
curl -i -X POST "$BASE/users" -H 'Content-Type: application/json' \
  -d '{"user_id":42}'

curl -i -X POST "$BASE/segments" -H 'Content-Type: application/json' \
  -d '{"code":"MAIL_GPT","name":"GPT в письмах","description":"Эксперимент почты"}'
```

Подставьте `id` созданного сегмента:

```sh
SEGMENT_ID=1
curl "$BASE/segments?limit=100"
curl "$BASE/segments/$SEGMENT_ID"
curl -i -X PATCH "$BASE/segments/$SEGMENT_ID" -H 'Content-Type: application/json' \
  -d '{"name":"GPT-помощник","description":""}'
curl -i -X PUT "$BASE/users/42/segments/$SEGMENT_ID"
curl "$BASE/users/42/segments"
curl -i -X DELETE "$BASE/users/42/segments/$SEGMENT_ID"
curl -i -X DELETE "$BASE/segments/$SEGMENT_ID"
```

## Конфигурация

| Переменная | Значение по умолчанию | Назначение |
|---|---|---|
| `DATABASE_URL` | обязательно | PostgreSQL DSN для Go-процесса |
| `HTTP_ADDR` | `:8080` | адрес HTTP внутри процесса |
| `DB_MAX_CONNS` | `10` | размер пула pgx, 1–100 |
| `REQUEST_TIMEOUT` | `10s` | срок выполнения обработки и запросов к БД |
| `SHUTDOWN_TIMEOUT` | `10s` | срок graceful shutdown |
| `POSTGRES_PASSWORD` | `segments_local` | пароль локальной базы Compose |
| `POSTGRES_PORT` | `55432` | внешний порт базы Compose |
| `API_PORT` | `18080` | внешний порт API Compose |
| `TEST_DATABASE_URL` | обязательно для интеграционных тестов | отдельный DSN для проверок |

Значения `.env.example` предназначены для локального запуска. При SIGTERM/SIGINT
API прекращает приём новых соединений, завершает текущие запросы и закрывает пул БД.

## Миграции

SQL хранится в `internal/postgres/migrations` и встраивается в Go-бинарники через `embed`.
Применение защищено advisory lock PostgreSQL, выполняется в транзакции; версия и SHA-256
каждой миграции фиксируются в `schema_migrations`. Изменение уже применённого файла — ошибка запуска.

```sh
# Применить все новые миграции; DATABASE_URL должен быть в окружении.
go run ./cmd/migrate -direction=up
# Откатить последнюю миграцию. Откат 001 удаляет таблицы и их данные.
go run ./cmd/migrate -direction=down
```

Из контейнера:

```sh
docker compose run --rm --entrypoint /app/migrate api -direction=up
```

## Проверки

```sh
make check
make build
make test
make coverage
TEST_DATABASE_URL='postgres://segments:segments_local@localhost:55432/segments?sslmode=disable' \
  make integration
```

`make test` включает детектор гонок. Интеграционные тесты требуют реальную PostgreSQL:
каждый тест создаёт собственную схему и удаляет её после завершения; пользователь БД
должен иметь право создавать схемы. Таблицы приложения не очищаются.
`-tags=integration` без `TEST_DATABASE_URL` завершается ошибкой, а не пропуском проверок.

Модульные тесты охватывают бизнес-валидацию, ошибки, конфигурацию и JSON.
Интеграционные — CRUD, весь HTTP-сценарий, ограничения БД, миграции, параллельные назначения,
идемпотентность и конкурентное изменение разных полей сегмента.
Фактические результаты второго этапа находятся в `docs/stage-2-verification.md`.

## Структура

```text
cmd/api                   запуск Gin HTTP API
cmd/migrate               управление SQL-миграциями
internal/config           конфигурация окружения
internal/domain           модели и ошибки
internal/service          бизнес-валидация и интерфейс хранилища
internal/httpapi           маршруты Gin и HTTP-обработчики
internal/postgres         запросы pgx, транзакции, миграции
integration               тесты на реальной PostgreSQL
```

План следующих этапов: согласованный снимок пользователей и задания распределения;
transactional outbox и Kafka с четырьмя консьюмерами; проверки процентов, батчей, повторов
и SQL-замеры; генератор пользователей и сценарий назначения `MAIL_GPT` 30% пользователей.
