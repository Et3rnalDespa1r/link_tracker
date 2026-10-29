# LinkTracker

Telegram-бот отслеживает GitHub-репозитории и вопросы Stack Overflow. Три приложения: Bot принимает команды, Scrapper проверяет источники и хранит подписки, AI Agent фильтрует, суммирует и группирует уведомления.

## Запуск

Нужны Docker Desktop либо Docker Engine с Compose. Создайте `.env`:

```env
APP_TELEGRAM_TOKEN=токен_от_BotFather
DB_PASSWORD=password
```

```sh
docker compose -f docker-compose.yml -f compose.apps.yml up -d --build
```

Приложения собираются из общего Dockerfile с параметром `SERVICE=bot|scrapper|agent`. Инфраструктура: PostgreSQL, кластер Kafka, Schema Registry, Valkey, Prometheus и Grafana. В Telegram выполните `/start`, затем `/track`, отправьте ссылку и теги либо `нет`. `/list [тег]` показывает подписки, `/untrack URL` удаляет, `/cancel` отменяет диалог.

## Архитектура

Bot работает с подписками через REST API Scrapper. Scrapper сохраняет изменения и событие в PostgreSQL одной транзакцией. Outbox публикует JSON с автором в `link.raw-updates`. AI Agent обрабатывает пакет в настраиваемом окне и публикует JSON в `link.processed-updates`; Bot читает результат и отправляет в Telegram. Legacy Avro producer/consumer поддерживаются и проверяются интеграционными тестами.

`USE_KAFKA=false` включает HTTP-доставку Scrapper → Bot; при сбое применяется тот же Outbox. Bot слушает обработанные Kafka-сообщения и в этом режиме для fallback.

Доставка at-least-once: после сбоя возможны повторы. Сначала подтверждается выходное сообщение, затем входной offset. Exactly-once доставка в Telegram не заявляется.

## Разработка

Go 1.25+; Kafka-клиент агента требует CGO и C-компилятор. Для запуска с хоста сначала поднимите инфраструктуру: `docker compose up -d`. PostgreSQL доступен на `localhost:5433`; Kafka — `9092,9094,9095`. Затем в отдельных терминалах:

```sh
go run ./cmd/scrapper
go run ./cmd/agent
go run ./cmd/bot
```

Не публикуйте `.env`. Настройки интервала проверки, количества workers, размера батча, Kafka-топиков и фильтров агента описаны в `internal/config/config.go`.

## Проверки и мониторинг

```sh
go build ./cmd/...
go vet ./...
go test -race ./...
```

Для интеграционных тестов необходим Docker. Используются временные контейнеры PostgreSQL, Kafka и Valkey, внешние API подменяются HTTP-заглушками.

Grafana: http://localhost:3000, Prometheus: http://localhost:9090. Инструкция, метрики, ограничения доставки и публикация образов описаны в [OBSERVABILITY.md](OBSERVABILITY.md). JSON дашборда — `dashboard.json`, примеры PromQL — `example_pql.txt`.
