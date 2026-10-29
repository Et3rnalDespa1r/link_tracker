# Запуск и наблюдение

1. Создайте `.env` с `APP_TELEGRAM_TOKEN` и `DB_PASSWORD=password`. Не добавляйте его в Git.
2. Запустите `docker compose -f docker-compose.yml -f compose.apps.yml up -d --build`.
3. Grafana: http://localhost:3000, Prometheus: http://localhost:9090. Дашборд и RAM alert загружаются из `monitoring/grafana` автоматически. Начальный пароль Grafana задаётся в основном Compose; смените его для внешнего размещения.
4. В Telegram отправьте `/start`, `/track`, ссылку и теги. Скраппер сохраняет обновление с автором в Outbox, публикует JSON в `link.raw-updates`, агент фильтрует, группирует и публикует в `link.processed-updates`, бот доставляет сообщение.

Метрики доступны на `/metrics`: Scrapper — порт 8083, Bot — отдельный порт 8011. Prometheus опрашивает приложения; Pushgateway включается непустым `PUSHGATEWAY_URL`.

Основные метрики: `command_requests_total`, `command_duration_ms_total`, `api_requests_total`, `request_duration_ms_total`, `links_on_track_total`, `sent_notification_total`, стандартные Go/process метрики. Запросы и перцентили приведены в `example_pql.txt`, визуализации — в `dashboard.json`.

Доставка сообщений — at-least-once: сбой между отправкой и подтверждением может привести к повтору. Агент подтверждает входной пакет только после подтверждения выходных сообщений Kafka; при штатной остановке отправляет накопленный пакет. При недоступной DLQ бот сохраняет входной offset неподтверждённым. Автоматическое устранение дублей Telegram не гарантируется.

Проверки: `go test -race ./...`, `go vet ./...`, `go build ./cmd/...`. Интеграционные тесты поднимают отдельные временные контейнеры; нужен работающий Docker. Они не отправляют сообщения в Telegram.

Публикация образа: `docker build --build-arg SERVICE=bot -t REGISTRY/linktracker-bot:VERSION .`, затем `docker push REGISTRY/linktracker-bot:VERSION`. Повторите для `scrapper` и `agent`; адрес реестра и авторизацию задаёт владелец проекта.
