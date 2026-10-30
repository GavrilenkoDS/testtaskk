# Асинхронный сервис платежей

FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ/FastStream, Alembic.

## Запуск

Нужен Docker с Compose v2. Из каталога проекта:

```sh
cp .env.example .env
docker compose --profile demo up -d --build
```

API: `http://localhost:8000`. Миграции применяются автоматически.
Ключ API, подключения и таймауты задаются в `.env`; образец — [.env.example](.env.example).

## Проверка API

На всех эндпоинтах обязателен `X-API-Key`. Для создания также нужен `Idempotency-Key`.
Пример для PowerShell:

```powershell
$key = (Select-String -Path .env -Pattern '^API_KEY=').Line.Split('=', 2)[1]
$headers = @{ 'X-API-Key' = $key; 'Idempotency-Key' = 'demo-1' }
$body = @{
  amount = '100.00'; currency = 'RUB'; description = 'Demo'
  webhook_url = 'http://webhook:8080/webhook'
} | ConvertTo-Json

$payment = Invoke-RestMethod -Method Post -Uri 'http://localhost:8000/api/v1/payments' `
  -Headers $headers -ContentType 'application/json' -Body $body
$payment

Invoke-RestMethod "http://localhost:8000/api/v1/payments/$($payment.payment_id)" -Headers $headers
```

POST возвращает `202`, `payment_id`, `status`, `created_at`; GET — детали платежа.
Повтор с тем же ключом и данными возвращает тот же платёж, с другими данными — `409`.
Неконечные числа и metadata глубже 32 уровней отклоняются с `422`.

Полученные webhook: `http://localhost:8080/events`.
Для проверки ошибок укажите `webhook_url=http://webhook:8080/fail` и новый ключ.
DLQ `payments.dlq` доступна в RabbitMQ Management: `http://localhost:15672`;
логин и пароль — `RABBITMQ_DEFAULT_USER` / `RABBITMQ_DEFAULT_PASS` из `.env`.

## Обработка

- Платёж и outbox-событие сохраняются одной транзакцией. Relay публикует событие в `payments.new`.
- Один consumer эмулирует шлюз: 2–5 секунд, 90% `succeeded`, 10% `failed`, затем отправляет webhook.
- Всего три попытки доставки webhook с задержками 2 и 4 секунды; после третьей ошибки — DLQ.
- Результат оплаты сохраняется до webhook. Повтор уведомления не запускает оплату заново.
- Доставка at least once: получатель дедуплицирует webhook по `event_id`.

## Тесты

Полный набор с PostgreSQL и RabbitMQ, в отдельной тестовой БД:

```sh
docker compose --profile test run --rm --build tests
```

Smoke и девять E2E-сценариев для запущенного профиля `demo` (нужен Python 3.12+):

```sh
pip install -e '.[dev]'
python -m scripts.smoke
python -m scripts.e2e
```

E2E временно останавливает и восстанавливает сервисы демо-окружения.
Отчёт: `reports/e2e-results.json`.

Логи и остановка:

```sh
docker compose logs -f api consumer
docker compose --profile demo down
```
