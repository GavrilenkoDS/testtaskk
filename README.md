# Асинхронный сервис процессинга платежей

Тестовое задание: FastAPI + Pydantic v2, асинхронный SQLAlchemy 2.0,
PostgreSQL, RabbitMQ/FastStream, Alembic и Docker Compose.

## Быстрый запуск

Нужен Docker Desktop с Linux containers или Docker Engine с Compose v2.
Из каталога проекта:

```sh
cp .env.example .env
docker compose --profile demo up -d --build
docker compose ps
```

Миграции применяются автоматически отдельным сервисом `migrate`. API доступен
на `http://localhost:8000` при настройках из примера. API-ключ (не короче 16 символов),
пароли, адреса подключений, порты и таймауты задаются в `.env`.

В PowerShell вместо `cp` можно использовать `Copy-Item .env.example .env`.
Настройки Compose для адресов БД и брокера указывают на внутреннюю сеть контейнеров.

В репозитории хранится `.env.example` с демонстрационными значениями.
Рабочие `.env` и `.env.*` исключены из Git и Docker build context.
Перед запуском скопируйте пример в `.env` и при
необходимости измените настройки. Python-код содержит только схему конфигурации
и валидацию; значений ключей, паролей и URL подключений в нём нет.
При отсутствии обязательной настройки приложение завершится с ошибкой валидации.
Переменные окружения имеют приоритет над `.env`.

Основные группы настроек:

| Переменные | Назначение |
| --- | --- |
| `API_KEY` | Ключ доступа к API |
| `POSTGRES_*`, `RABBITMQ_DEFAULT_*` | Учётные данные инфраструктуры |
| `DATABASE_URL`, `RABBITMQ_URL` | Подключения приложения; в примере собираются из переменных выше |
| `RETRY_BASE_SECONDS`, `OUTBOX_POLL_SECONDS` | Задержка повторов и опрос outbox |
| `WEBHOOK_TIMEOUT_SECONDS`, `PUBLISH_TIMEOUT_SECONDS` | Таймауты внешних запросов |
| `DATABASE_REQUEUE_DELAY_SECONDS` | Пауза при недоступности БД |
| `API_BIND_HOST`, `API_PORT`, `WEBHOOK_PORT`, `RABBITMQ_MANAGEMENT_PORT` | Публикуемые адрес и порты Docker |
| `TEST_DATABASE_URL`, `TEST_RABBITMQ_URL` | Подключения интеграционных тестов |
| `SMOKE_*` | Адреса сквозной проверки, включая callback внутри Docker |

Названия событий, маршруты API, три попытки и параметры эмуляции из ТЗ являются
частью контракта и алгоритма сервиса. Они остаются в коде.

### Создать платёж (PowerShell)

```powershell
$apiKey = (Select-String -Path .env -Pattern '^API_KEY=').Line.Split('=', 2)[1]
$headers = @{
  'X-API-Key' = $apiKey
  'Idempotency-Key' = 'order-1001'
}
$body = @{
  amount = '1250.50'
  currency = 'RUB'
  description = 'Order 1001'
  metadata = @{ order_id = 1001 }
  webhook_url = 'http://webhook:8080/webhook'
} | ConvertTo-Json -Depth 5
$payment = Invoke-RestMethod -Method Post `
  -Uri 'http://localhost:8000/api/v1/payments' `
  -Headers $headers -ContentType 'application/json' -Body $body
$payment
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "c86a168b-30df-40b5-b25c-605b9c175b63",
  "status": "pending",
  "created_at": "2026-10-28T12:00:00Z"
}
```

### Получить результат

```powershell
Invoke-RestMethod `
  -Uri "http://localhost:8000/api/v1/payments/$($payment.payment_id)" `
  -Headers @{ 'X-API-Key' = $apiKey }
Invoke-RestMethod 'http://localhost:8080/events'
```

Обработка занимает 2-5 секунд, плюс время публикации и HTTP-запроса.
С вероятностью 90% статус становится `succeeded`, с вероятностью 10% - `failed`.
Webhook отправляется в обоих случаях. Сумма в API и webhook сериализуется строкой,
чтобы не терять точность decimal.

### Пример curl (bash)

```sh
# Загрузить настройки из собственного .env (bash).
set -a
. ./.env
set +a
curl -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: order-curl-1001' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"1250.50","currency":"RUB","description":"Order 1001","metadata":{"order_id":1001},"webhook_url":"http://webhook:8080/webhook"}'
```

`localhost` внутри consumer означает сам контейнер. Для получателя на хосте
Docker Desktop используйте `http://host.docker.internal:PORT/path`.

## API

На всех эндпоинтах, включая `/health` и `/openapi.json`, обязателен `X-API-Key`.
OpenAPI JSON доступен с этим заголовком; публичный Swagger UI отключён.

| Метод и путь | Назначение |
| --- | --- |
| `POST /api/v1/payments` | Создание, обязательный `Idempotency-Key`, ответ 202 |
| `GET /api/v1/payments/{payment_id}` | Все данные платежа и состояние доставки webhook |
| `GET /health` | Проверка доступности API и PostgreSQL |
| `GET /openapi.json` | Схема API |

Обязательные поля: `amount`, `currency`, `description`, `webhook_url`.
`metadata` по умолчанию `{}`. Сумма положительная, до 16 цифр до точки и 2 после;
валюта RUB/USD/EUR; описание от 1 до 1000 символов; URL HTTP(S) без credentials.
Ключ идемпотентности от 1 до 255 символов, не из одних пробелов.
Описание, строки и ключи во вложенных метаданных не должны содержать NUL (`\u0000`)
или одиночные Unicode-суррогаты; такой запрос получает `422`. Обычный Unicode,
включая кириллицу и emoji, поддерживается.

Повтор с тем же ключом и теми же данными возвращает тот же платёж и текущий статус.
Числа `100.5` и `100.50`, а также перестановка ключей JSON считаются одинаковыми.
Тот же ключ с другими данными даёт `409`. Неизвестный UUID даёт `404`, некорректные
данные `422`, неверный или отсутствующий API-ключ `401`.

## Как устроена доставка

```text
POST /payments
  -> PostgreSQL: payments + outbox (одна транзакция)
  -> relay внутри consumer: publish + publisher confirm
  -> exchange payments -> durable queue payments.new
  -> один обработчик FastStream
     -> эмуляция шлюза -> COMMIT статуса
     -> webhook -> COMMIT результата доставки
        -> ошибка: outbox с available_at -> повтор в payments.new
        -> третья ошибка: outbox -> payments.dlx -> payments.dlq
```

- Уникальный индекс `idempotency_key` и `INSERT ON CONFLICT DO NOTHING` защищают
  от конкурентных POST. Платёж и первое событие коммитятся вместе.
- Relay выбирает готовую строку через `FOR UPDATE SKIP LOCKED`, публикует
  persistent-сообщение с `mandatory` и publisher confirms, затем ставит
  `published_at`. При ошибке транзакция откатывается, публикация повторяется.
  Даже если consumer выключен, API может принимать платежи в outbox.
- Один consumer обрабатывает очередь и запускает фоновую задачу relay в том же
  процессе. Отдельного пятого обязательного сервиса для outbox не требуется.
- Блокировка строки платежа защищает от одновременной обработки дублей.
  Уже сохранённый результат шлюза не пересчитывается при ошибках webhook.
- Три попытки означают **первую отправку и два повтора**. По умолчанию задержки
  2 и 4 секунды; они хранятся в PostgreSQL как `available_at` и переживают рестарт.
  Во время ожидания очередь не блокируется. Повтор и счётчик попыток записываются
  одной транзакцией, только после этого исходное сообщение подтверждается.
- После третьей ошибки создаётся событие `payments.dead`. Его доставка в DLQ
  также проходит через outbox. Некорректные события и неизвестные платежи
  отклоняются без requeue и попадают в DLQ через DLX основной очереди.
- Неожиданные ошибки обработчика также сохраняют повтор через outbox и после
  третьей попытки ведут в DLQ. Если PostgreSQL недоступен и записать повтор нельзя,
  исходное сообщение возвращается в очередь без ACK до восстановления БД.
- Отказ шлюза - конечный бизнес-результат `failed`, а не повод повторно списывать
  деньги. Уведомление о таком результате проходит те же три попытки доставки.

Доставка имеет семантику **at least once**. Сбой после подтверждения RabbitMQ,
но до коммита outbox может дать дубль; обработчик проверяет состояние в БД.
Сбой после успешного webhook, но до коммита тоже может повторить HTTP-запрос.
Получатель должен дедуплицировать по постоянному `event_id` или заголовкам
`Idempotency-Key` / `X-Event-ID`: `payment:<payment_id>:result`.

Блокировка платежа удерживается во время внешнего запроса. Для небольшого
тестового сервиса с одним consumer это простое решение; в промышленном сервисе
понадобятся lease/claim и идемпотентный реальный шлюз по `payment_id`.
Эмуляция не выполняет реальных списаний. Для публичного сервиса также нужны
ограничения исходящего доступа к webhook и управление секретами.

### Формат webhook

```json
{
  "event_id": "payment:c86a168b-30df-40b5-b25c-605b9c175b63:result",
  "payment_id": "c86a168b-30df-40b5-b25c-605b9c175b63",
  "status": "succeeded",
  "amount": "1250.50",
  "currency": "RUB",
  "metadata": {"order_id": 1001},
  "processed_at": "2026-10-28T12:00:03Z"
}
```

Успехом считается HTTP 2xx. `WEBHOOK_TIMEOUT_SECONDS` (по умолчанию 10 секунд)
ограничивает полную длительность каждой попытки, включая чтение тела ответа,
и ожидание отдельных сетевых операций. Превышение времени запускает обычные
повторы с экспоненциальной задержкой и доставку в DLQ после третьей ошибки.
Редиректы не следуются.
`webhook_sent_at`, `attempts_completed` и `last_error` доступны в GET платежа.
Отказ webhook не меняет `succeeded` на `failed`.

## Проверка retry и DLQ вручную

Создайте новый платёж с новым ключом и `webhook_url=http://webhook:8080/fail`.
Демо-получатель всегда отвечает 503. После обработки шлюзом и трёх отправок
в GET будет `attempts_completed=3`, `webhook_sent_at=null`; событие появится в DLQ.

RabbitMQ Management при порте из примера: `http://localhost:15672`;
логин и пароль берутся из `RABBITMQ_DEFAULT_USER` / `RABBITMQ_DEFAULT_PASS` в `.env`.
В Queues выберите `payments.dlq`. Успешные уведомления демо-получателя можно
просмотреть по `http://localhost:8080/events`; они хранятся только в его памяти.

Для демонстрации outbox остановите consumer, создайте платёж и запустите consumer:

```sh
docker compose stop consumer
# Создайте платёж через API.
docker compose start consumer
```

Платёж останется в БД и будет обработан после запуска consumer.

## Тесты

Полный набор с настоящими PostgreSQL и RabbitMQ:

```sh
docker compose --profile test run --rm tests
```

Тесты создают отдельную БД `payments_test`, применяют Alembic-миграцию и запускают
`alembic check` для проверки соответствия моделей. Данные основной БД не удаляются.
Проверяются конкурентная идемпотентность, атомарный outbox, дедупликация consumer,
оба результата шлюза, три попытки и backoff, восстановление webhook, DLQ,
откат outbox при сбое брокера, конкурентные relay и реальная persistent-публикация.

Проверка всей запущенной цепочки, включая настоящие HTTP-запросы и событие в DLQ:

```sh
python -m scripts.smoke
```

Для неё нужен запущенный профиль `demo` и установленные зависимости проекта
(`pip install -e '.[dev]'`). Настройки читаются из `.env` через `SmokeSettings`.
Скрипт создаёт два тестовых платежа и оставляет событие в DLQ для просмотра.

Расширенный E2E-прогон:

```sh
python -m scripts.e2e
```

Он проверяет девять сценариев через работающие сервисы: авторизацию и валидацию,
платёж с webhook, десять конкурентных запросов с одним ключом, три попытки с
экспоненциальной задержкой и DLQ, остановку consumer, остановку RabbitMQ,
восстановление webhook, дубль сообщения и некорректный JSON.
Скрипт временно останавливает сервисы этого Compose-проекта и запускает их обратно
в блоках `finally`. Запускайте его на демонстрационном окружении, без параллельных
запросов. Результаты сохраняются в `reports/e2e-results.json`.
Каталог `reports/` исключён из Git. Результаты последнего подтверждённого прогона
описаны в [CHECKS.md](CHECKS.md).

Для локальных тестов Python без инфраструктуры:

```sh
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# PowerShell: .venv\Scripts\Activate.ps1
pip install -e '.[dev]'
# Сначала скопируйте .env.example в .env.
pytest -q
ruff check .
```

Без `TEST_DATABASE_URL` интеграционные проверки будут отмечены как skipped.
При локальном запуске интеграций имя тестовой БД обязано оканчиваться на `_test`.
Также задайте `TEST_RABBITMQ_URL` для теста настоящего брокера.

## Автоматические проверки

В `.github/workflows/ci.yml` подготовлен GitHub Actions workflow для push,
pull request и ручного запуска. Он устанавливает Python 3.12, проверяет Ruff,
собирает Docker-окружение, запускает unit- и интеграционные тесты, затем девять
E2E-сценариев. Используется `.env.example`; секреты репозитория для демо не нужны.
Итог E2E выводится в сводку задания, контейнеры останавливаются после завершения.

## Первый коммит и подключение remote

После инициализации локального репозитория проверьте состав первого коммита:

```sh
git status
git diff --cached --stat
git diff --cached --check
```

Создать первый коммит:

```sh
git commit -m "Initial implementation of async payment processing"
```

Создайте пустой удалённый репозиторий в выбранном Git-хостинге и подключите
его фактический URL:

```sh
git remote add origin <repository-url>
git push -u origin main
```

Замените `<repository-url>` адресом созданного репозитория. `.env`, окружение
Python, архивы, временные файлы и отчёты запусков не входят в индекс Git.

## Полезные команды

```sh
docker compose logs -f api consumer
docker compose run --rm migrate alembic check
docker compose --profile demo down
```

Volumes с данными сохраняются после `down`. Профиль demo опционален:
`docker compose up -d --build` запускает сам сервис без демо-получателя.

## Структура

```text
app/api.py           API и проверка X-API-Key
app/config.py        Схема и валидация настроек без значений секретов
.env.example         Пример конфигурации для копирования в .env
app/models.py        Модели payments и outbox
app/schemas.py       Pydantic, decimal, fingerprint запроса
app/processing.py    Шлюз, webhook, дедупликация, retry и DLQ
app/consumer.py      Единственный обработчик и жизненный цикл
app/outbox.py        Транзакционный relay
app/messaging.py     RabbitMQ exchanges, queues и bindings
migrations/         Alembic
tests/              Unit- и интеграционные тесты
examples/webhook.py  Демо-получатель
compose.yaml        Окружение, профили demo и test
.github/workflows/  CI: Ruff, интеграционные тесты и E2E
scripts/e2e.py       Повторяемый E2E-прогон
CHECKS.md            Результаты проверки
```
