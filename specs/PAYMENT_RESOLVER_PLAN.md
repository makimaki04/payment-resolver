# Payment Resolver — архитектура, инварианты и план реализации

## Context
Тестовое задание «Асинхронный сервис процессинга платежей» получено в PDF; локальный путь в среде подготовки — `/home/user/Загрузки/Telegram Desktop/5476ba97-2632-42f9-8460-a99a854fd97e.pdf`. На момент планирования проект `payment-resolver` содержал только `README.md` с заголовком `# Payment resolver`; все файлы ниже, кроме README, предстояло создать. Нужно полное ТЗ с проверкой сбоев, без дополнительных продуктовых функций; архитектура по форме AIS, но с независимым ядром.

Обязательный стек: FastAPI, Pydantic v2, SQLAlchemy 2.0 async, PostgreSQL, RabbitMQ через FastStream, Alembic, Docker Compose. Обязательны POST/GET платежа, статический X-API-Key, идемпотентность, Outbox, один consumer, эмуляция 2–5 секунд с 90% успеха/10% окончательного отказа, три попытки с экспоненциальной задержкой, DLQ, README с запуском и примерами.

Дополнительно к заданию выбран transactional Inbox: один AMQP subscriber сохраняет входящее событие и ACK-ает после commit; один исполнитель Inbox в том же процессе делает весь платёжный сценарий. Повторы исполняются из Inbox, не возвращаются в RabbitMQ. Это трактовка требования «один consumer»; отдельных consumer для оплаты и webhook не делать.

## Архитектура и инварианты

### Владение работой
1. API атомарно сохраняет Payment и исходный Outbox, затем возвращает 202.
2. Publisher публикует persistent-событие в `payments.new`; только положительный publisher confirm при успешном routing позволяет отметить Outbox опубликованным. Consumer ACK он не ждёт.
3. AMQP subscriber атомарно принимает событие в Inbox. Только после commit отправляет consumer ACK. Повтор после потерянного ACK находит прежнюю запись, не создаёт новую работу и не обнуляет счётчик.
4. После ACK обработкой, восстановлением и retry полностью владеет Inbox. В consumer-процессе параллельно работают приёмник, один Inbox-executor и Outbox-publisher; новых обязательных контейнеров нет.
5. После третьей неудачной/прерванной попытки одна транзакция переводит Inbox в `dead` и создаёт Outbox для DLQ. Исходному AMQP-сообщению уже нельзя сделать reject: оно было ACK-нуто при приёме.

### Семантика
- `Payment.status`: только `pending`, `succeeded`, `failed`. Переход только `pending → succeeded|failed`; терминальный статус и processed_at больше не меняются.
- 10% ошибок эмуляции — окончательный отказ: сохранить failed и отправить webhook. Это не причина для DLQ, если уведомление доставлено.
- Если результат оплаты уже сохранён, следующий запуск пропускает шлюз и продолжает webhook.
- Три попытки всего: начальная и два повтора всего сценария, не отдельные бюджеты каждого этапа. Счётчик увеличивается и коммитится ДО работы. Авария расходует начатый слот, даже если внешнее действие ещё не началось.
- Дубли AMQP и технические повторы Outbox не расходуют прикладной бюджет. Адаптеры не имеют скрытых HTTP-retry и не следуют редиректам.
- Финансовый исход без подтверждения остаётся pending, даже если Inbox.dead. Технический timeout/исчерпание попыток не означает финансовый failed. Если результат уже известен, проблемы webhook его не меняют.
- DLQ означает «завершение обработки не подтверждено», а не «получатель точно не получил webhook».
- Webhook: любой 2xx — успех; сеть/timeout и все не-2xx, включая 4xx и 3xx, — неуспешная попытка. Retry-After игнорируется. Стабильный event_id позволяет получателю дедуплицировать повтор после потерянного ответа; exactly-once delivery не обещать.
- Клиентский Idempotency-Key защищает наш POST. Ключ операции шлюза отдельно: `str(payment.id)`, одинаковый для всех попыток и после рестарта. Эмулятор моделирует стабильный исход по ключу, не настоящий банковский ledger.
- Вызовы шлюза и webhook вне транзакций PostgreSQL. Запись результатов требует действующего token/lease: просроченный исполнитель не может завершить или испортить новую попытку.
- Сохранность предполагает сохранность volumes PostgreSQL/RabbitMQ и их последующее восстановление. Один узел RabbitMQ не даёт HA при потере диска/узла.

### Слои
Сохранить форму AIS: `core/` — независимые модели/DTO/инварианты/порты; `modules/` — application orchestration, реализации и композиция; `api/` — HTTP. `core` не импортирует modules/api, SQLAlchemy, FastAPI, FastStream или HTTPX; application logic импортирует только core/stdlib, не конкретные адаптеры. Pydantic v2 допустим в DTO ядра. Доменные сущности — dataclasses, ORM-модели отдельно; преобразования явные, без универсального mapper/CRUD-фреймворка.

Ориентир — `/home/user/dev/dev.dc.ai/sources/ais/ais.srv.ai/.agents/rules/00-common-architecture.md:25-46` (локальный путь в среде подготовки). В реальном `ais/core/payments/models.py` Payment наследуется от MongoBaseModel и price имеет тип float — это НЕ копировать. Не переносить bcf, Mongo, taskiq, глобальные коннекты при импорте, HTTPException в ядре или тестовые sys.modules-хаки. `__init__.py` не открывают ресурсы и не регистрируют модели глобально.

## Approach
Шаги 1–3 создают общую основу. После шага 3 HTTP (шаг 4) и брокерный приём (шаг 5) независимы. Исполнитель (шаг 6) использует их контракты; сборка и сквозная проверка следуют после него. Эквивалентного кода на момент планирования в проекте не было: вся реализация создавалась с нуля, AIS задаёт только соглашение о слоях.

### 1. Зафиксировать окружение и интерфейсы
Создать `pyproject.toml`, `.python-version` со значением `3.12`, uv.lock и пакет `payment_resolver/`. Использовать hatchling, пакет в корне репозитория. Runtime-зависимости: `fastapi>=0.115,<1`, `pydantic>=2,<3`, `pydantic-settings>=2,<3`, `sqlalchemy[asyncio]>=2.0,<2.1`, `asyncpg>=0.30,<1`, `faststream[rabbit]==0.7.7`, `httpx==0.28.1`, `alembic>=1.14,<2`, `uvicorn[standard]>=0.34,<1`. Dev: `pytest>=8,<10`, `pytest-asyncio>=1,<2`; `asyncio_mode=auto`. Разрешить версии uv под Python 3.12 и зафиксировать полученный lock; далее запускать с frozen/locked.

Роли стека зафиксированы: FastAPI + Uvicorn — HTTP API; Pydantic v2 — входные/выходные DTO, брокерные события и webhook payload; pydantic-settings v2 — типизированная конфигурация из environment и `.env`; SQLAlchemy 2.0 async + asyncpg — доступ к PostgreSQL 17; Alembic — миграции; FastStream 0.7.7 — RabbitMQ; HTTPX — исходящие webhook; Docker Compose — запуск; uv + uv.lock — зависимости; pytest + pytest-asyncio — проверки. Pydantic-валидация границ не заменяет доменные инварианты: сущности остаются dataclasses, ORM-модели отдельные. Своего dotenv-парсера, прямого чтения os.getenv в бизнес-коде и второго конфигурационного механизма не добавлять.

Новые файлы:
- `core/payments/models.py`: frozen dataclass Payment с финансовыми полями, idempotency_key, webhook_url и request_fingerprint; метод `finalize(status: PaymentStatus, processed_at: datetime) -> Payment` допускает только переход из pending в терминальный исход. Повтор того же сохранённого результата возвращает прежний объект; смена терминального исхода — доменная ошибка.
- `core/payments/enums.py`: `Currency(StrEnum)` = RUB/USD/EUR и `PaymentStatus(StrEnum)` = pending/succeeded/failed.
- `core/payments/dtos.py`: `CreatePaymentRequestDTO`, `CreatePaymentResponseDTO`, `PaymentResponseDTO`, `PaymentCreatedEventDTO`, `PaymentResultWebhookDTO`; Pydantic extra=forbid, конечные JSON-числа в metadata.
- `core/payments/exceptions.py`: `PaymentNotFound`, `IdempotencyConflict`, `InvalidIncomingEvent`, `StorageUnavailable`, `GatewayTemporaryError`, `WebhookDeliveryError`. Последняя несёт безопасный error_code и optional http_status, не HTTPException/HTTPX response.
- `core/processing/models.py`: InboxEntry, OutboxEntry, ProcessingClaim; `InboxStatus` = ready/processing/retry_wait/completed/dead; `OutboxKind` = payment.created/payment.processing_failed.
- `core/processing/retry.py`: `retry_delay(attempts_started: int, jitter_factor: float, base_seconds: float) -> timedelta`; после неудачной попытки n=1/2: `base_seconds * 2**(n-1) * jitter_factor`, jitter_factor равномерно [0.8,1.2]. После n=3 новый retry не создаётся.
- `core/ports.py`: Protocol-интерфейсы ниже. Async SQLAlchemy-типы не выходят из адаптеров.

Контракты портов (имена и семантика фиксированы):
- `PaymentRepository.create_or_get(candidate: Payment) -> tuple[Payment, bool]`, `get(payment_id: UUID) -> Payment | None`, `finalize(payment_id: UUID, status: PaymentStatus, processed_at: datetime) -> Payment` — async; finalize не перезаписывает терминальный результат.
- `InboxRepository.receive(event: PaymentCreatedEventDTO, now: datetime) -> None`, `lock_next_due(now: datetime) -> InboxEntry | None`, `lock_next_expired(now: datetime) -> InboxEntry | None`, `lock(event_id: UUID) -> InboxEntry | None`, `save(entry: InboxEntry) -> None` — async. Методы lock используют FOR UPDATE, выбор следующей записи — SKIP LOCKED. receive идемпотентен, но конфликт event_id/payment_id не проглатывает.
- `OutboxRepository.add(entry: OutboxEntry) -> None`, `lock_next_due(now: datetime) -> OutboxEntry | None`, `save(entry: OutboxEntry) -> None` — async.
- `UnitOfWork`: async context manager, свойства payments/inbox/outbox, `now() -> datetime` и `commit() -> None` async; выход без commit откатывает. `UnitOfWorkFactory.__call__() -> UnitOfWork`. now использует PostgreSQL clock_timestamp(); timestamps UTC-aware.
- `Gateway.charge(*, payment_id: UUID, amount: Decimal, currency: Currency, description: str, idempotency_key: str) -> PaymentStatus` — async, нормальный результат только succeeded/failed, техническая ошибка исключением. Ключ всегда str(payment_id).
- `WebhookSender.send(url: str, event: PaymentResultWebhookDTO) -> None` — async, только 2xx возвращает None, остальное WebhookDeliveryError. Один вызов — максимум один HTTP-запрос.
- `EventPublisher.publish(entry: OutboxEntry) -> None` — async, возвращает только после положительного broker confirm и успешного routing; неуспех исключением.

### 2. Определить HTTP-контракты и нормализацию
`POST /api/v1/payments`: обязательный Idempotency-Key длиной 1–255 символов, не whitespace-only; ключ непрозрачный, регистр/содержимое не нормализовать. Все пять полей body обязательны: amount, currency, description, metadata, webhook_url. Пустое описание и `{}` разрешены; null не заменяет их.
- amount только строка обычной десятичной записи: регулярное выражение `^(?:0|[1-9][0-9]{0,15})(?:\.[0-9]{1,2})?$`, затем Decimal > 0. Диапазон NUMERIC(18,2), без экспоненты, float, знака плюс, ведущих нулей и округления. Внутри привести к scale=2 без изменения значения; в JSON выводить ровно два знака: `"100.00"`.
- currency строго RUB/USD/EUR; description строка; metadata объект `dict[str, JsonValue]`, не массив/скаляр. NaN/Infinity отвергать.
- webhook_url валидный абсолютный HTTP/HTTPS URL без userinfo и fragment. Использовать Pydantic AnyHttpUrl для нормализации. Разрешение — точное совпадение нормализованного origin `(scheme, host, effective_port)` с WEBHOOK_ALLOWED_ORIGINS, без wildcard/subdomain matching. Проверять при создании и перед отправкой. URL целиком, включая path/query, сохраняется; редиректы запрещены.
- Фingerprint: SHA-256 от JSON всех пяти нормализованных полей с sorted keys, компактными separators, ensure_ascii=False, allow_nan=False; amount — двухзначная строка, URL — нормализованная строка. Порядок ключей metadata не важен; значения/типы JSON сравниваются в этой канонической сериализации. Никакой fingerprint не включает API key/время/номер попытки.

Ответ POST всегда 202: `{payment_id, status, created_at}`. Повтор того же ключа с тем же fingerprint возвращает прежние payment_id/created_at и актуальный статус без новой работы; иной fingerprint — 409.
`GET /api/v1/payments/{payment_id}` возвращает `{payment_id, amount, currency, description, metadata, status, idempotency_key, webhook_url, created_at, processed_at}`; processed_at null при pending. Никаких processing/attempts/error/Inbox-полей и административных маршрутов.
Ошибки: отсутствующий/неверный X-API-Key — 401 `{"detail":"Invalid API key"}`; неизвестный корректный UUID — 404 `{"detail":"Payment not found"}`; конфликт ключа — 409 `{"detail":"Idempotency key conflicts with an existing payment"}`; неверные DTO/UUID/ключ/URL — 422; недоступная БД до успешного commit — 503 `{"detail":"Storage temporarily unavailable"}`. Проверять API key до бизнес-обработки.

Webhook POST JSON: `{event_id, payment_id, status, amount, currency, created_at, processed_at}`; только терминальные статусы. `event_id = uuid5(payment.id, "payment.result")`, одинаковый во всех попытках. Тело берётся из уже сохранённого Payment, а не пересоздаётся с новым временем. description/metadata, API key и клиентский Idempotency-Key не пересылать.

### 3. Создать атомарное хранение и миграцию трёх таблиц
`modules/database/models/` и `modules/database/repositories/`: в каждом пакете `base.py`, `payment.py`, `inbox.py`, `outbox.py` и явные экспорты в `__init__.py`; рядом `uow.py`. `alembic.ini`, `migrations/env.py`, одна начальная миграция. Асинхронный engine/sessionmaker, отдельная AsyncSession на UoW, expire_on_commit=False. Миграции используют тот же DATABASE_URL и target_metadata; приложение не вызывает create_all. В SQLAlchemy атрибут JSONB назвать `metadata_json`, колонку — `metadata`, потому что metadata зарезервировано DeclarativeBase.

| Таблица | Поля и ограничения |
|---|---|
| payments | id UUID PK; amount NUMERIC(18,2) NOT NULL CHECK >0; currency TEXT NOT NULL CHECK RUB/USD/EUR; description TEXT NOT NULL; metadata JSONB NOT NULL; status TEXT NOT NULL CHECK pending/succeeded/failed; idempotency_key VARCHAR(255) NOT NULL UNIQUE; request_fingerprint BYTEA NOT NULL длиной 32; webhook_url TEXT NOT NULL; created_at TIMESTAMPTZ NOT NULL; processed_at TIMESTAMPTZ NULL. CHECK: pending ⇔ processed_at IS NULL |
| inbox | event_id UUID PK; payment_id UUID NOT NULL UNIQUE FK payments.id RESTRICT; status TEXT NOT NULL CHECK ready/processing/retry_wait/completed/dead; attempts SMALLINT NOT NULL DEFAULT 0 CHECK 0..3; next_attempt_at TIMESTAMPTZ NULL; lock_token UUID NULL; locked_until TIMESTAMPTZ NULL; last_error_code TEXT NULL; last_http_status SMALLINT NULL CHECK 100..599; created_at/updated_at TIMESTAMPTZ NOT NULL |
| outbox | id UUID PK; payment_id UUID NOT NULL FK payments.id RESTRICT; kind TEXT NOT NULL CHECK payment.created/payment.processing_failed; payload JSONB NOT NULL; created_at TIMESTAMPTZ NOT NULL; published_at TIMESTAMPTZ NULL; publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK >=0; next_publish_at TIMESTAMPTZ NOT NULL; last_error_code TEXT NULL; UNIQUE(payment_id, kind) |

Inbox CHECK: processing требует attempts>=1, token и locked_until, next_attempt_at NULL; ready/retry_wait требуют next_attempt_at и отсутствие token/lease; completed/dead не имеют next_attempt_at/token/lease. ready требует attempts=0, retry_wait — attempts IN (1,2), completed — attempts IN (1,2,3), dead — attempts=3. created_at сохраняется, updated_at обновляется при переходе.
Индексы: inbox(next_attempt_at,event_id) WHERE status IN ('ready','retry_wait'); inbox(locked_until,event_id) WHERE status='processing'; outbox(next_publish_at,id) WHERE published_at IS NULL. Использовать READ COMMITTED и блокировки строк, не длинные транзакции вокруг внешней работы.

create_or_get: INSERT ... ON CONFLICT(idempotency_key) DO NOTHING RETURNING; если не вставлено — SELECT существующего платежа. Не делать предварительный SELECT как единственную защиту. В одной UoW application service сравнивает fingerprint и, только для новой строки, добавляет начальный Outbox. Конкурентные одинаковые POST создают ровно один payment и один initial outbox.

### 4. Реализовать создание/чтение и HTTP
`modules/payments/logic.py`: `PaymentService.create(dto: CreatePaymentRequestDTO, idempotency_key: str) -> Payment`, `get(payment_id: UUID) -> Payment`, оба async. UUID4 и время создаются до сохранения; время взять через uow.now(). После commit можно вернуть 202; RabbitMQ/consumer могут быть выключены, это не препятствует приёму платежа при живой БД.
Начальный Outbox: kind=payment.created, новый UUID4 `id`; payload `{"event_id": str(outbox.id), "payment_id": str(payment.id)}`; next_publish_at=created_at. API никогда не публикует напрямую.
`api/v1/payments.py`: роутер с префиксом `/payments`, явными DTO и переводом доменных ошибок в HTTP. Общий `/api/v1` задаётся при `app.include_router(...)`; внешние пути не меняются.
`api/security.py`: APIKeyHeader(auto_error=False), общий dependency для платёжных маршрутов.
Извлечь SecretStr.get_secret_value(); сравнивать UTF-8 bytes предоставленного заголовка
и настроенного ключа через secrets.compare_digest, не unrestricted str.
Отсутствующий/неравный ключ, в том числе некорректный non-ASCII заголовок, даёт 401, не исключение.
`app.py::create_app(settings: Settings | None = None) -> FastAPI` создаёт engine/lifecycle
и dependencies, не consumer. Swagger UI `/docs` и OpenAPI `/openapi.json` открыты для
ручных проверок; Authorize задаёт `X-API-Key` для платёжных операций. `redoc_url=None`.
Не добавлять health/admin endpoint; Docker readiness проверяет порт API после миграций.

### 5. Реализовать подтверждённую публикацию и durable Inbox handoff
`modules/messaging/broker.py`:
- FastStream 0.7.7 `RabbitBroker`, `Channel(publisher_confirms=True, on_return_raises=True, prefetch_count=1)`, AckPolicy.MANUAL. Custom decoder возвращает raw body bytes, чтобы JSON/Pydantic-валидация происходила внутри обработчика и не оставляла сообщение зависшим до входа в него.
- Durable direct exchange `payments`, binding `payments.new → payments.new`; durable direct exchange `payments.dlx`, binding `payments.dlq → payments.dlq`. Обе очереди durable quorum, не exclusive/auto-delete. Для рабочей очереди x-delivery-limit=-1: прикладной бюджет хранится в Inbox, broker-redelivery не должен незаметно удалить событие до handoff.
- Перед start consuming/publishing вызвать broker.connect(), broker.declare_exchange(), broker.declare_queue(); связать полученные RobustQueue через bind(exchange, routing_key=...). В FastStream 0.7.7 declare_queue/declare_exchange возвращают RobustQueue/RobustExchange. DLQ subscriber не создавать.
- `publish(entry)`: kind payment.created → exchange payments/key payments.new; payment.processing_failed → payments.dlx/payments.dlq. Весь `await broker.publish(payload, exchange=..., routing_key=..., persist=True, mandatory=True, message_id=str(entry.id), timeout=5.0)` обернуть в `asyncio.timeout(PUBLISH_TIMEOUT_SECONDS)` и проверить aiormq.spec.Basic.Ack. Внешний deadline нужен также для ожидания robust reconnect/разрешения exchange, не покрытых одним аргументом timeout. Timeout/DeliveryError/PublishError/потеря соединения не считаются успехом; внешнюю отмену не поглощать. Использовать явный publish, не publishing-декоратор, скрывающий confirm.
- Реализация порта — `RabbitEventPublisher`; один экземпляр разделяют Outbox-loop и receiver. Общий `_publish_confirmed(...)` последовательно выполняет публикации через asyncio.Lock; внешний deadline включает ожидание lock и весь broker.publish. Это не допускает одновременных публикаций одного message_id на общем канале драйвера. Метод `publish_invalid(body: bytes, original_message_id: str | None) -> None` формирует bounded diagnostic и использует этот же helper; stable transport ID — `uuid5(NAMESPACE_URL, "invalid-message:" + (original_message_id or "") + ":" + sha256(body).hexdigest())`. Для original_message_id брать raw AMQP property `message.raw_message.message_id`, не случайный fallback FastStream для отсутствующего заголовка.

`modules/messaging/workers.py::OutboxPublisher.publish_one() -> bool` async:
1. В UoW взять одну due unpublished строку FOR UPDATE SKIP LOCKED; нет строки → False.
2. Увеличить publish_attempts и выполнить bounded publish, удерживая только блокировку этой Outbox-строки. Это сознательное исключение: ограниченный 5 сек broker confirm внутри транзакции, не шлюз/webhook.
3. Confirm → published_at=uow.now(), очистить last_error_code, commit. Ожидаемая ошибка публикации → оставить published_at NULL, записать безопасный код и next_publish_at = now + min(30, 2**min(publish_attempts-1,5)) секунд, commit. Лимита трёх публикаций нет.
4. Сбой/отмена до commit оставляет запись доступной после rollback. Возможен дубль после потерянного confirm/commit; Inbox его поглощает. Published-строки не удалять и не переиспользовать для retry.

`modules/messaging/receiver.py::receive_payment(body: bytes, message: RabbitMessage) -> None` async:
- Проверить PaymentCreatedEventDTO и соответствие AMQP message_id event_id; в UoW проверить существование payment, вставить Inbox.ready с attempts=0 и next_attempt_at=now. На повторе того же event_id/payment_id не менять запись. Конфликт идентичностей считать InvalidIncomingEvent.
- Только после commit `await message.ack()`. Ошибка commit → не ACK; при StorageUnavailable подождать 1 сек и nack(requeue=True), чтобы не создавать горячий цикл. При потере канала сообщение возвращается брокером.
- Невалидный JSON/конверт, отсутствующий payment или конфликт идентичностей не запускать как оплату: передать диагностический конверт в payments.dlq с подтверждённой persistent mandatory-публикацией и тем же внешним deadline 5 сек, затем ACK источника. Поля: reason="invalid_message", original_message_id (может отсутствовать), body_base64 только первых 4096 входных bytes, original_body_bytes, body_truncated. Полное base64 большого входа не публиковать: оно может превысить лимит сообщения брокера и создать бесконечный requeue. При ошибке передачи не ACK, requeue с той же короткой задержкой. Это poison-вход до прикладной обработки; три попытки относятся к принятой работе Inbox, не к заведомо невалидному конверту.
- Неожиданная программная ошибка сообщает fatal supervisor и останавливает процесс, не подтверждая исходное сообщение. Нет default auto-ACK/reject на исключении.

### 6. Реализовать одного исполнителя Inbox и восстановление
`modules/payments/logic.py::ProcessingService` содержит async `claim_next() -> ProcessingClaim | None`, `process(claim: ProcessingClaim) -> None`, `recover_expired() -> bool`; приём Inbox можно выделить в `receive(event)` этого же service. `modules/messaging/workers.py` только вызывает эти методы последовательно.

Claim:
1. UoW: lock_next_due(now), status ready/retry_wait, next_attempt_at<=now, attempts<3.
2. Увеличить attempts; status=processing; новый lock_token UUID4; locked_until=now+30 сек; next_attempt_at=NULL; updated_at=now. Commit ДО вызовов. Вернуть snapshot с event_id/payment_id/attempt number/token/lease.
3. Один executor держит максимум один такой claim одновременно. При возможном втором процессе SKIP LOCKED и token защищают общие строки, но горизонтальное масштабирование не является отдельной функцией проекта.

Process (общий async deadline 20 сек; HTTP deadline 5 сек, lease 30 сек):
1. Прочитать Payment. Если pending — вызвать Gateway.charge с сохранёнными параметрами и ключом str(payment.id). Эмулятор `modules/gateway/emulator.py::SimulatedGateway` асинхронно ждёт random.uniform(2,5). Исход стабилен: int.from_bytes(sha256(idempotency_key.encode('ascii')).digest()[:8], 'big') % 10 == 0 → failed, иначе succeeded. Это распределение между новыми случайными payment_id, а не ровно 90 успешных из каждых 100 запросов. Не менять исход при retry/restart; никаких сетевых запросов, provider SDK или дополнительных таблиц шлюза.
2. Короткая UoW: lock Inbox, убедиться status=processing, token совпадает и locked_until>now. Если claim устарел — прекратить эту обработку, не писать результат. Иначе payments.finalize(...) и commit; processed_at записывается один раз. Эта проверка обязательна перед записью результатов внешней работы.
3. Если Payment уже terminal, шлюз не вызывать. Перед webhook ещё раз коротко проверить действующий claim; сформировать стабильный PaymentResultWebhookDTO из сохранённых данных.
4. `modules/webhooks/client.py::HttpWebhookSender`: один общий AsyncClient на процесс, TLS verify=True, trust_env=False, follow_redirects=False, транспорт без внутренних retry; connect timeout 2 сек, общий deadline запроса 5 сек через asyncio.timeout вокруг всей операции. Проверить allowlist. Использовать streaming response, считать успех по 2xx заголовкам и закрывать response, не загружая произвольное тело. Никакой подписи webhook/авторизации получателя сверх ТЗ.
5. Успех: короткая UoW lock Inbox и та же проверка token/lease; status=completed, убрать token/lease/next_attempt_at и last_error, commit. Если владение утрачено, не перезаписывать состояние нового исполнителя.
6. Неуспех: короткая UoW с проверкой владения. Код из набора gateway_error/webhook_timeout/webhook_network_error/webhook_http_error/webhook_url_not_allowed/processing_timeout/processing_error; http_status только если получен. Если attempts<3 → retry_wait, next_attempt_at=now+retry_delay(...), очистить token/lease. Если attempts=3 → dead и Outbox для DLQ в той же транзакции. Payment не менять на failed из-за технической ошибки.
7. При невозможности записать результат из-за БД не выполнять HTTP повторно в этом вызове; оставить processing для восстановления. CancelledError не превращать в успешное завершение/финансовый отказ, дать отмене пройти.

Recovery:
- Перед каждым поиском ready-работы executor обрабатывает одну expired processing строку: locked_until<=DB now, FOR UPDATE SKIP LOCKED. Сохранённый attempts НЕ уменьшать и номер прерванной попытки НЕ переиспользовать.
- error_code=processing_interrupted. Если attempts<3 — retry_wait с обычной задержкой; иначе dead + DLQ Outbox. Очистить token/lease; commit. Затем обычный claim получит новый номер/token.
- Все записи результатов проверяют token, status и ещё действующую lease под блокировкой строки: результат старого исполнителя после recovery не меняет Payment/Inbox. Уже ушедший внешний запрос отменить задним числом нельзя; это покрывается стабильным ключом операции/уведомления, а не обещанием отсутствия дублей.

DLQ Outbox: id=uuid5(inbox.event_id,"dead-letter"), kind=payment.processing_failed, payment_id прежний, next_publish_at=now. Payload `{event_id: original inbox event_id, payment_id, attempts: 3, payment_status, error_code, last_http_status, failed_at}`; UNIQUE(payment_id,kind) и терминальность Inbox исключают повторное создание логического dead letter. Transport-дубликат при потерянном confirm допустим, message_id стабилен.

### 7. Собрать lifecycle и Docker-окружение
`modules/config.py::Settings`: API_KEY (непустой ASCII SecretStr; неверная конфигурация отвергается при startup), DATABASE_URL, RABBITMQ_URL, WEBHOOK_ALLOWED_ORIGINS (непустой JSON-массив точных origins), RETRY_BASE_SECONDS=5 (>0). Остальные выбранные пределы — именованные константы: MAX_ATTEMPTS=3, PROCESSING_TIMEOUT_SECONDS=20, PROCESSING_LEASE_SECONDS=30, WEBHOOK_TIMEOUT_SECONDS=5, PUBLISH_TIMEOUT_SECONDS=5, POLL_INTERVAL_SECONDS=0.5. Не делать 90/10 или 2–5 сек незаметно настраиваемыми обходами ТЗ; в тестах внедрять контролируемые адаптеры.

`Settings` наследуется от `pydantic_settings.BaseSettings`; импортировать `SettingsConfigDict` из того же пакета. Поля Settings именовать как перечисленные uppercase env-переменные. Задать `model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=True, extra="ignore", hide_input_in_errors=True)`. extra=ignore нужен для общей `.env`, содержащей также bootstrap-переменные PostgreSQL/RabbitMQ; это исключение только для Settings, DTO сохраняют extra=forbid. Сохранять стандартный приоритет: явно переданные Settings kwargs > environment процесса > `.env` > defaults. `.env.example` автоматически не загружать. Отсутствие `.env` допустимо при всех обязательных значениях в environment; отсутствие или невалидность обязательного значения — отказ startup до открытия ресурсов. Создавать Settings в composition roots API/consumer/Alembic, передавать зависимостям явно, не создавать при импорте. `.env` разрешается относительно рабочего каталога: запускать из корня проекта, в контейнере использовать единый WORKDIR приложения. Полный Settings/model_dump и значения секретов не логировать.

`.env.example` содержит работоспособные локальные значения, не production-секреты: `API_KEY=local-development-key`, `POSTGRES_USER=payments`, `POSTGRES_DB=payments`, `POSTGRES_PASSWORD=payments-dev-password`, `RABBITMQ_DEFAULT_USER=payments`, `RABBITMQ_DEFAULT_PASS=payments-dev-password`, `DATABASE_URL=postgresql+asyncpg://payments:payments-dev-password@postgres:5432/payments`, `RABBITMQ_URL=amqp://payments:payments-dev-password@rabbitmq:5672/`, `WEBHOOK_ALLOWED_ORIGINS=["http://webhook:8081"]`. Compose передаёт PostgreSQL/RabbitMQ соответствующие bootstrap-переменные. Отсутствующие обязательные настройки/пустой allowlist — явная ошибка startup, никаких insecure fallback. Все origins нормализуются тем же кодом, что URL запроса; доверие к администраторскому allowlist и DNS документировать, не объявлять это полной сетевой sandbox.

В Git включить `.env.example`, но не рабочую `.env` и её локальные варианты. В `.gitignore` задать отдельные строки `.env`, `.env.*`, `!.env.example`; в `.dockerignore` — `.env` и `.env.*`, чтобы локальные секреты не попадали в build context/image. Для api/consumer в Compose задать `env_file: .env`: приложение получает конфигурацию через environment, копирование/монтирование `.env` в образ не требуется. Локальный запуск подготавливается копированием `.env.example` в отсутствующую `.env`; существующую пользовательскую `.env` не перезаписывать. `.env.example` содержит только приведённые демонстрационные значения и `RETRY_BASE_SECONDS=5`, а не реальные токены или пароли.

Composition root consumer и управление его ресурсами — `modules/processing/runtime.py`; `consumer.py` остаётся тонким CLI для прежней команды запуска. Автоматический `alembic upgrade head` при старте API сохраняется. Lifespan использует `@asynccontextmanager` с `AsyncGenerator[None, None]`.

Quality и observability:
- `APP_ENV`, `LOG_LEVEL`, `LOG_FORMAT`: dev text / production JSON, INFO по
  умолчанию; stdlib logging с ограниченным безопасным контекстом и без секретов.
- INFO только для lifecycle и бизнес-переходов после commit, WARNING для retry
  и первого сбоя инфраструктуры, ERROR для dead/unexpected; никаких polling/access INFO.
- Локальные pre-commit hooks: Ruff format/lint, strict mypy приложения и миграций,
  unit-тесты. Реальные PostgreSQL/RabbitMQ интеграции — отдельно и в CI.
- GitHub Actions: frozen зависимости, lint/format/types/unit, сборка и запуск
  runtime/test контейнеров, readiness и интеграция. Обоснование логирования —
  `specs/LOGGING_RESEARCH.md`, команды и эксплуатационные границы — README.

`modules/processing/runtime.py`: composition root, `build_consumer(settings: Settings)`, `async run_consumer(settings: Settings) -> None`; тонкий `consumer.py` сохраняет запуск `python -m payment_resolver.consumer`. Через FastStream RabbitBroker подключиться, объявить topology, запустить subscriber, затем Outbox-loop и Inbox-loop как asyncio.Task. Supervisor через `asyncio.wait(..., FIRST_COMPLETED)` следит за stop event, задачами и fatal future. Между пустыми выборками ждать 0.5 сек; ожидаемая недоступность БД даёт лог и паузу 5 сек, не новую прикладную попытку. Неожиданный сбой фоновой задачи/receiver приводит к ненулевому завершению всего процесса, а не молчаливой остановке одного loop.
SIGTERM/SIGINT: установить stop event, не брать новые задания; дать текущим Outbox/Inbox действиям завершиться в пределах 25 сек, затем отменить оставшиеся; после остановки loop закрыть broker и дождаться приёмников, затем HTTP client и engine. При аварии незавершённый Inbox будет восстановлен по lease. Engine/client создаются и закрываются в lifecycle, не при импорте core/modules.

`Dockerfile`: Python 3.12-slim, uv 0.11.25, установка из frozen lock; runtime stage без dev-зависимостей; test stage добавляет dev-зависимости и tests. `docker-compose.yaml`: обязательные postgres (`postgres:17`), rabbitmq (`rabbitmq:4.3.6-management`), api, consumer, persistent volumes для postgres/rabbitmq. API ждёт healthy PostgreSQL, выполняет `uv run --no-sync alembic upgrade head`, затем `uv run --no-sync uvicorn payment_resolver.app:create_app --factory --host 0.0.0.0 --port 8000`. Его readiness — TCP 8000 после запуска сервера, consumer зависит от ready API (миграция завершена), PostgreSQL и RabbitMQ. Consumer command `uv run --no-sync python -m payment_resolver.consumer`, restart=unless-stopped, stop_grace_period=35s. API не зависит от доступности брокера для обработки POST.

Дополнительно только профиль `test`: `webhook` на 8081 и одноразовый `tests`, из test stage. Это проверочная инфраструктура, не дополнительные production-workers/API. `tests/support/webhook_receiver.py` — реальный HTTP-получатель: POST `/ok` → 204, `/fail` → 503, `/flaky` → первые две попытки каждого event_id 503, третья 204, `/timeout` сохраняет receipt и задерживает ответ на 6 сек, `/redirect` → 307 на `/redirect-target`; записывает receipts, GET `/receipts` возвращает их. `/redirect-target` записывает факт обращения для проверки запрета redirect. Изолировать записи по event_id; тестовый приёмник не делает финансовых действий. Порт 8081 при публикации на хост привязать к 127.0.0.1; как и 8000/15672/5432/5672 локального compose.

В `tests.environment` явно задать `TEST_POSTGRES_ADMIN_DSN=postgresql://payments:payments-dev-password@postgres:5432/postgres`, `TEST_RABBITMQ_MANAGEMENT_URL=http://rabbitmq:15672`, `TEST_RABBITMQ_USERNAME=payments`, `TEST_RABBITMQ_PASSWORD=payments-dev-password` (значения паролей связать с теми же Compose env, не дублировать независимо). Только в локальной среде provisioning использует bootstrap-роли: официальный postgres создаёт POSTGRES_USER как superuser; RabbitMQ default_user_tags.administrator=true даёт default user права управления vhosts. Fixtures сначала проверяют CREATE DATABASE и management-доступ/administrator tag, при нехватке прав завершаются с точной ошибкой, не меняют чужие роли. Дождаться healthy PostgreSQL и HTTP 200 RabbitMQ management (poll до 30 сек), а не только открытого AMQP-порта. Существующие volumes не пересоздавать ради смены bootstrap-учётных данных.

## Verification
Команды выполняются из корня репозитория; в среде подготовки это `/home/user/dev/smth/payment-resolver`. На этапе планирования приложение не запускалось: кода ещё не было. Проверялись только uv 0.11.25, системный Python 3.10.12, Docker Engine и Docker Compose v2.39.4. Для реализации нужен Python 3.12 (uv или контейнер), доступ к пакетным индексам и Docker images. Перед запуском создать `.env` из `.env.example`; существующую пользовательскую `.env` не перезаписывать. Результаты предыдущего прогона — в [TESTING_RESEARCH.md](TESTING_RESEARCH.md#зафиксированный-прогон).

### Поведенческие тесты
Создать `tests/unit/` и `tests/integration/`. Проверять наблюдаемые переходы/ошибки, не структуру исходников, wiring или копии значений. Контролируемые gateway/clock/jitter использовать только там, где надо детерминированно воспроизвести исход/границу; основную интеграцию выполнять с настоящими PostgreSQL и RabbitMQ, не SQLite/TestRabbitBroker вместо них.
`tests/integration/conftest.py` создаёт собственную БД `test_<uuidhex>` через TEST_POSTGRES_ADMIN_DSN и прогоняет Alembic, собственный RabbitMQ vhost `test_<uuidhex>` через TEST_RABBITMQ_MANAGEMENT_URL и задаёт permissions для тестового пользователя. Application Settings указывают исключительно на созданные БД/vhost. Закрыть test app lifespans, consumer/broker connections и engines до удаления только своих ресурсов. Для HTTPX ASGITransport явно войти в `app.router.lifespan_context(app)` — transport сам не запускает lifecycle. Не чистить общую payments БД/default vhost и не выполнять docker down -v. Runtime factory принимает Settings, без глобального monkeypatch импорта.

Для конфигурации добавить изолированные проверки с временным dotenv-файлом и контролируемым environment: значение RETRY_BASE_SECONDS из environment перекрывает значение файла, явный аргумент Settings перекрывает оба; отсутствие обязательного API_KEY и пустой WEBHOOK_ALLOWED_ORIGINS вызывают ValidationError; bootstrap-поля POSTGRES_USER/RABBITMQ_DEFAULT_USER в общей `.env` не мешают загрузке. Не читать пользовательскую `.env` в этих тестах. При runtime smoke штатный запуск из `.env.example → .env` должен пройти валидацию и обслужить описанный ниже POST; без обязательного API_KEY процесс должен завершиться с ошибкой конфигурации до приёма HTTP/AMQP-заданий.

Обязательные проверки:
1. POST/GET/X-API-Key: 401 без/с неверным ключом, включая неверный non-ASCII заголовок, отправленный raw bytes, без TypeError; успешный 202 после commit; 404/422; amount число/нулевая/отрицательная/слишком точная/выходящая за NUMERIC сумма отвергается, точная строка проходит и возвращается с двумя знаками.
2. Конкурентные одинаковые POST (не менее 20) → один payment и один initial outbox, одинаковый payment_id. Конкурентные разные тела с одним ключом → один победитель, остальные 409. После terminal-перехода повтор POST возвращает актуальный статус, не создаёт новую работу. Порядок ключей metadata и формат допустимой суммы не создают ложный конфликт.
3. Broker down при POST → 202 и неопубликованный Outbox; после восстановления → Inbox и конечная обработка. Не подтверждённая/unroutable публикация не помечается published.
4. Падение после broker confirm до Outbox commit → повтор с тем же message_id, один Inbox. Падение после Inbox commit до consumer ACK → redelivery не меняет attempts/status и не создаёт новую работу.
5. Gateway succeeded и declined: корректный терминальный статус/processed_at; оба вызывают webhook. Declined + успешный webhook → Inbox.completed без DLQ.
6. Webhook 503,503,204 → три начатые попытки, один финансовый результат, один вызов шлюза после его сохранения, одинаковые event_id/body уведомлений, completed без DLQ. До next_attempt_at работа не выбирается; после — выбирается, задержка соответствует экспоненциальной базе и диапазону jitter.
7. Webhook постоянно 400 или 503/timeout → максимум три начатые попытки, Inbox.dead и DLQ с attempts=3; Payment остаётся сохранённым terminal. Retry-After не меняет наше расписание.
8. Прерванная начатая попытка расходует слот; новый executor получает новый token/номер. При попытках=3 recovery создаёт DLQ, не четвёртый вызов. До финансового результата статус остаётся pending, processed_at NULL; технический сбой не порождает failed webhook.
9. Просроченный исполнитель возвращается после нового claim: его успех/ошибка не перезаписывает Payment/Inbox нового владельца. Искусственно задержать результат адаптера и управлять lease в изолированной БД, не проверять это случайными sleep.
10. Ответ потерян после принятия webhook → возможен дубль, но event_id/body одинаковы и повтор оплаты отсутствует. Стабильный ключ Gateway передаётся повторно после прерывания до сохранения результата; не полагаться на автоматический SDK key.
11. Запрещённый origin отклоняется; userinfo/fragment запрещены; 307 не вызывает `/redirect-target`; API key не попадает в webhook. Невалидный брокерный конверт попадает в DLQ без финансового действия. В том числе malformed body 13 MiB, принимаемый стандартным RabbitMQ 4.3, не застревает из-за base64-расширения: diagnostic body ограничен, original_body_bytes/body_truncated точны.
12. Исчерпание Inbox и создание DLQ Outbox атомарны; недоступность брокера в этот момент не теряет dead letter, публикация происходит после восстановления. Published/ACK неопределённость может дать транспортный дубль с тем же ID, но не новую логическую работу.

Команды после реализации:
```bash
uv sync --python 3.12 --frozen
docker compose --profile test up -d --build postgres rabbitmq webhook api consumer
docker compose --profile test run --rm tests uv run --frozen pytest tests/unit tests/integration -q
```

### Отдельный runtime smoke, не только тесты
На реально запущенных сервисах, с `.env.example` API key и разрешённым origin, выполнить:
```bash
curl -i http://localhost:8000/api/v1/payments/00000000-0000-0000-0000-000000000000
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: local-development-key' -H 'Idempotency-Key: smoke-flaky-1' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"100.25","currency":"RUB","description":"smoke","metadata":{},"webhook_url":"http://webhook:8081/flaky"}'
curl -s http://localhost:8081/receipts
docker compose exec -T postgres psql -U payments -d payments -c "SELECT p.id,p.status,i.status AS processing,i.attempts FROM payments p JOIN inbox i ON i.payment_id=p.id WHERE p.idempotency_key='smoke-flaky-1';"
docker compose exec -T rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```
Первый запрос → 401. POST → 202, сохранить фактический payment_id из ответа. Затем poll SQL для ЭТОГО платежа до `Inbox.status=completed AND attempts=3` с deadline 60 сек; терминальный статус Payment сам по себе не означает завершения webhook. Только после этого выполнить GET с X-API-Key: terminal status, processed_at не null, amount="100.25"; `/receipts` для соответствующего event_id содержит ровно три обращения `/flaky` с одинаковым телом; DLQ для этого платежа отсутствует. Истечение deadline — провал smoke с сохранением наблюдавшегося состояния. Повторить тот же POST → тот же id и текущий статус; поменять amount при том же ключе → 409.

Отдельным новым ключом выполнить тот же POST с `/fail`: poll до `Inbox.status=dead AND attempts=3` И non-null published_at у соответствующего `payment.processing_failed` Outbox, deadline 60 сек. Затем проверить terminal финансовый статус и конкретный DLQ payload с payment_id/attempts=3 через RabbitMQ management, не только общий счётчик. Inbox.dead без публикации Outbox ещё не подтверждает доставку в DLQ. Если smoke-ключи уже есть, выбрать новый уникальный суффикс, не удалять чужие данные.
Затем остановить только consumer, создать платёж новым ключом, наблюдать durable pending Outbox; запустить consumer и наблюдать завершение. Отдельно остановить rabbitmq при работающем API, создать платёж, восстановить rabbitmq и наблюдать тот же payment_id в Inbox/обработке. Перезапустить consumer после приёма Inbox и подтвердить, что работа не потеряна после ACK. Сохранять фактические HTTP/SQL/очередные результаты; не объявлять приложение проверенным по одному успешному pytest.

## Assumptions & contingencies
- Зафиксированы: строгое ТЗ, независимый core, transactional Inbox с ранним ACK, общий бюджет трёх начатых попыток, все не-2xx как ошибки доставки, собственный jittered backoff без Retry-After, allowlist, минимальное уведомление, amount-строка и публичный GET без технических полей. Не заменять эти решения более привычной брокерной retry-схемой.
- pending после исчерпания до финансового результата допустим и согласован: он означает «окончательный результат не установлен», а не «активно повторяем». Нового unknown-статуса и административного replay API не добавлять.
- Поля description/metadata обязательны, но могут быть пустыми; metadata — объект; NUMERIC(18,2). HTTP-контракт описан в README и доступен через Swagger UI `/docs` с OpenAPI `/openapi.json`. Это зафиксированные минимальные уточнения недоопределённого HTTP-контракта.
- Локальный allowlist доверяет администратору и его DNS. Для теста явно разрешён HTTP origin webhook:8081; production-only HTTPS, HMAC-подпись, управление получателями и network sandbox не добавлять.
- Если при реализации рабочий каталог уже содержит пользовательские файлы, сначала прочитать их и адаптировать конкретные размещения без перезаписи чужого кода; сохранять перечисленные контракты и согласованную архитектуру. Если нельзя сохранить контракт без изменения пользовательского решения, остановить только соответствующее изменение и сообщить точный конфликт.
- Если Docker daemon/registry недоступен, завершить доступные локальные проверки и подготовку, явно зафиксировать недостающую runtime-проверку; не подменять PostgreSQL SQLite, RabbitMQ in-memory mock и не заявлять сквозную готовность.

## Источники для спорных гарантий
Поведение FastStream и выбор проверок разобраны в [исследовании тестирования](TESTING_RESEARCH.md), формат и безопасность логов — в [исследовании логирования](LOGGING_RESEARCH.md).
- FastStream 0.7.7: https://github.com/ag2ai/faststream/blob/0.7.7/faststream/rabbit/broker/broker.py — publish/declare_queue/declare_exchange; https://faststream.ag2.ai/latest/getting-started/acknowledgement/ — режимы ACK. Проверка Basic.Ack и on_return_raises обязательна, не копировать молча дефолты.
- RabbitMQ: https://www.rabbitmq.com/docs/confirms ; https://www.rabbitmq.com/docs/ttl ; https://www.rabbitmq.com/docs/quorum-queues . В выбранной Inbox-схеме нет TTL retry/DLX-переносов: DLQ публикуется явно с confirm, поэтому дефолтная at-most-once DLX-передача не является механизмом сохранности.
- Webhook retry: https://docs.stripe.com/webhooks/versioning (400 запускает retry); https://docs.adyen.com/development-resources/webhooks/troubleshoot ; https://yookassa.ru/developers/using-api/webhooks .
- Исходники sender: https://github.com/svix/svix-webhooks/blob/6e014fdea91b21cb87e35eabe854ec0a0381af53/server/svix-server/src/worker.rs ; https://github.com/frain-dev/convoy/blob/e1f3d41789ea436c4f7c25028ec14b285cbf03d6/worker/task/process_event_delivery.go . Их дополнительные внутренние retry, hosted-only функции и production-расписания не переносить.
- Внешняя оплата: https://docs.stripe.com/error-low-level.md ; https://docs.adyen.com/development-resources/api-idempotency/ ; https://yookassa.ru/developers/using-api/response-handling/http-codes#500 . SDK key lifetime: https://github.com/stripe/stripe-python/blob/89e8528a61d838600097ffe5c9615c9b7d35778a/stripe/_api_requestor.py#L584-L590 .
- Pydantic Settings: https://docs.pydantic.dev/latest/concepts/pydantic_settings/ — BaseSettings, SettingsConfigDict, dotenv, приоритет источников и extra=ignore для общей `.env`.
