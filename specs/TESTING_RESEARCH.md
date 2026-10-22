# Проверки асинхронного платёжного процессинга

Основная граница проверки: HTTP → настоящий PostgreSQL/Outbox → настоящий
RabbitMQ/Inbox → обработка → HTTP receipts или конкретный DLQ payload.
Контрольные адаптеры задают только воспроизводимый исход и момент сбоя.
Хранилище и брокер они не заменяют и ожидаемым результатом теста не являются.

## Что проверяем и почему

- **Разные подтверждения.** Publisher confirm не означает выполнение consumer.
  Persistent-сообщение в durable quorum queue подтверждается после принятия
  кворумом; после потерянного confirm возможен дубль. Проверяем commit Outbox
  отдельно от commit Inbox и ACK источника.
  [RabbitMQ confirms](https://www.rabbitmq.com/docs/confirms),
  [reliability](https://www.rabbitmq.com/docs/reliability),
  [quorum queues](https://www.rabbitmq.com/docs/quorum-queues).
- **Конкурентная идемпотентность.** READ COMMITTED даёт новый snapshot каждому
  statement. INSERT ON CONFLICT DO NOTHING ждёт конкурентную транзакцию;
  следующий SELECT видит её commit. Проверяем не менее 20 одновременных POST,
  единственный Payment/Outbox, один ID либо 409 для другого fingerprint.
  [PostgreSQL isolation](https://www.postgresql.org/docs/17/transaction-iso.html),
  [unique indexes](https://www.postgresql.org/docs/17/index-unique-checks.html).
- **Владение, а не случайные задержки.** Для lease/retry меняем расписание только
  в собственной тестовой БД; задержанный gateway синхронизируется Event.
  Проверяем поздний успех и ошибку старого token, расход прерванного слота и
  отсутствие четвёртой попытки. clock_timestamp() отражает текущее время, тогда
  как now() фиксирован началом транзакции.
  [PostgreSQL date/time](https://www.postgresql.org/docs/17/functions-datetime.html),
  [locking/SKIP LOCKED](https://www.postgresql.org/docs/17/sql-select.html),
  [asyncio synchronization](https://docs.python.org/3/library/asyncio-sync.html).
- **Повтор внешнего эффекта.** Таймаут после приёма webhook не доказывает, что
  получатель его не обработал. Проверяем стабильные event_id/тело и сохранённый
  финансовый результат, а не только счётчик вызовов адаптера. Ключ шлюза явно
  сохраняет одну логическую операцию после перезапуска.
  [Stripe webhooks](https://docs.stripe.com/webhooks),
  [Adyen idempotency](https://docs.adyen.com/development-resources/api-idempotency/),
  [Adyen webhook troubleshooting](https://docs.adyen.com/development-resources/webhooks/troubleshoot).
- **Изоляция.** Для проверки создаются UUID database/vhost; миграции применяются
  к этой БД, permissions — к этому vhost. После закрытия соединений удаляются
  только свои ресурсы. CREATE/DROP DATABASE выполняются вне транзакции.
  [CREATE DATABASE](https://www.postgresql.org/docs/17/sql-createdatabase.html),
  [DROP DATABASE](https://www.postgresql.org/docs/17/sql-dropdatabase.html),
  [RabbitMQ HTTP API](https://www.rabbitmq.com/docs/http-api-reference).
- **Реальный lifecycle.** У каждого конкурентного задания отдельная AsyncSession.
  ASGITransport не запускает lifespan сам: тесты явно входят в контекст приложения.
  [SQLAlchemy asyncio](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html),
  [HTTPX transports](https://www.python-httpx.org/advanced/transports/).

## Существенные особенности FastStream 0.7.7

1. AckPolicy.MANUAL не ставит автоматический ACK/reject middleware. Handler
   обязан подтвердить после commit; закрытие канала возвращает unacked delivery.
   Не проверяем внутренний флаг committed вместо состояния БД/брокера.
   [subscriber usecase](https://github.com/ag2ai/faststream/blob/0.7.7/faststream/_internal/endpoint/subscriber/usecase.py),
   [message wrapper](https://github.com/ag2ai/faststream/blob/0.7.7/faststream/rabbit/message.py).
2. Handler exceptions библиотека логирует и поглощает. Для отказа всего процесса
   нужен явный fatal signal supervisor; тестируется завершение consumer, а не
   факт raise внутри callback.
   [subscriber usecase](https://github.com/ag2ai/faststream/blob/0.7.7/faststream/_internal/endpoint/subscriber/usecase.py).
3. on_return_raises по умолчанию False. Нужны True, mandatory=True и проверка
   именно Basic.Ack, не truthiness результата. Unroutable проверяется снятием
   binding только в собственном vhost.
   [channel defaults](https://github.com/ag2ai/faststream/blob/0.7.7/faststream/rabbit/schemas/channel.py),
   [RabbitMQ confirms](https://www.rabbitmq.com/docs/confirms).
4. broker.publish(timeout=5) в версии 0.7.7 сохраняет timeout в команде, но producer
   не передаёт его в _publish. Факт подтверждён и в установленном исходнике.
   Параметр передаётся согласно плану, но гарантию полного deadline обеспечивает
   внешний asyncio.timeout(5), включая ожидание lock/reconnect. Не полагаемся на
   неработающий внутренний timeout и не патчим библиотеку.
   [publish command](https://github.com/ag2ai/faststream/blob/0.7.7/faststream/rabbit/response.py),
   [producer](https://github.com/ag2ai/faststream/blob/0.7.7/faststream/rabbit/publisher/producer.py).
5. При отсутствии message_id parser генерирует fallback. Diagnostic использует
   raw AMQP property и SHA-256 исходного тела. Base64 кодируется только для первых
   4096 bytes; отдельный сценарий отправляет невалидное тело 13 MiB через брокер.
   [parser](https://github.com/ag2ai/faststream/blob/0.7.7/faststream/rabbit/parser.py).
6. Management statistics обновляются с задержкой (обычно 5 секунд). Для состояния
   очереди нужны ограниченный polling либо конкретное получение сообщения;
   мгновенная проверка messages_unacknowledged ненадёжна.
   [statistics interval](https://www.rabbitmq.com/docs/management#statistics-interval).

## Unit и runtime

Unit-проверки покрывают точную сумму, канонический fingerprint, URL origin,
неизменяемость терминального результата, retry-границы и приоритет Settings.
Ожидаемые результаты выводятся из спецификации, а не из эхо моков.

Отдельный runtime smoke обращается к контейнерному API, читает реальные SQL
состояния, receipts и DLQ, останавливает consumer/брокер и прерывает claim.
Терминальный Payment сам по себе не доказывает завершение доставки уведомления.

