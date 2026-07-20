# Theme 5: Messaging, Queue, and Cache Security Audit

Audit date: 2026-07-20

Issue: [#42](https://github.com/KiarashMinoo/ThunderPropagator.Feeviders/issues/42)

Reviewed revision: `30dac9d` (`develop`)

## Scope and interpretation

This is a source-code audit of `ThunderPropagator.Feeviders`. It evaluates the
security guarantees implemented by the transport adapters and distinguishes them
from controls that must be enforced by applications, brokers, cloud resources, or
network infrastructure.

The Redis integration in this repository is a pub/sub transport. It is not a
general-purpose cache or a recovery snapshot store. Cache storage, cache keys,
distributed locks, and recovery handlers are therefore outside this repository's
implementation boundary.

Status meanings:

- **Verified**: directly enforced by this repository and covered by tests.
- **Partial**: supported for some transports or exposed as configuration, but not
  guaranteed across all transports.
- **Operational**: must be verified in deployed broker, identity, secret, or
  network configuration.
- **Gap**: applicable to the transport library but not implemented consistently.
- **External**: owned by another repository or by the consuming application.
- **Not applicable**: this repository does not provide the affected capability.

## 19. Messaging, queue, and event security

| Control | Status | Evidence and remaining work |
| --- | --- | --- |
| Producers and consumers authenticate before accessing brokers | **Partial / Operational** | RabbitMQ, NATS, MQTT, Pulsar, ActiveMQ, Azure Service Bus, AWS, GCP, TCP, and Redis expose authentication or credential configuration. TLS is also configurable for the applicable clients. Authentication is optional in several configurations, and a source audit cannot prove that deployed endpoints reject anonymous access. WebSocket endpoint authentication is application middleware responsibility. |
| Topic and queue permissions are restricted by role | **Operational** | Adapters connect to configured topics, queues, subjects, and channels, but broker ACL/IAM policy is not provisioned or inspected by this library. Verify least-privilege producer and consumer identities in each deployment. |
| MQTT feeders subscribe only to the configured topic filter | **Verified** | `MqttSubscriptionOptionsFactory.Create` requires a non-empty topic and calls `WithTopicFilter(feederConfiguration.Topic)`. Unit and integration tests cover the selected topic and reject a missing topic. This resolves [#38](https://github.com/KiarashMinoo/ThunderPropagator.Feeviders/issues/38). |
| Message schemas are validated before processing | **Partial** | Kafka supports Schema Registry JSON/Avro serializers, and Pulsar uses a typed JSON schema. Other transports perform typed deserialization but do not enforce a formal, versioned schema contract before handler execution. |
| Poison messages do not crash the consumer loop | **Partial** | ActiveMQ and Redis isolate per-message handler exceptions; RabbitMQ, SQS, Azure Service Bus, GCP Pub/Sub, Pulsar, and NATS JetStream implement acknowledgement or redelivery behavior; iterative socket/stream consumers catch receive or processing failures. Outcomes still vary by transport, and malformed messages can be retried indefinitely when no delivery limit is configured. |
| Dead-letter queues are configured for all transports | **Gap** | Azure Service Bus explicitly dead-letters at the configured delivery limit and has settlement tests. RabbitMQ, SQS, Pulsar, NATS, Kafka, MQTT, ActiveMQ, GCP Pub/Sub, and Redis do not receive a uniform DLQ guarantee from this repository; most require broker-side topology and retention policies. |
| Consumers are idempotent under duplicate delivery | **Partial / External** | Kafka can enable an idempotent producer and brokers such as NATS JetStream and Pulsar expose deduplication features. The library does not provide a transport-independent consumer inbox or deduplication store. Handlers must be idempotent. |
| Replay protection exists where ordering matters | **Partial / External** | Several brokers expose ordering, offsets, replay policies, or deduplication windows. The library does not provide application-level timestamp, nonce, sequence, or signature validation, so anti-replay semantics remain message-contract and handler responsibilities. |
| Correlation identifiers are included for tracing | **Partial** | Providers and feeders propagate OpenTelemetry `ActivityContext` and `Baggage` across the principal transports; Azure Service Bus also exposes its broker correlation ID. Trace context is omitted when no activity exists, and the repository does not mandate a distinct business correlation ID in every message. |
| Sensitive message data is minimized or encrypted | **Operational / External** | TLS is configurable for applicable transports and UDP offers optional encryption and integrity protection. Neither TLS nor message-field minimization is globally enforced. Classify payloads, avoid secrets/PII where possible, require encrypted broker connections, and use payload encryption when broker at-rest protection is insufficient. |
| Outbox and inbox patterns protect service-boundary consistency | **External** | No durable outbox or inbox is implemented here. These patterns require integration with the consuming application's transaction and persistence boundary. |
| Queue and topic retention policies are defined | **Operational** | Some client options expose TTL, retention, or stream policies, but deployed broker resources determine actual retention. Maintain and verify per-environment broker policies. |
| `IsEnabled` prevents disabled feeders from connecting | **Verified** | Startup guards and tests cover Kafka, RabbitMQ, NATS, MQTT, Pulsar, and WebSocket registration/startup paths. This resolves [#39](https://github.com/KiarashMinoo/ThunderPropagator.Feeviders/issues/39). |
| `DisuseFeeder` verifies `channelKey` ownership | **External — open** | This behavior belongs to the core `ThunderPropagator` repository. [ThunderPropagator#330](https://github.com/KiarashMinoo/ThunderPropagator/issues/330) remains open as of the audit date. |
| Redis Pub/Sub wildcard patterns are explicit and validated | **Verified** | Auto mode rejects wildcard channel reads and rejects switching a wildcard channel back to Auto. Explicit Pattern and Literal modes are supported, including configuration binding where `Channel` is assigned before `PatternMode`. Tests cover wildcard characters, modes, and binding order. This resolves [#41](https://github.com/KiarashMinoo/ThunderPropagator.Feeviders/issues/41). |

## 20. Cache security

| Control | Status | Evidence and remaining work |
| --- | --- | --- |
| Sensitive data is not cached unnecessarily | **Not applicable** | No cache store is implemented by this repository. Audit the applications or cache/recovery packages that own stored values. |
| Tenant or user context is included in cache keys | **Not applicable** | Redis Pub/Sub channels are message routing names, not cache keys. Cache-key construction belongs to the cache owner. |
| Cached values are validated before serving | **Not applicable** | The repository neither stores nor serves cached values. Typed message deserialization is covered separately by the messaging controls. |
| Every cache entry has a clear TTL | **Not applicable** | No cache entries are created here. Message TTL and broker retention are separate operational controls. |
| Cache entries are invalidated after permission or data changes | **Not applicable** | Redis Pub/Sub can carry invalidation events, but the consuming application owns cache invalidation behavior. |
| Redis requires authentication and TLS | **Operational** | Redis Pub/Sub accepts a StackExchange.Redis connection string, which can contain credentials and TLS settings, but this library does not require secure values. Enforce and verify authentication, secret rotation, TLS, and certificate validation in deployment configuration. |
| Redis is private-network only | **Operational** | Endpoint exposure is controlled by network and cloud infrastructure, not by the client adapter. |
| Distributed locks have timeouts | **Not applicable** | No distributed-lock implementation exists in this repository. |
| Cached PII has retention and eviction policy | **Not applicable** | No cache values are stored by this repository. |
| Recovery snapshots are encrypted at rest | **External** | Redis, MongoDB, and PostgreSQL recovery snapshot handlers are not implemented in this repository. Verify encryption in the repository and infrastructure that own those handlers and databases. |

## Recommended follow-up

1. Keep issue #42 open as a tracking checklist; only the three directly verified
   controls should be checked from this repository.
2. Resolve upstream `ThunderPropagator#330` before marking ownership validation
   complete.
3. Create transport-specific work for poison-message delivery limits and DLQ
   behavior. Define whether the library provisions topology or only validates and
   documents required broker configuration.
4. Define a common application contract for consumer idempotency, replay
   protection, business correlation IDs, and schema/version metadata.
5. Maintain a deployment security checklist for broker authentication, ACL/IAM,
   TLS, private networking, at-rest encryption, and retention. These controls
   cannot be proven by this repository's unit tests.
6. Move the cache and recovery snapshot controls to the repositories that own
   cache persistence and recovery handlers, while retaining cross-links from
   issue #42.
