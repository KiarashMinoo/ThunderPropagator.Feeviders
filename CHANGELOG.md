# Changelog

All notable changes to this project will be documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.1-beta.40] — 2026-07-21

### 🏠 Chores

- update BuildingBlocks and ThunderPropagator versions to 1.0.1-beta.114 and 1.0.1-beta.186 respectively `(bfb6dbc)` — Kiarash Minoo

## [1.0.1-beta.39] — 2026-07-20

### 🏠 Chores

- suppress false-positive OWASP findings `(eaca107)` — Kiarash Minoo

## [1.0.1-beta.38] — 2026-07-20

### 📦 Dependencies

| Package | Old | New |
|---------|-----|-----|
| NATS.Net | 3.0.0 | 3.0.1 |

- Bump the messaging group with 1 update `(6041ce6)` — dependabot[bot]

## [1.0.1-beta.37] — 2026-07-20

### ♻️ Refactoring

- centralize transport instrumentation `(f835284)` — Kiarash Minoo

### 🏠 Chores

- Add OTel ActivitySource/Meter instrumentation to all 11 transports `(74aae13)` — Kiarash Minoo
- update dependency ignore list and clean up patterns `(ce6f103)` — Kiarash Minoo

## [1.0.1-beta.36] — 2026-07-20

### 🏠 Chores

- Migrate all Feeder/Provider ILogger calls to LoggerMessageAttribute `(a47db6f)` — Kiarash Minoo

## [1.0.1-beta.35] — 2026-07-20

### 📝 Documentation

- audit messaging and cache controls `(b76a68a)` — Kiarash Minoo

## [1.0.1-beta.34] — 2026-07-19

### 🏠 Chores

- Handle poison messages gracefully in NATS, Pulsar, and WebApi feeders `(538de44)` — Kiarash Minoo
- Close remaining silent-drop and null-payload gaps in NATS/Pulsar `(87155e7)` — Kiarash Minoo

## [1.0.1-beta.33] — 2026-07-19

### 🏠 Chores

- Require explicit Redis wildcard mode `(b022970)` — Kiarash Minoo
- Make wildcard validation binding-order independent `(f90b492)` — Kiarash Minoo

## [1.0.1-beta.32] — 2026-07-19

### 🏠 Chores

- Enforce IsEnabled at feeder startup, not just in the receive loop `(92c8bee)` — Kiarash Minoo
- update ThunderPropagatorVersion to 1.0.1-beta.176 `(bf4ac78)` — Kiarash Minoo
- Harden disabled feeder startup guards `(cdd5709)` — Kiarash Minoo

## [1.0.1-beta.31] — 2026-07-19

### 🏠 Chores

- Add embedded-broker integration test for MQTT topic filtering `(0ca14ad)` — Kiarash Minoo

## [1.0.1-beta.30] — 2026-07-19

### 🚀 Features

- add feeder and provider support `(aa554c4)` — Kiarash Minoo

## [1.0.1-beta.29] — 2026-07-19

### 🚀 Features

- add feeder and provider transport `(c72151f)` — Kiarash Minoo

## [1.0.1-beta.28] — 2026-07-19

### 🏠 Chores

- Add AWS SQS/SNS Feevider + Provider core scaffold `(a6c1773)` — Kiarash Minoo
- Register AWS SQS projects in solution `(bfb141c)` — Kiarash Minoo

## [1.0.1-beta.27] — 2026-07-19

### 🏠 Chores

- Log TCP connectivity check failures `(2b46203)` — Kiarash Minoo

## [1.0.1-beta.26] — 2026-07-19

### 🏠 Chores

- Log Redis binary cast fallback `(97b134c)` — Kiarash Minoo

## [1.0.1-beta.25] — 2026-07-19

### 🐛 Bug Fixes

- Fix malformed structured log placeholder in KafkaFeeder `(a07bed5)` — Kiarash Minoo

## [1.0.1-beta.24] — 2026-07-18

### 🏠 Chores

- Initialize Redis feeder asynchronously `(595c1c6)` — Kiarash Minoo

## [1.0.1-beta.23] — 2026-07-18

### 🏠 Chores

- Make RabbitMQ cleanup ownership atomic `(266b707)` — Kiarash Minoo

## [1.0.1-beta.22] — 2026-07-18

### 🏠 Chores

- Clean up failed Kafka initialization `(0d29ef7)` — Kiarash Minoo

## [1.0.1-beta.21] — 2026-07-18

### 🏠 Chores

- Secure UDP encryption key derivation `(4326cb0)` — Kiarash Minoo

## [1.0.1-beta.20] — 2026-07-18

### 🏠 Chores

- Recreate terminal WebSocket clients `(9a9a74f)` — Kiarash Minoo

## [1.0.1-beta.19] — 2026-07-18

### 🏠 Chores

- Verify MQTT maximum packet size `(7e22bab)` — Kiarash Minoo

## [1.0.1-beta.18] — 2026-07-17

### 🏠 Chores

- Observe ActiveMQ listener failures `(ca00968)` — Kiarash Minoo
- Queue ActiveMQ listener processing `(49f3c8a)` — Kiarash Minoo

## [1.0.1-beta.17] — 2026-07-17

### 🏠 Chores

- Observe Redis subscriber processing failures `(d4d23c5)` — Kiarash Minoo

## [1.0.1-beta.16] — 2026-07-17

### 🏠 Chores

- Release TCP send lock on every failure `(ab7bbc9)` — Kiarash Minoo

## [1.0.1-beta.15] — 2026-07-17

### 🏠 Chores

- Settle Pulsar consumer messages after processing `(a077ba0)` — Kiarash Minoo

## [1.0.1-beta.14] — 2026-07-17

### 🏠 Chores

- Settle JetStream messages after processing `(656c4e8)` — Kiarash Minoo
- Implement acknowledgment and negative acknowledgment handling in NATS message processing `(1097e10)` — Kiarash Minoo

## [1.0.1-beta.13] — 2026-07-17

### 🏠 Chores

- Settle RabbitMQ manual-ack deliveries `(ce6478a)` — Kiarash Minoo

## [1.0.1-beta.12] — 2026-07-16

### 🏠 Chores

- Fail when RabbitMQ channel is unavailable `(214b4a5)` — Kiarash Minoo

## [1.0.1-beta.11] — 2026-07-16

### 🏠 Chores

- Reconnect RabbitMQ feeder after shutdown `(4c876cb)` — Kiarash Minoo

## [1.0.1-beta.10] — 2026-07-16

### 🏠 Chores

- Add MQTT feeder topic subscription `(c89dfa2)` — Kiarash Minoo

## [1.0.1-beta.9] — 2026-07-16

### 🏠 Chores

- Delegate provider serialization to format registry `(7ed33f1)` — Kiarash Minoo

## [1.0.1-beta.8] — 2026-07-16

### 🏠 Chores

- Coordinate feeder background lifecycles `(e94ba84)` — Kiarash Minoo

## [1.0.1-beta.7] — 2026-07-16

### 🏠 Chores

- Add binary feeder serialization support `(4923ad8)` — Kiarash Minoo

## [1.0.1-beta.6] — 2026-07-16

### 🏠 Chores

- Preserve structured feeder subscription logs `(2919065)` — Kiarash Minoo

## [1.0.1-beta.5] — 2026-07-16

### 🏠 Chores

- Move Kafka consume work off the ThreadPool `(a7aa105)` — Kiarash Minoo

## [1.0.1-beta.4] — 2026-07-16

### 🏠 Chores

- Adapt feeviders to upgraded framework APIs `(3e99524)` — Kiarash Minoo

## [1.0.1-beta.3] — 2026-07-16

### 📦 Dependencies

| Package | Old | New |
|---------|-----|-----|
| Confluent.Kafka | 2.14.0 | 2.15.0 |
| Confluent.SchemaRegistry | 2.14.0 | 2.15.0 |
| Confluent.SchemaRegistry.Serdes.Avro | 2.14.0 | 2.15.0 |
| Confluent.SchemaRegistry.Serdes.Json | 2.14.0 | 2.15.0 |
| MQTTnet | 5.1.0.1559 | 5.2.0.1603 |
| NATS.Net | 2.7.3 | 3.0.0 |
| StackExchange.Redis | 2.12.14 | 3.0.17 |
| Microsoft.NET.Test.Sdk | 18.5.1 | 18.7.0 |
| NSubstitute | 5.3.0 | 6.0.0 |
| coverlet.collector | 10.0.0 | 10.0.1 |
| OpenTelemetry.Api | 1.15.3 | 1.16.0 |
| OpenTelemetry.Api | 1.16.0 | 1.17.0 |
| JetBrains.Annotations | 2026.2.0 | 2026.2.0 |
| StackExchange.Redis | 3.0.17 | 3.0.17 |
| Microsoft.NET.Test.Sdk | 18.7.0 | 18.8.1 |

- Bump the messaging group with 6 updates `(6e98fd0)` — dependabot[bot]
- Bump the redis-and-resilience group with 1 update `(d517620)` — dependabot[bot]
- Bump the testing group with 3 updates `(4a87b63)` — dependabot[bot]
- Bump OpenTelemetry.Api from 1.15.3 to 1.16.0 `(6f88cd0)` — dependabot[bot]
- Update shared framework and tooling dependencies `(b88fe12)` — Kiarash Minoo

### ⚙️ CI / Tooling

- Add security and publishing GitHub workflows `(f2a2a41)` — Kiarash Minoo
- Consolidate CI workflows and add concurrency `(173b305)` — Kiarash Minoo

### 🏠 Chores

- Bump JetBrains.Annotations from 2025.2.4 to 2026.2.0 `(5242f30)` — dependabot[bot]

## [Unreleased]

### 🚀 Features
- Implement asynchronous initialization for JetStream consumer and context in NATS feeders `(d2519f7)` — Kiarash Minoo
- Enhance performance and add encryption support for UDP and TCP clients `(dc6d610)` — Kiarash Minoo
- Complete TCP and UDP Feeviders implementation `(803c6dd)` — Ahmad(Kia) Minoo
- Add MQTT feeder and provider `(1e711e4)` — Ahmad(Kia) Minoo
- Add NATS feeder and provider `(72719fb)` — Ahmad(Kia) Minoo
- Add Pulsar feeder and provider `(5e0ff81)` — Ahmad(Kia) Minoo
- Add Kafka Avro/JSON Schema Registry serializers `(bc459d1)` — Ahmad(Kia) Minoo
- Add health check handling for messaging transports `(913bfd4)` — Ahmad(Kia) Minoo
- Add channel provider extension methods `(3ffa32b)` — Kiarash Minoo
- Handle feeder arguments `(cb72c0a)` — Kiarash Minoo

### 🐛 Bug Fixes
- Revert version to 1.0.0 from 1.0.1-beta.3 `(c33e78d)` — Kiarash Minoo
- Add `ConfigureAwait(false)` to async calls and improve resource disposal error handling `(2218ad1)` — Kiarash Minoo
- Fix build on Release configuration `(c7f4231)` — Ahmad(Kia) Minoo
- Fix MQTT projects build configurations `(82b5964)` — Ahmad(Kia) Minoo
- Fix version references `(af6d70e)` — Ahmad(Kia) Minoo
- Fix schema generation for RabbitMQ configurations `(41a5ed0)` — Kiarash Minoo
- Fix package version references `(f0ab9a1)` — Kiarash Minoo
- Fix build on Release mode for RabbitMQ `(20df038)` — Kiarash Minoo
- Fix RabbitMQ connection creation `(edffa90)` — Kiarash Minoo
- Fix packages builder for x86/x64 platforms `(09a7ab2)` — Kiarash Minoo
- Fix NuGet settings `(e896b96)` — Kiarash Minoo
- Fix project build errors `(e30ee47)` — Kiarash Minoo
- Trim split strings to remove whitespace bugs `(0772012)` — Ahmad(Kia) Minoo
- Minor fix `(84a0e0f)` — Kiarash Minoo

### ⚡ Performance
- Improve message throughput performance `(a8f8d39)` — Kiarash Minoo

### ♻️ Refactoring
- Replace `Thread` with `Task.Run` for async handling and add top-level exception logging in feeders `(6cae6a2)` — Kiarash Minoo
- Refactor unit tests and update project structure `(4530bed)` — Ahmad(Kia) Minoo

### 📦 Dependencies

| Package | Old | New |
|---------|-----|-----|
| ThunderPropagator.BuildingBlocks | 1.0.1-beta.2 | 1.0.1-beta.26 |
| Confluent.Kafka | 2.12.0 | 2.14.0 |
| Confluent.SchemaRegistry | 2.12.0 | 2.14.0 |
| Confluent.SchemaRegistry.Serdes.Avro | 2.12.0 | 2.14.0 |
| Confluent.SchemaRegistry.Serdes.Json | 2.12.0 | 2.14.0 |
| NATS.Net | 2.6.11 | 2.7.3 |
| DotPulsar | 5.1.0 | 5.3.1 |
| MQTTnet | 5.0.1.1416 | 5.1.0.1559 |
| RabbitMQ.Client | 7.2.0 | 7.2.1 |
| Apache.NMS.ActiveMQ | 2.1.1 | 2.2.0 |
| StackExchange.Redis | 2.10.1 | 2.12.14 |
| OpenTelemetry.Api | 1.14.0 | 1.15.3 |
| NJsonSchema | 11.5.2 | 11.6.1 |
| NJsonSchema.Annotations | 11.5.2 | 11.6.1 |
| Microsoft.NET.Test.Sdk | 18.0.1 | 18.5.1 |
| coverlet.collector | 6.0.4 | 10.0.0 |

### ⚙️ CI / Tooling
- Add CI workflows for beta and release processes with version bumping, packaging, and publishing `(3a09e3d)` — Kiarash Minoo
- Update CI workflows and package versioning `(a44c64b)` — Kiarash Minoo
- Update package versioning and project structure for improved clarity `(40c2b0d)` — Kiarash Minoo
- Add ARM64 platform support `(53359ac)` — Kiarash Minoo
- Add .NET 9 multi-targeting support `(9e05658)` — Kiarash Minoo
- Add multi-platform package builds `(6cfd186)` — Kiarash Minoo
- Update beta CI and package configuration `(6fb0ac5)` — Ahmad(Kia) Minoo
- Update `nuget.config` `(68f99f3)` — Ahmad(Kia) Minoo
- Update pack-and-push workflow `(7489fe2)` — Kiarash Minoo
- Configure multi-architecture build matrix `(9f57a98)` — Ahmad(Kia) Minoo
- Fix GitHub Actions builder `(6d98a46)` — Kiarash Minoo
- Disable stale workflows `(ab7ab50)` — Ahmad(Kia) Minoo

### 📝 Documentation
- Add comprehensive WebSocket documentation with protocol overview, architecture, frame structure, and security `(fc65887)` — Kiarash Minoo
- Add per-transport documentation and diagrams `(676769e)` — Kiarash Minoo
- Update README.md `(a528d41)` — Ahmad(Kia) Minoo
- Update README.md with usage examples `(b601ac4)` — Ahmad(Kia) Minoo

### 🧪 Tests
- Refactor architecture tests and add comprehensive NATS serializer tests `(ab8191f)` — Kiarash Minoo

### 🏠 Chores
- Add ThunderPropagator SVG logo `(5a595d7)` — Kiarash Minoo
- Update package versions to 1.0.1-beta.4/beta.5 `(10c6e64)` — Kiarash Minoo
- Update versioning to 1.0.79-beta.2 `(1de05dd)` — Kiarash Minoo
- Add project icons `(4d4c35c)` — Ahmad(Kia) Minoo
- Update `.gitignore` `(7d9e2c7)` — Kiarash Minoo

