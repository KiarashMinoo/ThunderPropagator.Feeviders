# ThunderPropagator.Feeviders Documentation

Feeders and providers connecting ThunderPropagator to Kafka, RabbitMQ, NATS, Pulsar, MQTT, ActiveMQ, Redis, HTTP, WebSocket, TCP, and UDP.

## Contents

- [Documentation areas](#documentation-areas)
- [Package dependencies](#package-dependencies)
- [Coverage audit](#coverage-audit)

## Documentation areas

- [Feeviders](./Feeviders/README.md) `Types:0` `Files:0` `Diagrams:✓`

## Package dependencies

| Package | Version | Registry |
|---|---|---|
| `Apache.NMS` | `2.2.0` | [Package](https://www.nuget.org/packages/Apache.NMS) |
| `Apache.NMS.ActiveMQ` | `2.2.0` | [Package](https://www.nuget.org/packages/Apache.NMS.ActiveMQ) |
| `AWSSDK.SimpleNotificationService` | `4.0.100.5` | [Package](https://www.nuget.org/packages/AWSSDK.SimpleNotificationService) |
| `AWSSDK.SQS` | `4.0.100.5` | [Package](https://www.nuget.org/packages/AWSSDK.SQS) |
| `Azure.Identity` | `1.21.0` | [Package](https://www.nuget.org/packages/Azure.Identity) |
| `Azure.Messaging.ServiceBus` | `7.20.2` | [Package](https://www.nuget.org/packages/Azure.Messaging.ServiceBus) |
| `Confluent.Kafka` | `2.15.0` | [Package](https://www.nuget.org/packages/Confluent.Kafka) |
| `Confluent.SchemaRegistry` | `2.15.0` | [Package](https://www.nuget.org/packages/Confluent.SchemaRegistry) |
| `Confluent.SchemaRegistry.Serdes.Avro` | `2.15.0` | [Package](https://www.nuget.org/packages/Confluent.SchemaRegistry.Serdes.Avro) |
| `Confluent.SchemaRegistry.Serdes.Json` | `2.15.0` | [Package](https://www.nuget.org/packages/Confluent.SchemaRegistry.Serdes.Json) |
| `DotPulsar` | `5.3.1` | [Package](https://www.nuget.org/packages/DotPulsar) |
| `Google.Cloud.PubSub.V1` | `3.36.0` | [Package](https://www.nuget.org/packages/Google.Cloud.PubSub.V1) |
| `Google.Protobuf` | `3.35.1` | [Package](https://www.nuget.org/packages/Google.Protobuf) |
| `Grpc.Net.Client` | `2.80.0` | [Package](https://www.nuget.org/packages/Grpc.Net.Client) |
| `Grpc.Tools` | `2.83.0` | [Package](https://www.nuget.org/packages/Grpc.Tools) |
| `JetBrains.Annotations` | `2026.2.0` | [Package](https://www.nuget.org/packages/JetBrains.Annotations) |
| `Microsoft.AspNetCore.Connections.Abstractions` | `10.*` | [Package](https://www.nuget.org/packages/Microsoft.AspNetCore.Connections.Abstractions) |
| `Microsoft.Extensions.Configuration.Binder` | `10.*` | [Package](https://www.nuget.org/packages/Microsoft.Extensions.Configuration.Binder) |
| `Microsoft.Extensions.Diagnostics.HealthChecks` | `10.*` | [Package](https://www.nuget.org/packages/Microsoft.Extensions.Diagnostics.HealthChecks) |
| `Microsoft.Extensions.Http.Resilience` | `10.*` | [Package](https://www.nuget.org/packages/Microsoft.Extensions.Http.Resilience) |
| `MQTTnet` | `5.2.0.1603` | [Package](https://www.nuget.org/packages/MQTTnet) |
| `NATS.Net` | `3.0.1` | [Package](https://www.nuget.org/packages/NATS.Net) |
| `NetMQ` | `4.0.4.2` | [Package](https://www.nuget.org/packages/NetMQ) |
| `NJsonSchema.Annotations` | `11.6.1` | [Package](https://www.nuget.org/packages/NJsonSchema.Annotations) |
| `OpenTelemetry.Api` | `1.17.0` | [Package](https://www.nuget.org/packages/OpenTelemetry.Api) |
| `RabbitMQ.Client` | `7.2.1` | [Package](https://www.nuget.org/packages/RabbitMQ.Client) |
| `StackExchange.Redis` | `3.0.17` | [Package](https://www.nuget.org/packages/StackExchange.Redis) |

## Coverage audit

| Documentation area | Status | Files | Types | Retry passes |
|---|---|---:|---:|---:|
| [`Feeviders`](./Feeviders/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/ActiveMQ`](./Feeviders/ActiveMQ/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/ActiveMQ/ActiveMQ.SharedKernel`](./Feeviders/ActiveMQ/ActiveMQ.SharedKernel/README.md) | ✅ Complete | 5 | 1 | 1 |
| [`Feeviders/ActiveMQ/Feeders.ActiveMQ`](./Feeviders/ActiveMQ/Feeders.ActiveMQ/README.md) | ✅ Complete | 7 | 3 | 1 |
| [`Feeviders/ActiveMQ/Providers.DotNet.ActiveMQ`](./Feeviders/ActiveMQ/Providers.DotNet.ActiveMQ/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/AwsSqs`](./Feeviders/AwsSqs/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/AwsSqs/AwsSqs.SharedKernel`](./Feeviders/AwsSqs/AwsSqs.SharedKernel/README.md) | ✅ Complete | 4 | 1 | 1 |
| [`Feeviders/AwsSqs/Feeders.AwsSqs`](./Feeviders/AwsSqs/Feeders.AwsSqs/README.md) | ✅ Complete | 7 | 3 | 1 |
| [`Feeviders/AwsSqs/Providers.DotNet.AwsSqs`](./Feeviders/AwsSqs/Providers.DotNet.AwsSqs/README.md) | ✅ Complete | 12 | 6 | 1 |
| [`Feeviders/AzureServiceBus`](./Feeviders/AzureServiceBus/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/AzureServiceBus/AzureServiceBus.SharedKernel`](./Feeviders/AzureServiceBus/AzureServiceBus.SharedKernel/README.md) | ✅ Complete | 6 | 1 | 1 |
| [`Feeviders/AzureServiceBus/Feeders.AzureServiceBus`](./Feeviders/AzureServiceBus/Feeders.AzureServiceBus/README.md) | ✅ Complete | 8 | 3 | 1 |
| [`Feeviders/AzureServiceBus/Providers.DotNet.AzureServiceBus`](./Feeviders/AzureServiceBus/Providers.DotNet.AzureServiceBus/README.md) | ✅ Complete | 7 | 4 | 1 |
| [`Feeviders/GcpPubSub`](./Feeviders/GcpPubSub/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/GcpPubSub/Feeders.GcpPubSub`](./Feeviders/GcpPubSub/Feeders.GcpPubSub/README.md) | ✅ Complete | 8 | 3 | 1 |
| [`Feeviders/GcpPubSub/GcpPubSub.SharedKernel`](./Feeviders/GcpPubSub/GcpPubSub.SharedKernel/README.md) | ✅ Complete | 5 | 1 | 1 |
| [`Feeviders/GcpPubSub/Providers.DotNet.GcpPubSub`](./Feeviders/GcpPubSub/Providers.DotNet.GcpPubSub/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/Grpc`](./Feeviders/Grpc/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/Grpc/Feeders.Grpc`](./Feeviders/Grpc/Feeders.Grpc/README.md) | ✅ Complete | 7 | 3 | 1 |
| [`Feeviders/Grpc/Grpc.SharedKernel`](./Feeviders/Grpc/Grpc.SharedKernel/README.md) | ✅ Complete | 5 | 1 | 1 |
| [`Feeviders/Grpc/Grpc.SharedKernel/Protos`](./Feeviders/Grpc/Grpc.SharedKernel/Protos/README.md) | ✅ Complete | 1 | 0 | 1 |
| [`Feeviders/Grpc/Providers.DotNet.Grpc`](./Feeviders/Grpc/Providers.DotNet.Grpc/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/Kafka`](./Feeviders/Kafka/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/Kafka/Feeders.Kafka`](./Feeviders/Kafka/Feeders.Kafka/README.md) | ✅ Complete | 9 | 3 | 1 |
| [`Feeviders/Kafka/Providers.DotNet.Kafka`](./Feeviders/Kafka/Providers.DotNet.Kafka/README.md) | ✅ Complete | 7 | 3 | 1 |
| [`Feeviders/Mqtt`](./Feeviders/Mqtt/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/Mqtt/Feeders.Mqtt`](./Feeviders/Mqtt/Feeders.Mqtt/README.md) | ✅ Complete | 7 | 3 | 1 |
| [`Feeviders/Mqtt/Mqtt.SharedKernel`](./Feeviders/Mqtt/Mqtt.SharedKernel/README.md) | ✅ Complete | 4 | 1 | 1 |
| [`Feeviders/Mqtt/Providers.DotNet.Mqtt`](./Feeviders/Mqtt/Providers.DotNet.Mqtt/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/NATS`](./Feeviders/NATS/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/NATS/Feeders.NATS`](./Feeviders/NATS/Feeders.NATS/README.md) | ✅ Complete | 7 | 3 | 1 |
| [`Feeviders/NATS/NATS.SharedKernel`](./Feeviders/NATS/NATS.SharedKernel/README.md) | ✅ Complete | 8 | 5 | 1 |
| [`Feeviders/NATS/Providers.DotNet.NATS`](./Feeviders/NATS/Providers.DotNet.NATS/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/Pulsar`](./Feeviders/Pulsar/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/Pulsar/Feeders.Pulsar`](./Feeviders/Pulsar/Feeders.Pulsar/README.md) | ✅ Complete | 7 | 3 | 1 |
| [`Feeviders/Pulsar/Providers.DotNet.Pulsar`](./Feeviders/Pulsar/Providers.DotNet.Pulsar/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/Pulsar/Pulsar.SharedKernel`](./Feeviders/Pulsar/Pulsar.SharedKernel/README.md) | ✅ Complete | 5 | 1 | 1 |
| [`Feeviders/RabbitMQ`](./Feeviders/RabbitMQ/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/RabbitMQ/Feeders.RabbitMQ`](./Feeviders/RabbitMQ/Feeders.RabbitMQ/README.md) | ✅ Complete | 8 | 3 | 1 |
| [`Feeviders/RabbitMQ/Providers.DotNet.RabbitMQ`](./Feeviders/RabbitMQ/Providers.DotNet.RabbitMQ/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/RabbitMQ/RabbitMQ.SharedKernel`](./Feeviders/RabbitMQ/RabbitMQ.SharedKernel/README.md) | ✅ Complete | 4 | 1 | 1 |
| [`Feeviders/RedisPubSub`](./Feeviders/RedisPubSub/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/RedisPubSub/Feeders.RedisPubSub`](./Feeviders/RedisPubSub/Feeders.RedisPubSub/README.md) | ✅ Complete | 7 | 3 | 1 |
| [`Feeviders/RedisPubSub/Providers.DotNet.RedisPubSub`](./Feeviders/RedisPubSub/Providers.DotNet.RedisPubSub/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/RedisPubSub/RedisPubSub.SharedKernel`](./Feeviders/RedisPubSub/RedisPubSub.SharedKernel/README.md) | ✅ Complete | 3 | 1 | 1 |
| [`Feeviders/SharedKernel`](./Feeviders/SharedKernel/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/SharedKernel/Feeders.SharedKernel`](./Feeviders/SharedKernel/Feeders.SharedKernel/README.md) | ✅ Complete | 5 | 2 | 1 |
| [`Feeviders/SharedKernel/Providers.DotNet.SharedKernel`](./Feeviders/SharedKernel/Providers.DotNet.SharedKernel/README.md) | ✅ Complete | 9 | 7 | 1 |
| [`Feeviders/SharedKernel/Providers.DotNet.SharedKernel/Extensions`](./Feeviders/SharedKernel/Providers.DotNet.SharedKernel/Extensions/README.md) | ✅ Complete | 1 | 1 | 1 |
| [`Feeviders/TcpSocket`](./Feeviders/TcpSocket/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/TcpSocket/Feeders.TcpSocket`](./Feeviders/TcpSocket/Feeders.TcpSocket/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/TcpSocket/Providers.DotNet.TcpSocket`](./Feeviders/TcpSocket/Providers.DotNet.TcpSocket/README.md) | ✅ Complete | 7 | 3 | 1 |
| [`Feeviders/TcpSocket/TcpSocket.SharedKernel`](./Feeviders/TcpSocket/TcpSocket.SharedKernel/README.md) | ✅ Complete | 5 | 1 | 1 |
| [`Feeviders/UdpClient`](./Feeviders/UdpClient/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/UdpClient/Feeders.UdpClient`](./Feeviders/UdpClient/Feeders.UdpClient/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/UdpClient/Providers.DotNet.UdpClient`](./Feeviders/UdpClient/Providers.DotNet.UdpClient/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/UdpClient/UdpClient.SharedKernel`](./Feeviders/UdpClient/UdpClient.SharedKernel/README.md) | ✅ Complete | 4 | 2 | 1 |
| [`Feeviders/WebApi`](./Feeviders/WebApi/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/WebApi/Feeders.WebApi`](./Feeviders/WebApi/Feeders.WebApi/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/WebApi/Feeders.WebApi/Properties`](./Feeviders/WebApi/Feeders.WebApi/Properties/README.md) | ✅ Complete | 1 | 0 | 1 |
| [`Feeviders/WebApi/Providers.DotNet.WebApi`](./Feeviders/WebApi/Providers.DotNet.WebApi/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/WebSocket`](./Feeviders/WebSocket/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/WebSocket/Feeders.WebSocket`](./Feeviders/WebSocket/Feeders.WebSocket/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/WebSocket/Providers.DotNet.WebSocket`](./Feeviders/WebSocket/Providers.DotNet.WebSocket/README.md) | ✅ Complete | 7 | 3 | 1 |
| [`Feeviders/ZeroMQ`](./Feeviders/ZeroMQ/README.md) | ✅ Complete | 0 | 0 | 1 |
| [`Feeviders/ZeroMQ/Feeders.ZeroMQ`](./Feeviders/ZeroMQ/Feeders.ZeroMQ/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/ZeroMQ/Providers.DotNet.ZeroMQ`](./Feeviders/ZeroMQ/Providers.DotNet.ZeroMQ/README.md) | ✅ Complete | 6 | 3 | 1 |
| [`Feeviders/ZeroMQ/ZeroMQ.SharedKernel`](./Feeviders/ZeroMQ/ZeroMQ.SharedKernel/README.md) | ✅ Complete | 6 | 2 | 1 |

**Last generated:** July 27, 2026
