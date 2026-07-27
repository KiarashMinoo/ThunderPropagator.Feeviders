# NATS.SharedKernel

## Contents

- [Overview](#overview)
- [Files](#files)
- [Types and Members](#types-and-members)
- [Serialization and Contracts](#serialization-and-contracts)
- [Validation and Constraints](#validation-and-constraints)
- [Performance Notes](#performance-notes)
- [Package Dependencies](#package-dependencies)
- [Diagrams](#diagrams)
- [Examples](#examples)
- [See Also](#see-also)

## Overview

The **NATS.SharedKernel** area groups 5 documented types, including `AbstractNatsFeevidersConfiguration`, `JsonNatsDeserializer`, `JsonNatsSerializer`, `JsonNatsSerializerRegistry`, `MessagingType`. It provides the contracts and implementation used by this part of ThunderPropagator.Feeviders.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `AbstractNatsFeevidersConfiguration.cs` | `AbstractNatsFeevidersConfiguration` | 216 | Defines AbstractNatsFeevidersConfiguration and its related behavior. |
| `AssemblyInfo.cs` | — | 8 | Contains the assembly info implementation or configuration. |
| `JsonNatsDeserializer.cs` | `JsonNatsDeserializer` | 19 | Defines JsonNatsDeserializer and its related behavior. |
| `JsonNatsSerializer.cs` | `JsonNatsSerializer` | 20 | Defines JsonNatsSerializer and its related behavior. |
| `JsonNatsSerializerRegistry.cs` | `JsonNatsSerializerRegistry` | 26 | Defines JsonNatsSerializerRegistry and its related behavior. |
| `MessagingType.cs` | `MessagingType` | 11 | Defines MessagingType and its related behavior. |
| `NatsClientFactory.cs` | `NatsClientFactory` | 61 | Defines NatsClientFactory and its related behavior. |
| `ThunderPropagator.Feeviders.NATS.SharedKernel.csproj` | — | 14 | Defines project build targets, dependencies, and package metadata. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`AbstractNatsFeevidersConfiguration`](#abstractnatsfeevidersconfiguration) | class | Represents the AbstractNatsFeevidersConfiguration class. | `ServiceConfiguration` | — |
| [`JsonNatsDeserializer`](#jsonnatsdeserializer) | class | Represents the JsonNatsDeserializer class. | — | `Deserialize(…)` |
| [`JsonNatsSerializer`](#jsonnatsserializer) | class | Represents the JsonNatsSerializer class. | — | `Serialize(…)` |
| [`JsonNatsSerializerRegistry`](#jsonnatsserializerregistry) | class | Represents the JsonNatsSerializerRegistry class. | — | — |
| [`MessagingType`](#messagingtype) | enum | Represents the MessagingType enum. | — | — |

### AbstractNatsFeevidersConfiguration

- **Kind:** class
- **Namespace:** `ThunderPropagator.Feeviders.NATS.SharedKernel`
- **Inherits/implements:** `ServiceConfiguration`
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the AbstractNatsFeevidersConfiguration class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve AbstractNatsFeevidersConfiguration from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### JsonNatsDeserializer

- **Kind:** class
- **Namespace:** `ThunderPropagator.Feeviders.NATS.SharedKernel`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `Deserialize(…)`
- **Summary:** Represents the JsonNatsDeserializer class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve JsonNatsDeserializer from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### JsonNatsSerializer

- **Kind:** class
- **Namespace:** `ThunderPropagator.Feeviders.NATS.SharedKernel`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** `Serialize(…)`
- **Summary:** Represents the JsonNatsSerializer class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve JsonNatsSerializer from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### JsonNatsSerializerRegistry

- **Kind:** class
- **Namespace:** `ThunderPropagator.Feeviders.NATS.SharedKernel`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the JsonNatsSerializerRegistry class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve JsonNatsSerializerRegistry from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### MessagingType

- **Kind:** enum
- **Namespace:** `ThunderPropagator.Feeviders.NATS.SharedKernel`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the MessagingType enum.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve MessagingType from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

## Serialization and Contracts

Serialization behavior is part of the public wire or persistence contract in this area. Preserve field names, ordering rules, content negotiation, and backward-compatibility expectations when changing these types.

## Validation and Constraints

Inputs are validated at component boundaries. Callers should provide non-null required values and handle domain or argument exceptions without retrying invalid requests unchanged.

## Performance Notes

This area contains performance-sensitive constructs such as pooled buffers, spans, asynchronous value types, or concurrent collections. Avoid unnecessary allocations and blocking calls on streaming or message-processing paths.

## Package Dependencies

| Package | Version | Description | Links |
|---|---|---|---|
| `Apache.NMS` | `2.2.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Apache.NMS) |
| `Apache.NMS.ActiveMQ` | `2.2.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Apache.NMS.ActiveMQ) |
| `AWSSDK.SimpleNotificationService` | `4.0.100.5` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/AWSSDK.SimpleNotificationService) |
| `AWSSDK.SQS` | `4.0.100.5` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/AWSSDK.SQS) |
| `Azure.Identity` | `1.21.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Azure.Identity) |
| `Azure.Messaging.ServiceBus` | `7.20.2` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Azure.Messaging.ServiceBus) |
| `Confluent.Kafka` | `2.15.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Confluent.Kafka) |
| `Confluent.SchemaRegistry` | `2.15.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Confluent.SchemaRegistry) |
| `Confluent.SchemaRegistry.Serdes.Avro` | `2.15.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Confluent.SchemaRegistry.Serdes.Avro) |
| `Confluent.SchemaRegistry.Serdes.Json` | `2.15.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Confluent.SchemaRegistry.Serdes.Json) |
| `DotPulsar` | `5.3.1` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/DotPulsar) |
| `Google.Cloud.PubSub.V1` | `3.36.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Google.Cloud.PubSub.V1) |
| `Google.Protobuf` | `3.35.1` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Google.Protobuf) |
| `Grpc.Net.Client` | `2.80.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Grpc.Net.Client) |
| `Grpc.Tools` | `2.83.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Grpc.Tools) |
| `JetBrains.Annotations` | `2026.2.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/JetBrains.Annotations) |
| `Microsoft.AspNetCore.Connections.Abstractions` | `10.*` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Microsoft.AspNetCore.Connections.Abstractions) |
| `Microsoft.Extensions.Configuration.Binder` | `10.*` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Microsoft.Extensions.Configuration.Binder) |
| `Microsoft.Extensions.Diagnostics.HealthChecks` | `10.*` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Microsoft.Extensions.Diagnostics.HealthChecks) |
| `Microsoft.Extensions.Http.Resilience` | `10.*` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/Microsoft.Extensions.Http.Resilience) |
| `MQTTnet` | `5.2.0.1603` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/MQTTnet) |
| `NATS.Net` | `3.0.1` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/NATS.Net) |
| `NetMQ` | `4.0.4.2` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/NetMQ) |
| `NJsonSchema.Annotations` | `11.6.1` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/NJsonSchema.Annotations) |
| `OpenTelemetry.Api` | `1.17.0` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/OpenTelemetry.Api) |
| `RabbitMQ.Client` | `7.2.1` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/RabbitMQ.Client) |
| `StackExchange.Redis` | `3.0.17` | External dependency used by the repository. | [Registry](https://www.nuget.org/packages/StackExchange.Redis) |

## Diagrams

### Component overview

```mermaid
graph TD
  Current["NATS.SharedKernel"]
  Current --> T0["AbstractNatsFeevidersConfiguration"]
  Current --> T1["JsonNatsDeserializer"]
  Current --> T2["JsonNatsSerializer"]
  Current --> T3["JsonNatsSerializerRegistry"]
  Current --> T4["MessagingType"]
```

The diagram shows the direct components documented by the **NATS.SharedKernel** area.

## Examples

Start with `AbstractNatsFeevidersConfiguration` as the primary entry point for this folder, then follow its linked contracts and collaborators.

## See Also

- [Parent area](../README.md)
- [Feeders.NATS](../Feeders.NATS/README.md)
- [Providers.DotNet.NATS](../Providers.DotNet.NATS/README.md)

[↑ Back to top](#contents)
