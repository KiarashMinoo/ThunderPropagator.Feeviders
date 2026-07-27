# Providers.DotNet.AwsSqs

## Contents

- [Overview](#overview)
- [Files](#files)
- [Types and Members](#types-and-members)
- [Serialization and Contracts](#serialization-and-contracts)
- [Performance Notes](#performance-notes)
- [Package Dependencies](#package-dependencies)
- [Diagrams](#diagrams)
- [Examples](#examples)
- [See Also](#see-also)

## Overview

The **Providers.DotNet.AwsSqs** area groups 6 documented types, including `SnsProviderConfiguration`, `SnsProviderExtensions`, `SnsProviderMessage`, `SqsProviderConfiguration`, `SqsProviderExtensions`. It provides the contracts and implementation used by this part of ThunderPropagator.Feeviders.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `AssemblyInfo.cs` | — | 6 | Contains the assembly info implementation or configuration. |
| `SnsMessageAttributeBuilder.cs` | `SnsMessageAttributeBuilder` | 32 | Defines SnsMessageAttributeBuilder and its related behavior. |
| `SnsProvider.cs` | `SnsProvider`, `Log` | 62 | Defines SnsProvider, Log and its related behavior. |
| `SnsProviderConfiguration.cs` | `SnsProviderConfiguration` | 38 | Defines SnsProviderConfiguration and its related behavior. |
| `SnsProviderExtensions.cs` | `SnsProviderExtensions` | 27 | Defines SnsProviderExtensions and its related behavior. |
| `SnsProviderMessage.cs` | `SnsProviderMessage` | 6 | Defines SnsProviderMessage and its related behavior. |
| `SqsMessageAttributeBuilder.cs` | `SqsMessageAttributeBuilder` | 32 | Defines SqsMessageAttributeBuilder and its related behavior. |
| `SqsProvider.cs` | `SqsProvider`, `Log` | 62 | Defines SqsProvider, Log and its related behavior. |
| `SqsProviderConfiguration.cs` | `SqsProviderConfiguration` | 38 | Defines SqsProviderConfiguration and its related behavior. |
| `SqsProviderExtensions.cs` | `SqsProviderExtensions` | 27 | Defines SqsProviderExtensions and its related behavior. |
| `SqsProviderMessage.cs` | `SqsProviderMessage` | 6 | Defines SqsProviderMessage and its related behavior. |
| `ThunderPropagator.Providers.DotNet.AwsSqs.csproj` | — | 8 | Defines project build targets, dependencies, and package metadata. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`SnsProviderConfiguration`](#snsproviderconfiguration) | class | Represents the SnsProviderConfiguration class. | `AbstractProviderConfiguration, IAwsFeeviderConfiguration` | — |
| [`SnsProviderExtensions`](#snsproviderextensions) | class | Represents the SnsProviderExtensions class. | — | — |
| [`SnsProviderMessage`](#snsprovidermessage) | class | Represents the SnsProviderMessage class. | `FeederMessage;` | — |
| [`SqsProviderConfiguration`](#sqsproviderconfiguration) | class | Represents the SqsProviderConfiguration class. | `AbstractProviderConfiguration, IAwsFeeviderConfiguration` | — |
| [`SqsProviderExtensions`](#sqsproviderextensions) | class | Represents the SqsProviderExtensions class. | — | — |
| [`SqsProviderMessage`](#sqsprovidermessage) | class | Represents the SqsProviderMessage class. | `FeederMessage;` | — |

### SnsProviderConfiguration

- **Kind:** class
- **Namespace:** `ThunderPropagator.Providers.DotNet.AwsSqs`
- **Inherits/implements:** `AbstractProviderConfiguration, IAwsFeeviderConfiguration`
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the SnsProviderConfiguration class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve SnsProviderConfiguration from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### SnsProviderExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.Providers.DotNet.AwsSqs`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the SnsProviderExtensions class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve SnsProviderExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### SnsProviderMessage

- **Kind:** class
- **Namespace:** `ThunderPropagator.Providers.DotNet.AwsSqs`
- **Inherits/implements:** `FeederMessage;`
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the SnsProviderMessage class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve SnsProviderMessage from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### SqsProviderConfiguration

- **Kind:** class
- **Namespace:** `ThunderPropagator.Providers.DotNet.AwsSqs`
- **Inherits/implements:** `AbstractProviderConfiguration, IAwsFeeviderConfiguration`
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the SqsProviderConfiguration class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve SqsProviderConfiguration from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### SqsProviderExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.Providers.DotNet.AwsSqs`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the SqsProviderExtensions class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve SqsProviderExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### SqsProviderMessage

- **Kind:** class
- **Namespace:** `ThunderPropagator.Providers.DotNet.AwsSqs`
- **Inherits/implements:** `FeederMessage;`
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the SqsProviderMessage class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve SqsProviderMessage from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

## Serialization and Contracts

Serialization behavior is part of the public wire or persistence contract in this area. Preserve field names, ordering rules, content negotiation, and backward-compatibility expectations when changing these types.

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
  Current["Providers.DotNet.AwsSqs"]
  Current --> T0["SnsProviderConfiguration"]
  Current --> T1["SnsProviderExtensions"]
  Current --> T2["SnsProviderMessage"]
  Current --> T3["SqsProviderConfiguration"]
  Current --> T4["SqsProviderExtensions"]
  Current --> T5["SqsProviderMessage"]
```

The diagram shows the direct components documented by the **Providers.DotNet.AwsSqs** area.

## Examples

Start with `SnsProviderConfiguration` as the primary entry point for this folder, then follow its linked contracts and collaborators.

## See Also

- [Parent area](../README.md)
- [AwsSqs.SharedKernel](../AwsSqs.SharedKernel/README.md)
- [Feeders.AwsSqs](../Feeders.AwsSqs/README.md)

[↑ Back to top](#contents)
