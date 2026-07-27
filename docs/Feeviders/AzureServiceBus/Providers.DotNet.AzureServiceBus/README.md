# Providers.DotNet.AzureServiceBus

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

The **Providers.DotNet.AzureServiceBus** area groups 4 documented types, including `AzureServiceBusProviderExtensions`, `IServiceBusBatchProvider`, `ServiceBusProviderConfiguration`, `ServiceBusProviderMessage`. It provides the contracts and implementation used by this part of ThunderPropagator.Feeviders.

## Files

| File | Primary type(s)/symbol(s) | LOC (approx.) | Responsibility |
|---|---|---:|---|
| `AssemblyInfo.cs` | — | 7 | Contains the assembly info implementation or configuration. |
| `AzureServiceBusProviderExtensions.cs` | `AzureServiceBusProviderExtensions` | 38 | Defines AzureServiceBusProviderExtensions and its related behavior. |
| `IServiceBusBatchProvider.cs` | `IServiceBusBatchProvider` | 6 | Defines IServiceBusBatchProvider and its related behavior. |
| `ServiceBusProvider.cs` | `ServiceBusProvider`, `Log` | 110 | Defines ServiceBusProvider, Log and its related behavior. |
| `ServiceBusProviderConfiguration.cs` | `ServiceBusProviderConfiguration` | 25 | Defines ServiceBusProviderConfiguration and its related behavior. |
| `ServiceBusProviderMessage.cs` | `ServiceBusProviderMessage` | 5 | Defines ServiceBusProviderMessage and its related behavior. |
| `ThunderPropagator.Providers.DotNet.AzureServiceBus.csproj` | — | 8 | Defines project build targets, dependencies, and package metadata. |

## Types and Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|---|---|---|---|---|
| [`AzureServiceBusProviderExtensions`](#azureservicebusproviderextensions) | class | Represents the AzureServiceBusProviderExtensions class. | — | — |
| [`IServiceBusBatchProvider`](#iservicebusbatchprovider) | interface | Represents the IServiceBusBatchProvider interface. | — | — |
| [`ServiceBusProviderConfiguration`](#servicebusproviderconfiguration) | class | Represents the ServiceBusProviderConfiguration class. | `AbstractProviderConfiguration, IAzureServiceBusFeeviderConfiguration` | — |
| [`ServiceBusProviderMessage`](#servicebusprovidermessage) | class | Represents the ServiceBusProviderMessage class. | `FeederMessage;` | — |

### AzureServiceBusProviderExtensions

- **Kind:** class
- **Namespace:** `ThunderPropagator.Providers.DotNet.AzureServiceBus`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the AzureServiceBusProviderExtensions class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve AzureServiceBusProviderExtensions from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### IServiceBusBatchProvider

- **Kind:** interface
- **Namespace:** `ThunderPropagator.Providers.DotNet.AzureServiceBus`
- **Inherits/implements:** None declared
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the IServiceBusBatchProvider interface.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve IServiceBusBatchProvider from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### ServiceBusProviderConfiguration

- **Kind:** class
- **Namespace:** `ThunderPropagator.Providers.DotNet.AzureServiceBus`
- **Inherits/implements:** `AbstractProviderConfiguration, IAzureServiceBusFeeviderConfiguration`
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the ServiceBusProviderConfiguration class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve ServiceBusProviderConfiguration from the configured service container or construct it with its declared dependencies.
```

[↑ Back to top](#contents)

### ServiceBusProviderMessage

- **Kind:** class
- **Namespace:** `ThunderPropagator.Providers.DotNet.AzureServiceBus`
- **Inherits/implements:** `FeederMessage;`
- **Attributes:** None detected
- **Key members:** Refer to the API surface in the source package
- **Summary:** Represents the ServiceBusProviderMessage class.
- **Thread safety:** Follow the lifetime and concurrency guarantees of the owning component; no additional guarantee is inferred.

**Usage recipe**

```csharp
// Resolve ServiceBusProviderMessage from the configured service container or construct it with its declared dependencies.
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
  Current["Providers.DotNet.AzureServiceBus"]
  Current --> T0["AzureServiceBusProviderExtensions"]
  Current --> T1["IServiceBusBatchProvider"]
  Current --> T2["ServiceBusProviderConfiguration"]
  Current --> T3["ServiceBusProviderMessage"]
```

The diagram shows the direct components documented by the **Providers.DotNet.AzureServiceBus** area.

## Examples

Start with `AzureServiceBusProviderExtensions` as the primary entry point for this folder, then follow its linked contracts and collaborators.

## See Also

- [Parent area](../README.md)
- [AzureServiceBus.SharedKernel](../AzureServiceBus.SharedKernel/README.md)
- [Feeders.AzureServiceBus](../Feeders.AzureServiceBus/README.md)

[↑ Back to top](#contents)
