# Kafka Integration

## Contents

- [Overview](#overview)
- [Files](#files)
- [Types & Members](#types--members)
- [Serialization & Contracts](#serialization--contracts)
- [Performance Notes](#performance-notes)
- [RapidStreamer Dependencies](#rapidstreamer-dependencies)
- [Examples](#examples)
- [See Also](#see-also)

[↑ Back to top](#contents)

## Overview

High-performance Apache Kafka integration supporting both message consumption (Feeders) and publishing (Providers) with enterprise-grade features including Schema Registry support, multiple serialization formats, and distributed tracing. Designed for high-throughput streaming scenarios with throughput capabilities exceeding 1M messages/second.

Key capabilities include consumer group management, topic-based routing, schema evolution support, and seamless integration with Confluent Schema Registry for Avro and JSON Schema serialization.

[↑ Back to top](#contents)

## Files

| File | Primary type(s) | LOC (approx) | Responsibility |
|------|----------------|---------------|----------------|
| **RapidStreamer.Feeders.Kafka** | | | |
| `KafkaFeeder.cs` | KafkaFeeder<> | 120 | Core message consumption from Kafka topics |
| `KafkaFeederMessage.cs` | KafkaFeederMessage | 5 | Base message contract for Kafka consumption |
| `KafkaFeederConfiguration.cs` | KafkaFeederConfiguration | 100 | Consumer configuration with Kafka-specific settings |
| `KafkaFeederExtensions.cs` | KafkaFeederExtensions | 60 | Dependency injection and service registration |
| `KafkaDeserializers/KafkaJsonDeserializer.cs` | KafkaJsonDeserializer<> | 20 | JSON deserialization for consumed messages |
| `KafkaDeserializers/KafkaNJsonDeserializer.cs` | KafkaNJsonDeserializer<> | 20 | NJson deserialization for consumed messages |
| **RapidStreamer.Providers.DotNet.Kafka** | | | |
| `KafkaProvider.cs` | KafkaProvider<> | 80 | Core message publishing to Kafka topics |
| `KafkaProviderMessage.cs` | KafkaProviderMessage | 10 | Base message contract for Kafka publishing |
| `KafkaProviderConfiguration.cs` | KafkaProviderConfiguration | 60 | Producer configuration with Kafka-specific settings |
| `KafkaProviderExtensions.cs` | KafkaProviderExtensions | 30 | Dependency injection and service registration |
| `KafkaSerializers/KafkaJsonSerializer.cs` | KafkaJsonSerializer<> | 20 | JSON serialization for published messages |
| `KafkaSerializers/KafkaNJsonSerializer.cs` | KafkaNJsonSerializer<> | 20 | NJson serialization for published messages |

[↑ Back to top](#contents)

## Types & Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|------|------|---------|---------------------|-------------|
| **Feeders** | | | | |
| `KafkaFeeder<TChannel, TMessage, TConfig>` | Class | High-performance Kafka consumer implementation | `IterativeFeeder<>` | ReceiveAsync, HandleExceptionAsync |
| `KafkaFeederMessage` | Abstract Class | Base contract for Kafka consumed messages | `FeederMessage` | (inheritance only) |
| `KafkaFeederConfiguration` | Abstract Class | Consumer configuration with Kafka settings | `ConsumerConfig`, `IAbstractFeederConfiguration` | TopicNames, SchemaRegistryUrl, SerializerType |
| `KafkaFeederExtensions` | Static Class | Service registration extensions for consumers | - | AddKafkaFeeder, AddKafkaFeederResolver |
| **Providers** | | | | |
| `KafkaProvider<TMessage, TConfig>` | Class | High-performance Kafka producer implementation | `AbstractProvider<>` | InternalExecuteAsync |
| `KafkaProviderMessage` | Abstract Class | Base contract for Kafka published messages | `FeederMessage` | KafkaProviderKey |
| `KafkaProviderConfiguration` | Abstract Class | Producer configuration with Kafka settings | `ProducerConfig`, `IAbstractProviderConfiguration` | TopicName, SchemaRegistryUrl, SerializerType |
| `KafkaProviderExtensions` | Static Class | Service registration extensions for producers | - | AddKafkaProvider |
| **Serializers** | | | | |
| `KafkaJsonDeserializer<T>` | Class | JSON deserialization for Kafka messages | `AbstractKafkaDeserializer<T>` | InternalDeserializeAsync |
| `KafkaJsonSerializer<T>` | Class | JSON serialization for Kafka messages | `AbstractKafkaSerializer<T>` | InternalSerializeAsync |

[↑ Back to top](#contents)

### KafkaFeeder<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>

- **Kind**: Internal generic class
- **Namespace**: `RapidStreamer.Feeders.Kafka`
- **Inherits**: `IterativeFeeder<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>`
- **Attributes**: Internal visibility, sealed in Release builds

**Key Properties**:
- `_consumer : IConsumer<string, TKafkaFeederMessage>` — Confluent Kafka consumer instance
- `_schemaRegistry : CachedSchemaRegistryClient?` — Lazy-loaded Schema Registry client
- `SchemaRegistryClient : ISchemaRegistryClient` — Schema Registry client accessor

**Key Methods**:
- `ReceiveAsync(CancellationToken) : IAsyncEnumerable<FeederReceivedMessage<TKafkaFeederMessage>>` — Async message consumption from topics
- `HandleExceptionAsync(Exception, CancellationToken) : Task<bool>` — Exception handling with retry logic

**Thread-safety**: Thread-safe consumer operations with proper disposal
**Serialization**: Supports JSON, NJson, Schema Registry (JSON/Avro) formats

**Usage Recipe**:
```csharp
// Define message type
public class OrderMessage : KafkaFeederMessage
{
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
}

// Define configuration
public class OrderFeederConfig : KafkaFeederConfiguration
{
    // Kafka consumer properties inherited
}

// Register feeder
services.AddKafkaFeeder<OrderChannel, OrderMessage, OrderFeederConfig>(
    configuration, "Messaging:Kafka:OrderFeeder");

// Use feeder resolver
app.UseKafkaFeederResolver<OrderChannel, OrderMessage, OrderFeederConfig>(
    channelKey, feederConfiguration);
```

[↑ Back to top](#contents)

### KafkaFeederConfiguration

- **Kind**: Public abstract class
- **Namespace**: `RapidStreamer.Feeders.Kafka`
- **Inherits**: `ConsumerConfig`, `IAbstractFeederConfiguration`
- **Attributes**: Abstract base for consumer configurations

**Key Properties**:
- `IsEnabled : bool` — Feeder activation state
- `Id : Guid` — Unique feeder identifier  
- `TopicNames : string[]` — Kafka topics to consume from
- `SchemaRegistryUrl : string?` — Optional Schema Registry endpoint
- `SerializerType : KafkaSerializerType` — Message serialization format
- `EnrichmentScript : string?` — Optional message enrichment script
- `MetadataReferences : string[]?` — Assembly references for enrichment

**Key Methods**:
- `ToConsumerConfig() : ConsumerConfig` — Convert to Confluent consumer configuration
- `ToProducerConfig() : ProducerConfig` — Convert to producer configuration for enrichment
- `Set(string, string?) : void` — Set configuration property with prefix handling
- `Get(string) : string?` — Get configuration property with prefix handling

**Configuration Overrides**:
- Prefixes internal properties with `-` to avoid conflicts
- `AutoOffsetReset` defaults to `Latest`
- Supports all Confluent Kafka consumer properties

[↑ Back to top](#contents)

### KafkaProvider<TKafkaProviderMessage, TKafkaProviderConfiguration>

- **Kind**: Internal generic class
- **Namespace**: `RapidStreamer.Providers.DotNet.Kafka`
- **Inherits**: `AbstractProvider<TKafkaProviderMessage, TKafkaProviderConfiguration>`
- **Attributes**: Internal visibility, sealed in Release builds

**Key Properties**:
- `_producer : IProducer<string, TKafkaProviderMessage>` — Confluent Kafka producer instance
- `_schemaRegistry : CachedSchemaRegistryClient?` — Lazy-loaded Schema Registry client
- `SchemaRegistryClient : ISchemaRegistryClient` — Schema Registry client accessor

**Key Methods**:
- `InternalExecuteAsync(TKafkaProviderMessage, CancellationToken) : Task` — Publish message to Kafka topic
- `InternalExecuteAsync(byte[], CancellationToken) : Task` — Raw byte publishing (no-op)

**Distributed Tracing**:
- Automatic `ActivityContext` header injection
- `Baggage` propagation for correlation
- Error logging with topic context

**Usage Recipe**:
```csharp
// Define message type
public class OrderMessage : KafkaProviderMessage
{
    public OrderMessage(string orderId) : base(orderId) { }
    public decimal Amount { get; set; }
}

// Define configuration
public class OrderProviderConfig : KafkaProviderConfiguration
{
    // Producer properties inherited
}

// Register provider
services.AddKafkaProvider<OrderMessage, OrderProviderConfig>(
    configuration, "Messaging:Kafka:OrderProvider");

// Use provider
public class OrderService
{
    private readonly IProvider<OrderMessage> _provider;
    
    public OrderService(IProvider<OrderMessage> provider)
    {
        _provider = provider;
    }
    
    public async Task PublishOrderAsync(Order order)
    {
        await _provider.ExecuteAsync(new OrderMessage(order.Id) 
        { 
            Amount = order.Total 
        });
    }
}
```

[↑ Back to top](#contents)

### KafkaProviderConfiguration

- **Kind**: Public abstract class
- **Namespace**: `RapidStreamer.Providers.DotNet.Kafka`
- **Inherits**: `ProducerConfig`, `IAbstractProviderConfiguration`
- **Attributes**: Abstract base for producer configurations

**Key Properties**:
- `TopicName : string` — Required target Kafka topic (required property)
- `SchemaRegistryUrl : string?` — Optional Schema Registry endpoint
- `SerializerType : KafkaSerializerType` — Message serialization format

**Key Methods**:
- `ToProducerConfig() : ProducerConfig` — Convert to Confluent producer configuration
- `Set(string, string?) : void` — Set configuration property with prefix handling
- `Get(string) : string?` — Get configuration property with prefix handling

**Configuration**:
- Supports all Confluent Kafka producer properties
- Internal properties prefixed with `-` to avoid conflicts
- Compatible with Schema Registry authentication

[↑ Back to top](#contents)

## Serialization & Contracts

### Serialization Types

| Type | Format | Schema Registry | Performance | Use Case |
|------|--------|----------------|-------------|----------|
| `Json` | UTF-8 JSON | No | High | Development, human-readable |
| `NJson` | NetJson | No | Highest | Production, maximum throughput |
| `SchemaJson` | JSON Schema | Yes | Medium | Schema evolution, validation |
| `Avro` | Apache Avro | Yes | High | Cross-language, compact binary |

### Message Contracts

**KafkaFeederMessage**:
- Abstract base for all consumed messages
- Inherits from `FeederMessage` (RapidStreamer core)
- No additional properties (pure inheritance)

**KafkaProviderMessage**:
- Abstract base for all published messages  
- Inherits from `FeederMessage` (RapidStreamer core)
- Required `KafkaProviderKey : string` for Kafka partitioning
- Constructor requires key parameter for partition routing

### Schema Registry Integration

- **Lazy Connection**: Schema Registry clients created on-demand
- **Caching**: `CachedSchemaRegistryClient` for performance
- **Authentication**: Supports Schema Registry authentication via configuration
- **Subject Strategy**: Default subject naming follows Confluent standards
- **Evolution**: Full schema evolution support for Avro and JSON Schema

[↑ Back to top](#contents)

## Performance Notes

### Throughput Characteristics

- **Peak Throughput**: 1M+ messages/second (depends on message size and configuration)
- **Latency**: <10ms end-to-end for JSON serialization
- **Memory**: Efficient streaming with minimal memory overhead
- **Batching**: Producer-level batching for optimal throughput

### Optimization Recommendations

1. **Batch Configuration**:
   ```csharp
   public class HighThroughputConfig : KafkaProviderConfiguration
   {
       public HighThroughputConfig()
       {
           LingerMs = 5;          // Small batching delay
           BatchSize = 65536;     // Larger batch size
           CompressionType = CompressionType.Lz4;
       }
   }
   ```

2. **Consumer Performance**:
   ```csharp
   public class OptimizedConsumerConfig : KafkaFeederConfiguration
   {
       public OptimizedConsumerConfig()
       {
           FetchMinBytes = 50000;     // Larger fetch sizes
           FetchWaitMaxMs = 500;      // Batch waiting
           MaxPartitionFetchBytes = 1048576; // 1MB partitions
       }
   }
   ```

3. **Serialization Choice**:
   - Use `NJson` for maximum throughput
   - Use `Avro` for cross-language compatibility
   - Use `SchemaJson` for schema validation requirements

[↑ Back to top](#contents)

## RapidStreamer Dependencies

| Package | Version | Description | Links |
|---------|---------|-------------|-------|
| **Core Dependencies** | | | |
| RapidStreamer.Feeders.SharedKernel | 1.0.76+ | Feeder base classes and interfaces | [SharedKernel](../SharedKernel/README.md#rapidstreamer-dependencies) |
| RapidStreamer.Providers.DotNet.SharedKernel | 1.0.76+ | Provider base classes and serialization | [SharedKernel](../SharedKernel/README.md#rapidstreamer-dependencies) |
| **Kafka Packages** | | | |
| RapidStreamer.Feeders.Kafka | 1.0.78+ | Kafka message consumption and consumer groups | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| RapidStreamer.Providers.DotNet.Kafka | 1.0.78+ | Kafka message publishing and producer pools | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |

### External Dependencies

| Package | Version | Purpose | Documentation |
|---------|---------|---------|---------------|
| Confluent.Kafka | 2.5.0+ | Apache Kafka .NET client | [Confluent Docs](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html) |
| Confluent.SchemaRegistry | 2.5.0+ | Schema Registry integration | [Schema Registry Docs](https://docs.confluent.io/platform/current/schema-registry/index.html) |
| Confluent.SchemaRegistry.Serdes.Avro | 2.5.0+ | Avro serialization support | [Avro Serdes](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html) |
| Confluent.SchemaRegistry.Serdes.Json | 2.5.0+ | JSON Schema serialization | [JSON Schema Serdes](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html) |

[↑ Back to top](#contents)

## Examples

### Basic Producer Setup

```csharp
// Configuration (appsettings.json)
{
  "Messaging": {
    "Kafka": {
      "OrderProducer": {
        "BootstrapServers": "localhost:9092",
        "TopicName": "order-events",
        "SerializerType": "Json",
        "Acks": "all",
        "Retries": 3
      }
    }
  }
}

// Message definition
public class OrderEvent : KafkaProviderMessage
{
    public OrderEvent(string orderId) : base(orderId) { }
    
    public DateTime OrderDate { get; set; }
    public decimal Amount { get; set; }
    public string Status { get; set; }
}

// Configuration class
public class OrderProducerConfig : KafkaProviderConfiguration { }

// Registration
services.AddKafkaProvider<OrderEvent, OrderProducerConfig>(
    configuration, "Messaging:Kafka:OrderProducer");

// Usage
public class OrderService
{
    private readonly IProvider<OrderEvent> _provider;
    
    public OrderService(IProvider<OrderEvent> provider)
    {
        _provider = provider;
    }
    
    public async Task ProcessOrderAsync(Order order)
    {
        await _provider.ExecuteAsync(new OrderEvent(order.Id)
        {
            OrderDate = order.CreatedAt,
            Amount = order.Total,
            Status = "Created"
        });
    }
}
```

### Consumer with Schema Registry

```csharp
// Configuration (appsettings.json)
{
  "Messaging": {
    "Kafka": {
      "OrderConsumer": {
        "BootstrapServers": "localhost:9092",
        "GroupId": "order-processor",
        "TopicNames": ["order-events", "payment-events"],
        "AutoOffsetReset": "Latest",
        "SchemaRegistryUrl": "http://localhost:8081",
        "SerializerType": "Avro",
        "IsEnabled": true
      }
    }
  }
}

// Message definition
public class OrderEvent : KafkaFeederMessage
{
    public string OrderId { get; set; }
    public DateTime OrderDate { get; set; }
    public decimal Amount { get; set; }
}

// Configuration class
public class OrderConsumerConfig : KafkaFeederConfiguration { }

// Channel definition
public class OrderChannel : IChannel
{
    public Guid Key { get; set; }
    public string Name { get; set; } = "OrderChannel";
}

// Registration
services.AddKafkaFeeder<OrderChannel, OrderEvent, OrderConsumerConfig>(
    configuration, "Messaging:Kafka:OrderConsumer");

// Feeder resolution
app.UseKafkaFeederResolver<OrderChannel, OrderEvent, OrderConsumerConfig>(
    channelKey, consumerConfiguration);
```

### High-Performance Configuration

```csharp
// High-throughput producer configuration
public class HighThroughputProducerConfig : KafkaProviderConfiguration
{
    public HighThroughputProducerConfig()
    {
        // Producer optimizations
        LingerMs = 5;                        // Small batching delay
        BatchSize = 65536;                   // 64KB batches
        CompressionType = CompressionType.Lz4; // Fast compression
        Acks = Acks.Leader;                  // Leader acknowledgment only
        EnableIdempotence = true;            // Exactly-once semantics
        MaxInFlight = 5;                     // Pipeline requests
        
        // Serialization
        SerializerType = KafkaSerializerType.NJson; // Fastest serialization
    }
}

// Optimized consumer configuration  
public class HighThroughputConsumerConfig : KafkaFeederConfiguration
{
    public HighThroughputConsumerConfig()
    {
        // Consumer optimizations
        FetchMinBytes = 50000;               // Larger fetch sizes
        FetchWaitMaxMs = 500;                // Batch waiting time
        MaxPartitionFetchBytes = 1048576;    // 1MB per partition
        SessionTimeoutMs = 30000;            // Longer session timeout
        HeartbeatIntervalMs = 10000;         // Less frequent heartbeats
        
        // Offset management
        EnableAutoCommit = true;
        AutoCommitIntervalMs = 5000;         // Commit every 5 seconds
        
        // Serialization
        SerializerType = KafkaSerializerType.NJson; // Match producer
    }
}
```

[↑ Back to top](#contents)

## See Also

- [SharedKernel](../SharedKernel/README.md) - Base interfaces and utilities
- [RabbitMQ](../RabbitMQ/README.md) - Alternative message broker implementation
- [NATS](../NATS/README.md) - Cloud-native messaging alternative
- [Documentation Home](../README.md) - Framework overview and navigation

[↑ Back to top](#contents)

---

**Generated**: October 1, 2025  
**Kafka Version**: Confluent.Kafka 2.5.0+  
**RapidStreamer Version**: 1.0.78