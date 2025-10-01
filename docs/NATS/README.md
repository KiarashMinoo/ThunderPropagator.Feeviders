# NATS Integration

## Contents

- [Overview](#overview)
- [Files](#files)
- [Types & Members](#types--members)
- [Cloud-Native Messaging Features](#cloud-native-messaging-features)
- [Performance Notes](#performance-notes)
- [RapidStreamer Dependencies](#rapidstreamer-dependencies)
- [Examples](#examples)
- [See Also](#see-also)

[↑ Back to top](#contents)

## Overview

High-performance cloud-native NATS integration supporting both core NATS messaging and JetStream persistence. Provides message consumption (Feeders) and publishing (Providers) with excellent performance characteristics designed for microservices architectures and distributed systems.

Designed for cloud-native environments with throughput capabilities up to 750K messages/second. Features include subject-based routing, queue groups for load balancing, JetStream for persistence and guaranteed delivery, and comprehensive distributed tracing support.

Key capabilities include lightweight messaging patterns, subject-based pub/sub, stream processing with JetStream, and seamless integration with NATS ecosystem including NATS Services and Key-Value stores.

[↑ Back to top](#contents)

## Files

| File | Primary type(s) | LOC (approx) | Responsibility |
|------|----------------|---------------|----------------|
| **RapidStreamer.Feeders.NATS** | | | |
| `NatsFeeder.cs` | NatsFeeder<> | 110 | NATS message consumption with core/JetStream support |
| `NatsFeederMessage.cs` | NatsFeederMessage | 5 | Base message contract for NATS consumption |
| `NatsFeederConfiguration.cs` | NatsFeederConfiguration | 100 | Consumer configuration with NATS settings |
| `NatsFeederExtensions.cs` | NatsFeederExtensions | 55 | Dependency injection and service registration |
| **RapidStreamer.Providers.DotNet.NATS** | | | |
| `NatsProvider.cs` | NatsProvider<> | 95 | NATS message publishing with core/JetStream support |
| `NatsProviderMessage.cs` | NatsProviderMessage | 5 | Base message contract for NATS publishing |
| `NatsProviderConfiguration.cs` | NatsProviderConfiguration | 85 | Producer configuration with NATS settings |
| `NatsProviderExtensions.cs` | NatsProviderExtensions | 25 | Dependency injection and service registration |
| **RapidStreamer.Feeviders.NATS.SharedKernel** | | | |
| `NatsClientFactory.cs` | NatsClientFactory | 65 | NATS client factory with configuration |
| `AbstractNatsFeevidersConfiguration.cs` | AbstractNatsFeevidersConfiguration | 150 | Shared NATS configuration base class |

[↑ Back to top](#contents)

## Types & Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|------|------|---------|---------------------|-------------|
| **Feeders** | | | | |
| `NatsFeeder<TChannel, TMessage, TConfig>` | Class | NATS consumer with core and JetStream support | `IterativeFeeder<>`, `IFeature` | ReceiveAsync, messaging type handling |
| `NatsFeederMessage` | Abstract Class | Base contract for NATS consumed messages | `FeederMessage` | (inheritance only) |
| `NatsFeederConfiguration` | Abstract Class | Consumer configuration with NATS settings | `AbstractNatsFeevidersConfiguration`, `IAbstractFeederConfiguration` | Subject, QueueGroup, MessagingType |
| `NatsFeederExtensions` | Static Class | Service registration extensions for consumers | - | AddNatsFeeder, AddNatsFeederResolver |
| **Providers** | | | | |
| `NatsProvider<TMessage, TConfig>` | Class | NATS producer with core and JetStream support | `AbstractProvider<>` | InternalExecuteAsync |
| `NatsProviderMessage` | Abstract Class | Base contract for NATS published messages | `FeederMessage` | (inheritance only) |
| `NatsProviderConfiguration` | Abstract Class | Producer configuration with NATS settings | `AbstractNatsFeevidersConfiguration` | Subject, ReplyTo, MessagingType |
| `NatsProviderExtensions` | Static Class | Service registration extensions for producers | - | AddNatsProvider |
| **Shared Kernel** | | | | |
| `NatsClientFactory` | Static Class | NATS client factory with configuration | - | CreateClient |
| `AbstractNatsFeevidersConfiguration` | Abstract Class | Shared NATS configuration base | `ServiceConfiguration` | Connection settings, authentication, TLS |

[↑ Back to top](#contents)

### NatsFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>

- **Kind**: Internal generic class
- **Namespace**: `RapidStreamer.Feeders.NATS`
- **Inherits**: `IterativeFeeder<TChannel, TNatsFeederMessage, TNatsFeederConfiguration>`, `IFeature`
- **Attributes**: `IsAvailableOnDemo`, Internal visibility, sealed in Release builds

**Key Properties**:
- `_client : INatsClient` — NATS.Net client instance
- `_natsJsConsumer : INatsJSConsumer?` — JetStream consumer for persistent messaging

**Key Methods**:
- `ReceiveAsync(CancellationToken) : IAsyncEnumerable<FeederReceivedMessage<TNatsFeederMessage>>` — Async message consumption from subjects
- Dual-mode messaging support (Core NATS and JetStream)

**Messaging Types**:
- **Basic**: Core NATS pub/sub with subject subscription
- **JetStream**: Persistent messaging with stream and consumer configuration

**Message Processing**:
- Subject-based message routing
- Queue group support for load balancing
- Distributed tracing context extraction from headers
- Automatic acknowledgment for JetStream messages

**Usage Recipe**:
```csharp
// Define message type
public class EventMessage : NatsFeederMessage
{
    public string EventType { get; set; }
    public string Payload { get; set; }
    public DateTime Timestamp { get; set; }
}

// Define configuration
public class EventFeederConfig : NatsFeederConfiguration
{
    // NATS configuration properties inherited
}

// Register feeder
services.AddNatsFeeder<EventChannel, EventMessage, EventFeederConfig>(
    configuration, "Messaging:NATS:EventFeeder");

// Use feeder resolver
app.UseNatsFeederResolver<EventChannel, EventMessage, EventFeederConfig>(
    channelKey, feederConfiguration);
```

[↑ Back to top](#contents)

### NatsFeederConfiguration

- **Kind**: Public abstract class
- **Namespace**: `RapidStreamer.Feeders.NATS`
- **Inherits**: `AbstractNatsFeevidersConfiguration`, `IAbstractFeederConfiguration`
- **Attributes**: Abstract base for consumer configurations

**Key Properties**:
- **Core Configuration**:
  - `Subject : string` — Required NATS subject to subscribe to
  - `QueueGroup : string?` — Optional queue group for load balancing
  - `MaxMsgs : int?` — Maximum messages to consume
  - `Timeout : TimeSpan?` — Subscription timeout
  - `MessagingType : MessagingType` — Core NATS or JetStream

- **JetStream Configuration**:
  - `StreamName : string?` — JetStream stream name
  - `ConsumerConfig : ConsumerConfig?` — JetStream consumer configuration
  - `NatsSvcConfig : NatsSvcConfig?` — NATS Services configuration

- **Inherited from AbstractNatsFeevidersConfiguration**:
  - `Url : string` — NATS server URL (default: "nats://localhost:4222")
  - `Name : string` — Client name
  - `AuthOpts : NatsAuthOpts` — Authentication options
  - `TlsOpts : NatsTlsOpts` — TLS configuration

**NATS Features**:
- Full NATS.Net client configuration support
- JetStream persistence and delivery guarantees
- Authentication and security options
- Performance tuning parameters

[↑ Back to top](#contents)

### NatsProvider<TNatsProviderMessage, TNatsProviderConfiguration>

- **Kind**: Internal generic class
- **Namespace**: `RapidStreamer.Providers.DotNet.NATS`
- **Inherits**: `AbstractProvider<TNatsProviderMessage, TNatsProviderConfiguration>`
- **Attributes**: Internal visibility, sealed in Release builds

**Key Properties**:
- `_client : INatsClient` — NATS.Net client instance
- `_jetStreamContext : INatsJSContext?` — JetStream context for persistent messaging

**Key Methods**:
- `InternalExecuteAsync(TNatsProviderMessage, CancellationToken) : Task` — Publish message to NATS subject

**Messaging Type Support**:
- **Basic**: Core NATS fire-and-forget messaging
- **JetStream**: Persistent messaging with acknowledgments and delivery guarantees

**Publishing Features**:
- Subject-based message routing
- Optional reply-to subjects for request-response patterns
- Message headers for distributed tracing
- JetStream publish options and acknowledgments

**Distributed Tracing**:
- Automatic `ActivityContext` header injection
- `Baggage` propagation for correlation
- Error logging with subject context

**Usage Recipe**:
```csharp
// Define message type
public class CommandMessage : NatsProviderMessage
{
    public string CommandType { get; set; }
    public string Target { get; set; }
    public object Parameters { get; set; }
}

// Define configuration
public class CommandProviderConfig : NatsProviderConfiguration
{
    // Producer properties inherited
}

// Register provider
services.AddNatsProvider<CommandMessage, CommandProviderConfig>(
    configuration, "Messaging:NATS:CommandProvider");

// Use provider
public class CommandService
{
    private readonly IProvider<CommandMessage> _provider;
    
    public CommandService(IProvider<CommandMessage> provider)
    {
        _provider = provider;
    }
    
    public async Task ExecuteCommandAsync(string commandType, string target, object parameters)
    {
        await _provider.ExecuteAsync(new CommandMessage 
        { 
            CommandType = commandType,
            Target = target,
            Parameters = parameters
        });
    }
}
```

[↑ Back to top](#contents)

### NatsProviderConfiguration

- **Kind**: Public abstract class
- **Namespace**: `RapidStreamer.Providers.DotNet.NATS`
- **Inherits**: `AbstractNatsFeevidersConfiguration`
- **Attributes**: Abstract base for producer configurations

**Key Properties**:
- **Publishing Configuration**:
  - `Subject : string` — Required NATS subject to publish to
  - `ReplyTo : string?` — Optional reply-to subject for request-response
  - `MessagingType : MessagingType` — Core NATS or JetStream

- **JetStream Configuration**:
  - `StreamConfig : StreamConfig?` — JetStream stream configuration
  - `NatsJSPubOpts : NatsJSPubOpts?` — JetStream publish options

- **Inherited Configuration**: Connection, authentication, and client settings from base class

**Publishing Modes**:
- **Fire-and-Forget**: Core NATS with minimal overhead
- **Guaranteed Delivery**: JetStream with persistence and acknowledgments

[↑ Back to top](#contents)

## Cloud-Native Messaging Features

### Core NATS Features

| Feature | Description | Use Case |
|---------|-------------|----------|
| **Subject-Based Routing** | Hierarchical subject namespace (e.g., `orders.us.west`) | Message routing and filtering |
| **Queue Groups** | Load balancing across multiple consumers | Horizontal scaling |
| **Request-Response** | Built-in request-reply pattern | Synchronous communication |
| **Wildcard Subscriptions** | `*` and `>` wildcards for flexible routing | Event aggregation |

### JetStream Persistence

| Feature | Description | Benefit |
|---------|-------------|---------|
| **Stream Storage** | Persistent message storage | Message durability |
| **Consumer Delivery** | At-least-once, exactly-once delivery | Reliability guarantees |
| **Message Deduplication** | Automatic duplicate detection | Data integrity |
| **Retention Policies** | Time, count, and size-based retention | Storage management |

### Authentication & Security

- **Token Authentication**: JWT tokens for secure access
- **Username/Password**: Basic authentication support
- **TLS Encryption**: Secure transport layer
- **NKEY Authentication**: NATS-specific key-based authentication
- **Credentials Files**: File-based credential management

### Monitoring & Observability

- **Built-in Metrics**: Connection and message statistics
- **Distributed Tracing**: OpenTelemetry integration
- **Health Checks**: Connection and stream health monitoring
- **Logging**: Comprehensive error and debug logging

[↑ Back to top](#contents)

## Performance Notes

### Throughput Characteristics

- **Peak Throughput**: 750K messages/second (Core NATS)
- **JetStream Throughput**: 500K messages/second (with persistence)
- **Latency**: <1ms end-to-end for Core NATS
- **Memory**: Minimal memory footprint with efficient client
- **Network**: Optimal wire protocol with binary efficiency

### Optimization Recommendations

1. **High-Throughput Core NATS**:
   ```csharp
   public class HighThroughputConfig : NatsProviderConfiguration
   {
       public HighThroughputConfig()
       {
           Subject = "events.high-volume";
           MessagingType = MessagingType.Basic;
           
           // Client optimizations
           Url = "nats://nats-cluster:4222";
           Name = "high-throughput-producer";
           Echo = false;           // Disable echo for producers
           Verbose = false;        // Minimize protocol overhead
       }
   }
   ```

2. **Reliable JetStream Configuration**:
   ```csharp
   public class ReliableConfig : NatsProviderConfiguration
   {
       public ReliableConfig()
       {
           Subject = "orders.processing";
           MessagingType = MessagingType.JetStream;
           
           // JetStream stream configuration
           StreamConfig = new StreamConfig
           {
               Name = "ORDERS",
               Subjects = new[] { "orders.*" },
               Retention = StreamConfigRetention.Workqueue,
               Storage = StreamConfigStorage.File
           };
           
           // Publish options for reliability
           NatsJSPubOpts = new NatsJSPubOpts
           {
               WaitUntilPersisted = true
           };
       }
   }
   ```

3. **Consumer Optimization**:
   ```csharp
   public class OptimizedConsumerConfig : NatsFeederConfiguration
   {
       public OptimizedConsumerConfig()
       {
           Subject = "events.*";
           QueueGroup = "event-processors";  // Load balancing
           MaxMsgs = 1000;                   // Batch processing
           
           // JetStream consumer configuration
           ConsumerConfig = new ConsumerConfig
           {
               DurableName = "event-processor",
               AckPolicy = ConsumerConfigAckPolicy.Explicit,
               MaxDeliver = 3,
               AckWait = TimeSpan.FromSeconds(30)
           };
       }
   }
   ```

[↑ Back to top](#contents)

## RapidStreamer Dependencies

| Package | Version | Description | Links |
|---------|---------|-------------|-------|
| **Core Dependencies** | | | |
| RapidStreamer.Feeders.SharedKernel | 1.0.76+ | Feeder base classes and interfaces | [SharedKernel](../SharedKernel/README.md#rapidstreamer-dependencies) |
| RapidStreamer.Providers.DotNet.SharedKernel | 1.0.76+ | Provider base classes and serialization | [SharedKernel](../SharedKernel/README.md#rapidstreamer-dependencies) |
| **NATS Packages** | | | |
| RapidStreamer.Feeders.NATS | 1.0.78+ | NATS message consumption and JetStream | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| RapidStreamer.Providers.DotNet.NATS | 1.0.78+ | NATS message publishing and JetStream | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| RapidStreamer.Feeviders.NATS.SharedKernel | 1.0.78+ | NATS shared configuration and client factory | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |

### External Dependencies

| Package | Version | Purpose | Documentation |
|---------|---------|---------|---------------|
| NATS.Client.Core | 2.4.0+ | Core NATS .NET client | [NATS .NET Docs](https://docs.nats.io/using-nats/developer/connecting/csharp) |
| NATS.Client.JetStream | 2.4.0+ | JetStream persistence and streaming | [JetStream Docs](https://docs.nats.io/nats-concepts/jetstream) |
| NATS.Client.Services | 2.4.0+ | NATS Services framework | [Services Docs](https://docs.nats.io/using-nats/developer/services) |
| NATS.Net | 2.4.0+ | Additional NATS utilities | [NATS Utilities](https://github.com/nats-io/nats.net.v2) |

[↑ Back to top](#contents)

## Examples

### Microservices Event Bus

```csharp
// Configuration (appsettings.json)
{
  "Messaging": {
    "NATS": {
      "EventProducer": {
        "Url": "nats://nats.company.com:4222",
        "Subject": "microservices.events",
        "MessagingType": "Basic",
        "Name": "order-service-producer"
      }
    }
  }
}

// Message definition
public class MicroserviceEvent : NatsProviderMessage
{
    public string ServiceName { get; set; }
    public string EventType { get; set; }
    public string EntityId { get; set; }
    public object EventData { get; set; }
    public DateTime Timestamp { get; set; }
}

// Configuration class
public class EventProducerConfig : NatsProviderConfiguration { }

// Registration
services.AddNatsProvider<MicroserviceEvent, EventProducerConfig>(
    configuration, "Messaging:NATS:EventProducer");

// Usage
public class OrderService
{
    private readonly IProvider<MicroserviceEvent> _eventProvider;
    
    public OrderService(IProvider<MicroserviceEvent> eventProvider)
    {
        _eventProvider = eventProvider;
    }
    
    public async Task ProcessOrderAsync(Order order)
    {
        // Process order logic...
        
        // Publish event
        await _eventProvider.ExecuteAsync(new MicroserviceEvent
        {
            ServiceName = "order-service",
            EventType = "OrderCreated",
            EntityId = order.Id,
            EventData = new { order.CustomerId, order.Amount },
            Timestamp = DateTime.UtcNow
        });
    }
}
```

### JetStream Reliable Processing

```csharp
// Configuration (appsettings.json)
{
  "Messaging": {
    "NATS": {
      "ReliableConsumer": {
        "Url": "nats://nats-cluster.company.com:4222",
        "Subject": "orders.processing",
        "MessagingType": "JetStream",
        "StreamName": "ORDERS",
        "QueueGroup": "order-processors",
        "IsEnabled": true
      }
    }
  }
}

// Message definition
public class OrderProcessingMessage : NatsFeederMessage
{
    public string OrderId { get; set; }
    public string CustomerId { get; set; }
    public decimal Amount { get; set; }
    public string Status { get; set; }
    public DateTime ProcessingStarted { get; set; }
}

// Configuration class with JetStream setup
public class ReliableConsumerConfig : NatsFeederConfiguration
{
    public ReliableConsumerConfig()
    {
        // JetStream consumer configuration
        ConsumerConfig = new ConsumerConfig
        {
            DurableName = "order-processor",
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            MaxDeliver = 3,
            AckWait = TimeSpan.FromSeconds(30),
            BackOff = new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10) }
        };
    }
}

// Channel definition
public class OrderProcessingChannel : IChannel
{
    public Guid Key { get; set; }
    public string Name { get; set; } = "OrderProcessingChannel";
}

// Registration
services.AddNatsFeeder<OrderProcessingChannel, OrderProcessingMessage, ReliableConsumerConfig>(
    configuration, "Messaging:NATS:ReliableConsumer");

// Feeder resolution
app.UseNatsFeederResolver<OrderProcessingChannel, OrderProcessingMessage, ReliableConsumerConfig>(
    channelKey, consumerConfiguration);
```

### Request-Response Pattern

```csharp
// Request message
public class OrderStatusRequest : NatsProviderMessage
{
    public string OrderId { get; set; }
    public string RequestId { get; set; }
}

// Configuration with reply-to
public class RequestConfig : NatsProviderConfiguration
{
    public RequestConfig()
    {
        Subject = "orders.status.request";
        ReplyTo = "orders.status.response";
        MessagingType = MessagingType.Basic;
    }
}

// Request service
public class OrderStatusService
{
    private readonly IProvider<OrderStatusRequest> _requestProvider;
    
    public OrderStatusService(IProvider<OrderStatusRequest> requestProvider)
    {
        _requestProvider = requestProvider;
    }
    
    public async Task RequestOrderStatusAsync(string orderId)
    {
        await _requestProvider.ExecuteAsync(new OrderStatusRequest
        {
            OrderId = orderId,
            RequestId = Guid.NewGuid().ToString()
        });
    }
}

// Response consumer configuration
public class ResponseConsumerConfig : NatsFeederConfiguration
{
    public ResponseConsumerConfig()
    {
        Subject = "orders.status.response";
        QueueGroup = "status-response-handlers";
    }
}
```

[↑ Back to top](#contents)

## See Also

- [SharedKernel](../SharedKernel/README.md) - Base interfaces and utilities
- [Kafka](../Kafka/README.md) - Alternative high-throughput streaming
- [RabbitMQ](../RabbitMQ/README.md) - Alternative enterprise messaging
- [Documentation Home](../README.md) - Framework overview and navigation

[↑ Back to top](#contents)

---

**Generated**: October 1, 2025  
**NATS Version**: NATS.Client.Core 2.4.0+  
**RapidStreamer Version**: 1.0.78