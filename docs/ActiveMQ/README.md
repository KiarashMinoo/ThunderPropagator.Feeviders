# ActiveMQ Integration

## Contents

- [Overview](#overview)
- [Files](#files)
- [Types & Members](#types--members)
- [JMS Enterprise Features](#jms-enterprise-features)
- [Performance Notes](#performance-notes)
- [RapidStreamer Dependencies](#rapidstreamer-dependencies)
- [Examples](#examples)
- [See Also](#see-also)

[↑ Back to top](#contents)

## Overview

Enterprise-grade Apache ActiveMQ integration supporting JMS (Java Message Service) protocol for reliable enterprise messaging. Provides both message consumption (Feeders) and publishing (Providers) with comprehensive JMS features including message persistence, transactional messaging, and enterprise-level delivery guarantees.

Designed for enterprise environments requiring robust message delivery with throughput capabilities up to 75K messages/second. Features include connection pooling, automatic reconnection, message prioritization, and comprehensive distributed tracing support.

Key capabilities include JMS queue and topic support, message persistence options, delivery mode configuration, and seamless integration with enterprise message brokers.

[↑ Back to top](#contents)

## Files

| File | Primary type(s) | LOC (approx) | Responsibility |
|------|----------------|---------------|----------------|
| **RapidStreamer.Feeders.ActiveMQ** | | | |
| `ActiveMQFeeder.cs` | ActiveMQFeeder<> | 90 | JMS message consumption from ActiveMQ brokers |
| `ActiveMQFeederMessage.cs` | ActiveMQFeederMessage | 5 | Base message contract for ActiveMQ consumption |
| `ActiveMQFeederConfiguration.cs` | ActiveMQFeederConfiguration | 120 | Consumer configuration with JMS settings |
| `ActiveMQFeederExtensions.cs` | ActiveMQFeederExtensions | 55 | Dependency injection and service registration |
| **RapidStreamer.Providers.DotNet.ActiveMQ** | | | |
| `ActiveMQProvider.cs` | ActiveMQProvider<> | 85 | JMS message publishing to ActiveMQ brokers |
| `ActiveMQProviderMessage.cs` | ActiveMQProviderMessage | 5 | Base message contract for ActiveMQ publishing |
| `ActiveMQProviderConfiguration.cs` | ActiveMQProviderConfiguration | 190 | Producer configuration with JMS settings |
| `ActiveMQProviderExtensions.cs` | ActiveMQProviderExtensions | 25 | Dependency injection and service registration |
| **RapidStreamer.Feeviders.ActiveMQ.SharedKernel** | | | |
| `ActiveMQFeeviderConnectionFactory.cs` | ActiveMQFeeviderConnectionFactory | 45 | Connection factory for ActiveMQ connections |
| `IActiveMQFeeviderConfiguration.cs` | IActiveMQFeeviderConfiguration | 15 | Shared configuration interface |

[↑ Back to top](#contents)

## Types & Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|------|------|---------|---------------------|-------------|
| **Feeders** | | | | |
| `ActiveMQFeeder<TChannel, TMessage, TConfig>` | Class | JMS consumer implementation with event-driven processing | `DelegativeFeeder<>`, `IFeature` | Listener event handling |
| `ActiveMQFeederMessage` | Abstract Class | Base contract for ActiveMQ consumed messages | `FeederMessage` | (inheritance only) |
| `ActiveMQFeederConfiguration` | Abstract Class | Consumer configuration with JMS settings | `AbstractFeederConfiguration`, `IActiveMQFeeviderConfiguration` | BrokerUri, Queue, UseCompression |
| `ActiveMQFeederExtensions` | Static Class | Service registration extensions for consumers | - | AddActiveMQFeeder, AddActiveMQFeederResolver |
| **Providers** | | | | |
| `ActiveMQProvider<TMessage, TConfig>` | Class | JMS producer implementation with connection management | `AbstractProvider<>` | InternalExecuteAsync |
| `ActiveMQProviderMessage` | Abstract Class | Base contract for ActiveMQ published messages | `FeederMessage` | (inheritance only) |
| `ActiveMQProviderConfiguration` | Abstract Class | Producer configuration with JMS settings | `AbstractProviderConfiguration`, `IActiveMQFeeviderConfiguration` | DeliveryMode, TimeToLive, Priority |
| `ActiveMQProviderExtensions` | Static Class | Service registration extensions for producers | - | AddActiveMQProvider |
| **Shared Kernel** | | | | |
| `ActiveMQFeeviderConnectionFactory` | Static Class | Connection factory for ActiveMQ connections | - | CreateConnection |
| `IActiveMQFeeviderConfiguration` | Interface | Shared configuration contract | - | BrokerUri, Queue, authentication properties |

[↑ Back to top](#contents)

### ActiveMQFeeder<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>

- **Kind**: Internal generic class
- **Namespace**: `RapidStreamer.Feeders.ActiveMQ`
- **Inherits**: `DelegativeFeeder<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>`, `IFeature`
- **Attributes**: `IsAvailableOnDemo`, Internal visibility, sealed in Release builds

**Key Properties**:
- `_connection : IConnection` — Apache.NMS connection instance
- `_session : ISession` — JMS session for message processing
- `_consumer : IMessageConsumer` — JMS consumer for queue subscription

**Key Methods**:
- Event-driven processing via `_consumer.Listener` delegate
- Automatic message type handling (ObjectMessage, BytesMessage)
- Distributed tracing context extraction from message properties

**JMS Features**:
- Queue-based message consumption
- Message property handling for tracing context
- Automatic connection and session management
- Exception handling with logging

**Health Monitoring**:
- `HealthName` format: `"feeder_ActiveMQ_{queue_name}"`
- `HealthTags` include `nameof(ActiveMQ)` and queue name

**Usage Recipe**:
```csharp
// Define message type
public class OrderMessage : ActiveMQFeederMessage
{
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

// Define configuration
public class OrderFeederConfig : ActiveMQFeederConfiguration
{
    // JMS configuration properties inherited
}

// Register feeder
services.AddActiveMQFeeder<OrderChannel, OrderMessage, OrderFeederConfig>(
    configuration, "Messaging:ActiveMQ:OrderFeeder");

// Use feeder resolver
app.UseActiveMQFeederResolver<OrderChannel, OrderMessage, OrderFeederConfig>(
    channelKey, feederConfiguration);
```

[↑ Back to top](#contents)

### ActiveMQFeederConfiguration

- **Kind**: Public abstract class
- **Namespace**: `RapidStreamer.Feeders.ActiveMQ`
- **Inherits**: `AbstractFeederConfiguration`, `IActiveMQFeeviderConfiguration`
- **Attributes**: Abstract base for consumer configurations

**Key Properties**:
- `BrokerUri : Uri` — Required ActiveMQ broker endpoint
- `Queue : string` — Required queue name for consumption
- `ClientId : string?` — Optional JMS client identifier
- `ClientIdPrefix : string?` — Optional client ID prefix
- `UserName : string?` — Optional authentication username
- `Password : string?` — Optional authentication password
- `UseCompression : bool?` — Optional message compression setting
- `AuditMaximumProducerNumber : int?` — Optional audit configuration

**JMS Configuration**:
- Full JMS client configuration support
- Connection authentication and security
- Queue-specific settings and routing
- Performance tuning parameters

[↑ Back to top](#contents)

### ActiveMQProvider<TActiveMQProviderMessage, TActiveMQProviderConfiguration>

- **Kind**: Internal generic class
- **Namespace**: `RapidStreamer.Providers.DotNet.ActiveMQ`
- **Inherits**: `AbstractProvider<TActiveMQProviderMessage, TActiveMQProviderConfiguration>`
- **Attributes**: Internal visibility, sealed in Release builds

**Key Properties**:
- `_connection : IConnection` — Apache.NMS connection instance
- `_session : ISession` — JMS session for message publishing
- `_producer : IMessageProducer` — JMS producer for message sending

**Key Methods**:
- `InternalExecuteAsync(byte[], CancellationToken) : Task` — Send byte array as JMS BytesMessage

**Producer Configuration**:
- Configurable delivery mode (Persistent/NonPersistent)
- Message time-to-live settings
- Producer request timeout configuration
- Message priority levels (0-9)
- Message ID and timestamp control
- Delivery delay settings

**Distributed Tracing**:
- Automatic `ActivityContext` property injection
- `Baggage` propagation for correlation
- Error logging with queue context

**Usage Recipe**:
```csharp
// Define message type
public class NotificationMessage : ActiveMQProviderMessage
{
    public string UserId { get; set; }
    public string Content { get; set; }
    public string Priority { get; set; }
}

// Define configuration
public class NotificationProviderConfig : ActiveMQProviderConfiguration
{
    // Producer properties inherited
}

// Register provider
services.AddActiveMQProvider<NotificationMessage, NotificationProviderConfig>(
    configuration, "Messaging:ActiveMQ:NotificationProvider");

// Use provider
public class NotificationService
{
    private readonly IProvider<NotificationMessage> _provider;
    
    public NotificationService(IProvider<NotificationMessage> provider)
    {
        _provider = provider;
    }
    
    public async Task SendNotificationAsync(string userId, string content)
    {
        await _provider.ExecuteAsync(new NotificationMessage 
        { 
            UserId = userId,
            Content = content,
            Priority = "High"
        });
    }
}
```

[↑ Back to top](#contents)

### ActiveMQProviderConfiguration

- **Kind**: Public abstract class
- **Namespace**: `RapidStreamer.Providers.DotNet.ActiveMQ`
- **Inherits**: `AbstractProviderConfiguration`, `IActiveMQFeeviderConfiguration`
- **Attributes**: Abstract base for producer configurations

**Key Properties**:
- **Connection Properties**:
  - `BrokerUri : Uri` — Required ActiveMQ broker endpoint
  - `Queue : string` — Required target queue name
  - `ClientId : string?` — Optional JMS client identifier
  - `UserName : string?` — Optional authentication username
  - `Password : string?` — Optional authentication password

- **Producer Properties**:
  - `DeliveryMode : MsgDeliveryMode?` — Message persistence (Persistent/NonPersistent)
  - `TimeToLive : TimeSpan?` — Message expiration time
  - `ProducerRequestTimeout : TimeSpan?` — Producer request timeout
  - `Priority : MsgPriority?` — Message priority (0-9)
  - `DisableMessageID : bool?` — Disable automatic message ID generation
  - `DisableMessageTimestamp : bool?` — Disable automatic timestamp generation
  - `DeliveryDelay : TimeSpan?` — Scheduled message delivery delay

**JMS Configuration Support**:
- Full Apache.NMS configuration compatibility
- Enterprise messaging features
- Performance and reliability settings

[↑ Back to top](#contents)

## JMS Enterprise Features

### Message Delivery Modes

| Mode | Persistence | Performance | Use Case |
|------|-------------|-------------|----------|
| `Persistent` | Disk-based | Medium | Critical messages requiring durability |
| `NonPersistent` | Memory-based | High | High-throughput, non-critical messages |

### Message Priority Levels

| Priority | Level | Description | Typical Usage |
|----------|-------|-------------|---------------|
| `Lowest` | 0-1 | Background processing | Batch jobs, cleanup tasks |
| `Low` | 2-3 | Standard processing | Regular business operations |
| `Normal` | 4-5 | Default priority | Most application messages |
| `High` | 6-7 | Important messages | Priority business events |
| `Critical` | 8-9 | Urgent processing | System alerts, critical notifications |

### Connection Management

- **Connection Pooling**: Efficient connection reuse
- **Automatic Reconnection**: Built-in fault tolerance
- **Session Management**: Transactional and non-transactional sessions
- **Authentication**: Username/password and certificate-based
- **Compression**: Optional message compression for network efficiency

### Enterprise Integration

- **Queue-based Messaging**: Point-to-point communication
- **Topic Support**: Publish-subscribe patterns (via destination configuration)
- **Message Selectors**: Server-side message filtering
- **Transaction Support**: JMS transactional sessions
- **Dead Letter Queues**: Failed message handling

[↑ Back to top](#contents)

## Performance Notes

### Throughput Characteristics

- **Peak Throughput**: 75K messages/second (depends on message size and persistence)
- **Latency**: 10-50ms end-to-end (varies by delivery mode)
- **Memory**: Moderate memory usage with connection pooling
- **Persistence**: Disk I/O impact for persistent messages

### Optimization Recommendations

1. **High-Throughput Configuration**:
   ```csharp
   public class HighThroughputConfig : ActiveMQProviderConfiguration
   {
       public HighThroughputConfig()
       {
           DeliveryMode = MsgDeliveryMode.NonPersistent; // Memory-based
           DisableMessageID = true;                      // Reduce overhead
           DisableMessageTimestamp = true;               // Reduce overhead
           UseCompression = false;                       // CPU vs. network trade-off
       }
   }
   ```

2. **Reliable Messaging Configuration**:
   ```csharp
   public class ReliableConfig : ActiveMQProviderConfiguration
   {
       public ReliableConfig()
       {
           DeliveryMode = MsgDeliveryMode.Persistent;    // Disk persistence
           TimeToLive = TimeSpan.FromHours(24);          // 24-hour expiration
           Priority = MsgPriority.Normal;                // Standard priority
       }
   }
   ```

3. **Connection Optimization**:
   - Reuse connections and sessions when possible
   - Configure appropriate connection pool sizes
   - Use appropriate client ID patterns for monitoring
   - Monitor broker memory and disk usage

[↑ Back to top](#contents)

## RapidStreamer Dependencies

| Package | Version | Description | Links |
|---------|---------|-------------|-------|
| **Core Dependencies** | | | |
| RapidStreamer.Feeders.SharedKernel | 1.0.76+ | Feeder base classes and interfaces | [SharedKernel](../SharedKernel/README.md#rapidstreamer-dependencies) |
| RapidStreamer.Providers.DotNet.SharedKernel | 1.0.76+ | Provider base classes and serialization | [SharedKernel](../SharedKernel/README.md#rapidstreamer-dependencies) |
| **ActiveMQ Packages** | | | |
| RapidStreamer.Feeders.ActiveMQ | 1.0.78+ | ActiveMQ JMS message consumption | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| RapidStreamer.Providers.DotNet.ActiveMQ | 1.0.78+ | ActiveMQ JMS message publishing | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| RapidStreamer.Feeviders.ActiveMQ.SharedKernel | 1.0.78+ | ActiveMQ shared configuration and utilities | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |

### External Dependencies

| Package | Version | Purpose | Documentation |
|---------|---------|---------|---------------|
| Apache.NMS.ActiveMQ | 2.1.0+ | Apache ActiveMQ .NET client | [Apache NMS Docs](https://activemq.apache.org/nms/) |
| Apache.NMS | 2.1.0+ | .NET Message Service API | [NMS API Reference](https://activemq.apache.org/nms/apache-nms-api.html) |

[↑ Back to top](#contents)

## Examples

### Basic Enterprise Messaging

```csharp
// Configuration (appsettings.json)
{
  "Messaging": {
    "ActiveMQ": {
      "OrderProducer": {
        "BrokerUri": "tcp://activemq.company.com:61616",
        "Queue": "enterprise.orders",
        "UserName": "app-user",
        "Password": "secure-password",
        "ClientId": "order-service-producer",
        "DeliveryMode": "Persistent",
        "TimeToLive": "01:00:00",
        "Priority": "Normal"
      }
    }
  }
}

// Message definition
public class OrderMessage : ActiveMQProviderMessage
{
    public string OrderId { get; set; }
    public string CustomerId { get; set; }
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
    public string Status { get; set; }
}

// Configuration class
public class OrderProducerConfig : ActiveMQProviderConfiguration { }

// Registration
services.AddActiveMQProvider<OrderMessage, OrderProducerConfig>(
    configuration, "Messaging:ActiveMQ:OrderProducer");

// Usage
public class OrderService
{
    private readonly IProvider<OrderMessage> _provider;
    
    public OrderService(IProvider<OrderMessage> provider)
    {
        _provider = provider;
    }
    
    public async Task ProcessOrderAsync(Order order)
    {
        await _provider.ExecuteAsync(new OrderMessage
        {
            OrderId = order.Id,
            CustomerId = order.CustomerId,
            Amount = order.Total,
            OrderDate = order.CreatedAt,
            Status = "Processing"
        });
    }
}
```

### Consumer with Priority Handling

```csharp
// Configuration (appsettings.json)
{
  "Messaging": {
    "ActiveMQ": {
      "OrderConsumer": {
        "BrokerUri": "tcp://activemq.company.com:61616",
        "Queue": "enterprise.orders",
        "UserName": "app-consumer",
        "Password": "secure-password",
        "ClientId": "order-service-consumer",
        "UseCompression": true,
        "IsEnabled": true
      }
    }
  }
}

// Message definition
public class OrderMessage : ActiveMQFeederMessage
{
    public string OrderId { get; set; }
    public string CustomerId { get; set; }
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
    public string Status { get; set; }
}

// Configuration class
public class OrderConsumerConfig : ActiveMQFeederConfiguration { }

// Channel definition
public class OrderChannel : IChannel
{
    public Guid Key { get; set; }
    public string Name { get; set; } = "OrderProcessingChannel";
}

// Registration
services.AddActiveMQFeeder<OrderChannel, OrderMessage, OrderConsumerConfig>(
    configuration, "Messaging:ActiveMQ:OrderConsumer");

// Feeder resolution
app.UseActiveMQFeederResolver<OrderChannel, OrderMessage, OrderConsumerConfig>(
    channelKey, consumerConfiguration);
```

### High-Performance Configuration

```csharp
// High-throughput producer configuration
public class HighPerformanceProducerConfig : ActiveMQProviderConfiguration
{
    public HighPerformanceProducerConfig()
    {
        BrokerUri = new Uri("tcp://activemq.company.com:61616");
        Queue = "high-throughput.events";
        
        // Performance optimizations
        DeliveryMode = MsgDeliveryMode.NonPersistent;  // Memory-based
        DisableMessageID = true;                       // Reduce overhead
        DisableMessageTimestamp = true;                // Reduce overhead
        UseCompression = false;                        // Minimize CPU usage
        
        // Connection settings
        ClientId = "high-perf-producer";
        ProducerRequestTimeout = TimeSpan.FromSeconds(30);
    }
}

// Reliable messaging configuration
public class ReliableMessagingConfig : ActiveMQProviderConfiguration
{
    public ReliableMessagingConfig()
    {
        BrokerUri = new Uri("tcp://activemq.company.com:61616");
        Queue = "critical.notifications";
        
        // Reliability settings
        DeliveryMode = MsgDeliveryMode.Persistent;     // Disk persistence
        Priority = MsgPriority.High;                   // High priority
        TimeToLive = TimeSpan.FromHours(24);           // 24-hour retention
        
        // Authentication
        UserName = "critical-service";
        Password = "ultra-secure-password";
        ClientId = "critical-notification-service";
    }
}
```

[↑ Back to top](#contents)

## See Also

- [SharedKernel](../SharedKernel/README.md) - Base interfaces and utilities
- [RabbitMQ](../RabbitMQ/README.md) - Alternative AMQP messaging implementation
- [Kafka](../Kafka/README.md) - High-throughput streaming alternative
- [Documentation Home](../README.md) - Framework overview and navigation

[↑ Back to top](#contents)

---

**Generated**: October 1, 2025  
**ActiveMQ Version**: Apache.NMS.ActiveMQ 2.1.0+  
**RapidStreamer Version**: 1.0.78