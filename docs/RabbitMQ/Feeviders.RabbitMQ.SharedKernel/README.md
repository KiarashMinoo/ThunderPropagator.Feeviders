# ThunderPropagator.Feeviders.RabbitMQ.SharedKernel

> Shared RabbitMQ Utilities - Common configuration, connection factory, and AMQP helpers

[◂ Back to RabbitMQ](../README.md) | [◂ Back to Documentation](../../README.md)

## Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Files](#files)
- [Configuration](#configuration)
- [Dependencies](#dependencies)
- [API Reference](#api-reference)
- [Usage Examples](#usage-examples)
- [Advanced Patterns](#advanced-patterns)
- [See Also](#see-also)

## Overview

**Type**: Shared Library  
**Target Frameworks**: .NET 8, 9, 10  
**Package**: ThunderPropagator.Feeviders.RabbitMQ.SharedKernel

This project provides shared infrastructure for both RabbitMQ Feeder and Provider implementations. It includes a comprehensive configuration model based on RabbitMQ.Client's ConnectionFactory, a connection factory helper for consistent channel initialization, and common AMQP utilities.

### Key Features

- ✅ **Unified Configuration**: Single configuration class used by both Feeder and Provider
- ✅ **Connection Factory**: Centralized connection and channel initialization logic
- ✅ **AMQP 0.9.1 Support**: Full support for RabbitMQ's AMQP implementation
- ✅ **Connection Management**: Automatic recovery, heartbeats, and topology restoration
- ✅ **TLS/SSL Support**: Secure connections with configurable SSL options
- ✅ **Virtual Host Support**: Multi-tenancy via RabbitMQ virtual hosts
- ✅ **Queue Configuration**: Queue, exchange, and binding declarations
- ✅ **Extensibility**: Abstract base classes for custom configurations
- ✅ **Type Safety**: Strong typing for all AMQP properties

## Architecture

```mermaid
classDiagram
    class RabbitMQFeeviderConfiguration {
        <<abstract>>
        +string HostName
        +int Port
        +string UserName
        +string Password
        +string VirtualHost
        +string Queue
        +string Exchange
        +string RoutingKey
        +bool AutoAck
        +bool Durable
        +bool Exclusive
        +bool AutoDelete
        +Dictionary Arguments
        +bool AutomaticRecoveryEnabled
        +TimeSpan NetworkRecoveryInterval
        +ushort ConsumerDispatchConcurrency
        +TimeSpan RequestedConnectionTimeout
        +TimeSpan SocketReadTimeout
        +TimeSpan SocketWriteTimeout
        +SslOption Ssl
        +bool TopologyRecoveryEnabled
        +ushort RequestedChannelMax
        +uint RequestedFrameMax
        +TimeSpan RequestedHeartbeat
        +uint MaxInboundMessageBodySize
        +Uri Uri
        +string ClientProvidedName
    }
    
    class RabbitMQFeeviderConnectionFactory {
        <<internal sealed>>
        +CreateConnectionAsync(config, ct)$ IConnection
        +InitializeChannelAsync(config, ct)$ (IConnection, IChannel)
    }
    
    class RabbitMQFeederConfiguration {
        +Guid Id
        +SerializerType SerializerType
        +string EnrichmentScript
        +string[] MetadataReferences
    }
    
    class RabbitMQProviderConfiguration {
        +SerializerType SerializerType
    }
    
    RabbitMQFeederConfiguration --|> RabbitMQFeeviderConfiguration
    RabbitMQProviderConfiguration --|> RabbitMQFeeviderConfiguration
    RabbitMQFeeviderConnectionFactory ..> RabbitMQFeeviderConfiguration : uses
    
    note for RabbitMQFeeviderConnectionFactory "Factory methods for creating\nRabbitMQ connections and channels\nwith consistent configuration"
    note for RabbitMQFeeviderConfiguration "Shared base configuration\nfor both Feeder and Provider"
```

## Files

**Total**: 3 C# source files

| File | LOC | Responsibility |
|------|-----|----------------|
| [RabbitMQFeeviderConfiguration.cs](../../../Feeviders/RabbitMQ/ThunderPropagator.Feeviders.RabbitMQ.SharedKernel/RabbitMQFeeviderConfiguration.cs) | ~237 | Abstract configuration base class with comprehensive RabbitMQ ConnectionFactory properties |
| [RabbitMQFeeviderConnectionFactory.cs](../../../Feeviders/RabbitMQ/ThunderPropagator.Feeviders.RabbitMQ.SharedKernel/RabbitMQFeeviderConnectionFactory.cs) | ~113 | Static factory for creating RabbitMQ connections and channels from configuration |
| [AssemblyInfo.cs](../../../Feeviders/RabbitMQ/ThunderPropagator.Feeviders.RabbitMQ.SharedKernel/AssemblyInfo.cs) | ~10 | Assembly metadata and internals visibility |

### Key Implementation Details

#### RabbitMQFeeviderConfiguration.cs

```csharp
public abstract class RabbitMQFeeviderConfiguration : ServiceConfiguration
{
    // Enable/disable configuration
    public bool IsEnabled { get; set; }
    
    // Connection settings
    public string HostName { get; set; }          // Required
    public int Port { get; set; }                 // Default: 5672 (AmqpTcpEndpoint.UseDefaultPort)
    public string? UserName { get; set; }         // Default: "guest"
    public string? Password { get; set; }         // Default: "guest"
    public string? VirtualHost { get; set; }      // Default: "/"
    
    // Authentication & Security
    public SslOption? Ssl { get; set; }
    public SslProtocols? AmqpUriSslProtocols { get; set; }
    public ICredentialsProvider? CredentialsProvider { get; set; }
    public IEnumerable<IAuthMechanismFactory>? AuthMechanisms { get; set; }
    
    // Connection recovery
    public bool? AutomaticRecoveryEnabled { get; set; }      // Default: true
    public TimeSpan? NetworkRecoveryInterval { get; set; }   // Default: 5s
    public bool? TopologyRecoveryEnabled { get; set; }       // Default: true
    public TopologyRecoveryFilter? TopologyRecoveryFilter { get; set; }
    public TopologyRecoveryExceptionHandler? TopologyRecoveryExceptionHandler { get; set; }
    
    // Performance tuning
    public ushort? ConsumerDispatchConcurrency { get; set; }  // Default: 1
    public ushort? RequestedChannelMax { get; set; }
    public uint? RequestedFrameMax { get; set; }
    public TimeSpan? RequestedHeartbeat { get; set; }
    public uint? MaxInboundMessageBodySize { get; set; }
    
    // Timeouts
    public TimeSpan? RequestedConnectionTimeout { get; set; }
    public TimeSpan? HandshakeContinuationTimeout { get; set; }
    public TimeSpan? ContinuationTimeout { get; set; }
    public TimeSpan? SocketReadTimeout { get; set; }
    public TimeSpan? SocketWriteTimeout { get; set; }
    
    // Advanced
    public Func<IEnumerable<AmqpTcpEndpoint>, IEndpointResolver>? EndpointResolverFactory { get; set; }
    public IDictionary<string, object?>? ClientProperties { get; set; }
    public Uri? Uri { get; set; }                           // Alternative to individual settings
    public string? ClientProvidedName { get; set; }         // Client identification
    
    // Queue/Exchange configuration
    public string Queue { get; set; }                       // Required
    public string Exchange { get; set; }                    // Default: "" (default exchange)
    public string RoutingKey { get; set; }                  // Default: Queue name
    public bool Durable { get; set; }                       // Default: false
    public bool Exclusive { get; set; }                     // Default: false
    public bool AutoDelete { get; set; }                    // Default: false
    public Dictionary<string, object?>? Arguments { get; set; }  // Queue arguments
    
    // Consumer settings (Feeder only)
    public bool AutoAck { get; set; }                       // Default: true
}
```

**Property Categories:**

1. **Connection Basics**: HostName, Port, UserName, Password, VirtualHost
2. **Security**: Ssl, AmqpUriSslProtocols, CredentialsProvider, AuthMechanisms
3. **Recovery**: AutomaticRecoveryEnabled, NetworkRecoveryInterval, TopologyRecoveryEnabled
4. **Performance**: ConsumerDispatchConcurrency, RequestedChannelMax, RequestedFrameMax, RequestedHeartbeat
5. **Timeouts**: RequestedConnectionTimeout, SocketReadTimeout, SocketWriteTimeout
6. **AMQP Topology**: Queue, Exchange, RoutingKey, Durable, Exclusive, AutoDelete, Arguments
7. **Consumer**: AutoAck

#### RabbitMQFeeviderConnectionFactory.cs

```csharp
internal sealed class RabbitMQFeeviderConnectionFactory : DisposableObject
{
    // Create RabbitMQ connection from configuration
    public static Task<IConnection> CreateConnectionAsync(
        RabbitMQFeeviderConfiguration configuration,
        CancellationToken cancellationToken = default)
    {
        var factory = new ConnectionFactory
        {
            HostName = configuration.HostName,
            Port = configuration.Port
        };
        
        // Map all configuration properties to ConnectionFactory
        if (configuration.AmqpUriSslProtocols != null)
            factory.AmqpUriSslProtocols = configuration.AmqpUriSslProtocols.Value;
        
        if (configuration.AutomaticRecoveryEnabled != null)
            factory.AutomaticRecoveryEnabled = configuration.AutomaticRecoveryEnabled.Value;
        
        if (configuration.ConsumerDispatchConcurrency != null)
            factory.ConsumerDispatchConcurrency = configuration.ConsumerDispatchConcurrency.Value;
        
        // ... (50+ property mappings)
        
        return factory.CreateConnectionAsync(cancellationToken);
    }
    
    // Create connection and initialize channel with queue/exchange
    internal static async Task<(IConnection, IChannel)> InitializeChannelAsync(
        RabbitMQFeeviderConfiguration configuration,
        CancellationToken cancellationToken = default)
    {
        var connection = await CreateConnectionAsync(configuration, cancellationToken);
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
        
        // Declare queue
        await channel.QueueDeclareAsync(
            queue: configuration.Queue,
            durable: configuration.Durable,
            exclusive: configuration.Exclusive,
            autoDelete: configuration.AutoDelete,
            arguments: configuration.Arguments,
            cancellationToken: cancellationToken);
        
        // Bind queue to exchange if specified
        if (!string.IsNullOrWhiteSpace(configuration.Exchange))
        {
            await channel.QueueBindAsync(
                queue: configuration.Queue,
                exchange: configuration.Exchange,
                routingKey: configuration.RoutingKey,
                cancellationToken: cancellationToken);
        }
        
        return (connection, channel);
    }
}
```

**Key Responsibilities:**
1. Maps configuration properties to RabbitMQ.Client's ConnectionFactory
2. Creates IConnection from mapped factory
3. Creates IChannel from connection
4. Declares queue with specified properties
5. Binds queue to exchange with routing key

## Configuration

### Complete Configuration Example

```json
{
  "Messaging": {
    "RabbitMQ": {
      "IsEnabled": true,
      
      // Connection basics
      "HostName": "rabbitmq.example.com",
      "Port": 5672,
      "UserName": "app-user",
      "Password": "secure-password",
      "VirtualHost": "/production",
      
      // Queue/Exchange topology
      "Queue": "orders-queue",
      "Exchange": "orders-exchange",
      "RoutingKey": "order.*",
      "Durable": true,
      "Exclusive": false,
      "AutoDelete": false,
      "Arguments": {
        "x-message-ttl": 300000,
        "x-dead-letter-exchange": "orders-dlx",
        "x-dead-letter-routing-key": "order.failed",
        "x-max-priority": 10,
        "x-queue-type": "quorum"
      },
      
      // Consumer settings (Feeder)
      "AutoAck": false,
      
      // Connection recovery
      "AutomaticRecoveryEnabled": true,
      "NetworkRecoveryInterval": "00:00:05",
      "TopologyRecoveryEnabled": true,
      
      // Performance tuning
      "ConsumerDispatchConcurrency": 10,
      "RequestedChannelMax": 2047,
      "RequestedFrameMax": 131072,
      "RequestedHeartbeat": "00:00:30",
      "MaxInboundMessageBodySize": 134217728,
      
      // Timeouts
      "RequestedConnectionTimeout": "00:00:30",
      "HandshakeContinuationTimeout": "00:00:10",
      "ContinuationTimeout": "00:00:20",
      "SocketReadTimeout": "00:00:30",
      "SocketWriteTimeout": "00:00:30",
      
      // SSL/TLS
      "Ssl": {
        "Enabled": true,
        "ServerName": "rabbitmq.example.com",
        "Version": "Tls12",
        "CertPath": "/certs/client.pfx",
        "CertPassphrase": "cert-password"
      },
      
      // Client identification
      "ClientProvidedName": "OrderService-v1.2.3",
      
      // Client properties (visible in management UI)
      "ClientProperties": {
        "application": "OrderProcessingService",
        "version": "1.2.3",
        "environment": "production",
        "host": "pod-1234"
      }
    }
  }
}
```

### Minimal Configuration

```json
{
  "Messaging": {
    "RabbitMQ": {
      "HostName": "localhost",
      "Queue": "my-queue"
    }
  }
}
```

All other properties have sensible defaults from RabbitMQ.Client's ConnectionFactory.

### URI-based Configuration

```json
{
  "Messaging": {
    "RabbitMQ": {
      "Uri": "amqp://user:password@rabbitmq.example.com:5672/production"
    }
  }
}
```

**Note**: Individual properties override URI settings if both are specified.

## Dependencies

### NuGet Packages

| Package | Version | Purpose |
|---------|---------|---------|
| **ThunderPropagator.BuildingBlocks** | 1.0.1-beta.2 | ServiceConfiguration base class and utilities |
| **RabbitMQ.Client** | 7.0+ | Official RabbitMQ .NET client |
| **NJsonSchema.Annotations** | Latest | JSON Schema metadata for configuration serialization |

### Project References

None - this is a shared utility library used by Feeder and Provider projects.

## API Reference

### RabbitMQFeeviderConfiguration Class

```csharp
public abstract class RabbitMQFeeviderConfiguration : ServiceConfiguration
```

**Inheritance:** ServiceConfiguration (from ThunderPropagator.BuildingBlocks)

**Key Methods (inherited from ServiceConfiguration):**
- `Get<T>(T defaultValue)`: Gets property value with default
- `Get<T>()`: Gets property value (nullable)
- `Set<T>(T value)`: Sets property value

**Usage Pattern:**
```csharp
// Concrete implementation
public class MyRabbitMQConfig : RabbitMQFeeviderConfiguration
{
    // Inherits all properties
}

// Access properties
var hostName = config.HostName;
var port = config.Port;
```

### RabbitMQFeeviderConnectionFactory Class

```csharp
internal sealed class RabbitMQFeeviderConnectionFactory : DisposableObject
```

**Static Methods:**

#### CreateConnectionAsync

```csharp
public static Task<IConnection> CreateConnectionAsync(
    RabbitMQFeeviderConfiguration configuration,
    CancellationToken cancellationToken = default)
```

**Purpose**: Creates RabbitMQ connection from configuration

**Parameters:**
- `configuration`: Configuration with connection settings
- `cancellationToken`: Optional cancellation token

**Returns**: Task<IConnection> - RabbitMQ connection

**Example:**
```csharp
var connection = await RabbitMQFeeviderConnectionFactory.CreateConnectionAsync(
    config, cancellationToken);
```

#### InitializeChannelAsync

```csharp
internal static async Task<(IConnection, IChannel)> InitializeChannelAsync(
    RabbitMQFeeviderConfiguration configuration,
    CancellationToken cancellationToken = default)
```

**Purpose**: Creates connection, channel, declares queue, and binds to exchange

**Parameters:**
- `configuration`: Complete RabbitMQ configuration
- `cancellationToken`: Optional cancellation token

**Returns**: Task<(IConnection, IChannel)> - Tuple of connection and initialized channel

**Side Effects:**
- Declares queue (if not exists)
- Binds queue to exchange (if Exchange is specified)

**Example:**
```csharp
var (connection, channel) = await RabbitMQFeeviderConnectionFactory
    .InitializeChannelAsync(config, cancellationToken);

// Channel ready to consume/publish
await channel.BasicConsumeAsync(config.Queue, config.AutoAck, consumer);
```

## Usage Examples

### Example 1: Basic Configuration Usage

```csharp
// Define concrete configuration
public class OrderQueueConfig : RabbitMQFeeviderConfiguration
{
    // Inherits all properties
}

// Load from appsettings.json
var config = new OrderQueueConfig();
configuration.GetSection("Messaging:RabbitMQ").Bind(config);

// Use with connection factory
var (connection, channel) = await RabbitMQFeeviderConnectionFactory
    .InitializeChannelAsync(config);

// Channel is ready with queue declared and bound
await channel.BasicPublishAsync(config.Exchange, config.RoutingKey, body);
```

### Example 2: Multi-Environment Configuration

```csharp
// Base configuration in appsettings.json
{
  "Messaging": {
    "RabbitMQ": {
      "HostName": "localhost",
      "Queue": "dev-queue"
    }
  }
}

// Override in appsettings.Production.json
{
  "Messaging": {
    "RabbitMQ": {
      "HostName": "rabbitmq-cluster.prod.svc.cluster.local",
      "Queue": "prod-queue",
      "UserName": "prod-user",
      "Password": "${RABBITMQ_PASSWORD}",
      "VirtualHost": "/production",
      "Durable": true,
      "AutomaticRecoveryEnabled": true,
      "Ssl": {
        "Enabled": true
      }
    }
  }
}
```

### Example 3: Queue with Dead Letter Exchange

```csharp
public class ReliableQueueConfig : RabbitMQFeeviderConfiguration
{
    public ReliableQueueConfig()
    {
        Queue = "orders-queue";
        Exchange = "orders-exchange";
        RoutingKey = "order.created";
        Durable = true;
        AutoAck = false;
        
        // Configure DLX for failed messages
        Arguments = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", "orders-dlx" },
            { "x-dead-letter-routing-key", "order.failed" },
            { "x-message-ttl", 300000 },  // 5 minutes
            { "x-max-length", 10000 }     // Max 10k messages
        };
    }
}
```

### Example 4: Priority Queue Configuration

```csharp
public class PriorityQueueConfig : RabbitMQFeeviderConfiguration
{
    public PriorityQueueConfig()
    {
        Queue = "priority-tasks-queue";
        Durable = true;
        
        // Enable priority 0-10
        Arguments = new Dictionary<string, object?>
        {
            { "x-max-priority", 10 }
        };
    }
}
```

### Example 5: Quorum Queue for High Availability

```csharp
public class QuorumQueueConfig : RabbitMQFeeviderConfiguration
{
    public QuorumQueueConfig()
    {
        Queue = "ha-orders-queue";
        Durable = true;
        
        // Use quorum queue (RabbitMQ 3.8+)
        Arguments = new Dictionary<string, object?>
        {
            { "x-queue-type", "quorum" }
        };
    }
}
```

### Example 6: Lazy Queue for Large Backlogs

```csharp
public class LazyQueueConfig : RabbitMQFeeviderConfiguration
{
    public LazyQueueConfig()
    {
        Queue = "large-files-queue";
        Durable = true;
        
        // Lazy queue moves messages to disk ASAP
        Arguments = new Dictionary<string, object?>
        {
            { "x-queue-mode", "lazy" }
        };
    }
}
```

### Example 7: SSL/TLS Configuration

```csharp
public class SecureRabbitMQConfig : RabbitMQFeeviderConfiguration
{
    public SecureRabbitMQConfig()
    {
        HostName = "rabbitmq.example.com";
        Port = 5671;  // AMQPS port
        UserName = "secure-user";
        Password = Environment.GetEnvironmentVariable("RABBITMQ_PASSWORD");
        
        Ssl = new SslOption
        {
            Enabled = true,
            ServerName = "rabbitmq.example.com",
            Version = SslProtocols.Tls12 | SslProtocols.Tls13,
            AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateNameMismatch,
            CertPath = "/certs/client.pfx",
            CertPassphrase = Environment.GetEnvironmentVariable("CERT_PASSWORD")
        };
    }
}
```

### Example 8: Connection with Custom Client Properties

```csharp
public class MonitoredConnectionConfig : RabbitMQFeeviderConfiguration
{
    public MonitoredConnectionConfig()
    {
        HostName = "rabbitmq.example.com";
        ClientProvidedName = $"OrderService-{Environment.MachineName}";
        
        // Visible in RabbitMQ Management UI
        ClientProperties = new Dictionary<string, object?>
        {
            { "application", "OrderProcessingService" },
            { "version", "1.2.3" },
            { "environment", Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") },
            { "host", Environment.MachineName },
            { "pid", Environment.ProcessId },
            { "framework", "net9.0" }
        };
    }
}
```

## Advanced Patterns

### Pattern 1: Dynamic Configuration at Runtime

```csharp
public class DynamicRabbitMQConfigFactory
{
    public RabbitMQFeeviderConfiguration CreateTenantConfig(string tenantId)
    {
        return new TenantQueueConfig
        {
            HostName = "rabbitmq.example.com",
            VirtualHost = $"/tenant-{tenantId}",
            Queue = $"tenant-{tenantId}-events",
            Exchange = "tenant-events",
            RoutingKey = $"tenant.{tenantId}.*",
            Durable = true,
            ClientProvidedName = $"TenantService-{tenantId}"
        };
    }
}
```

### Pattern 2: Configuration Validation

```csharp
public static class RabbitMQConfigValidator
{
    public static bool Validate(
        RabbitMQFeeviderConfiguration config, 
        out List<string> errors)
    {
        errors = new List<string>();
        
        if (string.IsNullOrWhiteSpace(config.HostName))
            errors.Add("HostName is required");
        
        if (string.IsNullOrWhiteSpace(config.Queue))
            errors.Add("Queue name is required");
        
        if (config.Port <= 0 || config.Port > 65535)
            errors.Add("Port must be between 1 and 65535");
        
        if (config.Durable && config.AutoDelete)
            errors.Add("Cannot have both Durable and AutoDelete");
        
        return errors.Count == 0;
    }
}
```

### Pattern 3: Configuration Builder

```csharp
public class RabbitMQConfigBuilder<T> where T : RabbitMQFeeviderConfiguration, new()
{
    private readonly T _config = new();
    
    public RabbitMQConfigBuilder<T> WithHost(string host, int port = 5672)
    {
        _config.HostName = host;
        _config.Port = port;
        return this;
    }
    
    public RabbitMQConfigBuilder<T> WithCredentials(string username, string password)
    {
        _config.UserName = username;
        _config.Password = password;
        return this;
    }
    
    public RabbitMQConfigBuilder<T> WithQueue(
        string queue, 
        bool durable = true, 
        bool autoDelete = false)
    {
        _config.Queue = queue;
        _config.Durable = durable;
        _config.AutoDelete = autoDelete;
        return this;
    }
    
    public RabbitMQConfigBuilder<T> WithExchange(string exchange, string routingKey)
    {
        _config.Exchange = exchange;
        _config.RoutingKey = routingKey;
        return this;
    }
    
    public RabbitMQConfigBuilder<T> WithDeadLetterExchange(string dlx, string routingKey)
    {
        _config.Arguments ??= new Dictionary<string, object?>();
        _config.Arguments["x-dead-letter-exchange"] = dlx;
        _config.Arguments["x-dead-letter-routing-key"] = routingKey;
        return this;
    }
    
    public RabbitMQConfigBuilder<T> WithPriority(byte maxPriority = 10)
    {
        _config.Arguments ??= new Dictionary<string, object?>();
        _config.Arguments["x-max-priority"] = maxPriority;
        return this;
    }
    
    public T Build() => _config;
}

// Usage
var config = new RabbitMQConfigBuilder<OrderQueueConfig>()
    .WithHost("localhost")
    .WithQueue("orders-queue", durable: true)
    .WithExchange("orders-exchange", "order.*")
    .WithDeadLetterExchange("orders-dlx", "order.failed")
    .WithPriority(10)
    .Build();
```

### Pattern 4: Connection Pool Manager

```csharp
public class RabbitMQConnectionPool : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, IConnection> _connections = new();
    private readonly SemaphoreSlim _lock = new(1, 1);
    
    public async Task<IConnection> GetOrCreateConnectionAsync(
        RabbitMQFeeviderConfiguration config,
        CancellationToken cancellationToken = default)
    {
        var key = $"{config.HostName}:{config.Port}:{config.VirtualHost}";
        
        if (_connections.TryGetValue(key, out var existingConnection) 
            && existingConnection.IsOpen)
        {
            return existingConnection;
        }
        
        await _lock.WaitAsync(cancellationToken);
        try
        {
            // Double-check after acquiring lock
            if (_connections.TryGetValue(key, out existingConnection) 
                && existingConnection.IsOpen)
            {
                return existingConnection;
            }
            
            var connection = await RabbitMQFeeviderConnectionFactory
                .CreateConnectionAsync(config, cancellationToken);
            
            _connections[key] = connection;
            return connection;
        }
        finally
        {
            _lock.Release();
        }
    }
    
    public async ValueTask DisposeAsync()
    {
        foreach (var connection in _connections.Values)
        {
            if (connection.IsOpen)
                await connection.CloseAsync();
            await connection.DisposeAsync();
        }
        _connections.Clear();
        _lock.Dispose();
    }
}
```

### Pattern 5: Health Check Integration

```csharp
public class RabbitMQHealthCheck : IHealthCheck
{
    private readonly RabbitMQFeeviderConfiguration _config;
    
    public RabbitMQHealthCheck(RabbitMQFeeviderConfiguration config)
    {
        _config = config;
    }
    
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var connection = await RabbitMQFeeviderConnectionFactory
                .CreateConnectionAsync(_config, cancellationToken);
            
            if (connection.IsOpen)
            {
                await connection.CloseAsync(cancellationToken);
                await connection.DisposeAsync();
                
                return HealthCheckResult.Healthy(
                    $"RabbitMQ connection to {_config.HostName}:{_config.Port} successful");
            }
            
            return HealthCheckResult.Unhealthy("Connection not open");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(
                $"RabbitMQ connection failed: {ex.Message}", ex);
        }
    }
}

// Register in Program.cs
builder.Services.AddHealthChecks()
    .AddCheck<RabbitMQHealthCheck>("rabbitmq");
```

## Queue Arguments Reference

Common queue arguments used in `Arguments` dictionary:

| Argument | Type | Description |
|----------|------|-------------|
| `x-message-ttl` | int | Message TTL in milliseconds |
| `x-expires` | int | Queue expires after N ms of disuse |
| `x-max-length` | int | Maximum queue length |
| `x-max-length-bytes` | int | Maximum queue size in bytes |
| `x-dead-letter-exchange` | string | DLX for rejected/expired messages |
| `x-dead-letter-routing-key` | string | Routing key for DLX |
| `x-max-priority` | int | Enable priority queuing (0-255) |
| `x-queue-mode` | string | "default" or "lazy" |
| `x-queue-type` | string | "classic" or "quorum" (RabbitMQ 3.8+) |
| `x-single-active-consumer` | bool | Only one consumer at a time |
| `x-overflow` | string | "drop-head", "reject-publish", "reject-publish-dlx" |

**Example with multiple arguments:**
```csharp
Arguments = new Dictionary<string, object?>
{
    { "x-message-ttl", 300000 },           // 5 minutes
    { "x-max-length", 10000 },             // Max 10k messages
    { "x-overflow", "reject-publish" },    // Reject when full
    { "x-dead-letter-exchange", "dlx" },   // Send expired to DLX
    { "x-max-priority", 10 },              // Priority 0-10
    { "x-queue-type", "quorum" }           // Use quorum queue
}
```

## Performance Considerations

### Connection Management

1. **Reuse connections**: One connection per application
2. **Multiple channels**: One channel per thread/task
3. **Connection recovery**: Enable automatic recovery
4. **Heartbeats**: Configure based on load balancer timeout

```json
{
  "AutomaticRecoveryEnabled": true,
  "NetworkRecoveryInterval": "00:00:05",
  "RequestedHeartbeat": "00:00:30"
}
```

### Queue Configuration

1. **Durable queues**: Survive broker restarts
2. **Lazy queues**: For large backlogs
3. **Quorum queues**: For high availability
4. **Max length**: Prevent unbounded growth

```json
{
  "Durable": true,
  "Arguments": {
    "x-max-length": 100000,
    "x-queue-mode": "lazy"
  }
}
```

### Network Settings

1. **Frame size**: Larger for throughput, smaller for latency
2. **Channel max**: Balance between concurrency and overhead
3. **Timeouts**: Match network conditions

```json
{
  "RequestedFrameMax": 131072,
  "RequestedChannelMax": 2047,
  "SocketReadTimeout": "00:00:30",
  "SocketWriteTimeout": "00:00:30"
}
```

## Troubleshooting

### Common Configuration Issues

**1. Connection Timeout**
```
TimeoutException: Connection timeout
```
- Increase `RequestedConnectionTimeout`
- Check network connectivity
- Verify firewall rules

**2. Authentication Failed**
```
PossibleAuthenticationFailureException
```
- Verify `UserName` and `Password`
- Check user exists: `rabbitmqctl list_users`
- Verify permissions: `rabbitmqctl list_user_permissions {user}`

**3. VirtualHost Not Found**
```
NOT_ALLOWED - vhost /xxx not found
```
- Create vhost: `rabbitmqctl add_vhost /xxx`
- Grant permissions: `rabbitmqctl set_permissions -p /xxx user ".*" ".*" ".*"`

**4. Topology Recovery Failed**
```
TopologyRecoveryException
```
- Enable `TopologyRecoveryEnabled`
- Implement `TopologyRecoveryExceptionHandler`
- Check exchange/queue declarations

## See Also

### Related Documentation

- [RabbitMQ System Overview](../README.md) - Complete RabbitMQ integration guide
- [Feeders.RabbitMQ](../Feeders.RabbitMQ/README.md) - Message consumption using this configuration
- [Providers.DotNet.RabbitMQ](../Providers.DotNet.RabbitMQ/README.md) - Message publishing using this configuration
- [ThunderPropagator.BuildingBlocks](../../README.md#buildingblocks) - ServiceConfiguration base class

### External Resources

- [RabbitMQ .NET Client API](https://www.rabbitmq.com/dotnet-api-guide.html)
- [ConnectionFactory Documentation](https://rabbitmq.github.io/rabbitmq-dotnet-client/api/RabbitMQ.Client.ConnectionFactory.html)
- [Queue Arguments Reference](https://www.rabbitmq.com/queues.html#optional-arguments)
- [Virtual Hosts](https://www.rabbitmq.com/vhosts.html)
- [SSL/TLS Support](https://www.rabbitmq.com/ssl.html)

### Framework Documentation

- [ThunderPropagator Documentation](../../README.md)
- [Configuration Patterns](../../README.md#configuration)
