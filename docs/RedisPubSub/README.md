# RapidStreamer Redis Pub/Sub Integration Documentation

## Overview

The RapidStreamer Redis Pub/Sub implementation provides high-performance, real-time messaging capabilities using Redis as a message broker. Built on the robust StackExchange.Redis library (v2.9.25), this implementation offers ultra-low latency communication ideal for real-time applications, caching scenarios, and distributed system coordination.

## Architecture

### Core Components

```
┌─────────────────────────┐    ┌──────────────────────────┐
│  Redis Pub/Sub Feeder   │    │ Redis Pub/Sub Provider   │
│   (Message Consumer)    │    │  (Message Publisher)     │
├─────────────────────────┤    ├──────────────────────────┤
│ • Channel Subscription  │    │ • Channel Publishing     │
│ • Pattern Matching      │    │ • Fire-and-Forget Mode   │
│ • Real-time Processing  │    │ • Connection Pooling     │
│ • Health Monitoring     │    │ • OpenTelemetry Support  │
└─────────────────────────┘    └──────────────────────────┘
           │                              │
           └──────────┬───────────────────┘
                      │
        ┌─────────────────────────────────────┐
        │     Redis Connection Management     │
        │ • ConnectionMultiplexer             │
        │ • Subscriber Management             │
        │ • Pattern Mode Support              │
        │ • Connection Resilience             │
        └─────────────────────────────────────┘
```

### Key Features

- **Ultra-Low Latency**: Sub-millisecond message delivery
- **Pattern Matching**: Wildcard subscription support
- **Fire-and-Forget**: High-throughput publishing
- **Connection Pooling**: Efficient connection management
- **Real-time**: Instant message delivery
- **Lightweight**: Minimal overhead messaging
- **Scalable**: Horizontal scaling support

## API Reference

### RedisPubSubFeeder&lt;TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration&gt;

Real-time message consumer with pattern matching support.

#### Key Methods
```csharp
public RedisPubSubFeeder(
    TChannel channel,
    TRedisPubSubFeederConfiguration redisPubSubFeederConfiguration,
    IFeederHandler<TChannel, TRedisPubSubFeederMessage> feederHandler,
    IServiceProvider serviceProvider)

protected override async Task StopAsync(CancellationToken cancellationToken = default)
protected override ValueTask DisposeManagedResourcesAsync()
```

#### Features
- **Pattern Subscription**: Wildcard channel matching
- **Instant Processing**: Real-time message handling
- **Health Monitoring**: Built-in health checks
- **Connection Management**: Automatic connection handling

### RedisPubSubProvider&lt;TRedisPubSubProviderMessage, TRedisPubSubProviderConfiguration&gt;

High-performance Redis publisher with fire-and-forget semantics.

#### Key Methods
```csharp
public RedisPubSubProvider(
    TRedisPubSubProviderConfiguration redisPubSubProviderConfiguration, 
    IServiceProvider serviceProvider)

protected override async Task InternalExecuteAsync(
    TRedisPubSubProviderMessage feederMessage, 
    CancellationToken cancellationToken = default)

protected override async Task InternalExecuteAsync(
    byte[] bytes, 
    CancellationToken cancellationToken = default)
```

#### Publishing Features
- **Fire-and-Forget**: CommandFlags.FireAndForget for maximum performance
- **Connection Reuse**: Efficient connection multiplexing
- **Trace Integration**: OpenTelemetry distributed tracing
- **Error Handling**: Comprehensive error logging

### RedisPubSubFeederConfiguration

Consumer configuration with channel and pattern support.

#### Core Properties
```csharp
public string ConnectionString { get; set; }  // Redis connection string
public string Channel { get; set; }           // Channel name or pattern
public RedisChannel.PatternMode PatternMode { get; set; } // Auto, Literal, Pattern
```

### RedisPubSubProviderConfiguration

Producer configuration for Redis publishing.

#### Core Properties
```csharp
public required string ConnectionString { get; set; }
public required string Channel { get; set; }
public RedisChannel.PatternMode PatternMode { get; set; } = PatternMode.Auto
```

## Configuration Examples

### Basic Redis Pub/Sub Configuration

```json
{
  "RedisPubSubFeeder": {
    "ConnectionString": "localhost:6379",
    "Channel": "notifications",
    "PatternMode": "Literal",
    "SerializerType": "Json"
  }
}
```

### Pattern-Based Subscription

```json
{
  "RedisPubSubFeeder": {
    "ConnectionString": "redis-cluster:6379",
    "Channel": "events:*",
    "PatternMode": "Pattern",
    "SerializerType": "NJson"
  }
}
```

### Secure Redis with Authentication

```json
{
  "RedisPubSubProvider": {
    "ConnectionString": "secure-redis:6380,password=mypassword,ssl=true",
    "Channel": "secure-channel",
    "PatternMode": "Literal"
  }
}
```

### High-Performance Configuration

```json
{
  "RedisPubSubProvider": {
    "ConnectionString": "localhost:6379,abortConnect=false,connectTimeout=5000",
    "Channel": "high-throughput",
    "PatternMode": "Literal"
  }
}
```

## Channel Patterns and Matching

### Literal Channels
```csharp
Channel = "user:notifications"     // Exact match only
Channel = "order:created"          // Specific channel
Channel = "system:health"          // System events
```

### Pattern Channels
```csharp
Channel = "user:*"                 // All user channels
Channel = "order:*"                // All order events
Channel = "system:*"               // All system events
Channel = "*:error"                // All error channels
Channel = "app:*:log"              // Multi-level patterns
```

### Pattern Mode Options

```csharp
// Automatic detection
PatternMode = RedisChannel.PatternMode.Auto

// Literal channel name (exact match)
PatternMode = RedisChannel.PatternMode.Literal

// Pattern matching with wildcards
PatternMode = RedisChannel.PatternMode.Pattern
```

## Performance Characteristics

### Latency and Throughput

| Configuration | Latency | Throughput | Use Case |
|---------------|---------|------------|----------|
| Local Redis | <1ms | 100k+ msg/s | Real-time apps |
| Network Redis | 1-5ms | 50k+ msg/s | Distributed systems |
| Cluster Redis | 2-10ms | 200k+ msg/s | High availability |
| Secured Redis | 2-8ms | 40k+ msg/s | Production systems |

### Memory Usage

```csharp
// Minimal memory footprint
// Messages are not persisted in Redis
// Only active subscribers consume memory
// Connection pooling reduces overhead
```

## Message Patterns

### Real-time Notifications

```csharp
public class NotificationMessage : RedisPubSubFeederMessage
{
    public string UserId { get; set; }
    public string Type { get; set; }
    public string Content { get; set; }
    public DateTime Timestamp { get; set; }
}

// Publisher
Channel = "notifications:user:" + userId

// Subscriber
Channel = "notifications:*"  // All notifications
```

### System Events

```csharp
public class SystemEventMessage : RedisPubSubProviderMessage
{
    public string EventType { get; set; }
    public string Source { get; set; }
    public object Data { get; set; }
    public string Severity { get; set; }
}

// Publisher
Channel = $"system:{eventType}"

// Subscriber  
Channel = "system:*"  // All system events
```

### Chat and Messaging

```csharp
public class ChatMessage : RedisPubSubFeederMessage
{
    public string RoomId { get; set; }
    public string SenderId { get; set; }
    public string Message { get; set; }
    public DateTime SentAt { get; set; }
}

// Publisher
Channel = $"chat:room:{roomId}"

// Subscriber
Channel = "chat:room:*"  // All chat rooms
```

## Integration Examples

### Service Registration

```csharp
// Feeder registration
services.AddRedisPubSubFeeder<MyChannel, MyRedisMessage, MyRedisConfiguration>(
    configuration, "RedisSettings");

// Provider registration
services.AddRedisPubSubProvider<MyProviderMessage, MyProviderConfiguration>(
    configuration, "RedisProvider");
```

### Application Pipeline

```csharp
// Feeder resolver usage
app.UseRedisPubSubFeederResolver<MyChannel, MyRedisMessage, MyRedisConfiguration>(
    channelKey, redisConfiguration);
```

### Custom Implementation

```csharp
public class AlertMessage : RedisPubSubFeederMessage
{
    public string AlertType { get; set; }
    public string Source { get; set; }
    public int Severity { get; set; }
    public string Description { get; set; }
}

public class AlertConfiguration : RedisPubSubFeederConfiguration
{
    public string Environment { get; set; } = "production";
    
    public override string Channel => $"alerts:{Environment}:*";
}
```

## Connection Management

### Connection String Options

```csharp
// Basic connection
"localhost:6379"

// With timeout settings
"localhost:6379,connectTimeout=5000,syncTimeout=3000"

// With authentication
"localhost:6379,password=mypassword"

// SSL/TLS connection
"secure-redis:6380,ssl=true,sslHost=secure-redis"

// Cluster configuration
"redis1:6379,redis2:6379,redis3:6379"

// Advanced options
"localhost:6379,abortConnect=false,connectRetry=3,keepAlive=60"
```

### Connection Resilience

```csharp
// Built-in reconnection handling
_connectionMultiplexer = ConnectionMultiplexer.Connect(connectionString);

// Health monitoring
HealthName = $"feeder_RedisPubSub_{Channel}";
HealthTags = [nameof(RedisPubSub), Channel];
```

## Error Handling and Recovery

### Publisher Error Handling

```csharp
try
{
    await _subscriber.PublishAsync(_redisChannel, bytes, CommandFlags.FireAndForget);
}
catch (Exception exception)
{
    Logger.LogError(exception,
        "Error publishing to Redis channel {Channel}", Channel);
    throw;
}
```

### Subscriber Error Handling

```csharp
try
{
    await ReceiveAsync(message, activityContext, baggage);
    ReportHealth(HealthStatus.Healthy);
}
catch (Exception exception)
{
    ReportHealth(HealthStatus.Unhealthy, exception);
    Logger.LogError(exception, 
        "Error processing message on channel {Channel}", Channel);
}
```

## Monitoring and Observability

### OpenTelemetry Integration

```csharp
// Automatic trace propagation
if (Activity.Current?.Context is not null)
    feederMessage.TryAdd(nameof(ActivityContext), Activity.Current.Context.ToNJsonBytes());

feederMessage.TryAdd(nameof(Baggage), Baggage.Current.ToNJsonBytes());
```

### Health Checks

```csharp
// Automatic health reporting
HealthName = $"feeder_RedisPubSub_{Channel}";
HealthTags = [nameof(RedisPubSub), Channel];

ReportHealth(HealthStatus.Healthy);           // Normal operation
ReportHealth(HealthStatus.Unhealthy, error);  // Error condition
```

### Logging

```csharp
Logger.LogInformation(
    "{FeederType}/{ChannelName} on Channel {Channel} configured",
    GetType().Name, channel.Metadata.ChannelName, Channel);

Logger.LogError(exception, 
    "Error on Redis channel {Channel}", Channel);
```

## Best Practices

### Channel Design
- Use hierarchical naming: `service:feature:action`
- Keep channel names short and descriptive
- Use patterns for logical grouping
- Avoid deep nesting levels

### Message Design
- Keep messages small and focused
- Use efficient serialization (NJson for .NET)
- Include timestamps for ordering
- Design for idempotency

### Performance Optimization
- Use fire-and-forget for high throughput
- Implement connection pooling
- Monitor Redis memory usage
- Use appropriate serialization

### Security
- Use authentication in production
- Enable SSL/TLS for network security
- Implement proper access controls
- Monitor connection patterns

## Use Cases

### Real-time Web Applications
```csharp
// Live updates, notifications, chat
Channel = "webapp:updates"
Channel = "chat:room:*"
Channel = "notifications:user:*"
```

### Microservices Communication
```csharp
// Service coordination, events
Channel = "services:events"
Channel = "coordination:*"
Channel = "health:*"
```

### IoT and Telemetry
```csharp
// Sensor data, device status
Channel = "iot:sensors:*"
Channel = "devices:status"
Channel = "telemetry:*"
```

### Cache Invalidation
```csharp
// Cache coordination
Channel = "cache:invalidate:*"
Channel = "cache:refresh"
```

## Limitations and Considerations

### Message Persistence
- Messages are **not persisted** in Redis
- Subscribers must be active to receive messages
- No message replay capability
- Consider Redis Streams for persistence needs

### Delivery Guarantees
- **At-most-once** delivery semantics
- No acknowledgment mechanism
- Messages may be lost if subscribers are offline
- Use Redis Streams or other systems for guaranteed delivery

### Scalability
- Pattern subscriptions can impact performance
- Monitor Redis memory and CPU usage
- Consider sharding for very high loads
- Use Redis Cluster for horizontal scaling

## Troubleshooting

### Common Issues

**Connection Failures**
- Check Redis server availability
- Verify connection string format
- Check firewall and network settings
- Monitor connection timeouts

**Missing Messages**
- Ensure subscribers are active before publishing
- Check pattern matching configuration
- Verify channel names are correct
- Monitor Redis logs for errors

**Performance Issues**
- Monitor Redis memory usage
- Check network latency
- Optimize message size
- Consider connection pooling

### Diagnostic Tools

```csharp
// Connection status monitoring
Logger.LogDebug("Redis connection state: {State}", 
    _connectionMultiplexer.IsConnected);

// Channel activity logging
Logger.LogTrace("Subscribed to channel pattern {Pattern}", Channel);
Logger.LogTrace("Published message to channel {Channel}", Channel);
```

## Version Compatibility

- **.NET Support**: .NET 8.0, .NET 9.0
- **Redis Version**: 3.0+, 4.0+, 5.0+, 6.0+, 7.0+
- **StackExchange.Redis**: v2.9.25
- **Platform Support**: Windows, Linux, macOS
- **Container Support**: Docker, Kubernetes ready

## Package Information

**Package Name**: `RapidStreamer.Feeviders.RedisPubSub.*`  
**Version**: 1.0.78  
**License**: Apache-2.0  
**Repository**: [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json)

## Related Documentation

- [SharedKernel Documentation](../SharedKernel/README.md) - Core abstractions and interfaces
- [WebSocket Documentation](../WebSocket/README.md) - Real-time web communication alternative
- [NATS Documentation](../NATS/README.md) - Cloud-native messaging comparison
- [RabbitMQ Documentation](../RabbitMQ/README.md) - Persistent messaging alternative

---

*This documentation covers the comprehensive Redis Pub/Sub implementation in RapidStreamer Feeviders, providing ultra-low latency real-time messaging capabilities for distributed applications.*