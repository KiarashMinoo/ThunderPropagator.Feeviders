# WebSocket Integration

## Contents

- [Overview](#overview)
- [Files](#files)
- [Types & Members](#types--members)
- [Real-time Communication Patterns](#real-time-communication-patterns)
- [Performance Notes](#performance-notes)
- [RapidStreamer Dependencies](#rapidstreamer-dependencies)
- [Examples](#examples)
- [See Also](#see-also)

[↑ Back to top](#contents)

## Overview

High-performance WebSocket integration enabling real-time bidirectional communication for web applications. Supports both server-side message consumption (Feeders) and client-side message publishing (Providers) with persistent connection management, automatic reconnection, and distributed tracing support.

Designed for real-time scenarios including live dashboards, chat applications, gaming, and IoT device communication with throughput capabilities up to 50K messages/second per connection.

Key features include path-based routing, configurable buffer sizes, automatic connection lifecycle management, and seamless integration with ASP.NET Core middleware pipeline.

[↑ Back to top](#contents)

## Files

| File | Primary type(s) | LOC (approx) | Responsibility |
|------|----------------|---------------|----------------|
| **RapidStreamer.Feeders.WebSocket** | | | |
| `WebSocketFeeder.cs` | WebSocketFeeder<> | 40 | Server-side WebSocket message consumption |
| `WebSocketFeederMessage.cs` | WebSocketFeederMessage | 5 | Base message contract for WebSocket consumption |
| `WebSocketFeederConfiguration.cs` | WebSocketFeederConfiguration | 30 | Server configuration with WebSocket settings |
| `WebSocketFeederExtensions.cs` | WebSocketFeederExtensions | 120 | ASP.NET Core middleware integration and DI |
| **RapidStreamer.Providers.DotNet.WebSocket** | | | |
| `WebSocketProvider.cs` | WebSocketProvider<> | 70 | Client-side WebSocket message publishing |
| `WebSocketProviderMessage.cs` | WebSocketProviderMessage | 5 | Base message contract for WebSocket publishing |
| `WebSocketProviderConfiguration.cs` | WebSocketProviderConfiguration | 15 | Client configuration with endpoint settings |
| `WebSocketProviderExtensions.cs` | WebSocketProviderExtensions | 25 | Dependency injection and service registration |

[↑ Back to top](#contents)

## Types & Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|------|------|---------|---------------------|-------------|
| **Feeders** | | | | |
| `WebSocketFeeder<TChannel, TMessage, TConfig>` | Class | Server-side WebSocket message receiver | `DelegativeFeeder<>`, `IFeature` | EnqueueAsync |
| `WebSocketFeederMessage` | Abstract Class | Base contract for WebSocket consumed messages | `FeederMessage` | (inheritance only) |
| `WebSocketFeederConfiguration` | Abstract Class | Server configuration with WebSocket settings | `WebSocketConfiguration`, `IAbstractFeederConfiguration` | IsEnabled, Path, SerializerType |
| `WebSocketFeederExtensions` | Static Class | ASP.NET Core middleware and service registration | - | AddWebSocketFeeder, UseWebSocketFeeder |
| **Providers** | | | | |
| `WebSocketProvider<TMessage, TConfig>` | Class | Client-side WebSocket message sender | `AbstractProvider<>` | InternalExecuteAsync |
| `WebSocketProviderMessage` | Abstract Class | Base contract for WebSocket published messages | `FeederMessage` | (inheritance only) |
| `WebSocketProviderConfiguration` | Abstract Class | Client configuration with endpoint settings | `AbstractProviderConfiguration` | Endpoint |
| `WebSocketProviderExtensions` | Static Class | Service registration extensions for providers | - | AddWebSocketProvider |

[↑ Back to top](#contents)

### WebSocketFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>

- **Kind**: Internal generic class  
- **Namespace**: `RapidStreamer.Feeders.WebSocket`
- **Inherits**: `DelegativeFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>`, `IFeature`
- **Attributes**: `IsAvailableOnDemo`, Internal visibility, sealed in Release builds

**Key Properties**:
- Inherits from `DelegativeFeeder` for message processing delegation
- Implements `IFeature` for feature discovery and management

**Key Methods**:
- `EnqueueAsync(byte[], CancellationToken) : ValueTask` — Process incoming WebSocket messages as byte arrays

**Health Monitoring**:
- `HealthName` format: `"feeder_WebSocket_{path_sanitized}"`
- `HealthTags` include `nameof(WebSocket)` and sanitized path segments

**Thread-safety**: Thread-safe message processing with delegated handling
**Serialization**: Supports configurable serialization via `SerializerType`

**Usage Recipe**:
```csharp
// Define message type
public class ChatMessage : WebSocketFeederMessage
{
    public string UserId { get; set; }
    public string Message { get; set; }
    public DateTime Timestamp { get; set; }
}

// Define configuration
public class ChatFeederConfig : WebSocketFeederConfiguration
{
    // WebSocket configuration properties inherited
}

// Register feeder
services.AddWebSocketFeeder<ChatChannel, ChatMessage, ChatFeederConfig>(
    configuration, "Messaging:WebSocket:ChatFeeder");

// Use middleware
app.UseWebSocketFeeder<ChatChannel, ChatMessage, ChatFeederConfig>();
```

[↑ Back to top](#contents)

### WebSocketFeederConfiguration

- **Kind**: Public abstract class
- **Namespace**: `RapidStreamer.Feeders.WebSocket`
- **Inherits**: `WebSocketConfiguration`, `IAbstractFeederConfiguration`
- **Attributes**: Abstract base for server configurations

**Key Properties**:
- `IsEnabled : bool` — Feeder activation state (default: false)
- `Id : Guid` — Unique feeder identifier (default: new GUID)
- `SerializerType : SerializerType` — Message serialization format (default: NJson)
- `EnrichmentScript : string?` — Optional message enrichment script
- `MetadataReferences : string[]?` — Assembly references for enrichment
- Inherits `Path`, `BufferSize`, and other WebSocket settings from `WebSocketConfiguration`

**Configuration Pattern**:
- Uses property-based configuration with automatic defaults
- Integrates with ASP.NET Core configuration binding
- Supports WebSocket-specific settings like buffer size and keep-alive

[↑ Back to top](#contents)

### WebSocketProvider<TWebSocketProviderMessage, TWebSocketProviderConfiguration>

- **Kind**: Internal generic class
- **Namespace**: `RapidStreamer.Providers.DotNet.WebSocket`
- **Inherits**: `AbstractProvider<TWebSocketProviderMessage, TWebSocketProviderConfiguration>`
- **Attributes**: Internal visibility, sealed in Release builds

**Key Properties**:
- `_clientWebSocket : ClientWebSocket` — System.Net.WebSockets client instance
- `_semaphoreSlim : SemaphoreSlim` — Thread-safe connection management (1,1)
- `_webSocketProviderConfiguration : TWebSocketProviderConfiguration` — Configuration instance

**Key Methods**:
- `InternalExecuteAsync(TWebSocketProviderMessage, CancellationToken) : Task` — Add tracing context to message (no network send)
- `InternalExecuteAsync(byte[], CancellationToken) : Task` — Send byte array to WebSocket endpoint

**Connection Management**:
- Automatic connection establishment on first send
- Thread-safe operations with semaphore protection
- Automatic reconnection on connection failures
- Proper disposal with async cleanup

**Distributed Tracing**:
- Automatic `ActivityContext` injection into messages
- `Baggage` propagation for correlation
- Error logging with endpoint context

**Usage Recipe**:
```csharp
// Define message type
public class NotificationMessage : WebSocketProviderMessage
{
    public string UserId { get; set; }
    public string Content { get; set; }
    public string Type { get; set; }
}

// Define configuration
public class NotificationProviderConfig : WebSocketProviderConfiguration
{
    // Endpoint configuration inherited
}

// Register provider
services.AddWebSocketProvider<NotificationMessage, NotificationProviderConfig>(
    configuration, "Messaging:WebSocket:NotificationProvider");

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
            Type = "info"
        });
    }
}
```

[↑ Back to top](#contents)

### WebSocketProviderConfiguration

- **Kind**: Public abstract class
- **Namespace**: `RapidStreamer.Providers.DotNet.WebSocket`
- **Inherits**: `AbstractProviderConfiguration`
- **Attributes**: Abstract base for client configurations

**Key Properties**:
- `Endpoint : string` — Required WebSocket endpoint URL (ws:// or wss://)

**Configuration**:
- Simple property-based configuration
- Required endpoint property for connection target
- Supports secure (wss://) and insecure (ws://) connections

[↑ Back to top](#contents)

## Real-time Communication Patterns

### Server-Side WebSocket Feeder Integration

The WebSocket feeder integrates directly with ASP.NET Core middleware pipeline:

1. **Middleware Registration**: `UseWebSocketFeeder<>()` configures WebSocket handling
2. **Path Routing**: Matches configured path for WebSocket upgrade requests
3. **Connection Handling**: Accepts WebSocket connections and manages lifecycle
4. **Message Processing**: Receives messages and delegates to feeder pipeline

**Message Flow**:
```
Client WebSocket → ASP.NET Core → WebSocket Middleware → WebSocketFeeder → Channel Processing
```

### Client-Side WebSocket Provider

The WebSocket provider manages persistent client connections:

1. **Lazy Connection**: Connection established on first message send
2. **Thread Safety**: Semaphore-protected operations for concurrent access
3. **Error Handling**: Automatic reconnection and error propagation
4. **Resource Management**: Proper disposal and cleanup

**Message Flow**:
```
Application Code → WebSocketProvider → ClientWebSocket → Remote WebSocket Server
```

### Connection Lifecycle

**Server (Feeder)**:
- Listens on configured path
- Accepts WebSocket upgrade requests  
- Processes messages until disconnection
- Handles client disconnections gracefully

**Client (Provider)**:
- Connects on-demand when sending first message
- Maintains persistent connection for subsequent sends
- Automatically reconnects on connection failures
- Disposes connection on provider disposal

[↑ Back to top](#contents)

## Performance Notes

### Throughput Characteristics

- **Peak Throughput**: 50K messages/second per connection
- **Latency**: <5ms end-to-end for small messages
- **Memory**: Efficient streaming with configurable buffer sizes
- **Concurrent Connections**: Scales with available system resources

### Optimization Recommendations

1. **Buffer Configuration**:
   ```csharp
   public class HighThroughputWebSocketConfig : WebSocketFeederConfiguration
   {
       public HighThroughputWebSocketConfig()
       {
           BufferSize = 8192;     // Larger buffer for batching
           SerializerType = SerializerType.NJson; // Fastest serialization
       }
   }
   ```

2. **Connection Pooling** (Provider):
   - Consider connection pooling for high-frequency scenarios
   - Reuse WebSocketProvider instances when possible
   - Monitor connection state for optimal performance

3. **Message Batching**:
   - Batch small messages for improved throughput
   - Use binary frames for optimal network utilization
   - Consider compression for large payloads

### Resource Management

- **Memory**: Fixed buffer allocation per connection
- **Threads**: Minimal thread overhead with async/await patterns
- **Network**: Persistent TCP connections with WebSocket framing
- **CPU**: Low CPU usage for message processing

[↑ Back to top](#contents)

## RapidStreamer Dependencies

| Package | Version | Description | Links |
|---------|---------|-------------|-------|
| **Core Dependencies** | | | |
| RapidStreamer.Feeders.SharedKernel | 1.0.76+ | Feeder base classes and interfaces | [SharedKernel](../SharedKernel/README.md#rapidstreamer-dependencies) |
| RapidStreamer.Providers.DotNet.SharedKernel | 1.0.76+ | Provider base classes and serialization | [SharedKernel](../SharedKernel/README.md#rapidstreamer-dependencies) |
| **WebSocket Packages** | | | |
| RapidStreamer.Feeders.WebSocket | 1.0.78+ | WebSocket server-side message consumption | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| RapidStreamer.Providers.DotNet.WebSocket | 1.0.78+ | WebSocket client-side message publishing | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |

### .NET Platform Dependencies

| Component | Version | Purpose | Documentation |
|-----------|---------|---------|---------------|
| System.Net.WebSockets | .NET 8.0+ | Core WebSocket client implementation | [.NET WebSockets](https://docs.microsoft.com/en-us/dotnet/api/system.net.websockets) |
| Microsoft.AspNetCore.WebSockets | .NET 8.0+ | ASP.NET Core WebSocket middleware | [ASP.NET Core WebSockets](https://docs.microsoft.com/en-us/aspnet/core/fundamentals/websockets) |
| System.Threading | .NET 8.0+ | Semaphore and synchronization primitives | [Threading Docs](https://docs.microsoft.com/en-us/dotnet/api/system.threading) |

[↑ Back to top](#contents)

## Examples

### Real-time Chat Application

```csharp
// Configuration (appsettings.json)
{
  "Messaging": {
    "WebSocket": {
      "ChatFeeder": {
        "Path": "/chat",
        "BufferSize": 4096,
        "SerializerType": "Json",
        "IsEnabled": true
      }
    }
  }
}

// Message definition
public class ChatMessage : WebSocketFeederMessage
{
    public string UserId { get; set; }
    public string Username { get; set; }
    public string Message { get; set; }
    public DateTime Timestamp { get; set; }
    public string RoomId { get; set; }
}

// Configuration class
public class ChatFeederConfig : WebSocketFeederConfiguration { }

// Channel definition
public class ChatChannel : IChannel
{
    public Guid Key { get; set; }
    public string Name { get; set; } = "ChatChannel";
}

// Startup registration
services.AddWebSocketFeeder<ChatChannel, ChatMessage, ChatFeederConfig>(
    configuration, "Messaging:WebSocket:ChatFeeder");

// Middleware registration
app.UseWebSocketFeeder<ChatChannel, ChatMessage, ChatFeederConfig>();

// Client-side JavaScript
const ws = new WebSocket('ws://localhost:5000/chat');
ws.onopen = () => console.log('Connected to chat');
ws.send(JSON.stringify({
    userId: 'user123',
    username: 'John',
    message: 'Hello, World!',
    timestamp: new Date().toISOString(),
    roomId: 'general'
}));
```

### Live Dashboard Updates

```csharp
// Configuration for dashboard provider
{
  "Messaging": {
    "WebSocket": {
      "DashboardProvider": {
        "Endpoint": "wss://dashboard.example.com/live"
      }
    }
  }
}

// Dashboard update message
public class DashboardUpdate : WebSocketProviderMessage
{
    public string MetricName { get; set; }
    public decimal Value { get; set; }
    public DateTime Timestamp { get; set; }
    public string Unit { get; set; }
}

// Configuration class
public class DashboardProviderConfig : WebSocketProviderConfiguration { }

// Registration
services.AddWebSocketProvider<DashboardUpdate, DashboardProviderConfig>(
    configuration, "Messaging:WebSocket:DashboardProvider");

// Usage in service
public class MetricsService
{
    private readonly IProvider<DashboardUpdate> _dashboardProvider;
    
    public MetricsService(IProvider<DashboardUpdate> dashboardProvider)
    {
        _dashboardProvider = dashboardProvider;
    }
    
    public async Task PublishMetricAsync(string metricName, decimal value, string unit)
    {
        await _dashboardProvider.ExecuteAsync(new DashboardUpdate
        {
            MetricName = metricName,
            Value = value,
            Timestamp = DateTime.UtcNow,
            Unit = unit
        });
    }
}

// Background service for periodic updates
public class MetricsBackgroundService : BackgroundService
{
    private readonly MetricsService _metricsService;
    
    public MetricsBackgroundService(MetricsService metricsService)
    {
        _metricsService = metricsService;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            // Collect metrics
            var cpuUsage = GetCpuUsage();
            var memoryUsage = GetMemoryUsage();
            
            // Publish to dashboard
            await _metricsService.PublishMetricAsync("cpu_usage", cpuUsage, "%");
            await _metricsService.PublishMetricAsync("memory_usage", memoryUsage, "MB");
            
            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }
}
```

### IoT Device Communication

```csharp
// IoT sensor data message
public class SensorReading : WebSocketFeederMessage
{
    public string DeviceId { get; set; }
    public string SensorType { get; set; }
    public double Value { get; set; }
    public DateTime ReadingTime { get; set; }
    public string Unit { get; set; }
    public Dictionary<string, object> Metadata { get; set; }
}

// Configuration for IoT gateway
public class IoTGatewayConfig : WebSocketFeederConfiguration
{
    public IoTGatewayConfig()
    {
        Path = "/iot/sensors";
        BufferSize = 8192; // Larger buffer for sensor data
        SerializerType = SerializerType.NJson; // Fast serialization
    }
}

// Device channel
public class IoTChannel : IChannel
{
    public Guid Key { get; set; }
    public string Name { get; set; } = "IoTSensorChannel";
}

// Message handler
public class SensorDataHandler : IFeederHandler<IoTChannel, SensorReading>
{
    private readonly ILogger<SensorDataHandler> _logger;
    
    public SensorDataHandler(ILogger<SensorDataHandler> logger)
    {
        _logger = logger;
    }
    
    public async Task<bool> HandleAsync(FeederReceivedMessage<SensorReading> receivedMessage, 
        CancellationToken cancellationToken = default)
    {
        var reading = receivedMessage.FeederMessage;
        
        _logger.LogInformation("Received sensor reading: {DeviceId} - {SensorType}: {Value} {Unit}",
            reading.DeviceId, reading.SensorType, reading.Value, reading.Unit);
            
        // Process sensor data (store in database, trigger alerts, etc.)
        await ProcessSensorDataAsync(reading);
        
        return true;
    }
    
    private async Task ProcessSensorDataAsync(SensorReading reading)
    {
        // Implementation for sensor data processing
        await Task.CompletedTask;
    }
}
```

[↑ Back to top](#contents)

## See Also

- [SharedKernel](../SharedKernel/README.md) - Base interfaces and utilities
- [WebAPI](../WebAPI/README.md) - HTTP REST integration alternative
- [TcpSocket](../TcpSocket/README.md) - Low-level TCP communication
- [Documentation Home](../README.md) - Framework overview and navigation

[↑ Back to top](#contents)

---

**Generated**: October 1, 2025  
**WebSocket Version**: System.Net.WebSockets (.NET 8.0+)  
**RapidStreamer Version**: 1.0.78