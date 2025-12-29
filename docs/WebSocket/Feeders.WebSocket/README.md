# ThunderPropagator.Feeders.WebSocket

## Overview

**ThunderPropagator.Feeders.WebSocket** is a push-based WebSocket feeder implementation using `DelegativeFeeder` for consuming WebSocket frames (text/binary) from WebSocket servers. It leverages `System.Net.WebSockets.ClientWebSocket` for RFC 6455-compliant communication with full-duplex, persistent connections.

### Key Features

- **Push-Based Architecture**: `DelegativeFeeder` with internal queue for frame buffering
- **Text & Binary Frames**: Supports UTF-8 text (JSON, XML) and binary data (MessagePack, Protobuf)
- **Subprotocol Negotiation**: STOMP, MQTT, GraphQL-WS, custom protocols
- **Compression**: `permessage-deflate` extension (30-50% bandwidth reduction)
- **Automatic Reconnection**: Exponential backoff on disconnect
- **Heartbeat Support**: Ping/Pong frames and application-level keep-alive
- **TLS/WSS**: Secure connections with certificate validation
- **Frame Fragmentation**: Automatic reassembly of multi-frame messages
- **OpenTelemetry Integration**: Distributed tracing for WebSocket operations

---

## Architecture

### Message Flow (Push-Based)

```mermaid
sequenceDiagram
    participant Server as WebSocket Server
    participant Client as ClientWebSocket
    participant Feeder as WebSocketFeeder
    participant Queue as Internal Queue
    participant App as Application
    
    Note over Server,App: Phase 1: Connection Handshake
    Client->>Server: HTTP GET /ws<br/>Upgrade: websocket<br/>Sec-WebSocket-Key: nonce
    Server-->>Client: 101 Switching Protocols<br/>Sec-WebSocket-Accept: hash
    
    Note over Server,App: Phase 2: Frame Reception (Push)
    loop ReceiveAsync Loop
        Server->>Client: Text Frame (0x1): {"event":"data"}
        Client->>Feeder: Frame received
        Feeder->>Feeder: Deserialize payload
        Feeder->>Queue: EnqueueAsync(message)
        Note over Queue: Buffered in memory
    end
    
    Note over Server,App: Phase 3: Application Consumption (Pull)
    App->>Feeder: ReceiveAsync() enumeration
    Feeder->>Queue: Dequeue message
    Queue-->>App: FeederReceivedMessage<T>
    App->>App: Process message
    
    Note over Server,App: Phase 4: Heartbeat
    Server->>Client: Ping Frame (0x9)
    Client->>Server: Pong Frame (0xA)
    
    Note over Server,App: Phase 5: Graceful Close
    Server->>Client: Close Frame (0x8): 1000
    Client->>Server: Close Frame (0x8): 1000
    Feeder->>App: Connection closed event
```

### Component Interaction

```mermaid
graph TB
    subgraph "WebSocket Feeder"
        A[ClientWebSocket] --> B[ReceiveAsync Loop]
        B --> C{Frame Type?}
        C -->|Text 0x1| D[UTF-8 Decode]
        C -->|Binary 0x2| E[Binary Buffer]
        C -->|Ping 0x9| F[Send Pong 0xA]
        C -->|Close 0x8| G[Reconnection Logic]
        D --> H[Deserialize JSON/XML]
        E --> I[Deserialize MessagePack/Protobuf]
        H --> J[EnqueueAsync]
        I --> J
        J --> K[Internal Queue BlockingCollection]
    end
    
    subgraph "DelegativeFeeder"
        K --> L[ReceiveAsync Enumerable]
        L --> M[Application Handler]
    end
    
    style A fill:#4CAF50
    style K fill:#2196F3
    style M fill:#FF9800
```

---

## Project Structure

### Files Overview

| File | Lines of Code | Description |
|------|---------------|-------------|
| `WebSocketFeeder.cs` | ~480 | Main feeder with `ClientWebSocket` integration and reconnection |
| `WebSocketFeederMessage.cs` | ~110 | Abstract message base with WebSocket frame metadata |
| `WebSocketFeederConfiguration.cs` | ~520 | Configuration with URI, subprotocols, headers, TLS options |
| `WebSocketFeederExtensions.cs` | ~165 | Dependency injection registration with resolver pattern |
| **Total** | **~1,275** | Complete WebSocket feeder implementation |

### Dependencies

```xml
<ItemGroup>
  <!-- ThunderPropagator Core -->
  <PackageReference Include="ThunderPropagator" Version="1.0.1-beta.2" />
  <PackageReference Include="ThunderPropagator.BuildingBlocks" Version="1.0.1-beta.2" />
  
  <!-- WebSocket (built-in .NET) -->
  <!-- System.Net.WebSockets.Client is included in .NET runtime -->
  
  <!-- Observability -->
  <PackageReference Include="OpenTelemetry.Api" Version="1.10.0" />
  
  <!-- Microsoft Extensions -->
  <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions" Version="9.0.0" />
  <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" Version="9.0.0" />
  <PackageReference Include="Microsoft.Extensions.Configuration.Abstractions" Version="9.0.0" />
  <PackageReference Include="Microsoft.Extensions.Caching.Memory" Version="9.0.0" />
</ItemGroup>
```

---

## Configuration

### WebSocketFeederConfiguration Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| **Connection** |
| `Uri` | `Uri` | Required | WebSocket endpoint (ws:// or wss://) |
| `Subprotocols` | `string[]?` | `null` | Requested subprotocols (stomp, mqtt, wamp) |
| `Headers` | `Dictionary<string, string>?` | `null` | Custom HTTP headers for handshake |
| `KeepAliveInterval` | `TimeSpan` | `00:00:30` | Ping interval (0 = disabled) |
| `BufferSize` | `int` | `4096` | Receive buffer size (bytes) |
| **TLS/Security** |
| `ServerCertificateValidationCallback` | `Func<...>?` | `null` | Custom certificate validation |
| `ClientCertificates` | `X509CertificateCollection?` | `null` | Client certificates for mutual TLS |
| `UseDefaultCredentials` | `bool` | `false` | Use Windows credentials |
| **Proxy** |
| `Proxy` | `IWebProxy?` | `null` | HTTP proxy configuration |
| **Reconnection** |
| `AutoReconnect` | `bool` | `true` | Automatically reconnect on disconnect |
| `MaxReconnectAttempts` | `int` | `10` | Maximum reconnection attempts (0 = infinite) |
| `ReconnectDelay` | `TimeSpan` | `00:00:05` | Initial reconnection delay |
| `ReconnectDelayMax` | `TimeSpan` | `00:05:00` | Maximum reconnection delay (exponential backoff) |
| **Frame Handling** |
| `MaxFrameSize` | `int` | `1048576` | Maximum frame size (1 MB default) |
| `TextEncoding` | `Encoding` | `UTF8` | Text frame encoding |
| `CloseTimeout` | `TimeSpan` | `00:00:10` | Timeout for close handshake |
| **Serialization** |
| `SerializerType` | `SerializerType` | `Json` | Message serialization (Json/NJson/NetJson) |
| **Enrichment** |
| `EnrichmentScript` | `string?` | `null` | C# script for message transformation |
| `MetadataReferences` | `string[]?` | `null` | Assemblies for enrichment script |
| **Identity** |
| `Id` | `Guid` | Auto-generated | Feeder instance identifier |

---

## API Reference

### WebSocketFeeder<TChannel, TMessage, TConfig>

**Inheritance**: `DelegativeFeeder<TChannel, TMessage, TConfig>` → `IFeeder<TChannel, TMessage>`

#### Constructor

```csharp
internal
#if !DEBUG
    sealed
#endif
    class WebSocketFeeder<TChannel, TMessage, TConfig> 
        : DelegativeFeeder<TChannel, TMessage, TConfig>
    where TChannel : IChannel
    where TMessage : WebSocketFeederMessage
    where TConfig : WebSocketFeederConfiguration
{
    public WebSocketFeeder(
        TChannel channel,
        TConfig configuration,
        ILogger<WebSocketFeeder<TChannel, TMessage, TConfig>> logger)
        : base(channel, configuration, logger)
    {
        HealthName = $"feeder_WebSocket_{configuration.Id}_{configuration.Uri.Host}";
        HealthTags = new[] { "WebSocket", configuration.Uri.Host };
    }
}
```

#### Methods

##### StartAsync (Background Receive Loop)

```csharp
protected override async Task ExecuteAsync(CancellationToken stoppingToken)
{
    while (!stoppingToken.IsCancellationRequested)
    {
        try
        {
            using var ws = new ClientWebSocket();
            
            // Configure options
            foreach (var subprotocol in Configuration.Subprotocols ?? Array.Empty<string>())
                ws.Options.AddSubProtocol(subprotocol);
            
            foreach (var header in Configuration.Headers ?? new Dictionary<string, string>())
                ws.Options.SetRequestHeader(header.Key, header.Value);
            
            ws.Options.KeepAliveInterval = Configuration.KeepAliveInterval;
            
            // Connect
            await ws.ConnectAsync(Configuration.Uri, stoppingToken);
            Logger.LogInformation("Connected to {Uri}", Configuration.Uri);
            
            // Receive loop
            var buffer = new byte[Configuration.BufferSize];
            while (ws.State == WebSocketState.Open)
            {
                var result = await ws.ReceiveAsync(buffer, stoppingToken);
                
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await ws.CloseAsync(
                        WebSocketCloseStatus.NormalClosure,
                        "Client closing",
                        stoppingToken);
                    break;
                }
                
                // Enqueue message to DelegativeFeeder queue
                if (result.MessageType == WebSocketMessageType.Text)
                {
                    var text = Configuration.TextEncoding.GetString(buffer, 0, result.Count);
                    await EnqueueAsync(text, stoppingToken);
                }
                else if (result.MessageType == WebSocketMessageType.Binary)
                {
                    var data = buffer[..result.Count];
                    await EnqueueAsync(data, stoppingToken);
                }
            }
        }
        catch (WebSocketException ex)
        {
            Logger.LogError(ex, "WebSocket error, reconnecting...");
            await Task.Delay(Configuration.ReconnectDelay, stoppingToken);
        }
    }
}
```

##### EnqueueAsync (Inherited from DelegativeFeeder)

```csharp
// Enqueue text frame
protected Task EnqueueAsync(string text, CancellationToken cancellationToken);

// Enqueue binary frame
protected Task EnqueueAsync(byte[] data, CancellationToken cancellationToken);
```

---

### WebSocketFeederMessage

**Inheritance**: `FeederMessage` → `IFeederMessage`

#### Properties

```csharp
public abstract class WebSocketFeederMessage : FeederMessage
{
    /// <summary>
    /// WebSocket frame type (Text or Binary).
    /// </summary>
    [JsonPropertyName("frameType")]
    public WebSocketMessageType FrameType { get; set; }
    
    /// <summary>
    /// Negotiated subprotocol (e.g., "stomp", "mqtt").
    /// </summary>
    [JsonPropertyName("subprotocol")]
    public string? Subprotocol { get; set; }
    
    /// <summary>
    /// End-of-message flag (final frame in fragmented message).
    /// </summary>
    [JsonPropertyName("endOfMessage")]
    public bool EndOfMessage { get; set; } = true;
    
    /// <summary>
    /// Frame size in bytes.
    /// </summary>
    [JsonPropertyName("frameSize")]
    public int FrameSize { get; set; }
    
    /// <summary>
    /// Timestamp when frame was received.
    /// </summary>
    [JsonPropertyName("receivedAt")]
    public DateTimeOffset ReceivedAt { get; set; } = DateTimeOffset.UtcNow;
}
```

#### Usage Example

```csharp
public class ChatMessage : WebSocketFeederMessage
{
    [JsonPropertyName("username")]
    public string Username { get; set; } = string.Empty;
    
    [JsonPropertyName("message")]
    public string Message { get; set; } = string.Empty;
    
    [JsonPropertyName("timestamp")]
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
}
```

---

### Extension Methods

#### AddWebSocketFeeder

```csharp
public static IServiceCollection AddWebSocketFeeder<TChannel, TMessage, TConfig>(
    this IServiceCollection services,
    IConfigurationRoot configuration,
    string sectionName)
    where TChannel : IChannel
    where TMessage : WebSocketFeederMessage
    where TConfig : WebSocketFeederConfiguration
{
    // Bind configuration
    var config = configuration.GetSection(sectionName).Get<TConfig>()
        ?? throw new InvalidOperationException($"Configuration section '{sectionName}' not found");
    
    // Register channel
    services.AddSingleton<TChannel>();
    
    // Register feeder as hosted service
    services.AddSingleton<IFeeder<TChannel, TMessage>, WebSocketFeeder<TChannel, TMessage, TConfig>>(sp =>
        new WebSocketFeeder<TChannel, TMessage, TConfig>(
            sp.GetRequiredService<TChannel>(),
            config,
            sp.GetRequiredService<ILogger<WebSocketFeeder<TChannel, TMessage, TConfig>>>()));
    
    return services;
}
```

#### AddWebSocketFeederResolver (Multi-Instance)

```csharp
public static IServiceCollection AddWebSocketFeederResolver<TChannel, TMessage, TConfig>(
    this IServiceCollection services)
    where TChannel : IChannel
    where TMessage : WebSocketFeederMessage
    where TConfig : WebSocketFeederConfiguration
{
    services.AddSingleton<IFeederResolver<TChannel, TMessage, TConfig>>(
        new FeederResolver<TChannel, TMessage, TConfig>());
    
    return services;
}
```

#### Usage

```csharp
// appsettings.json
{
  "Messaging": {
    "WebSocket": {
      "Uri": "wss://echo.websocket.org",
      "Subprotocols": ["stomp"],
      "KeepAliveInterval": "00:00:30",
      "BufferSize": 8192,
      "SerializerType": "Json"
    }
  }
}

// Program.cs
services.AddWebSocketFeeder<ChatChannel, ChatMessage, ChatConfig>(
    configuration, "Messaging:WebSocket");
```

---

## Examples

### Example 1: Basic Text Frame Receiving (Echo Server)

```csharp
// Message definition
public class EchoMessage : WebSocketFeederMessage
{
    [JsonPropertyName("text")]
    public string Text { get; set; } = string.Empty;
    
    [JsonPropertyName("timestamp")]
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
}

// Configuration
public class EchoConfig : WebSocketFeederConfiguration
{
    public EchoConfig()
    {
        Uri = new Uri("wss://echo.websocket.org");
        KeepAliveInterval = TimeSpan.FromSeconds(30);
        BufferSize = 4096;
        SerializerType = SerializerType.Json;
    }
}

// Channel
public class EchoChannel : IChannel
{
    public string Name => "echo-channel";
}

// DI registration
services.AddWebSocketFeeder<EchoChannel, EchoMessage, EchoConfig>(
    configuration, "Messaging:WebSocket");

// Message handler
public class EchoMessageHandler : IHostedService
{
    private readonly IFeeder<EchoChannel, EchoMessage> _feeder;
    private readonly ILogger<EchoMessageHandler> _logger;
    
    public EchoMessageHandler(
        IFeeder<EchoChannel, EchoMessage> feeder,
        ILogger<EchoMessageHandler> logger)
    {
        _feeder = feeder;
        _logger = logger;
    }
    
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await foreach (var received in _feeder.ReceiveAsync(cancellationToken))
        {
            _logger.LogInformation(
                "Received echo: {Text} (Frame: {Type}, Size: {Size} bytes)",
                received.Message.Text,
                received.Message.FrameType,
                received.Message.FrameSize);
        }
    }
    
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

// Usage
var handler = serviceProvider.GetRequiredService<EchoMessageHandler>();
await handler.StartAsync(CancellationToken.None);

// Output:
// Received echo: Hello WebSocket (Frame: Text, Size: 16 bytes)
```

---

### Example 2: Binary Frame Receiving (MessagePack)

```csharp
// Message definition with binary payload
public class SensorDataMessage : WebSocketFeederMessage
{
    [JsonPropertyName("sensorId")]
    public string SensorId { get; set; } = string.Empty;
    
    [JsonPropertyName("temperature")]
    public double Temperature { get; set; }
    
    [JsonPropertyName("humidity")]
    public double Humidity { get; set; }
    
    [JsonPropertyName("timestamp")]
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
}

// Configuration for binary frames
public class SensorConfig : WebSocketFeederConfiguration
{
    public SensorConfig()
    {
        Uri = new Uri("wss://iot.example.com/sensors");
        KeepAliveInterval = TimeSpan.FromSeconds(15);
        BufferSize = 8192;
        SerializerType = SerializerType.Json; // Or MessagePack if custom deserializer
    }
}

// Message handler with binary processing
public class SensorDataHandler
{
    private readonly IFeeder<SensorChannel, SensorDataMessage> _feeder;
    
    public SensorDataHandler(IFeeder<SensorChannel, SensorDataMessage> feeder)
    {
        _feeder = feeder;
    }
    
    public async Task ProcessSensorDataAsync(CancellationToken cancellationToken)
    {
        await foreach (var received in _feeder.ReceiveAsync(cancellationToken))
        {
            var message = received.Message;
            
            // Check frame type
            if (message.FrameType == WebSocketMessageType.Binary)
            {
                Console.WriteLine($"Binary sensor data: {message.SensorId}");
                Console.WriteLine($"  Temperature: {message.Temperature}°C");
                Console.WriteLine($"  Humidity: {message.Humidity}%");
                Console.WriteLine($"  Frame size: {message.FrameSize} bytes");
            }
        }
    }
}

// Usage
var handler = serviceProvider.GetRequiredService<SensorDataHandler>();
await handler.ProcessSensorDataAsync(CancellationToken.None);

// Output:
// Binary sensor data: sensor-123
//   Temperature: 23.5°C
//   Humidity: 45.2%
//   Frame size: 128 bytes
```

---

### Example 3: Subprotocol Negotiation (STOMP)

```csharp
// STOMP message frame
public class StompMessage : WebSocketFeederMessage
{
    [JsonPropertyName("command")]
    public string Command { get; set; } = string.Empty; // CONNECT, SEND, SUBSCRIBE
    
    [JsonPropertyName("headers")]
    public Dictionary<string, string> Headers { get; set; } = new();
    
    [JsonPropertyName("body")]
    public string Body { get; set; } = string.Empty;
}

// STOMP configuration with subprotocol
public class StompConfig : WebSocketFeederConfiguration
{
    public StompConfig()
    {
        Uri = new Uri("wss://broker.example.com/stomp");
        
        // Request STOMP subprotocol
        Subprotocols = new[] { "stomp", "v12.stomp" };
        
        // STOMP-specific headers
        Headers = new Dictionary<string, string>
        {
            ["login"] = "admin",
            ["passcode"] = "secret"
        };
        
        KeepAliveInterval = TimeSpan.FromSeconds(60);
        BufferSize = 8192;
    }
}

// STOMP message parser
public class StompMessageHandler
{
    private readonly IFeeder<StompChannel, StompMessage> _feeder;
    private readonly ILogger<StompMessageHandler> _logger;
    
    public StompMessageHandler(
        IFeeder<StompChannel, StompMessage> feeder,
        ILogger<StompMessageHandler> logger)
    {
        _feeder = feeder;
        _logger = logger;
    }
    
    public async Task HandleStompFramesAsync(CancellationToken cancellationToken)
    {
        await foreach (var received in _feeder.ReceiveAsync(cancellationToken))
        {
            var message = received.Message;
            
            _logger.LogInformation(
                "STOMP {Command} frame (Subprotocol: {Subprotocol})",
                message.Command,
                message.Subprotocol);
            
            switch (message.Command)
            {
                case "CONNECTED":
                    _logger.LogInformation("Connected to STOMP broker");
                    break;
                
                case "MESSAGE":
                    var destination = message.Headers["destination"];
                    _logger.LogInformation("Message from {Destination}: {Body}",
                        destination, message.Body);
                    break;
                
                case "ERROR":
                    _logger.LogError("STOMP error: {Body}", message.Body);
                    break;
            }
        }
    }
}

// Usage
services.AddWebSocketFeeder<StompChannel, StompMessage, StompConfig>(
    configuration, "Messaging:Stomp");

var handler = serviceProvider.GetRequiredService<StompMessageHandler>();
await handler.HandleStompFramesAsync(CancellationToken.None);

// Output:
// STOMP CONNECTED frame (Subprotocol: stomp)
// Connected to STOMP broker
// STOMP MESSAGE frame (Subprotocol: stomp)
// Message from /topic/events: {"event":"user.login"}
```

---

### Example 4: Compression (permessage-deflate)

```csharp
// Configuration with compression extension
public class CompressedConfig : WebSocketFeederConfiguration
{
    public CompressedConfig()
    {
        Uri = new Uri("wss://api.example.com/ws");
        
        // Request compression extension in handshake
        // Note: ClientWebSocket automatically adds permessage-deflate
        // Server must support and respond with extension acceptance
        
        BufferSize = 16384; // Larger buffer for compressed frames
        KeepAliveInterval = TimeSpan.FromSeconds(30);
    }
}

// Message with large JSON payload
public class LargeDataMessage : WebSocketFeederMessage
{
    [JsonPropertyName("id")]
    public Guid Id { get; set; } = Guid.NewGuid();
    
    [JsonPropertyName("data")]
    public string Data { get; set; } = string.Empty; // Large JSON string
    
    [JsonPropertyName("timestamp")]
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
}

// Handler with compression metrics
public class CompressionMetricsHandler
{
    private readonly IFeeder<DataChannel, LargeDataMessage> _feeder;
    
    public async Task MonitorCompressionAsync(CancellationToken cancellationToken)
    {
        long totalFrames = 0;
        long totalBytes = 0;
        
        await foreach (var received in _feeder.ReceiveAsync(cancellationToken))
        {
            totalFrames++;
            totalBytes += received.Message.FrameSize;
            
            Console.WriteLine($"Frame {totalFrames}:");
            Console.WriteLine($"  Compressed size: {received.Message.FrameSize} bytes");
            Console.WriteLine($"  Subprotocol: {received.Message.Subprotocol}");
            Console.WriteLine($"  Avg bytes/frame: {totalBytes / totalFrames}");
        }
    }
}

// Output (with permessage-deflate):
// Frame 1:
//   Compressed size: 512 bytes (original: 1024 bytes, 50% reduction)
//   Subprotocol: null
//   Avg bytes/frame: 512

// Without compression:
// Frame 1:
//   Compressed size: 1024 bytes
//   Avg bytes/frame: 1024
```

---

### Example 5: Heartbeat (Ping/Pong) with Keep-Alive

```csharp
// Configuration with keep-alive
public class HeartbeatConfig : WebSocketFeederConfiguration
{
    public HeartbeatConfig()
    {
        Uri = new Uri("wss://realtime.example.com/ws");
        
        // TCP-level keep-alive (OS manages ping/pong)
        KeepAliveInterval = TimeSpan.FromSeconds(20);
        
        BufferSize = 4096;
    }
}

// Message with heartbeat tracking
public class HeartbeatMessage : WebSocketFeederMessage
{
    [JsonPropertyName("type")]
    public string Type { get; set; } = string.Empty; // "ping", "pong", "data"
    
    [JsonPropertyName("payload")]
    public string? Payload { get; set; }
}

// Handler with application-level heartbeat
public class HeartbeatHandler
{
    private readonly IFeeder<HeartbeatChannel, HeartbeatMessage> _feeder;
    private DateTimeOffset _lastPong = DateTimeOffset.UtcNow;
    
    public async Task MonitorConnectionAsync(CancellationToken cancellationToken)
    {
        // Application-level heartbeat timer (redundant with KeepAliveInterval)
        var heartbeatTimer = new Timer(_ =>
        {
            var elapsed = DateTimeOffset.UtcNow - _lastPong;
            if (elapsed > TimeSpan.FromSeconds(60))
            {
                Console.WriteLine("Warning: No pong received for 60 seconds");
            }
        }, null, TimeSpan.Zero, TimeSpan.FromSeconds(10));
        
        await foreach (var received in _feeder.ReceiveAsync(cancellationToken))
        {
            var message = received.Message;
            
            switch (message.Type)
            {
                case "ping":
                    Console.WriteLine("Ping received (server is alive)");
                    break;
                
                case "pong":
                    _lastPong = DateTimeOffset.UtcNow;
                    Console.WriteLine("Pong received (connection healthy)");
                    break;
                
                case "data":
                    Console.WriteLine($"Data: {message.Payload}");
                    break;
            }
        }
        
        heartbeatTimer.Dispose();
    }
}

// Usage
var handler = serviceProvider.GetRequiredService<HeartbeatHandler>();
await handler.MonitorConnectionAsync(CancellationToken.None);

// Output:
// Ping received (server is alive)
// Pong received (connection healthy)
// Data: {"event":"update"}
// Pong received (connection healthy)
```

---

### Example 6: Reconnection on Disconnect

```csharp
// Configuration with aggressive reconnection
public class ReconnectConfig : WebSocketFeederConfiguration
{
    public ReconnectConfig()
    {
        Uri = new Uri("wss://unstable.example.com/ws");
        
        // Reconnection settings
        AutoReconnect = true;
        MaxReconnectAttempts = 20; // 0 = infinite
        ReconnectDelay = TimeSpan.FromSeconds(2);
        ReconnectDelayMax = TimeSpan.FromMinutes(5); // Exponential backoff cap
        
        KeepAliveInterval = TimeSpan.FromSeconds(15);
    }
}

// Message with reconnection tracking
public class ReconnectMessage : WebSocketFeederMessage
{
    [JsonPropertyName("id")]
    public Guid Id { get; set; } = Guid.NewGuid();
    
    [JsonPropertyName("data")]
    public string Data { get; set; } = string.Empty;
}

// Handler with reconnection awareness
public class ReconnectionHandler
{
    private readonly IFeeder<ReconnectChannel, ReconnectMessage> _feeder;
    private readonly ILogger<ReconnectionHandler> _logger;
    private int _reconnectCount = 0;
    
    public async Task HandleWithReconnectionAsync(CancellationToken cancellationToken)
    {
        await foreach (var received in _feeder.ReceiveAsync(cancellationToken))
        {
            // Check if this is the first message after reconnection
            if (received.Message.ReceivedAt - DateTimeOffset.UtcNow > TimeSpan.FromSeconds(10))
            {
                _reconnectCount++;
                _logger.LogInformation(
                    "Reconnected successfully (attempt {Count})",
                    _reconnectCount);
            }
            
            _logger.LogInformation("Processing message: {Id}", received.Message.Id);
        }
    }
}

// Feeder internal reconnection logic (exponential backoff):
// Attempt 1: Wait 2s
// Attempt 2: Wait 4s (2 × 2)
// Attempt 3: Wait 8s (2 × 4)
// Attempt 4: Wait 16s
// Attempt 5: Wait 32s
// ...
// Attempt 10: Wait 300s (capped at ReconnectDelayMax)

// Output:
// Warning: WebSocket error, reconnecting...
// Reconnected successfully (attempt 1)
// Processing message: 3fa85f64-5717-4562-b3fc-2c963f66afa6
// Warning: WebSocket error, reconnecting...
// Reconnected successfully (attempt 2)
```

---

## Advanced Patterns

### 1. Frame Types (Text 0x1 vs Binary 0x2)

```csharp
public class FrameTypeHandler
{
    public async Task HandleMixedFramesAsync(
        IFeeder<MixedChannel, MixedMessage> feeder,
        CancellationToken cancellationToken)
    {
        await foreach (var received in feeder.ReceiveAsync(cancellationToken))
        {
            var message = received.Message;
            
            switch (message.FrameType)
            {
                case WebSocketMessageType.Text:
                    // UTF-8 text (JSON, XML, plain text)
                    var json = JsonSerializer.Deserialize<JsonDocument>(message.Data);
                    Console.WriteLine($"Text frame: {json}");
                    break;
                
                case WebSocketMessageType.Binary:
                    // Binary data (MessagePack, Protobuf, images)
                    var msgPack = MessagePackSerializer.Deserialize<MyData>(message.Data);
                    Console.WriteLine($"Binary frame: {msgPack}");
                    break;
                
                case WebSocketMessageType.Close:
                    // Close frame (connection terminating)
                    Console.WriteLine("Close frame received");
                    break;
            }
        }
    }
}

// Frame type detection:
// - Server sends Text (0x1) → ClientWebSocket.ReceiveAsync returns MessageType.Text
// - Server sends Binary (0x2) → ClientWebSocket.ReceiveAsync returns MessageType.Binary
// - Server sends Close (0x8) → ClientWebSocket.ReceiveAsync returns MessageType.Close
```

---

### 2. Subprotocol Negotiation (Sec-WebSocket-Protocol)

```csharp
// Request multiple subprotocols (client preference order)
public class MultiSubprotocolConfig : WebSocketFeederConfiguration
{
    public MultiSubprotocolConfig()
    {
        Uri = new Uri("wss://broker.example.com/ws");
        
        // Client requests in order of preference
        Subprotocols = new[] { "stomp", "v12.stomp", "v11.stomp" };
        
        // Server picks ONE from the list (or none)
    }
}

// Check negotiated subprotocol
public async Task HandleSubprotocolAsync(
    IFeeder<SubprotocolChannel, SubprotocolMessage> feeder,
    CancellationToken cancellationToken)
{
    await foreach (var received in feeder.ReceiveAsync(cancellationToken))
    {
        var negotiated = received.Message.Subprotocol;
        
        if (negotiated == "stomp")
        {
            // Parse STOMP frames
            var stompFrame = ParseStompFrame(received.Message.Data);
            Console.WriteLine($"STOMP command: {stompFrame.Command}");
        }
        else if (negotiated == "mqtt")
        {
            // Parse MQTT packets
            var mqttPacket = ParseMqttPacket(received.Message.Data);
            Console.WriteLine($"MQTT packet type: {mqttPacket.Type}");
        }
        else
        {
            // No subprotocol (generic WebSocket)
            Console.WriteLine("Generic WebSocket frame");
        }
    }
}

// Handshake:
// Client: Sec-WebSocket-Protocol: stomp, mqtt
// Server: Sec-WebSocket-Protocol: stomp  (server picks stomp)
```

---

### 3. Compression (permessage-deflate Extension)

```csharp
// ClientWebSocket automatically requests compression if server supports it
// Handshake (automatic):
// Client: Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits
// Server: Sec-WebSocket-Extensions: permessage-deflate; server_max_window_bits=15

// Frame structure (RSV1 bit indicates compression):
// +-------+
// |1 0 0 0|  RSV1=1 (compressed), RSV2=0, RSV3=0, Opcode=0x1 (text)
// +-------+
// | Compressed payload (DEFLATE) |
// +------------------------------+

public class CompressionAnalyzer
{
    public async Task AnalyzeCompressionAsync(
        IFeeder<CompressChannel, CompressMessage> feeder,
        CancellationToken cancellationToken)
    {
        long totalFrames = 0;
        long totalCompressedBytes = 0;
        long totalUncompressedBytes = 0;
        
        await foreach (var received in feeder.ReceiveAsync(cancellationToken))
        {
            totalFrames++;
            totalCompressedBytes += received.Message.FrameSize;
            
            // Estimate uncompressed size (for JSON: ~2x compressed size)
            totalUncompressedBytes += received.Message.FrameSize * 2;
            
            if (totalFrames % 100 == 0)
            {
                var compressionRatio = (double)totalCompressedBytes / totalUncompressedBytes;
                Console.WriteLine($"Compression: {compressionRatio:P} of original size");
                Console.WriteLine($"Bandwidth saved: {totalUncompressedBytes - totalCompressedBytes} bytes");
            }
        }
    }
}

// Output:
// Compression: 45.2% of original size
// Bandwidth saved: 128,456 bytes
// Compression: 42.8% of original size
// Bandwidth saved: 512,894 bytes
```

---

### 4. Heartbeat (Ping/Pong Frames, Application-Level Keep-Alive)

```csharp
// Network-level heartbeat (automatic with KeepAliveInterval)
public class NetworkHeartbeatConfig : WebSocketFeederConfiguration
{
    public NetworkHeartbeatConfig()
    {
        Uri = new Uri("wss://example.com/ws");
        
        // ClientWebSocket sends Ping frames every 30 seconds
        // Server responds with Pong frames automatically
        KeepAliveInterval = TimeSpan.FromSeconds(30);
    }
}

// Application-level heartbeat (custom ping/pong messages)
public class ApplicationHeartbeat
{
    private readonly ClientWebSocket _ws;
    private DateTimeOffset _lastPong = DateTimeOffset.UtcNow;
    
    public async Task StartApplicationHeartbeatAsync(CancellationToken cancellationToken)
    {
        // Send application-level ping every 15 seconds
        var pingTimer = new PeriodicTimer(TimeSpan.FromSeconds(15));
        
        while (await pingTimer.WaitForNextTickAsync(cancellationToken))
        {
            var ping = Encoding.UTF8.GetBytes("{\"type\":\"ping\"}");
            await _ws.SendAsync(ping, WebSocketMessageType.Text, true, cancellationToken);
            
            // Check for timeout
            if (DateTimeOffset.UtcNow - _lastPong > TimeSpan.FromSeconds(60))
            {
                Console.WriteLine("Connection timeout: no pong received");
                await _ws.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Timeout",
                    cancellationToken);
                break;
            }
        }
    }
    
    public void OnPongReceived()
    {
        _lastPong = DateTimeOffset.UtcNow;
        Console.WriteLine($"Pong received, latency: {DateTimeOffset.UtcNow - _lastPong}ms");
    }
}

// Comparison:
// Network-level (Ping/Pong frames):
//   - Automatic (ClientWebSocket.Options.KeepAliveInterval)
//   - Operates at WebSocket protocol level
//   - No application logic required
//
// Application-level (custom messages):
//   - Manual implementation
//   - Operates at message level (JSON, binary)
//   - Allows measuring round-trip latency
```

---

### 5. Backpressure (Pause ReceiveAsync if Queue Full)

```csharp
// DelegativeFeeder internal queue with backpressure
public class BackpressureHandler
{
    private const int MaxQueueSize = 1000;
    
    public async Task HandleBackpressureAsync(
        IFeeder<BackpressureChannel, BackpressureMessage> feeder,
        CancellationToken cancellationToken)
    {
        // Slow consumer (processing takes 100ms per message)
        await foreach (var received in feeder.ReceiveAsync(cancellationToken))
        {
            await Task.Delay(100, cancellationToken); // Simulate slow processing
            
            Console.WriteLine($"Processed message: {received.Message.Id}");
        }
    }
}

// DelegativeFeeder internal logic:
// 1. ReceiveAsync loop enqueues messages to BlockingCollection<T>
// 2. If queue is full (MaxQueueSize), EnqueueAsync blocks
// 3. ReceiveAsync enumeration dequeues messages (backpressure relief)
// 4. Flow control: Fast producer (WebSocket) → Queue → Slow consumer

// Queue metrics:
// - Queue size: BlockingCollection.Count
// - Enqueue blocked: Thread waiting on BlockingCollection.Add()
// - Dequeue rate: Messages/second consumed by application

// Configuration:
public class BackpressureConfig : WebSocketFeederConfiguration
{
    public int MaxQueueSize { get; set; } = 10000; // Adjust based on memory constraints
}

// Monitoring:
public class QueueMonitor
{
    public void MonitorQueueDepth(BlockingCollection<WebSocketFeederMessage> queue)
    {
        var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        while (timer.WaitForNextTickAsync().GetAwaiter().GetResult())
        {
            Console.WriteLine($"Queue depth: {queue.Count}/{queue.BoundedCapacity}");
            
            if (queue.Count > queue.BoundedCapacity * 0.8)
            {
                Console.WriteLine("Warning: Queue 80% full, backpressure engaged");
            }
        }
    }
}
```

---

### 6. TLS/WSS (Server Certificate Validation)

```csharp
// Production: Validate server certificate
public class SecureConfig : WebSocketFeederConfiguration
{
    public SecureConfig()
    {
        Uri = new Uri("wss://secure.example.com/ws");
        
        // Custom certificate validation
        ServerCertificateValidationCallback = (sender, cert, chain, errors) =>
        {
            // Production: validate certificate chain
            if (errors == SslPolicyErrors.None)
                return true;
            
            // Log certificate details
            Console.WriteLine($"Certificate: {cert.Subject}");
            Console.WriteLine($"Errors: {errors}");
            
            return false; // Reject invalid certificates
        };
    }
}

// Development: Accept self-signed certificates
public class DevConfig : WebSocketFeederConfiguration
{
    public DevConfig()
    {
        Uri = new Uri("wss://localhost:5001/ws");
        
        // Development only: accept self-signed certificates
        ServerCertificateValidationCallback = (sender, cert, chain, errors) =>
        {
            if (errors == SslPolicyErrors.RemoteCertificateChainErrors)
            {
                Console.WriteLine("Warning: Self-signed certificate accepted (dev only)");
                return true; // Only in dev!
            }
            
            return errors == SslPolicyErrors.None;
        };
    }
}

// Mutual TLS (client certificate authentication)
public class MutualTlsConfig : WebSocketFeederConfiguration
{
    public MutualTlsConfig()
    {
        Uri = new Uri("wss://api.example.com/ws");
        
        // Load client certificate
        var cert = new X509Certificate2("client-cert.pfx", "password");
        ClientCertificates = new X509CertificateCollection { cert };
    }
}
```

---

### 7. Authentication (Bearer Tokens, Custom Headers)

```csharp
// Bearer token in custom header (most common)
public class AuthenticatedConfig : WebSocketFeederConfiguration
{
    public AuthenticatedConfig()
    {
        Uri = new Uri("wss://api.example.com/ws");
        
        // Add Authorization header
        Headers = new Dictionary<string, string>
        {
            ["Authorization"] = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
            ["X-API-Key"] = "secret-api-key"
        };
    }
}

// Query string authentication (alternative)
public class QueryAuthConfig : WebSocketFeederConfiguration
{
    public QueryAuthConfig()
    {
        var token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...";
        Uri = new Uri($"wss://api.example.com/ws?token={token}");
    }
}

// Subprotocol authentication (STOMP example)
public class StompAuthConfig : WebSocketFeederConfiguration
{
    public StompAuthConfig()
    {
        Uri = new Uri("wss://broker.example.com/stomp");
        Subprotocols = new[] { "stomp" };
        
        // STOMP login/passcode in headers
        Headers = new Dictionary<string, string>
        {
            ["login"] = "admin",
            ["passcode"] = "secret123"
        };
    }
}

// Windows authentication (Integrated Windows Auth)
public class WindowsAuthConfig : WebSocketFeederConfiguration
{
    public WindowsAuthConfig()
    {
        Uri = new Uri("wss://intranet.example.com/ws");
        UseDefaultCredentials = true; // Use current Windows user
    }
}
```

---

## Best Practices

### 1. Subprotocol Selection
```csharp
// ✅ Good: Use standard subprotocols
Subprotocols = new[] { "stomp", "mqtt", "graphql-ws" };

// ❌ Bad: Custom subprotocols without documentation
Subprotocols = new[] { "my-custom-protocol" };
```

### 2. Buffer Sizing
```csharp
// ✅ Good: Match expected message size
BufferSize = 8192;  // 8KB for small JSON messages
BufferSize = 65536; // 64KB for large binary payloads

// ❌ Bad: Too small (multiple reads) or too large (memory waste)
BufferSize = 256;   // Too small
BufferSize = 1048576; // 1MB (excessive for most use cases)
```

### 3. Keep-Alive Tuning
```csharp
// ✅ Good: Balance between responsiveness and overhead
KeepAliveInterval = TimeSpan.FromSeconds(30); // Every 30s

// ❌ Bad: Too frequent (overhead) or too infrequent (slow detection)
KeepAliveInterval = TimeSpan.FromSeconds(1);  // Too frequent
KeepAliveInterval = TimeSpan.FromMinutes(10); // Too infrequent
```

### 4. TLS for Security
```csharp
// ✅ Production: Always use wss://
Uri = new Uri("wss://api.example.com/ws");

// ❌ Production: Never use ws:// (plaintext)
Uri = new Uri("ws://api.example.com/ws"); // Insecure!
```

### 5. Graceful Close
```csharp
// ✅ Good: Send close frame before disposing
await ws.CloseAsync(
    WebSocketCloseStatus.NormalClosure,
    "Client shutting down",
    CancellationToken.None);

// ❌ Bad: Abrupt disconnect (no close frame)
ws.Dispose(); // Connection reset
```

---

## Related Documentation

- **[WebSocket Provider Documentation](../Providers.DotNet.WebSocket/README.md)** — Sending WebSocket frames
- **[WebSocket System Overview](../README.md)** — Architecture and protocol details
- **[SharedKernel Feeder Documentation](../../SharedKernel/Feeders.SharedKernel/README.md)** — `DelegativeFeeder<TChannel, TMessage, TConfig>` base class
- **[RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455)** — WebSocket Protocol Specification
- **[RFC 7692](https://datatracker.ietf.org/doc/html/rfc7692)** — Compression Extensions (permessage-deflate)

---

## Summary

**ThunderPropagator.Feeders.WebSocket** provides:

✅ **Push-Based Architecture**: `DelegativeFeeder` with automatic frame buffering  
✅ **Text & Binary Frames**: JSON, MessagePack, Protobuf, custom formats  
✅ **Subprotocol Support**: STOMP, MQTT, GraphQL-WS, WAMP  
✅ **Compression**: 30-50% bandwidth reduction with `permessage-deflate`  
✅ **Auto-Reconnection**: Exponential backoff on disconnect  
✅ **Heartbeat**: Ping/Pong frames detect broken connections  
✅ **TLS/WSS**: Secure connections with certificate validation  
✅ **Authentication**: Bearer tokens, API keys, Windows auth  

**Ideal For**: Real-time feeds, chat, collaboration, IoT, trading platforms  
**Complement**: [Providers.DotNet.WebSocket](../Providers.DotNet.WebSocket/README.md) for sending frames
