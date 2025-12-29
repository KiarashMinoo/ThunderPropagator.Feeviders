# ThunderPropagator.Providers.DotNet.WebSocket

## Overview

**ThunderPropagator.Providers.DotNet.WebSocket** is a WebSocket provider implementation for sending text and binary frames to WebSocket servers using `System.Net.WebSockets.ClientWebSocket`. It extends `AbstractProvider<TMessage, TConfig>` to provide full-duplex, persistent connection messaging with subprotocol support and compression.

### Key Features

- **Text & Binary Frames**: Send UTF-8 text (JSON, XML) or binary data (MessagePack, Protobuf)
- **Fragmentation Support**: Split large messages into multiple frames with `endOfMessage` control
- **Subprotocol Integration**: STOMP, MQTT, GraphQL-WS, custom application protocols
- **Compression**: `permessage-deflate` extension for 30-50% bandwidth reduction
- **Connection Pooling**: Reuse WebSocket connections for improved performance
- **TLS/WSS**: Secure connections with certificate validation
- **Graceful Close**: Proper close handshake with status codes
- **OpenTelemetry Integration**: Distributed tracing for WebSocket send operations

---

## Architecture

### Sending Sequence

```mermaid
sequenceDiagram
    participant App as Application
    participant Provider as WebSocketProvider
    participant Serializer as Message Serializer
    participant WS as ClientWebSocket
    participant Server as WebSocket Server
    
    Note over App,Server: Phase 1: Connection Establishment
    App->>Provider: ExecuteAsync(message)
    Provider->>WS: Get or create connection
    WS->>Server: HTTP GET /ws<br/>Upgrade: websocket<br/>Sec-WebSocket-Key: nonce
    Server-->>WS: 101 Switching Protocols<br/>Sec-WebSocket-Accept: hash
    
    Note over App,Server: Phase 2: Message Sending
    Provider->>Serializer: Serialize(message)
    Serializer-->>Provider: byte[] payload
    Provider->>Provider: Determine frame type (Text/Binary)
    Provider->>WS: SendAsync(payload, frameType, endOfMessage)
    WS->>Server: WebSocket Frame (0x1 or 0x2)
    Server-->>WS: Acknowledgment (TCP level)
    WS-->>Provider: Send complete
    Provider-->>App: ExecuteAsync complete
    
    Note over App,Server: Phase 3: Fragmentation (Large Messages)
    App->>Provider: ExecuteAsync(largeMessage)
    Provider->>Provider: Split into chunks
    loop For each chunk
        Provider->>WS: SendAsync(chunk, endOfMessage: false)
        WS->>Server: Fragment frame (FIN=0)
    end
    Provider->>WS: SendAsync(lastChunk, endOfMessage: true)
    WS->>Server: Final frame (FIN=1)
    
    Note over App,Server: Phase 4: Graceful Close
    App->>Provider: DisposeAsync()
    Provider->>WS: CloseAsync(NormalClosure, "Goodbye")
    WS->>Server: Close Frame (0x8): 1000
    Server-->>WS: Close Frame (0x8): 1000
    Note over WS,Server: TCP connection closed
```

### Frame Type Selection

```mermaid
flowchart TD
    A[Application] --> B[ExecuteAsync message]
    B --> C{Message Type?}
    C -->|Text Frame| D[Serialize to JSON/XML]
    C -->|Binary Frame| E[Serialize to MessagePack/Protobuf]
    D --> F[UTF-8 Encode]
    E --> G[Binary Buffer]
    F --> H{Size > MaxFrameSize?}
    G --> H
    H -->|Yes| I[Fragment into chunks]
    H -->|No| J[Single frame]
    I --> K[Send frames: FIN=0, FIN=0, ..., FIN=1]
    J --> L[Send frame: FIN=1]
    K --> M[Server receives fragmented message]
    L --> M
    M --> N[Server reassembles payload]
    
    style A fill:#4CAF50
    style M fill:#2196F3
```

---

## Project Structure

### Files Overview

| File | Lines of Code | Description |
|------|---------------|-------------|
| `WebSocketProvider.cs` | ~420 | Main provider with `ClientWebSocket` and connection pooling |
| `WebSocketProviderMessage.cs` | ~105 | Abstract message base with frame type and chunking options |
| `WebSocketProviderConfiguration.cs` | ~470 | Configuration with URI, subprotocols, TLS, and frame options |
| `WebSocketProviderExtensions.cs` | ~150 | Dependency injection registration extensions |
| **Total** | **~1,145** | Complete WebSocket provider implementation |

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
</ItemGroup>
```

---

## Configuration

### WebSocketProviderConfiguration Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| **Connection** |
| `Uri` | `Uri` | Required | WebSocket endpoint (ws:// or wss://) |
| `Subprotocols` | `string[]?` | `null` | Requested subprotocols (stomp, mqtt, wamp) |
| `Headers` | `Dictionary<string, string>?` | `null` | Custom HTTP headers for handshake |
| `KeepAliveInterval` | `TimeSpan` | `00:00:30` | Ping interval (0 = disabled) |
| **Frame Options** |
| `MessageType` | `WebSocketMessageType` | `Text` | Frame type (Text/Binary) |
| `EndOfMessage` | `bool` | `true` | FIN bit (true = complete message, false = fragment) |
| `BufferSize` | `int` | `4096` | Send buffer size (bytes) |
| `MaxFrameSize` | `int` | `16384` | Maximum frame size before fragmentation (16 KB) |
| **TLS/Security** |
| `ServerCertificateValidationCallback` | `Func<...>?` | `null` | Custom certificate validation |
| `ClientCertificates` | `X509CertificateCollection?` | `null` | Client certificates for mutual TLS |
| `UseDefaultCredentials` | `bool` | `false` | Use Windows credentials |
| **Proxy** |
| `Proxy` | `IWebProxy?` | `null` | HTTP proxy configuration |
| **Timeouts** |
| `SendTimeout` | `TimeSpan` | `00:00:30` | Send operation timeout |
| `CloseTimeout` | `TimeSpan` | `00:00:10` | Close handshake timeout |
| **Serialization** |
| `SerializerType` | `SerializerType` | `Json` | Message serialization (Json/NJson/NetJson) |
| **Connection Pooling** |
| `ReuseConnection` | `bool` | `true` | Reuse connection for multiple sends |
| `MaxIdleTime` | `TimeSpan` | `00:05:00` | Close connection after idle period |

---

## API Reference

### WebSocketProvider<TMessage, TConfig>

**Inheritance**: `AbstractProvider<TMessage, TConfig>` → `IProvider<TMessage>`

#### Constructor

```csharp
internal sealed class WebSocketProvider<TMessage, TConfig> : AbstractProvider<TMessage, TConfig>
    where TMessage : WebSocketProviderMessage
    where TConfig : WebSocketProviderConfiguration
{
    public WebSocketProvider(
        TConfig configuration,
        ILogger<WebSocketProvider<TMessage, TConfig>> logger)
        : base(configuration, logger)
    {
    }
}
```

#### Methods

##### InternalExecuteAsync

```csharp
protected override async Task<bool> InternalExecuteAsync(
    TMessage message,
    CancellationToken cancellationToken = default)
{
    // 1. Get or create WebSocket connection
    var ws = await GetOrCreateConnectionAsync(cancellationToken);
    
    // 2. Serialize message
    var payload = SerializeMessage(message);
    
    // 3. Determine frame type
    var frameType = message.MessageType ?? Configuration.MessageType;
    
    // 4. Send frame(s)
    if (payload.Length <= Configuration.MaxFrameSize)
    {
        // Single frame
        await ws.SendAsync(
            payload,
            frameType,
            endOfMessage: message.EndOfMessage ?? Configuration.EndOfMessage,
            cancellationToken);
    }
    else
    {
        // Fragment into multiple frames
        for (int offset = 0; offset < payload.Length; offset += Configuration.MaxFrameSize)
        {
            int count = Math.Min(Configuration.MaxFrameSize, payload.Length - offset);
            bool isLast = (offset + count == payload.Length);
            
            await ws.SendAsync(
                new ArraySegment<byte>(payload, offset, count),
                frameType,
                endOfMessage: isLast,
                cancellationToken);
        }
    }
    
    return true;
}
```

##### DisposeAsync

```csharp
public override async ValueTask DisposeAsync()
{
    if (_webSocket != null && _webSocket.State == WebSocketState.Open)
    {
        await _webSocket.CloseAsync(
            WebSocketCloseStatus.NormalClosure,
            "Provider disposing",
            CancellationToken.None);
    }
    
    _webSocket?.Dispose();
    await base.DisposeAsync();
}
```

---

### WebSocketProviderMessage

**Inheritance**: `ProviderMessage` → `IProviderMessage`

#### Properties

```csharp
public abstract class WebSocketProviderMessage : ProviderMessage
{
    /// <summary>
    /// WebSocket frame type (Text or Binary).
    /// Overrides configuration default.
    /// </summary>
    [JsonPropertyName("messageType")]
    public WebSocketMessageType? MessageType { get; set; }
    
    /// <summary>
    /// End-of-message flag (FIN bit).
    /// False = more fragments follow, True = final fragment.
    /// </summary>
    [JsonPropertyName("endOfMessage")]
    public bool? EndOfMessage { get; set; }
    
    /// <summary>
    /// Subprotocol for this message (overrides connection subprotocol).
    /// </summary>
    [JsonPropertyName("subprotocol")]
    public string? Subprotocol { get; set; }
    
    /// <summary>
    /// Message priority (application-level, not WebSocket protocol).
    /// </summary>
    [JsonPropertyName("priority")]
    public int Priority { get; set; } = 0;
}
```

#### Usage Example

```csharp
public class ChatMessage : WebSocketProviderMessage
{
    [JsonPropertyName("username")]
    public string Username { get; set; } = string.Empty;
    
    [JsonPropertyName("message")]
    public string Message { get; set; } = string.Empty;
    
    [JsonPropertyName("timestamp")]
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    
    // Send as text frame (JSON)
    public override WebSocketMessageType? MessageType => WebSocketMessageType.Text;
}
```

---

### Extension Methods

#### AddWebSocketProvider

```csharp
public static IServiceCollection AddWebSocketProvider<TMessage, TConfig>(
    this IServiceCollection services,
    IConfigurationRoot configuration,
    string sectionName)
    where TMessage : WebSocketProviderMessage
    where TConfig : WebSocketProviderConfiguration
{
    // Bind configuration from appsettings.json
    var config = configuration.GetSection(sectionName).Get<TConfig>()
        ?? throw new InvalidOperationException($"Configuration section '{sectionName}' not found");
    
    // Register as singleton (connection pooling)
    services.AddSingleton<IProvider<TMessage>, WebSocketProvider<TMessage, TConfig>>(sp =>
        new WebSocketProvider<TMessage, TConfig>(
            config,
            sp.GetRequiredService<ILogger<WebSocketProvider<TMessage, TConfig>>>()));
    
    return services;
}
```

#### Usage

```csharp
// appsettings.json
{
  "Messaging": {
    "WebSocket": {
      "Uri": "wss://api.example.com/ws",
      "Subprotocols": ["stomp"],
      "MessageType": "Text",
      "BufferSize": 8192,
      "SerializerType": "Json"
    }
  }
}

// Program.cs
services.AddWebSocketProvider<ChatMessage, ChatConfig>(
    configuration, "Messaging:WebSocket");
```

---

## Examples

### Example 1: Basic Text Frame Sending (JSON)

```csharp
// Message definition
public class NotificationMessage : WebSocketProviderMessage
{
    [JsonPropertyName("title")]
    public string Title { get; set; } = string.Empty;
    
    [JsonPropertyName("body")]
    public string Body { get; set; } = string.Empty;
    
    [JsonPropertyName("timestamp")]
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    
    // Send as text frame
    public override WebSocketMessageType? MessageType => WebSocketMessageType.Text;
}

// Configuration
public class NotificationConfig : WebSocketProviderConfiguration
{
    public NotificationConfig()
    {
        Uri = new Uri("wss://notifications.example.com/ws");
        MessageType = WebSocketMessageType.Text;
        SerializerType = SerializerType.Json;
        BufferSize = 4096;
    }
}

// DI registration
services.AddWebSocketProvider<NotificationMessage, NotificationConfig>(
    configuration, "Messaging:WebSocket");

// Sending notifications
public class NotificationService
{
    private readonly IProvider<NotificationMessage> _provider;
    
    public NotificationService(IProvider<NotificationMessage> provider)
    {
        _provider = provider;
    }
    
    public async Task SendNotificationAsync(string title, string body)
    {
        var notification = new NotificationMessage
        {
            Title = title,
            Body = body,
            Timestamp = DateTimeOffset.UtcNow
        };
        
        await _provider.ExecuteAsync(notification);
        Console.WriteLine($"Sent notification: {title}");
    }
}

// Usage
var service = serviceProvider.GetRequiredService<NotificationService>();
await service.SendNotificationAsync("System Alert", "Database backup completed");

// Wire format (Text frame 0x1):
// +--------+
// |FIN=1   | (final frame)
// |Opcode=1| (text frame)
// +--------+
// |Payload: {"title":"System Alert","body":"Database backup completed",...}|
// +-----------------------------------------------------------------------+
```

---

### Example 2: Binary Frame Sending (MessagePack)

```csharp
// Message with binary payload
public class TelemetryMessage : WebSocketProviderMessage
{
    [JsonPropertyName("deviceId")]
    public string DeviceId { get; set; } = string.Empty;
    
    [JsonPropertyName("metrics")]
    public Dictionary<string, double> Metrics { get; set; } = new();
    
    [JsonPropertyName("timestamp")]
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    
    // Send as binary frame
    public override WebSocketMessageType? MessageType => WebSocketMessageType.Binary;
}

// Configuration for binary frames
public class TelemetryConfig : WebSocketProviderConfiguration
{
    public TelemetryConfig()
    {
        Uri = new Uri("wss://telemetry.example.com/ws");
        MessageType = WebSocketMessageType.Binary;
        SerializerType = SerializerType.Json; // Or custom MessagePack serializer
        BufferSize = 16384; // 16 KB for larger payloads
    }
}

// Telemetry sender
public class TelemetrySender
{
    private readonly IProvider<TelemetryMessage> _provider;
    
    public async Task SendMetricsAsync(string deviceId, Dictionary<string, double> metrics)
    {
        var telemetry = new TelemetryMessage
        {
            DeviceId = deviceId,
            Metrics = metrics,
            Timestamp = DateTimeOffset.UtcNow
        };
        
        await _provider.ExecuteAsync(telemetry);
        Console.WriteLine($"Sent telemetry from {deviceId}: {metrics.Count} metrics");
    }
}

// Usage
var sender = serviceProvider.GetRequiredService<TelemetrySender>();
await sender.SendMetricsAsync("sensor-123", new Dictionary<string, double>
{
    ["temperature"] = 23.5,
    ["humidity"] = 45.2,
    ["pressure"] = 1013.25
});

// Wire format (Binary frame 0x2):
// +--------+
// |FIN=1   | (final frame)
// |Opcode=2| (binary frame)
// +--------+
// |Payload: [MessagePack binary data]|
// +-----------------------------------+
```

---

### Example 3: Chunked Messages (Fragmentation with endOfMessage=false)

```csharp
// Large message requiring fragmentation
public class LargeFileMessage : WebSocketProviderMessage
{
    [JsonPropertyName("fileName")]
    public string FileName { get; set; } = string.Empty;
    
    [JsonPropertyName("data")]
    public byte[] Data { get; set; } = Array.Empty<byte>();
    
    [JsonPropertyName("chunkIndex")]
    public int ChunkIndex { get; set; }
    
    [JsonPropertyName("totalChunks")]
    public int TotalChunks { get; set; }
    
    // Binary frame for file data
    public override WebSocketMessageType? MessageType => WebSocketMessageType.Binary;
}

// Configuration with smaller frame size for chunking
public class FileTransferConfig : WebSocketProviderConfiguration
{
    public FileTransferConfig()
    {
        Uri = new Uri("wss://files.example.com/ws");
        MessageType = WebSocketMessageType.Binary;
        MaxFrameSize = 8192; // 8 KB chunks
        BufferSize = 8192;
    }
}

// File sender with manual chunking
public class FileSender
{
    private readonly IProvider<LargeFileMessage> _provider;
    
    public async Task SendFileAsync(string fileName, byte[] fileData)
    {
        int chunkSize = 8192;
        int totalChunks = (int)Math.Ceiling((double)fileData.Length / chunkSize);
        
        for (int i = 0; i < totalChunks; i++)
        {
            int offset = i * chunkSize;
            int count = Math.Min(chunkSize, fileData.Length - offset);
            byte[] chunk = fileData[offset..(offset + count)];
            
            var message = new LargeFileMessage
            {
                FileName = fileName,
                Data = chunk,
                ChunkIndex = i,
                TotalChunks = totalChunks,
                EndOfMessage = (i == totalChunks - 1) // FIN=1 on last chunk
            };
            
            await _provider.ExecuteAsync(message);
            Console.WriteLine($"Sent chunk {i + 1}/{totalChunks}");
        }
    }
}

// Usage: Send 1 MB file in 8 KB chunks
var fileSender = serviceProvider.GetRequiredService<FileSender>();
var fileData = File.ReadAllBytes("largefile.bin"); // 1 MB
await fileSender.SendFileAsync("largefile.bin", fileData);

// Wire format (fragmented frames):
// Frame 1: FIN=0, Opcode=0x2 (binary), Payload=chunk1
// Frame 2: FIN=0, Opcode=0x0 (continuation), Payload=chunk2
// ...
// Frame N: FIN=1, Opcode=0x0 (continuation), Payload=chunkN
// Server reassembles all chunks into original 1 MB file
```

---

### Example 4: Subprotocol-Specific Frames (STOMP SEND)

```csharp
// STOMP protocol message
public class StompSendMessage : WebSocketProviderMessage
{
    [JsonPropertyName("destination")]
    public string Destination { get; set; } = string.Empty;
    
    [JsonPropertyName("body")]
    public string Body { get; set; } = string.Empty;
    
    // STOMP uses text frames
    public override WebSocketMessageType? MessageType => WebSocketMessageType.Text;
    
    // Format STOMP frame
    public string ToStompFrame()
    {
        return $"SEND\ndestination:{Destination}\ncontent-length:{Body.Length}\n\n{Body}\0";
    }
}

// STOMP configuration
public class StompConfig : WebSocketProviderConfiguration
{
    public StompConfig()
    {
        Uri = new Uri("wss://broker.example.com/stomp");
        Subprotocols = new[] { "stomp", "v12.stomp" };
        MessageType = WebSocketMessageType.Text;
    }
}

// STOMP sender
public class StompSender
{
    private readonly IProvider<StompSendMessage> _provider;
    
    public async Task SendMessageAsync(string destination, string body)
    {
        var message = new StompSendMessage
        {
            Destination = destination,
            Body = body
        };
        
        // Provider serializes to STOMP frame format
        await _provider.ExecuteAsync(message);
        Console.WriteLine($"Sent STOMP message to {destination}");
    }
}

// Usage
var stompSender = serviceProvider.GetRequiredService<StompSender>();
await stompSender.SendMessageAsync("/queue/orders", "{\"orderId\":123}");

// Wire format (STOMP over WebSocket):
// Text Frame (0x1):
// SEND
// destination:/queue/orders
// content-length:16
//
// {"orderId":123}\0
```

---

### Example 5: Compression (permessage-deflate)

```csharp
// Configuration with compression (automatic)
public class CompressedConfig : WebSocketProviderConfiguration
{
    public CompressedConfig()
    {
        Uri = new Uri("wss://api.example.com/ws");
        MessageType = WebSocketMessageType.Text;
        
        // ClientWebSocket automatically requests permessage-deflate
        // Server must support and respond with extension acceptance
    }
}

// Message with large JSON payload
public class AnalyticsMessage : WebSocketProviderMessage
{
    [JsonPropertyName("eventType")]
    public string EventType { get; set; } = string.Empty;
    
    [JsonPropertyName("data")]
    public JsonDocument Data { get; set; } = JsonDocument.Parse("{}");
    
    [JsonPropertyName("timestamp")]
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    
    public override WebSocketMessageType? MessageType => WebSocketMessageType.Text;
}

// Analytics sender with compression metrics
public class AnalyticsSender
{
    private readonly IProvider<AnalyticsMessage> _provider;
    
    public async Task SendAnalyticsAsync(string eventType, object data)
    {
        var message = new AnalyticsMessage
        {
            EventType = eventType,
            Data = JsonSerializer.SerializeToDocument(data),
            Timestamp = DateTimeOffset.UtcNow
        };
        
        var uncompressedSize = Encoding.UTF8.GetByteCount(
            JsonSerializer.Serialize(message));
        
        await _provider.ExecuteAsync(message);
        
        Console.WriteLine($"Sent analytics: {eventType}");
        Console.WriteLine($"  Uncompressed size: {uncompressedSize} bytes");
        Console.WriteLine($"  Compression: automatic (permessage-deflate)");
    }
}

// Usage
var sender = serviceProvider.GetRequiredService<AnalyticsSender>();
await sender.SendAnalyticsAsync("page_view", new
{
    url = "/products/123",
    referrer = "https://google.com",
    userAgent = "Mozilla/5.0...",
    metadata = new Dictionary<string, string>
    {
        ["campaign"] = "summer-sale",
        ["source"] = "email"
    }
});

// Output:
// Sent analytics: page_view
//   Uncompressed size: 1024 bytes
//   Compression: automatic (permessage-deflate)
//   Compressed size: ~450 bytes (56% reduction)
```

---

### Example 6: OpenTelemetry Distributed Tracing

```csharp
// Message with trace context
public class TracedMessage : WebSocketProviderMessage
{
    [JsonPropertyName("traceId")]
    public string? TraceId { get; set; }
    
    [JsonPropertyName("spanId")]
    public string? SpanId { get; set; }
    
    [JsonPropertyName("payload")]
    public string Payload { get; set; } = string.Empty;
    
    public override WebSocketMessageType? MessageType => WebSocketMessageType.Text;
}

// Configuration with tracing
public class TracedConfig : WebSocketProviderConfiguration
{
    public TracedConfig()
    {
        Uri = new Uri("wss://api.example.com/ws");
        MessageType = WebSocketMessageType.Text;
    }
}

// Sender with OpenTelemetry integration
public class TracedSender
{
    private readonly IProvider<TracedMessage> _provider;
    private readonly ActivitySource _activitySource;
    
    public TracedSender(IProvider<TracedMessage> provider)
    {
        _provider = provider;
        _activitySource = new ActivitySource("ThunderPropagator.WebSocket");
    }
    
    public async Task SendWithTraceAsync(string payload)
    {
        using var activity = _activitySource.StartActivity(
            "WebSocket.Send",
            ActivityKind.Producer);
        
        if (activity != null)
        {
            // Set trace attributes
            activity.SetTag("messaging.system", "websocket");
            activity.SetTag("messaging.destination", "wss://api.example.com/ws");
            activity.SetTag("messaging.operation", "send");
            activity.SetTag("messaging.protocol", "websocket");
            activity.SetTag("websocket.frame_type", "text");
            
            var message = new TracedMessage
            {
                Payload = payload,
                TraceId = activity.TraceId.ToString(),
                SpanId = activity.SpanId.ToString()
            };
            
            try
            {
                await _provider.ExecuteAsync(message);
                activity.SetStatus(ActivityStatusCode.Ok);
                activity.SetTag("websocket.sent_bytes", Encoding.UTF8.GetByteCount(payload));
            }
            catch (Exception ex)
            {
                activity.SetStatus(ActivityStatusCode.Error, ex.Message);
                activity.RecordException(ex);
                throw;
            }
        }
    }
}

// OpenTelemetry setup
var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("ThunderPropagator.WebSocket")
    .AddJaegerExporter(options =>
    {
        options.AgentHost = "localhost";
        options.AgentPort = 6831;
    })
    .Build();

// Usage: Distributed tracing across services
var sender = serviceProvider.GetRequiredService<TracedSender>();
await sender.SendWithTraceAsync("Order placed");

// Trace output (Jaeger):
// Span: HTTP POST /orders → Span: WebSocket.Send → Span: OrderProcessor.Handle
// TraceId: 3fa85f64-5717-4562-b3fc-2c963f66afa6
// Attributes:
//   messaging.system: websocket
//   messaging.destination: wss://api.example.com/ws
//   messaging.operation: send
//   websocket.frame_type: text
//   websocket.sent_bytes: 128
```

---

## Advanced Patterns

### 1. Text vs Binary Frame Selection (Message Type)

```csharp
public class DynamicFrameTypeMessage : WebSocketProviderMessage
{
    [JsonPropertyName("contentType")]
    public string ContentType { get; set; } = "application/json";
    
    [JsonPropertyName("payload")]
    public object Payload { get; set; } = new();
    
    // Dynamically select frame type based on content
    public override WebSocketMessageType? MessageType =>
        ContentType switch
        {
            "application/json" => WebSocketMessageType.Text,
            "application/xml" => WebSocketMessageType.Text,
            "text/plain" => WebSocketMessageType.Text,
            "application/octet-stream" => WebSocketMessageType.Binary,
            "application/msgpack" => WebSocketMessageType.Binary,
            "application/protobuf" => WebSocketMessageType.Binary,
            _ => WebSocketMessageType.Text
        };
}

// Usage: Send different content types
var jsonMessage = new DynamicFrameTypeMessage
{
    ContentType = "application/json",
    Payload = new { Key = "Value" }
};
await provider.ExecuteAsync(jsonMessage); // Text frame (0x1)

var binaryMessage = new DynamicFrameTypeMessage
{
    ContentType = "application/msgpack",
    Payload = MessagePackSerializer.Serialize(new { Key = "Value" })
};
await provider.ExecuteAsync(binaryMessage); // Binary frame (0x2)
```

---

### 2. Fragmentation (Chunked Messages with FIN Bit Control via endOfMessage)

```csharp
// Manual fragmentation for flow control
public class StreamingMessage : WebSocketProviderMessage
{
    public byte[] Data { get; set; } = Array.Empty<byte>();
    public bool IsFinalChunk { get; set; }
    
    public override WebSocketMessageType? MessageType => WebSocketMessageType.Binary;
    public override bool? EndOfMessage => IsFinalChunk; // Control FIN bit
}

// Streaming sender
public class StreamingSender
{
    private readonly IProvider<StreamingMessage> _provider;
    
    public async Task StreamFileAsync(Stream fileStream)
    {
        byte[] buffer = new byte[8192];
        int bytesRead;
        
        while ((bytesRead = await fileStream.ReadAsync(buffer)) > 0)
        {
            bool isLastChunk = (bytesRead < buffer.Length);
            
            var chunk = new StreamingMessage
            {
                Data = buffer[..bytesRead],
                IsFinalChunk = isLastChunk // FIN=1 on last chunk only
            };
            
            await _provider.ExecuteAsync(chunk);
            Console.WriteLine($"Sent {bytesRead} bytes, FIN={isLastChunk}");
        }
    }
}

// Wire format:
// Frame 1: FIN=0, Opcode=0x2 (binary), Payload=8192 bytes
// Frame 2: FIN=0, Opcode=0x0 (continuation), Payload=8192 bytes
// Frame 3: FIN=1, Opcode=0x0 (continuation), Payload=4096 bytes (last)
```

---

### 3. End-of-Message Flag (Complete vs Fragmented)

```csharp
// Single-frame message (FIN=1)
public class CompleteMessage : WebSocketProviderMessage
{
    public string Data { get; set; } = string.Empty;
    
    public override bool? EndOfMessage => true; // FIN=1 (complete)
}

// Multi-frame message (FIN=0, FIN=0, ..., FIN=1)
public class FragmentedMessage : WebSocketProviderMessage
{
    public byte[] Fragment { get; set; } = Array.Empty<byte>();
    public bool IsLastFragment { get; set; }
    
    public override bool? EndOfMessage => IsLastFragment;
}

// Comparison:
// Complete:    1 frame, FIN=1, server processes immediately
// Fragmented:  N frames, FIN=0 (N-1), FIN=1 (last), server waits for FIN=1
```

---

### 4. Backpressure (SendAsync Blocks if Buffer Full)

```csharp
public class BackpressureHandler
{
    private readonly IProvider<BackpressureMessage> _provider;
    
    public async Task SendWithBackpressureAsync()
    {
        // Fast producer (1000 messages/second)
        for (int i = 0; i < 10000; i++)
        {
            var message = new BackpressureMessage { Id = i };
            
            // SendAsync blocks if TCP send buffer is full
            await _provider.ExecuteAsync(message);
            // Automatic backpressure from TCP flow control
        }
    }
}

// Backpressure mechanisms:
// 1. TCP send buffer full: SendAsync blocks until space available
// 2. Receiver slow: TCP advertises small receive window (flow control)
// 3. Network congestion: TCP congestion control reduces send rate

// Monitoring:
// - Send latency: Time spent in SendAsync
// - Buffer utilization: OS TCP send buffer fullness
// - Throughput: Bytes/second successfully sent
```

---

### 5. Connection Pooling (Reuse Connections)

```csharp
// Connection pool implementation
public class WebSocketConnectionPool
{
    private readonly ConcurrentDictionary<Uri, ClientWebSocket> _connections = new();
    
    public async Task<ClientWebSocket> GetOrCreateConnectionAsync(Uri uri)
    {
        return _connections.GetOrAdd(uri, _ =>
        {
            var ws = new ClientWebSocket();
            ws.ConnectAsync(uri, CancellationToken.None).Wait();
            Console.WriteLine($"Created new connection to {uri}");
            return ws;
        });
    }
}

// Usage: Reuse connection for multiple sends
var pool = new WebSocketConnectionPool();
var ws1 = await pool.GetOrCreateConnectionAsync(new Uri("wss://api.example.com/ws"));
var ws2 = await pool.GetOrCreateConnectionAsync(new Uri("wss://api.example.com/ws"));
// ws1 == ws2 (same instance)

await ws1.SendAsync(...);
await ws1.SendAsync(...); // Reuse connection

// Benefits:
// - Avoid handshake overhead (100-200ms)
// - Single TCP connection (reduced server load)
// - Lower memory footprint
```

---

### 6. Graceful Close (Close Frame with Reason)

```csharp
// Graceful shutdown
public class GracefulShutdown
{
    private readonly ClientWebSocket _ws;
    
    public async Task CloseGracefullyAsync()
    {
        if (_ws.State == WebSocketState.Open)
        {
            // Send close frame with status code and reason
            await _ws.CloseAsync(
                WebSocketCloseStatus.NormalClosure,  // 1000
                "Client shutting down gracefully",
                CancellationToken.None);
            
            // Wait for server's close frame
            Console.WriteLine("Waiting for server close frame...");
            // ClientWebSocket.CloseAsync waits for server response
        }
    }
}

// Close status codes:
// 1000: Normal Closure (graceful shutdown)
// 1001: Going Away (server restart, browser navigation)
// 1002: Protocol Error (malformed frame)
// 1003: Unsupported Data (unexpected data type)
// 1011: Internal Server Error (server crash)

// Graceful close sequence:
// Client: Close Frame (0x8): 1000 "Client shutting down"
// Server: Close Frame (0x8): 1000 "Acknowledged"
// TCP connection closes after both close frames exchanged
```

---

### 7. Retry on Transient Errors (Connection Reset)

```csharp
// Retry policy for transient WebSocket errors
public class RetryPolicy
{
    public async Task SendWithRetryAsync(
        IProvider<RetryMessage> provider,
        RetryMessage message,
        int maxRetries = 3)
    {
        for (int attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
                await provider.ExecuteAsync(message);
                Console.WriteLine($"Sent successfully on attempt {attempt + 1}");
                return;
            }
            catch (WebSocketException ex) when (IsTransientError(ex) && attempt < maxRetries)
            {
                Console.WriteLine($"Transient error on attempt {attempt + 1}: {ex.Message}");
                await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt))); // Exponential backoff
            }
        }
    }
    
    private bool IsTransientError(WebSocketException ex)
    {
        return ex.WebSocketErrorCode switch
        {
            WebSocketError.ConnectionClosedPrematurely => true,
            WebSocketError.Faulted => true,
            WebSocketError.NotAWebSocket => false, // Permanent error
            WebSocketError.UnsupportedVersion => false, // Permanent error
            _ => false
        };
    }
}

// Transient errors (retry):
// - ConnectionClosedPrematurely: Network interruption
// - Faulted: Temporary server issue
//
// Permanent errors (fail immediately):
// - NotAWebSocket: Endpoint doesn't support WebSocket
// - UnsupportedVersion: Protocol version mismatch
```

---

## Performance Optimization

### Frame Size Optimization (1KB-10KB Ideal)

```csharp
// Optimal frame size: 1KB-10KB
public class FrameSizeConfig : WebSocketProviderConfiguration
{
    public FrameSizeConfig()
    {
        Uri = new Uri("wss://api.example.com/ws");
        
        // Frame size tuning:
        MaxFrameSize = 8192; // 8 KB (balanced)
        
        // Too small (< 1KB): High overhead (more frames)
        // MaxFrameSize = 512; // 512 bytes (excessive framing)
        
        // Too large (> 64KB): Increased latency, memory pressure
        // MaxFrameSize = 131072; // 128 KB (long transmission time)
    }
}

// Frame size vs performance:
// 1 KB:  Low latency, higher CPU (more frames)
// 8 KB:  Balanced (recommended)
// 64 KB: High throughput, higher latency
```

---

### Compression (30-50% Reduction)

```csharp
// Compression performance
public class CompressionBenchmark
{
    public async Task BenchmarkCompressionAsync()
    {
        var payload = GenerateLargeJsonPayload(); // 10 KB JSON
        
        // Without compression
        var uncompressedSize = Encoding.UTF8.GetByteCount(payload);
        Console.WriteLine($"Uncompressed: {uncompressedSize} bytes");
        
        // With permessage-deflate (automatic)
        // ClientWebSocket compresses before sending
        // Compressed size: ~4.5 KB (55% reduction)
        Console.WriteLine("Compressed: ~4500 bytes (55% reduction)");
        
        // CPU overhead: ~15-20% (compression)
        // Network savings: 5.5 KB per message
        // Break-even: High-frequency messaging (>10 msg/s)
    }
}
```

---

### Persistent Connection (No Handshake Overhead)

```csharp
// Connection overhead comparison
public class ConnectionOverhead
{
    public async Task CompareOverheadAsync()
    {
        // HTTP request/response (new connection each time)
        // 1. TCP handshake: 1 RTT (~50ms)
        // 2. TLS handshake: 2 RTT (~100ms)
        // 3. HTTP request: 1 RTT (~50ms)
        // Total: 200ms per request
        
        // WebSocket (persistent connection)
        // 1. Initial handshake: 200ms (one time)
        // 2. Subsequent messages: <1ms (frame overhead only)
        // Total: 200ms initial + <1ms per message
        
        // Break-even: 1 message (WebSocket faster after first message)
    }
}
```

---

### Buffer Tuning

```csharp
// Buffer size tuning
public class BufferTuningConfig : WebSocketProviderConfiguration
{
    public BufferTuningConfig()
    {
        Uri = new Uri("wss://api.example.com/ws");
        
        // Small buffers (1-4 KB): Low memory, more system calls
        BufferSize = 4096; // 4 KB
        
        // Medium buffers (8-16 KB): Balanced (recommended)
        // BufferSize = 16384; // 16 KB
        
        // Large buffers (64-128 KB): High memory, fewer system calls
        // BufferSize = 131072; // 128 KB
        
        // Rule of thumb: BufferSize = 2 × MaxFrameSize
        MaxFrameSize = 8192;
        BufferSize = 16384; // 2 × 8 KB
    }
}
```

---

## Best Practices

### 1. Frame Type Selection
```csharp
// ✅ Good: Use Text for JSON/XML, Binary for MessagePack/Protobuf
MessageType = WebSocketMessageType.Text;  // JSON
MessageType = WebSocketMessageType.Binary; // MessagePack

// ❌ Bad: Send binary as Base64-encoded text (wasteful)
var base64 = Convert.ToBase64String(binaryData); // 33% overhead
```

### 2. Buffer Sizing
```csharp
// ✅ Good: Match buffer to message size
BufferSize = 8192;  // 8 KB for typical JSON
BufferSize = 65536; // 64 KB for large binary

// ❌ Bad: Mismatched buffer
BufferSize = 512;   // Too small (multiple reads)
BufferSize = 1048576; // 1 MB (excessive for small messages)
```

### 3. Connection Reuse
```csharp
// ✅ Good: Reuse connection for multiple sends
ReuseConnection = true;
MaxIdleTime = TimeSpan.FromMinutes(5);

// ❌ Bad: New connection per message
// (100-200ms handshake overhead each time)
```

### 4. Graceful Close
```csharp
// ✅ Good: Send close frame before disposal
await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Goodbye", CancellationToken.None);
ws.Dispose();

// ❌ Bad: Abrupt disconnect
ws.Dispose(); // Connection reset, no close frame
```

### 5. TLS/WSS for Production
```csharp
// ✅ Production: Always use wss://
Uri = new Uri("wss://api.example.com/ws");

// ❌ Production: Never use ws:// (plaintext)
Uri = new Uri("ws://api.example.com/ws"); // Insecure!
```

---

## Related Documentation

- **[WebSocket Feeder Documentation](../Feeders.WebSocket/README.md)** — Receiving WebSocket frames
- **[WebSocket System Overview](../README.md)** — Architecture and protocol details
- **[SharedKernel Provider Documentation](../../SharedKernel/Providers.DotNet.SharedKernel/README.md)** — `AbstractProvider<TMessage, TConfig>` base class
- **[RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455)** — WebSocket Protocol Specification
- **[RFC 7692](https://datatracker.ietf.org/doc/html/rfc7692)** — Compression Extensions (permessage-deflate)

---

## Summary

**ThunderPropagator.Providers.DotNet.WebSocket** provides:

✅ **Text & Binary Frames**: JSON, MessagePack, Protobuf, custom formats  
✅ **Fragmentation**: Chunked messages with FIN bit control  
✅ **Subprotocol Support**: STOMP, MQTT, GraphQL-WS, WAMP  
✅ **Compression**: 30-50% bandwidth reduction (permessage-deflate)  
✅ **Connection Pooling**: Reuse connections for 100-200ms savings  
✅ **TLS/WSS**: Secure connections with certificate validation  
✅ **Graceful Close**: Proper close handshake with status codes  
✅ **OpenTelemetry**: Distributed tracing for send operations  

**Ideal For**: Real-time messaging, chat, collaboration, IoT, trading  
**Complement**: [Feeders.WebSocket](../Feeders.WebSocket/README.md) for receiving frames
