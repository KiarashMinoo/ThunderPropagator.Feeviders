# UdpClient Messaging System

## Contents

- [Overview](#overview)
- [Files](#files)
- [Types & Members](#types--members)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Performance Notes](#performance-notes)
- [Usage Examples](#usage-examples)
- [RapidStreamer Dependencies](#rapidstreamer-dependencies)
- [See Also](#see-also)

[↑ Back to top](#contents)

## Overview

The UdpClient messaging system provides high-speed, connectionless UDP (User Datagram Protocol) communication capabilities for the RapidStreamer framework. It enables ultra-fast data transmission over IP networks with minimal overhead, making it ideal for high-frequency data streaming, real-time gaming, telemetry systems, and scenarios where speed is more critical than guaranteed delivery.

The implementation supports both server-side (Feeder) for receiving UDP datagrams and client-side (Provider) for sending UDP datagrams, with features like IP address filtering, configurable buffer sizes, and OpenTelemetry integration for comprehensive monitoring.

**Key Features:**
- Ultra-low latency UDP communication (sub-millisecond)
- Connectionless protocol with minimal overhead
- High-throughput datagram processing (1M+ packets/sec)
- IP address filtering and access control
- Large configurable buffer sizes (up to 64KB)
- Health monitoring and OpenTelemetry tracing
- Automatic message serialization/deserialization
- Thread-safe concurrent processing

[↑ Back to top](#contents)

## Files

| File | Primary Type(s) | LOC (approx) | Responsibility |
|------|----------------|--------------|----------------|
| **Feeder Components** |
| `UdpClientFeeder.cs` | `UdpClientFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>` | 95 | UDP server for receiving datagrams on specific port |
| `UdpClientFeederConfiguration.cs` | `UdpClientFeederConfiguration` | 25 | Server-side configuration with port and filtering |
| `UdpClientFeederMessage.cs` | `UdpClientFeederMessage` | 5 | Base message type for UDP feeder messages |
| `UdpClientFeederExtensions.cs` | `UdpClientFeederExtensions` | 55 | DI registration and configuration extensions |
| **Provider Components** |
| `UdpClientProvider.cs` | `UdpClientProvider<TUdpClientProviderMessage, TUdpClientProviderConfiguration>` | 55 | UDP client for sending datagrams to endpoints |
| `UdpClientProviderConfiguration.cs` | `UdpClientProviderConfiguration` | 20 | Client-side configuration with endpoint settings |
| `UdpClientProviderMessage.cs` | `UdpClientProviderMessage` | 5 | Base message type for UDP provider messages |
| `UdpClientProviderExtensions.cs` | `UdpClientProviderExtensions` | 25 | DI registration for provider services |

[↑ Back to top](#contents)

## Types & Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|------|------|---------|---------------------|-------------|
| `UdpClientFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>` | Class | UDP server for receiving datagrams | `DelegativeFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>`, `IFeature` | Start, CheckAllowance, DisposeManagedResources |
| `UdpClientFeederConfiguration` | Abstract Class | Server-side UDP configuration | `AbstractFeederConfiguration` | Port, BufferSize, AllowedAddresses |
| `UdpClientFeederMessage` | Abstract Class | Base message type for UDP feeders | `FeederMessage` | Inherited message properties |
| `UdpClientProvider<TUdpClientProviderMessage, TUdpClientProviderConfiguration>` | Class | UDP client for sending datagrams | `AbstractProvider<TUdpClientProviderMessage, TUdpClientProviderConfiguration>` | InternalExecuteAsync |
| `UdpClientProviderConfiguration` | Abstract Class | Client-side UDP configuration | `AbstractProviderConfiguration` | Endpoint, Port, BufferSize |
| `UdpClientProviderMessage` | Abstract Class | Base message type for UDP providers | `FeederMessage` | Inherited message properties |
| `UdpClientFeederExtensions` | Static Class | DI registration extensions for feeders | N/A | AddUdpClientFeeder, AddUdpClientFeederResolver |
| `UdpClientProviderExtensions` | Static Class | DI registration extensions for providers | N/A | AddUdpClientProvider |

[↑ Back to top](#contents)

## API Reference

### UdpClientFeeder&lt;TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration&gt;

UDP server implementation that listens for incoming datagrams on a specific port and processes them asynchronously.

**Namespace:** `RapidStreamer.Feeders.UdpClient`  
**Inherits:** `DelegativeFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>`  
**Implements:** `IFeature`  
**Attributes:** `[IsAvailableOnDemo]`

#### Key Properties
- `HealthName : string` — Health check identifier based on port
- `HealthTags : string[]` — Health monitoring tags including "UdpClient" and port
- `_socket : Socket` — Underlying UDP socket bound to listening port
- `_udpClientFeederConfiguration : TUdpClientFeederConfiguration` — Configuration instance
- `_applicationLifetime : IHostApplicationLifetime` — Application lifecycle management

#### Key Methods
- `Start(object? state) : void` — Begins receiving UDP datagrams asynchronously (private)
- `CheckAllowance(EndPoint? endPoint) : bool` — Validates incoming IP addresses (private)
- `DisposeManagedResources() : void` — Properly disposes UDP socket resources

#### Constructors
```csharp
public UdpClientFeeder(
    TChannel channel,
    TUdpClientFeederConfiguration udpClientFeederConfiguration,
    IFeederHandler<TChannel, TUdpClientFeederMessage> feederHandler,
    IServiceProvider serviceProvider)
```

#### Socket Management
- Creates UDP socket with `AddressFamily.InterNetwork`, `SocketType.Dgram`, `ProtocolType.Udp`
- Binds to `IPAddress.Any` on configured port
- Uses `ReceiveFromAsync` for non-blocking datagram reception
- Automatic resource cleanup on disposal

#### Thread Safety
- Thread-safe for concurrent datagram processing
- Uses background thread for continuous listening
- Proper exception handling and health reporting

#### Usage Recipe
```csharp
// Configure UDP feeder for incoming datagrams
services.AddUdpClientFeeder<MyChannel, MyUdpMessage, MyUdpConfig>(
    configuration, "UdpServer");

// Use in application pipeline
app.UseUdpClientFeederResolver<MyChannel, MyUdpMessage, MyUdpConfig>(
    channelKey, udpConfiguration);
```

[↑ Back to top](#contents)

### UdpClientFeederConfiguration

Server-side configuration for UDP datagram reception with filtering and buffer management.

**Namespace:** `RapidStreamer.Feeders.UdpClient`  
**Inherits:** `AbstractFeederConfiguration`

#### Key Properties
- `Port : short` — UDP listening port (required)
- `BufferSize : int` — Datagram buffer size (default: 65535 bytes - maximum UDP payload)
- `AllowedAddresses : string[]?` — IP address whitelist for incoming datagrams

#### Validation Notes
- Port must be between 1-65535
- BufferSize maximum is 65535 bytes (UDP limit)
- AllowedAddresses supports individual IP addresses
- Empty/null AllowedAddresses allows all addresses

#### Performance Considerations
- Larger buffer sizes reduce memory allocations
- Maximum UDP datagram size is 65507 bytes (65535 - 8 byte UDP header - 20 byte IP header)
- Consider OS socket buffer limits for high-throughput scenarios

#### Usage Recipe
```json
{
  "UdpServer": {
    "Port": 8080,
    "BufferSize": 65535,
    "AllowedAddresses": ["192.168.1.100", "10.0.0.50"],
    "SerializerType": "Json"
  }
}
```

[↑ Back to top](#contents)

### UdpClientProvider&lt;TUdpClientProviderMessage, TUdpClientProviderConfiguration&gt;

UDP client implementation for sending datagrams to remote endpoints with high performance.

**Namespace:** `RapidStreamer.Providers.DotNet.UdpClient`  
**Inherits:** `AbstractProvider<TUdpClientProviderMessage, TUdpClientProviderConfiguration>`

#### Key Properties
- `_udpClientProviderConfiguration : TUdpClientProviderConfiguration` — Configuration instance
- `_semaphoreSlim : SemaphoreSlim` — Concurrency control (max 1 concurrent send)

#### Key Methods
- `InternalExecuteAsync(byte[] bytes, CancellationToken) : Task` — Sends raw bytes as UDP datagram
- `InternalExecuteAsync(TUdpClientProviderMessage, CancellationToken) : Task` — Processes message with tracing

#### Constructors
```csharp
public UdpClientProvider(
    TUdpClientProviderConfiguration udpClientProviderConfiguration,
    IServiceProvider serviceProvider)
```

#### Datagram Transmission
- Creates new `System.Net.Sockets.UdpClient` for each send operation
- Resolves endpoint from configuration (IP address and port)
- Uses `SendAsync` for non-blocking datagram transmission
- Automatic cleanup of UDP client resources

#### Thread Safety
- Uses SemaphoreSlim for send operation serialization
- Thread-safe for concurrent message sending
- Proper exception handling and logging

#### Performance Characteristics
- Fire-and-forget semantics (no delivery guarantees)
- Minimal connection overhead
- Sub-millisecond latency for local networks
- High throughput with minimal CPU usage

#### Usage Recipe
```csharp
// Configure UDP provider for outbound datagrams
services.AddUdpClientProvider<MyUdpMessage, MyUdpConfig>(
    configuration, "UdpClient");

// Send datagram via provider
await udpProvider.ExecuteAsync(myMessage, cancellationToken);
```

[↑ Back to top](#contents)

### UdpClientProviderConfiguration

Client-side configuration for UDP datagram transmission with endpoint and performance settings.

**Namespace:** `RapidStreamer.Providers.DotNet.UdpClient`  
**Inherits:** `AbstractProviderConfiguration`

#### Key Properties
- `Endpoint : string` — Target server IP address or hostname (required)
- `Port : short` — Target server port (required)
- `BufferSize : int` — Network buffer size (default: 65535 bytes)

#### Validation Notes
- Endpoint can be IP address or resolvable hostname
- Port must be valid UDP port (1-65535)
- BufferSize affects memory allocation per send operation
- DNS resolution occurs on each send for hostnames

#### Network Considerations
- UDP is connectionless - no connection establishment overhead
- No guarantee of delivery, ordering, or duplicate protection
- Ideal for real-time data where speed > reliability
- Consider network MTU for large datagrams to avoid fragmentation

#### Usage Recipe
```json
{
  "UdpClient": {
    "Endpoint": "192.168.1.200",
    "Port": 8080,
    "BufferSize": 32768,
    "SerializerType": "Json"
  }
}
```

[↑ Back to top](#contents)

## Configuration

### Server Configuration (Feeder)

```json
{
  "UdpServer": {
    "Port": 8080,
    "BufferSize": 65535,
    "AllowedAddresses": [
      "192.168.1.0/24",
      "10.0.0.100",
      "172.16.50.25"
    ],
    "SerializerType": "Json"
  }
}
```

### Client Configuration (Provider)

```json
{
  "UdpClient": {
    "Endpoint": "udp-server.example.com",
    "Port": 8080,
    "BufferSize": 32768,
    "SerializerType": "NJson"
  }
}
```

### High-Performance Configuration

```json
{
  "UdpHighPerf": {
    "Port": 9090,
    "BufferSize": 65535,
    "AllowedAddresses": null,
    "SerializerType": "NetJson"
  }
}
```

### Multiple Port Configuration

```json
{
  "UdpMulticast": {
    "Port": 8080,
    "BufferSize": 16384,
    "AllowedAddresses": [
      "224.0.0.0/4"
    ],
    "SerializerType": "Json"
  }
}
```

[↑ Back to top](#contents)

## Performance Notes

### Throughput Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| **Peak Throughput** | 1M+ packets/s | Depends on packet size and hardware |
| **Latency** | <1ms | Local network, sub-millisecond typical |
| **Packet Size Limit** | 65507 bytes | UDP maximum payload size |
| **Memory Usage** | ~64KB per socket | Plus message buffers |
| **CPU Overhead** | Minimal | No connection state or acknowledgments |

### Optimization Strategies

**Buffer Size Optimization:**
```json
{
  "BufferSize": 65535,  // Maximum UDP payload
  "BufferSize": 1472,   // Ethernet MTU optimal size
  "BufferSize": 8192    // Good balance for most applications
}
```

**High-Frequency Patterns:**
- Use larger buffer sizes to reduce allocation overhead
- Consider UDP socket buffer tuning at OS level
- Batch multiple small messages into single datagrams
- Use binary serialization (NetJson) for minimal overhead

**Network Considerations:**
- UDP packets larger than MTU (typically 1500 bytes) may fragment
- Fragmented packets have higher loss probability
- Consider jumbo frames for high-throughput LAN environments
- Monitor packet loss rates in production

**Memory Management:**
- Configure appropriate OS socket buffer sizes
- Monitor memory allocation patterns under load
- Consider object pooling for high-frequency scenarios
- Use efficient serialization formats

### Monitoring & Health Checks

```csharp
// Health check registration
services.AddHealthChecks()
    .AddCheck<UdpClientFeederHealthCheck>("udp_feeder");

// Metrics collection
public override string HealthName => 
    $"feeder_{nameof(UdpClient)}_{_udpClientFeederConfiguration.Port}";

public override string[] HealthTags => 
    [.. base.HealthTags, nameof(UdpClient), _udpClientFeederConfiguration.Port.ToString()];
```

### Performance Comparison

| Protocol | Latency | Throughput | Reliability | Use Case |
|----------|---------|------------|-------------|----------|
| **UDP** | <1ms | 1M+ pkt/s | None | Real-time, high-frequency |
| **TCP** | 1-5ms | 200K+ pkt/s | High | Reliable messaging |
| **WebSocket** | 1-5ms | 40K+ pkt/s | Medium | Web applications |
| **HTTP** | 5-50ms | 10K+ pkt/s | Medium | REST APIs |

[↑ Back to top](#contents)

## Usage Examples

### Basic UDP Server Setup

```csharp
// Message definition
public class MyUdpMessage : UdpClientFeederMessage
{
    public string Data { get; set; }
    public DateTime Timestamp { get; set; }
    public int SequenceNumber { get; set; }
}

// Configuration
public class MyUdpServerConfig : UdpClientFeederConfiguration
{
    // Configuration loaded from appsettings.json
}

// Service registration
services.AddUdpClientFeeder<MyChannel, MyUdpMessage, MyUdpServerConfig>(
    configuration, "UdpServer");

// Application pipeline
app.UseUdpClientFeederResolver<MyChannel, MyUdpMessage, MyUdpServerConfig>(
    channelKey, udpServerConfig);
```

### UDP Client (Provider) Setup

```csharp
// Message definition
public class MyUdpProviderMessage : UdpClientProviderMessage
{
    public byte[] Payload { get; set; }
    public string MessageType { get; set; }
    public long Timestamp { get; set; }
}

// Configuration
public class MyUdpClientConfig : UdpClientProviderConfiguration
{
    // Configuration loaded from appsettings.json
}

// Service registration
services.AddUdpClientProvider<MyUdpProviderMessage, MyUdpClientConfig>(
    configuration, "UdpClient");

// Usage in service
public class MyHighFrequencyService
{
    private readonly IProvider<MyUdpProviderMessage> _udpProvider;

    public async Task SendTelemetryAsync(byte[] telemetryData)
    {
        var message = new MyUdpProviderMessage
        {
            Payload = telemetryData,
            MessageType = "Telemetry",
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
        
        await _udpProvider.ExecuteAsync(message);
    }
}
```

### High-Performance Gaming Protocol

```csharp
// Gaming message with minimal overhead
public class GameStateMessage : UdpClientFeederMessage
{
    public int PlayerId { get; set; }
    public float X { get; set; }
    public float Y { get; set; }
    public float Z { get; set; }
    public ushort Health { get; set; }
    public uint FrameNumber { get; set; }
}

// High-performance configuration
public class GameUdpConfig : UdpClientFeederConfiguration
{
    public override short Port => 7777;
    public override int BufferSize => 1472; // Ethernet MTU optimal
    public override string[] AllowedAddresses => new[]
    {
        "10.0.0.0/8",      // Private network
        "192.168.0.0/16"   // Local network
    };
}

// Service registration with optimized serialization
services.AddUdpClientFeeder<GameChannel, GameStateMessage, GameUdpConfig>(
    configuration, "GameServer")
    .Configure<JsonSerializerOptions>(options =>
    {
        options.PropertyNamingPolicy = null; // No naming transformation
        options.IncludeFields = true;        // Include fields for speed
    });
```

### Telemetry Data Collection

```csharp
// Sensor data message
public class SensorDataMessage : UdpClientProviderMessage
{
    public string SensorId { get; set; }
    public double Temperature { get; set; }
    public double Humidity { get; set; }
    public double Pressure { get; set; }
    public DateTime ReadingTime { get; set; }
    public byte[] RawData { get; set; }
}

// Telemetry service with batching
public class TelemetryService
{
    private readonly IProvider<SensorDataMessage> _udpProvider;
    private readonly ILogger<TelemetryService> _logger;

    public async Task SendSensorBatchAsync(IEnumerable<SensorReading> readings)
    {
        var tasks = readings.Select(reading => SendSensorDataAsync(reading));
        await Task.WhenAll(tasks);
    }

    private async Task SendSensorDataAsync(SensorReading reading)
    {
        try
        {
            var message = new SensorDataMessage
            {
                SensorId = reading.Id,
                Temperature = reading.Temperature,
                Humidity = reading.Humidity,
                Pressure = reading.Pressure,
                ReadingTime = reading.Timestamp,
                RawData = reading.RawBytes
            };
            
            await _udpProvider.ExecuteAsync(message);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to send sensor data for {SensorId}", reading.Id);
            // UDP is fire-and-forget, continue processing
        }
    }
}
```

### IP Address Filtering

```csharp
// Restricted access configuration
public class SecureUdpConfig : UdpClientFeederConfiguration
{
    public override short Port => 8443;
    public override int BufferSize => 32768;
    public override string[] AllowedAddresses => new[]
    {
        "192.168.1.100",   // Specific server
        "192.168.1.200",   // Specific client
        "10.0.1.0/24"      // Subnet range
    };
}

// Message handler with additional validation
public class SecureMessageHandler : IFeederHandler<MyChannel, MyUdpMessage>
{
    public async Task<bool> HandleAsync(MyChannel channel, MyUdpMessage message, CancellationToken cancellationToken = default)
    {
        // Additional application-level validation
        if (!IsValidMessage(message))
        {
            return false; // Reject invalid messages
        }

        // Process valid message
        await ProcessMessageAsync(message);
        return true;
    }

    private bool IsValidMessage(MyUdpMessage message)
    {
        // Custom validation logic
        return !string.IsNullOrEmpty(message.Data) && 
               message.Timestamp > DateTime.UtcNow.AddMinutes(-5);
    }
}
```

[↑ Back to top](#contents)

## RapidStreamer Dependencies

| Package | Version | Description | Links |
|---------|---------|-------------|-------|
| **RapidStreamer.Feeders.SharedKernel** | 1.0.78 | Core feeder abstractions and utilities | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| **RapidStreamer.Providers.DotNet.SharedKernel** | 1.0.78 | Core provider abstractions and utilities | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| **RapidStreamer.BuildingBlocks.Application** | 1.0.78 | Application building blocks and base classes | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |

### Installation

```bash
# Add GitHub Packages source
dotnet nuget add source "https://nuget.pkg.github.com/KiarashMinoo/index.json" \
  --name "GitHub" --username YOUR_USERNAME --password YOUR_TOKEN

# Install UdpClient packages
dotnet add package RapidStreamer.Feeders.UdpClient
dotnet add package RapidStreamer.Providers.DotNet.UdpClient
```

### Framework Dependencies

- **.NET 8.0** or **.NET 9.0**
- **Microsoft.AspNetCore.App** (for hosting and DI)
- **System.Net.Sockets** (UDP socket operations)
- **System.Net.Primitives** (IP address handling)

### OS Socket Configuration

For high-performance scenarios, consider OS-level UDP socket tuning:

```bash
# Linux - Increase socket buffer sizes
echo 'net.core.rmem_max = 134217728' >> /etc/sysctl.conf
echo 'net.core.wmem_max = 134217728' >> /etc/sysctl.conf
echo 'net.core.rmem_default = 65536' >> /etc/sysctl.conf
echo 'net.core.wmem_default = 65536' >> /etc/sysctl.conf

# Windows - Increase socket buffer via registry
# HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\AFD\Parameters
# MaxFastSendDatagramThreshold = 65536
```

[↑ Back to top](#contents)

## See Also

- **[SharedKernel](../SharedKernel/README.md)** - Core abstractions and base classes
- **[TcpSocket](../TcpSocket/README.md)** - Reliable TCP socket communication
- **[WebSocket](../WebSocket/README.md)** - WebSocket-based real-time communication
- **[Main Documentation](../README.md)** - Complete framework overview

---

**Framework Integration:** UdpClient messaging system provides ultra-high-speed, connectionless communication within the RapidStreamer ecosystem, perfect for real-time applications where speed is prioritized over delivery guarantees.

[↑ Back to top](#contents)