# TcpSocket Messaging System

## Contents

- [Overview](#overview)
- [Files](#files)
- [Types & Members](#types--members)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Security & SSL](#security--ssl)
- [Performance Notes](#performance-notes)
- [Usage Examples](#usage-examples)
- [RapidStreamer Dependencies](#rapidstreamer-dependencies)
- [See Also](#see-also)

[↑ Back to top](#contents)

## Overview

The TcpSocket messaging system provides low-level TCP socket communication capabilities for the RapidStreamer framework. It enables reliable, connection-oriented data transmission over TCP/IP networks with support for SSL/TLS encryption, authentication, and framed message protocols. This system is ideal for custom network protocols, direct socket communication, and scenarios requiring fine-grained control over network connections.

The implementation supports both server-side (Feeder) and client-side (Provider) TCP socket operations with comprehensive security features, connection management, and performance optimizations for high-throughput scenarios.

**Key Features:**
- Reliable TCP connection management with automatic reconnection
- SSL/TLS encryption with certificate-based authentication
- Custom message framing with configurable end-of-message markers
- IP address filtering and access control
- Username/password authentication support
- Configurable buffer sizes and timeout settings
- Health monitoring and OpenTelemetry integration

[↑ Back to top](#contents)

## Files

| File | Primary Type(s) | LOC (approx) | Responsibility |
|------|----------------|--------------|----------------|
| **Feeder Components** |
| `TcpSocketFeeder.cs` | `TcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>` | 210 | TCP server implementation, accepts incoming connections |
| `TcpSocketFeederConfiguration.cs` | `TcpSocketFeederConfiguration` | 85 | Server-side configuration with SSL and authentication |
| `TcpSocketFeederMessage.cs` | `TcpSocketFeederMessage` | 5 | Base message type for TCP feeder messages |
| `TcpSocketFeederExtensions.cs` | `TcpSocketFeederExtensions` | 60 | DI registration and configuration extensions |
| **Provider Components** |
| `TcpSocketProvider.cs` | `TcpSocketProvider<TTcpSocketProviderMessage, TTcpSocketProviderConfiguration>` | 130 | TCP client implementation for outbound connections |
| `TcpSocketProviderConfiguration.cs` | `TcpSocketProviderConfiguration` | 55 | Client-side configuration with endpoint settings |
| `TcpSocketProviderMessage.cs` | `TcpSocketProviderMessage` | 5 | Base message type for TCP provider messages |
| `TcpSocketProviderExtensions.cs` | `TcpSocketProviderExtensions` | 25 | DI registration for provider services |
| **SharedKernel** |
| `ITcpSocketFeeviderConfiguration.cs` | `ITcpSocketFeeviderConfiguration` | 15 | Common configuration interface |

[↑ Back to top](#contents)

## Types & Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|------|------|---------|---------------------|-------------|
| `TcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>` | Class | TCP server for accepting incoming connections | `DelegativeFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>`, `IFeature` | Start, DisposeManagedResources |
| `TcpSocketFeederConfiguration` | Abstract Class | Server-side TCP configuration | `AbstractFeederConfiguration`, `ITcpSocketFeeviderConfiguration` | Port, Ssl, Certificate, AllowedAddresses |
| `TcpSocketFeederMessage` | Abstract Class | Base message type for TCP feeders | `FeederMessage` | Inherited message properties |
| `TcpSocketProvider<TTcpSocketProviderMessage, TTcpSocketProviderConfiguration>` | Class | TCP client for outbound connections | `AbstractProvider<TTcpSocketProviderMessage, TTcpSocketProviderConfiguration>` | InternalExecuteAsync, IsSocketConnected |
| `TcpSocketProviderConfiguration` | Abstract Class | Client-side TCP configuration | `AbstractProviderConfiguration`, `ITcpSocketFeeviderConfiguration` | Endpoint, Port, Ssl, Username, Password |
| `TcpSocketProviderMessage` | Abstract Class | Base message type for TCP providers | `FeederMessage` | Inherited message properties |
| `TcpSocketFeederExtensions` | Static Class | DI registration extensions for feeders | N/A | AddTcpSocketFeeder, AddTcpSocketFeederResolver |
| `TcpSocketProviderExtensions` | Static Class | DI registration extensions for providers | N/A | AddTcpSocketProvider |
| `ITcpSocketFeeviderConfiguration` | Interface | Common TCP configuration contract | N/A | Ssl, Port, BufferSize, Username, Password |

[↑ Back to top](#contents)

## API Reference

### TcpSocketFeeder&lt;TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration&gt;

TCP server implementation that listens for incoming connections and processes messages.

**Namespace:** `RapidStreamer.Feeders.TcpSocket`  
**Inherits:** `DelegativeFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>`  
**Implements:** `IFeature`  
**Attributes:** `[IsAvailableOnDemo]`

#### Key Properties
- `HealthName : string` — Health check identifier based on port
- `HealthTags : string[]` — Health monitoring tags including port number

#### Key Methods
- `Start(object? state) : void` — Begins accepting TCP client connections (private)
- `DisposeManagedResources() : void` — Properly disposes TCP listener resources

#### Constructors
```csharp
public TcpSocketFeeder(
    TChannel channel,
    TTcpSocketFeederConfiguration tcpSocketFeederConfiguration,
    IFeederHandler<TChannel, TTcpSocketFeederMessage> feederHandler,
    IServiceProvider serviceProvider)
```

#### Thread Safety
- Uses internal synchronization for connection handling
- Safe for concurrent client connections
- Automatic resource cleanup on disposal

#### Usage Recipe
```csharp
// Configure TCP feeder for incoming connections
services.AddTcpSocketFeeder<MyChannel, MyTcpMessage, MyTcpConfig>(
    configuration, "TcpServer");

// Use in application pipeline
app.UseTcpSocketFeederResolver<MyChannel, MyTcpMessage, MyTcpConfig>(
    channelKey, tcpConfiguration);
```

[↑ Back to top](#contents)

### TcpSocketFeederConfiguration

Server-side configuration for TCP socket feeders with SSL and security support.

**Namespace:** `RapidStreamer.Feeders.TcpSocket`  
**Inherits:** `AbstractFeederConfiguration`  
**Implements:** `ITcpSocketFeeviderConfiguration`

#### Key Properties
- `Port : short` — TCP listening port (required)
- `Ssl : bool?` — Enable SSL/TLS encryption
- `Certificate : CertificateModel?` — SSL certificate configuration
- `ClientCertificateRequired : bool` — Require client certificates (default: false)
- `EnabledSslProtocols : SslProtocols` — Allowed SSL protocols (default: Tls12)
- `CheckCertificateRevocation : bool` — Check certificate revocation (default: false)
- `BufferSize : int` — Network buffer size (default: 4096 bytes)
- `Username : string?` — Authentication username
- `Password : string?` — Authentication password
- `ReadTimeout : int?` — Socket read timeout in milliseconds
- `WriteTimeout : int?` — Socket write timeout in milliseconds
- `AllowedAddresses : string[]?` — IP address whitelist for connections

#### Validation Notes
- Port must be between 1-65535
- SSL certificate required when Ssl=true
- Username/Password used together for authentication
- AllowedAddresses supports CIDR notation

#### Usage Recipe
```json
{
  "TcpServer": {
    "Port": 8080,
    "Ssl": true,
    "BufferSize": 8192,
    "ReadTimeout": 30000,
    "WriteTimeout": 30000,
    "AllowedAddresses": ["192.168.1.0/24", "10.0.0.1"]
  }
}
```

[↑ Back to top](#contents)

### TcpSocketProvider&lt;TTcpSocketProviderMessage, TTcpSocketProviderConfiguration&gt;

TCP client implementation for establishing outbound connections and sending messages.

**Namespace:** `RapidStreamer.Providers.DotNet.TcpSocket`  
**Inherits:** `AbstractProvider<TTcpSocketProviderMessage, TTcpSocketProviderConfiguration>`

#### Key Properties
- `_tcpClient : TcpClient` — Underlying TCP client connection
- `_endPoint : IPEndPoint` — Target server endpoint
- `_stream : Stream?` — Network or SSL stream for communication
- `_semaphoreSlim : SemaphoreSlim` — Connection synchronization (max 1 concurrent)

#### Key Methods
- `InternalExecuteAsync(byte[] bytes, CancellationToken) : Task` — Sends raw bytes over TCP
- `InternalExecuteAsync(TTcpSocketProviderMessage, CancellationToken) : Task` — Processes message with tracing
- `IsSocketConnected() : bool` — Checks TCP connection status (private)
- `InitializeStreamAsync() : Task<Stream>` — Sets up network/SSL stream (private)
- `DisposeManagedResources() : void` — Cleans up TCP resources

#### Constructors
```csharp
public TcpSocketProvider(
    TTcpSocketProviderConfiguration tcpSocketProviderConfiguration,
    IServiceProvider serviceProvider)
```

#### Connection Management
- Automatic reconnection on connection loss
- SSL/TLS stream initialization when configured
- Username/password authentication support
- Configurable timeouts and buffer sizes

#### Thread Safety
- Uses SemaphoreSlim for connection serialization
- Thread-safe for concurrent message sending
- Automatic resource disposal

#### Usage Recipe
```csharp
// Configure TCP provider for outbound connections
services.AddTcpSocketProvider<MyTcpMessage, MyTcpConfig>(
    configuration, "TcpClient");

// Send message via provider
await tcpProvider.ExecuteAsync(myMessage, cancellationToken);
```

[↑ Back to top](#contents)

### TcpSocketProviderConfiguration

Client-side configuration for TCP socket providers with endpoint and security settings.

**Namespace:** `RapidStreamer.Providers.DotNet.TcpSocket`  
**Inherits:** `AbstractProviderConfiguration`  
**Implements:** `ITcpSocketFeeviderConfiguration`

#### Key Properties
- `Endpoint : string` — Target server IP address or hostname (required)
- `Port : short` — Target server port (required)
- `Ssl : bool?` — Enable SSL/TLS for connection
- `BufferSize : int` — Network buffer size (default: 4096 bytes)
- `Username : string?` — Authentication username
- `Password : string?` — Authentication password
- `ReadTimeout : int?` — Socket read timeout in milliseconds
- `WriteTimeout : int?` — Socket write timeout in milliseconds

#### Validation Notes
- Endpoint can be IP address or resolvable hostname
- Port must be valid TCP port (1-65535)
- SSL configuration must match server requirements
- Timeout values in milliseconds (null = infinite)

#### Usage Recipe
```json
{
  "TcpClient": {
    "Endpoint": "192.168.1.100",
    "Port": 8080,
    "Ssl": false,
    "BufferSize": 4096,
    "ReadTimeout": 30000,
    "WriteTimeout": 30000,
    "Username": "client_user",
    "Password": "client_pass"
  }
}
```

[↑ Back to top](#contents)

## Configuration

### Server Configuration (Feeder)

```json
{
  "TcpServer": {
    "Port": 8080,
    "Ssl": true,
    "Certificate": {
      "Path": "server.pfx",
      "Password": "cert_password"
    },
    "ClientCertificateRequired": false,
    "EnabledSslProtocols": "Tls12",
    "CheckCertificateRevocation": false,
    "BufferSize": 8192,
    "ReadTimeout": 30000,
    "WriteTimeout": 30000,
    "AllowedAddresses": ["192.168.1.0/24"],
    "Username": "server_user",
    "Password": "server_pass",
    "SerializerType": "Json"
  }
}
```

### Client Configuration (Provider)

```json
{
  "TcpClient": {
    "Endpoint": "server.example.com",
    "Port": 8080,
    "Ssl": true,
    "BufferSize": 4096,
    "ReadTimeout": 30000,
    "WriteTimeout": 30000,
    "Username": "client_user",
    "Password": "client_pass",
    "SerializerType": "Json"
  }
}
```

### Advanced Configuration

```json
{
  "TcpAdvanced": {
    "Port": 9090,
    "Ssl": true,
    "Certificate": {
      "StoreName": "My",
      "StoreLocation": "LocalMachine",
      "Thumbprint": "ABC123..."
    },
    "EnabledSslProtocols": "Tls12, Tls13",
    "ClientCertificateRequired": true,
    "CheckCertificateRevocation": true,
    "BufferSize": 16384,
    "AllowedAddresses": [
      "10.0.0.0/8",
      "172.16.0.0/12",
      "192.168.0.0/16"
    ]
  }
}
```

[↑ Back to top](#contents)

## Security & SSL

### SSL/TLS Configuration

The TcpSocket system provides comprehensive SSL/TLS support:

**Certificate Configuration:**
```csharp
public class MyCertificateConfig : TcpSocketFeederConfiguration
{
    // File-based certificate
    public override CertificateModel Certificate => new()
    {
        Path = "server.pfx",
        Password = "certificate_password"
    };

    // Certificate store-based
    public override CertificateModel Certificate => new()
    {
        StoreName = StoreName.My,
        StoreLocation = StoreLocation.LocalMachine,
        Thumbprint = "ABC123DEF456..."
    };
}
```

**SSL Protocol Support:**
- TLS 1.2 (default)
- TLS 1.3 (when available)
- Configurable cipher suites
- Certificate revocation checking

### Authentication

**Username/Password Authentication:**
```csharp
protected override bool AuthenticateUser(string username, string password)
{
    return username == _tcpSocketFeederConfiguration.Username &&
           password == _tcpSocketFeederConfiguration.Password;
}
```

**IP Address Filtering:**
```csharp
private bool CheckAllowance(EndPoint? endPoint)
{
    if (_tcpSocketFeederConfiguration.AllowedAddresses is null)
        return true;
        
    // Check against whitelist
    return _tcpSocketFeederConfiguration.AllowedAddresses
        .Any(allowed => IsAddressAllowed(endPoint, allowed));
}
```

[↑ Back to top](#contents)

## Performance Notes

### Throughput Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| **Peak Throughput** | 200K+ msg/s | Depends on message size and network |
| **Latency** | 1-5ms | Local network, varies with SSL overhead |
| **Connection Limit** | OS dependent | Typically 65K+ concurrent connections |
| **Memory Usage** | ~4KB per connection | Plus message buffers |

### Optimization Strategies

**Buffer Size Tuning:**
```json
{
  "BufferSize": 8192,  // 8KB for high throughput
  "BufferSize": 1024   // 1KB for low latency
}
```

**Connection Pooling:**
- Provider reuses TCP connections automatically
- Feeder handles multiple concurrent clients
- Automatic reconnection on connection loss

**SSL Performance:**
- TLS 1.3 provides better performance than TLS 1.2
- Certificate caching reduces handshake overhead
- Session resumption for repeated connections

**Memory Management:**
- Configurable buffer sizes
- Automatic cleanup of disconnected clients
- Resource disposal on application shutdown

### Monitoring & Health Checks

```csharp
// Health check registration
services.AddHealthChecks()
    .AddCheck<TcpSocketFeederHealthCheck>("tcp_feeder");

// Metrics collection
public override string HealthName => 
    $"feeder_{nameof(TcpSocket)}_{_tcpSocketFeederConfiguration.Port}";

public override string[] HealthTags => 
    [.. base.HealthTags, nameof(TcpSocket), _tcpSocketFeederConfiguration.Port.ToString()];
```

[↑ Back to top](#contents)

## Usage Examples

### Basic TCP Server Setup

```csharp
// Message definition
public class MyTcpMessage : TcpSocketFeederMessage
{
    public string Content { get; set; }
    public DateTime Timestamp { get; set; }
}

// Configuration
public class MyTcpServerConfig : TcpSocketFeederConfiguration
{
    // Configuration loaded from appsettings.json
}

// Service registration
services.AddTcpSocketFeeder<MyChannel, MyTcpMessage, MyTcpServerConfig>(
    configuration, "TcpServer");

// Application pipeline
app.UseTcpSocketFeederResolver<MyChannel, MyTcpMessage, MyTcpServerConfig>(
    channelKey, tcpServerConfig);
```

### TCP Client (Provider) Setup

```csharp
// Message definition
public class MyTcpProviderMessage : TcpSocketProviderMessage
{
    public string Data { get; set; }
    public int MessageId { get; set; }
}

// Configuration
public class MyTcpClientConfig : TcpSocketProviderConfiguration
{
    // Configuration loaded from appsettings.json
}

// Service registration
services.AddTcpSocketProvider<MyTcpProviderMessage, MyTcpClientConfig>(
    configuration, "TcpClient");

// Usage in service
public class MyService
{
    private readonly IProvider<MyTcpProviderMessage> _tcpProvider;

    public async Task SendDataAsync(string data)
    {
        var message = new MyTcpProviderMessage 
        { 
            Data = data, 
            MessageId = Random.Next() 
        };
        
        await _tcpProvider.ExecuteAsync(message);
    }
}
```

### SSL/TLS Secure Communication

```csharp
// Server configuration with SSL
public class SecureTcpConfig : TcpSocketFeederConfiguration
{
    public override bool? Ssl => true;
    public override CertificateModel Certificate => new()
    {
        Path = "server.pfx",
        Password = Environment.GetEnvironmentVariable("CERT_PASSWORD")
    };
    public override bool ClientCertificateRequired => true;
    public override SslProtocols EnabledSslProtocols => SslProtocols.Tls12 | SslProtocols.Tls13;
}

// Client configuration with SSL
public class SecureTcpClientConfig : TcpSocketProviderConfiguration
{
    public override bool? Ssl => true;
    public override string Endpoint => "secure-server.example.com";
    public override short Port => 8443;
}
```

### High-Performance Configuration

```csharp
// Optimized for throughput
public class HighThroughputConfig : TcpSocketFeederConfiguration
{
    public override short Port => 8080;
    public override int BufferSize => 16384; // 16KB buffers
    public override int? ReadTimeout => null; // No timeout
    public override int? WriteTimeout => null; // No timeout
    public override bool? Ssl => false; // Disable SSL for maximum speed
}

// Usage with performance monitoring
services.AddTcpSocketFeeder<MyChannel, MyMessage, HighThroughputConfig>(
    configuration, "HighPerf")
    .AddHealthChecks()
    .AddCheck<TcpSocketFeederHealthCheck>("tcp_performance");
```

### Custom Authentication

```csharp
public class AuthenticatedTcpConfig : TcpSocketFeederConfiguration
{
    public override string Username => "tcp_user";
    public override string Password => GetPasswordFromSecureStore();
    public override string[] AllowedAddresses => new[]
    {
        "192.168.1.0/24",
        "10.0.0.0/8"
    };

    private string GetPasswordFromSecureStore()
    {
        // Retrieve from secure configuration
        return Environment.GetEnvironmentVariable("TCP_PASSWORD") 
               ?? throw new InvalidOperationException("TCP password not configured");
    }
}
```

[↑ Back to top](#contents)

## RapidStreamer Dependencies

| Package | Version | Description | Links |
|---------|---------|-------------|-------|
| **RapidStreamer.Feeders.SharedKernel** | 1.0.78 | Core feeder abstractions and utilities | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| **RapidStreamer.Providers.DotNet.SharedKernel** | 1.0.78 | Core provider abstractions and utilities | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| **RapidStreamer.Feeviders.TcpSocket.SharedKernel** | 1.0.78 | TcpSocket-specific shared interfaces and types | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| **RapidStreamer.BuildingBlocks.Application.Certificate** | 1.0.78 | Certificate management utilities | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |

### Installation

```bash
# Add GitHub Packages source
dotnet nuget add source "https://nuget.pkg.github.com/KiarashMinoo/index.json" \
  --name "GitHub" --username YOUR_USERNAME --password YOUR_TOKEN

# Install TcpSocket packages
dotnet add package RapidStreamer.Feeders.TcpSocket
dotnet add package RapidStreamer.Providers.DotNet.TcpSocket
dotnet add package RapidStreamer.Feeviders.TcpSocket.SharedKernel
```

### Framework Dependencies

- **.NET 8.0** or **.NET 9.0**
- **Microsoft.AspNetCore.App** (for hosting and DI)
- **System.Net.Sockets** (TCP socket operations)
- **System.Net.Security** (SSL/TLS support)
- **System.Security.Authentication** (SSL protocols)

[↑ Back to top](#contents)

## See Also

- **[SharedKernel](../SharedKernel/README.md)** - Core abstractions and base classes
- **[WebSocket](../WebSocket/README.md)** - WebSocket-based real-time communication  
- **[RabbitMQ](../RabbitMQ/README.md)** - AMQP message broker integration
- **[Main Documentation](../README.md)** - Complete framework overview

---

**Framework Integration:** TcpSocket messaging system provides low-level TCP communication capabilities within the RapidStreamer ecosystem, offering direct socket control for custom protocols and high-performance networking scenarios.

[↑ Back to top](#contents)