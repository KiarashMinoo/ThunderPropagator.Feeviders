# ThunderPropagator UdpClient Integration

## Overview

The **ThunderPropagator UdpClient** integration provides high-performance, enterprise-grade UDP (User Datagram Protocol) messaging capabilities for real-time, connectionless communication scenarios. Built on .NET's `System.Net.Sockets.UdpClient`, this implementation offers fire-and-forget datagram transmission with optional encryption, address filtering, and comprehensive observability.

UDP is defined in **RFC 768** as a minimal, connectionless transport protocol providing unreliable datagram delivery without handshakes, acknowledgments, or ordering guarantees. The ThunderPropagator UdpClient integration embraces UDP's simplicity while adding production-ready features like encryption, health monitoring, and distributed tracing.

### Key Characteristics

- **Connectionless**: No connection establishment (3-way handshake) or teardown overhead
- **Unreliable**: No delivery guarantees, acknowledgments, or automatic retransmissions
- **Unordered**: Datagrams may arrive out-of-sequence
- **Low Overhead**: 8-byte header vs TCP's 20+ bytes
- **Broadcast/Multicast**: Native support for one-to-many communication
- **Speed**: Minimal latency for real-time applications

### Component Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    UDP Communication Flow                        │
└─────────────────────────────────────────────────────────────────┘

   Sender Application                      Network                   Receiver Application
   ┌──────────────────┐                                             ┌──────────────────┐
   │                  │                                             │                  │
   │  UdpClient       │─────[ UDP Datagram 1 ]────────────────────▶│  UdpClient       │
   │  Provider        │                                             │  Feeder          │
   │                  │─────[ UDP Datagram 2 ]────────────────────▶│                  │
   │  ┌────────────┐  │                                             │  ┌────────────┐  │
   │  │ Serialize  │  │     ╔═══════════════════════════════╗      │  │ Receive    │  │
   │  └─────┬──────┘  │     ║  UDP Header (8 bytes)         ║      │  └─────┬──────┘  │
   │        │         │     ║  - Source Port                ║      │        │         │
   │        ▼         │     ║  - Destination Port           ║      │        ▼         │
   │  ┌────────────┐  │     ║  - Length                     ║      │  ┌────────────┐  │
   │  │ Encrypt    │  │     ║  - Checksum                   ║      │  │ Decrypt    │  │
   │  │ (Optional) │  │     ╚═══════════════════════════════╝      │  │ (Optional) │  │
   │  └─────┬──────┘  │                                             │  └─────┬──────┘  │
   │        │         │     ╔═══════════════════════════════╗      │        │         │
   │        ▼         │     ║  Payload (up to 65,507 bytes) ║      │        ▼         │
   │  ┌────────────┐  │     ║  - Application Data           ║      │  ┌────────────┐  │
   │  │ SendAsync  │  │     ║  - Serialized Message         ║      │  │ Deserialize│  │
   │  └────────────┘  │     ╚═══════════════════════════════╝      │  └────────────┘  │
   │                  │                                             │                  │
   └──────────────────┘              No Acknowledgment             └──────────────────┘
                                     No Connection State
                                     No Ordering Guarantees

   Supported Communication Patterns:
   ┌─────────────────────────────────────────────────────────────┐
   │ ● Unicast:    One sender → One receiver (IP:Port)          │
   │ ● Broadcast:  One sender → All on subnet (255.255.255.255) │
   │ ● Multicast:  One sender → Multiple subscribers (224-239)  │
   └─────────────────────────────────────────────────────────────┘
```

### Protocol Overview

**UDP Datagram Structure** (RFC 768):
```
 0      7 8     15 16    23 24    31
+--------+--------+--------+--------+
|     Source      |   Destination   |
|      Port       |      Port       |
+--------+--------+--------+--------+
|                 |                 |
|     Length      |    Checksum     |
+--------+--------+--------+--------+
|                                   |
|          Data (Payload)           |
+-----------------------------------+

Field Descriptions:
- Source Port (16 bits): Sending application port (optional)
- Destination Port (16 bits): Receiving application port (required)
- Length (16 bits): Total datagram length (header + data)
- Checksum (16 bits): Optional integrity check (IPv4), mandatory (IPv6)
- Data: Application payload (0-65,507 bytes maximum)
```

**Maximum Datagram Size Calculation**:
```
IPv4 Maximum Transmission Unit (MTU):  1,500 bytes (Ethernet)
- IP Header:                              -20 bytes
- UDP Header:                              -8 bytes
= Maximum UDP Payload:                  1,472 bytes (safe size)

Theoretical Maximum:
IPv4 Maximum Packet Size:             65,535 bytes
- IP Header:                             -20 bytes
- UDP Header:                             -8 bytes
= Maximum UDP Payload:                65,507 bytes (may fragment)
```

## Features

### Core Capabilities

1. **Connectionless Communication**
   - No handshake or connection establishment overhead
   - Immediate datagram transmission
   - Fire-and-forget semantics
   - No connection state maintenance

2. **Performance Optimized**
   - Minimal protocol overhead (8-byte header)
   - Direct socket access via `System.Net.Sockets.Socket`
   - `ArrayPool<byte>` buffer management for zero-allocation receiving
   - Span-based data processing
   - Concurrent sending with `SemaphoreSlim` coordination

3. **Communication Patterns**
   - **Unicast**: Point-to-point communication (IP:Port addressing)
   - **Broadcast**: Subnet-wide messaging (255.255.255.255)
   - **Multicast**: Group-based messaging (224.0.0.0 - 239.255.255.255)

4. **Security Features**
   - AES-256 encryption (CBC mode)
   - HMAC-SHA256 message authentication
   - Address-based filtering (allow-list)
   - Cryptographic integrity verification

5. **Datagram Management**
   - Configurable buffer sizes (default 65,535 bytes)
   - Automatic MTU consideration
   - No fragmentation at application level
   - Datagram size validation

6. **Observability**
   - OpenTelemetry distributed tracing
   - Health monitoring (Healthy/Unhealthy)
   - Structured logging (Microsoft.Extensions.Logging)
   - Receive/send metrics

### UDP vs TCP Comparison

| Feature | UDP | TCP |
|---------|-----|-----|
| **Connection** | Connectionless | Connection-oriented (3-way handshake) |
| **Reliability** | Unreliable (no ACK) | Reliable (ACK, retransmission) |
| **Ordering** | Unordered delivery | Ordered delivery |
| **Header Size** | 8 bytes | 20-60 bytes |
| **Flow Control** | None | Window-based |
| **Congestion Control** | None | Yes (slow start, etc.) |
| **Latency** | Low (no handshake) | Higher (connection + ACK overhead) |
| **Broadcast/Multicast** | Yes | No |
| **Use Cases** | Real-time, streaming | File transfer, web, email |
| **Packet Loss** | Possible | Automatic recovery |
| **Speed** | Faster | Slower |

**When to Use UDP**:
- Real-time applications where latency matters more than reliability (VoIP, gaming)
- Broadcasting or multicasting to multiple receivers
- High-throughput scenarios where occasional packet loss is acceptable (video streaming)
- Request-response patterns with application-level retries (DNS)
- Time-sensitive data where old data becomes irrelevant (sensor readings)

**When to Use TCP**:
- Reliable data delivery is critical (file transfers, databases)
- Ordered message processing is required
- Connection state is beneficial (persistent sessions)
- Automatic flow control and congestion management are needed

## Use Cases

### 1. Real-Time Applications

**VoIP (Voice over IP)**:
```
Scenario: Bidirectional audio streaming
- Packet loss tolerance: 1-5% (human ear compensates)
- Latency requirement: <150ms for natural conversation
- Why UDP: Connection setup would add unacceptable delay
```

**Online Gaming**:
```
Scenario: Multiplayer game state synchronization
- Update frequency: 20-60 times/second
- Latency requirement: <50ms for responsive gameplay
- Why UDP: Old position data is worthless, speed > reliability
```

**Live Video Streaming**:
```
Scenario: Sports broadcast, security camera feeds
- Bitrate: 1-10 Mbps
- Latency requirement: <2 seconds end-to-end
- Why UDP: Retransmitting old frames causes stuttering
```

### 2. Broadcast/Multicast Scenarios

**Service Discovery**:
```csharp
// Broadcast availability announcement
var message = new ServiceAnnouncementMessage {
    ServiceName = "FileServer",
    IpAddress = "192.168.1.100",
    Port = 9000
};

// Broadcast to all on subnet (255.255.255.255:5000)
await provider.ExecuteAsync(message);
```

**Time Synchronization (NTP)**:
```
Network Time Protocol uses UDP for clock synchronization
- Port: 123
- Tolerance: Packet loss acceptable (retry logic)
- Why UDP: Minimal overhead for frequent time requests
```

### 3. DNS Queries

**Domain Name Resolution**:
```
DNS queries use UDP for performance:
- Query size: Typically <512 bytes
- Response time: <100ms expected
- Fallback: TCP for large responses (>512 bytes)
- Why UDP: Low latency for frequent lookups
```

### 4. IoT and Telemetry

**Sensor Data Collection**:
```csharp
// Temperature sensor reporting
var telemetry = new SensorTelemetryMessage {
    SensorId = "TEMP-001",
    Temperature = 22.5,
    Timestamp = DateTime.UtcNow
};

// Send to collector (fire-and-forget)
await provider.ExecuteAsync(telemetry);
```

**Why UDP for IoT**:
- Sensor readings are frequently transmitted (every second)
- Latest reading replaces stale data
- Low power consumption (no connection state)
- Network bandwidth conservation

### 5. Logging and Monitoring

**Centralized Log Aggregation**:
```
Application Logs → UDP → Syslog Server → Storage

Benefits:
- Non-blocking: App doesn't wait for log acknowledgment
- High throughput: Thousands of logs/second
- Acceptable loss: Some log entries missing is tolerable
```

## Quick Start

### 1. Installation

```powershell
# Install Feeder (message consumer)
dotnet add package ThunderPropagator.Feeders.UdpClient

# Install Provider (message publisher)
dotnet add package ThunderPropagator.Providers.DotNet.UdpClient
```

### 2. Configuration

**appsettings.json**:
```json
{
  "Messaging": {
    "UdpClient": {
      "Feeder": {
        "Id": "00000000-0000-0000-0000-000000000001",
        "Port": 5000,
        "BufferSize": 65535,
        "AllowedAddresses": ["192.168.1.100", "192.168.1.101"],
        "EnableEncryption": false,
        "SerializerType": "NJson"
      },
      "Provider": {
        "Endpoint": "192.168.1.200",
        "Port": 5000,
        "BufferSize": 65535,
        "EnableEncryption": false,
        "SerializerType": "NJson"
      }
    }
  }
}
```

### 3. Define Messages

```csharp
using ThunderPropagator.Feeders.UdpClient;
using ThunderPropagator.Providers.DotNet.UdpClient;

// Feeder message (receiver)
public class SensorDataFeederMessage : UdpClientFeederMessage
{
    public required string SensorId { get; set; }
    public required double Value { get; set; }
    public required DateTime Timestamp { get; set; }
}

// Provider message (sender)
public class SensorDataProviderMessage : UdpClientProviderMessage
{
    public required string SensorId { get; set; }
    public required double Value { get; set; }
    public required DateTime Timestamp { get; set; }
}
```

### 4. Define Configurations

```csharp
using ThunderPropagator.Feeders.UdpClient;
using ThunderPropagator.Providers.DotNet.UdpClient;

// Feeder configuration
public class SensorDataFeederConfiguration : UdpClientFeederConfiguration { }

// Provider configuration
public class SensorDataProviderConfiguration : UdpClientProviderConfiguration { }
```

### 5. Register Services

```csharp
using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Feeders.UdpClient;
using ThunderPropagator.Providers.DotNet.UdpClient;

public class Startup
{
    public void ConfigureServices(IServiceCollection services)
    {
        // Register Feeder (message consumer)
        services.AddUdpClientFeeder<
            SensorDataChannel,
            SensorDataFeederMessage,
            SensorDataFeederConfiguration>(
                Configuration,
                "Messaging:UdpClient:Feeder");

        // Register Provider (message publisher)
        services.AddUdpClientProvider<
            SensorDataProviderMessage,
            SensorDataProviderConfiguration>(
                Configuration,
                "Messaging:UdpClient:Provider");
    }
}
```

### 6. Consume Messages

```csharp
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Application.Channels;

public class SensorDataChannel : IChannel { }

public class SensorDataHandler : IFeederHandler<SensorDataChannel, SensorDataFeederMessage>
{
    private readonly ILogger<SensorDataHandler> _logger;

    public SensorDataHandler(ILogger<SensorDataHandler> logger)
    {
        _logger = logger;
    }

    public Task HandleAsync(
        FeederHandlerContext<SensorDataChannel, SensorDataFeederMessage> context,
        CancellationToken cancellationToken = default)
    {
        var message = context.FeederReceivedMessage.Message;
        
        _logger.LogInformation(
            "Sensor {SensorId} reading: {Value} at {Timestamp}",
            message.SensorId,
            message.Value,
            message.Timestamp);

        // Process sensor data (store, analyze, alert)
        return Task.CompletedTask;
    }
}
```

### 7. Send Messages

```csharp
using ThunderPropagator.Providers.DotNet.UdpClient;

public class SensorService
{
    private readonly IProvider<SensorDataProviderMessage> _provider;

    public SensorService(IProvider<SensorDataProviderMessage> provider)
    {
        _provider = provider;
    }

    public async Task PublishReadingAsync()
    {
        var message = new SensorDataProviderMessage
        {
            SensorId = "TEMP-001",
            Value = 22.5,
            Timestamp = DateTime.UtcNow
        };

        // Fire-and-forget UDP send
        await _provider.ExecuteAsync(message);
    }
}
```

## Key Concepts

### 1. Datagram Size and MTU

**Maximum Transmission Unit (MTU)**:
```
Ethernet MTU: 1,500 bytes (most common)
Wi-Fi MTU:    1,500 bytes
Token Ring:   4,464 bytes (legacy)
FDDI:         4,352 bytes (legacy)

Safe UDP Payload Calculation:
MTU 1,500 bytes
- IPv4 Header: 20 bytes (minimum, can be up to 60 bytes with options)
- UDP Header:   8 bytes
= Safe Payload: 1,472 bytes (no fragmentation)

Exceeding MTU causes IP fragmentation:
- Increased latency (reassembly overhead)
- Higher packet loss risk (any fragment loss = entire datagram lost)
- Firewall issues (some block fragmented packets)
```

**ThunderPropagator Default**:
```csharp
BufferSize = 65535 // Theoretical max, but not recommended for all scenarios
```

**Recommended Sizes**:
```
LAN Communication:     1,472 bytes (MTU-safe)
Internet Communication: 512 bytes (DNS-safe, no fragmentation risk)
Large Messages:        Split into multiple datagrams at application layer
```

### 2. Packet Loss Handling

**UDP Does NOT Provide**:
- Automatic retransmission
- Delivery acknowledgments
- Duplicate detection

**Application-Level Strategies**:

```csharp
// 1. Sequence Numbers (detect loss)
public class SequencedMessage : UdpClientProviderMessage
{
    public long SequenceNumber { get; set; }
    public string Data { get; set; }
}

// Receiver detects gaps: 1, 2, 4, 5 (missing 3)

// 2. Acknowledgments (request retransmission)
public class AckMessage : UdpClientProviderMessage
{
    public long AcknowledgedSequence { get; set; }
}

// 3. Forward Error Correction (FEC)
// Send redundant data to reconstruct lost packets
// Example: Send 10 data packets + 3 parity packets
//          Can reconstruct original 10 if ≤3 lost

// 4. Application-Level Timeouts
await provider.ExecuteAsync(message);
await Task.Delay(TimeSpan.FromMilliseconds(100));
if (!acknowledged)
{
    // Resend
    await provider.ExecuteAsync(message);
}
```

### 3. Time To Live (TTL)

**TTL in IP Header** (not UDP, but affects UDP packets):
```
Purpose: Prevent infinite routing loops
Mechanism: Decremented by each router, discarded at 0
Default: 64 (Linux/Windows), 255 (some routers)

Multicast TTL Scopes:
  0 = Restricted to same host (not forwarded)
  1 = Restricted to same subnet (local network)
 32 = Restricted to same site/organization
 64 = Restricted to same region
128 = Restricted to same continent
255 = Unrestricted (global)
```

**ThunderPropagator Usage**:
```csharp
// .NET doesn't expose TTL directly via UdpClient
// Use Socket.SetSocketOption for advanced control
socket.SetSocketOption(
    SocketOptionLevel.IP,
    SocketOptionName.IpTimeToLive,
    64);
```

### 4. Multicast Groups

**Multicast IP Range**: 224.0.0.0 - 239.255.255.255 (Class D)

**Reserved Addresses**:
```
224.0.0.0   - 224.0.0.255   : Local network control (not forwarded)
224.0.1.0   - 238.255.255.255: Internetwork control and public groups
239.0.0.0   - 239.255.255.255: Private/administratively scoped
```

**IGMP (Internet Group Management Protocol)**:
```
Purpose: Hosts inform routers of multicast group membership
Process:
1. Host sends IGMP Join (JoinMulticastGroup)
2. Router forwards multicast traffic for that group
3. Host sends IGMP Leave (DropMulticastGroup)
```

**ThunderPropagator Multicast** (not directly implemented in base, requires extension):
```csharp
// Extend UdpClientFeeder to support multicast
_socket.SetSocketOption(
    SocketOptionLevel.IP,
    SocketOptionName.AddMembership,
    new MulticastOption(IPAddress.Parse("239.1.1.1")));
```

### 5. Broadcast Addressing

**Broadcast Types**:
```
Limited Broadcast:  255.255.255.255 (not routed beyond subnet)
Directed Broadcast: 192.168.1.255 (specific subnet, often blocked by routers)
```

**ThunderPropagator Broadcast Setup**:
```csharp
// Enable broadcast on socket
_udpClient.EnableBroadcast = true;

// Send to broadcast address
var endpoint = new IPEndPoint(IPAddress.Broadcast, 5000);
await _udpClient.SendAsync(data, endpoint);
```

**Security Considerations**:
- Many networks disable directed broadcast (DDoS amplification)
- Limited broadcast (255.255.255.255) stays on local subnet
- Use multicast for controlled group messaging

### 6. Connectionless Nature

**No Connection State**:
```
TCP Connection:
1. SYN →
2. ← SYN-ACK
3. ACK →
4. Data exchange (with state tracking)
5. FIN/ACK teardown

UDP "Connection":
1. Send datagram (immediate, no handshake)
```

**Implications**:
- **No port exhaustion**: Thousands of senders can use same destination port
- **No TIME_WAIT state**: Immediate socket reuse
- **No congestion control**: Application must implement if needed
- **No ordering**: Datagrams may arrive out-of-sequence

## Performance Considerations

### Throughput

**UDP Advantages**:
- No ACK wait time: ~50ms round-trip eliminated per message
- No congestion window: Send at full line rate
- Minimal header: 8 bytes vs TCP's 20-60 bytes
- No retransmission delays

**Theoretical Limits**:
```
Gigabit Ethernet: 1,000,000,000 bits/sec
- Ethernet overhead: ~10%
= Available bandwidth: 900 Mbps

UDP datagram at MTU:
1,472 bytes payload + 8 UDP + 20 IP + 14 Ethernet = 1,514 bytes
= 12,112 bits per datagram

Max datagrams/sec: 900,000,000 / 12,112 ≈ 74,300 datagrams/sec
Max payload throughput: 74,300 * 1,472 bytes ≈ 109 MB/sec
```

**ThunderPropagator Optimizations**:
- `ArrayPool<byte>` for zero-allocation receiving
- Span-based processing
- Direct socket access
- Minimal serialization overhead (NJson)

### Latency

**UDP Latency Profile**:
```
Send side:
1. Serialize message:      ~0.1ms (JSON)
2. Encrypt (optional):     ~0.5ms (AES-256)
3. Socket.SendAsync:       ~0.01ms (system call)
4. Network transmission:   ~1-50ms (depends on distance)
5. Receive interrupt:      ~0.01ms
6. Decrypt (optional):     ~0.5ms
7. Deserialize:            ~0.1ms

Total: ~2-52ms (vs TCP: 50-100ms with handshake)
```

### Reliability Trade-offs

**Acceptable Packet Loss Scenarios**:
```
VoIP:           1-5% loss (human ear compensates)
Video Streaming: 0.1-1% loss (I-frame recovery)
Gaming:         <1% loss (client-side prediction)
DNS:            0.1% loss (client retries)
Telemetry:      5-10% loss (next reading compensates)
```

**Unacceptable Packet Loss**:
```
Financial transactions (use TCP)
File transfers (use TCP)
Database replication (use TCP)
```

## Documentation

### Detailed Component Guides

- **[Feeder Documentation](Feeders.UdpClient/README.md)** — UDP datagram receiver (DelegativeFeeder)
- **[Provider Documentation](Providers.DotNet.UdpClient/README.md)** — UDP datagram sender (AbstractProvider)

### Related Documentation

- **[SharedKernel Documentation](../SharedKernel/README.md)** — Core abstractions (Feeders & Providers)
- **[TcpSocket Documentation](../TcpSocket/README.md)** — Connection-oriented alternative
- **[WebSocket Documentation](../WebSocket/README.md)** — Bidirectional web communication

## Advanced Topics

### Encryption

ThunderPropagator UdpClient supports optional AES-256-CBC encryption with HMAC-SHA256 integrity:

```csharp
{
  "EnableEncryption": true,
  "EncryptionKey": "your-32-character-encryption-key-here!"
}
```

**Encrypted Datagram Structure**:
```
[IV (16 bytes)][HMAC (32 bytes)][Encrypted Payload (variable)]

Process:
1. Sender: Generate random IV
2. Sender: Encrypt payload with AES-CBC
3. Sender: Compute HMAC of encrypted payload
4. Sender: Concatenate IV + HMAC + encrypted data
5. Receiver: Extract IV, HMAC, encrypted payload
6. Receiver: Verify HMAC (integrity check)
7. Receiver: Decrypt payload with IV
```

**Security Considerations**:
- **No Key Exchange**: Pre-shared key must be securely distributed
- **Replay Protection**: Not built-in, add timestamps/nonces
- **No Perfect Forward Secrecy**: Key compromise exposes all past traffic

### Address Filtering

```csharp
{
  "AllowedAddresses": ["192.168.1.100", "192.168.1.101"]
}
```

Feeder rejects datagrams from unlisted IP addresses (whitelist-only).

## Troubleshooting

### Common Issues

**1. Datagrams Not Received**
```
Symptom: Sender reports success, receiver gets nothing
Causes:
- Firewall blocking UDP port
- Receiver not listening on correct port
- Network routing issues
- Packet loss (no error indication in UDP)

Solutions:
- Check firewall rules: netsh advfirewall firewall show rule name=all
- Verify port binding: netstat -an | findstr :5000
- Test with diagnostic tools: ncat -u -l 5000 (receiver), ncat -u <ip> 5000 (sender)
```

**2. High Packet Loss**
```
Symptom: Significant percentage of datagrams missing
Causes:
- Network congestion
- Receiver buffer overflow (slow processing)
- Datagram size exceeding MTU (fragmentation loss)

Solutions:
- Reduce send rate (application-level pacing)
- Increase BufferSize: 131072 or higher
- Reduce datagram size to <1,472 bytes
- Monitor with Wireshark for fragmentation
```

**3. Out-of-Order Delivery**
```
Symptom: Messages arrive in wrong sequence
Causes:
- Network path diversity (load balancing)
- Different datagram sizes (faster delivery of small packets)

Solutions:
- Add sequence numbers to messages
- Implement application-level reordering buffer
- Accept out-of-order as UDP characteristic
```

**4. Port Already in Use**
```
Symptom: SocketException: Address already in use
Causes:
- Another process using the port
- Previous instance not fully closed

Solutions:
- Find process: netstat -ano | findstr :5000
- Kill process: taskkill /PID <pid> /F
- Use SO_REUSEADDR (not default in ThunderPropagator)
```

## Best Practices

1. **Keep Datagrams Small**: <1,472 bytes to avoid fragmentation
2. **Implement Application ACKs**: For critical messages
3. **Add Sequence Numbers**: Detect loss and reordering
4. **Monitor Health**: Use built-in health checks
5. **Use Encryption**: For sensitive data
6. **Filter Addresses**: Restrict allowed senders
7. **Handle Packet Loss**: Design for it, don't fight it
8. **Avoid Large Payloads**: Split into multiple datagrams if needed
9. **Consider TCP Alternative**: For reliable delivery requirements
10. **Test Network Conditions**: Simulate packet loss (netem, clumsy)

## Version History

- **1.0.1-beta.2** — Current release with encryption support
- Feature parity with ThunderPropagator 1.0.1-beta.2

## License

Part of ThunderPropagator Feeviders framework. See repository license for details.
