# ThunderPropagator Feeviders Development Guide

## Project Overview
**ThunderPropagator Feeviders** is a comprehensive .NET messaging framework providing unified abstractions for 12+ messaging systems. Built with enterprise-grade requirements, it offers consistent APIs, OpenTelemetry integration, and production-ready reliability across diverse messaging technologies (Kafka, RabbitMQ, WebSocket, NATS, MQTT, Pulsar, RedisPubSub, ActiveMQ, TcpSocket, UdpClient, WebApi).

## Architecture: Provider/Feeder Pattern

The framework follows a **bidirectional messaging pattern**:
- **Feeders** consume messages from external systems (inbound)
- **Providers** publish messages to external systems (outbound)
- **SharedKernel** provides common abstractions and utilities

### Feeder Structure (Message Consumer)
Every feeder implementation in `Feeviders/{System}/ThunderPropagator.Feeders.{System}/`:

1. **{System}Feeder.cs** — Inherits `IterativeFeeder<TChannel, TMessage, TConfig>` or `DelegativeFeeder<>`
   - Mark `internal` and `sealed` in Release: `#if !DEBUG sealed #endif`
   - Implement `ReceiveAsync()` returning `IAsyncEnumerable<FeederReceivedMessage<T>>`
   - Set `HealthName` and `HealthTags` for monitoring
2. **{System}FeederMessage.cs** — Abstract class extends `FeederMessage`
3. **{System}FeederConfiguration.cs** — Abstract class implements `IAbstractFeederConfiguration`
4. **{System}FeederExtensions.cs** — DI registration:
   ```csharp
   AddXyzFeeder<TChannel, TMessage, TConfig>(IConfigurationRoot, string)
   AddXyzFeederResolver<TChannel, TMessage, TConfig>()
   UseXyzFeederResolver<TChannel, TMessage, TConfig>(Guid, TConfig)
   ```

### Provider Structure (Message Publisher)
Every provider implementation in `Feeviders/{System}/ThunderPropagator.Providers.DotNet.{System}/`:

1. **{System}Provider.cs** — Inherits `AbstractProvider<TMessage, TConfig>`
   - Override `InternalExecuteAsync(TMessage, CancellationToken)`
   - Automatic serialization via `AbstractProvider`
2. **{System}ProviderMessage.cs** — Abstract class extends `FeederMessage`
3. **{System}ProviderConfiguration.cs** — Abstract class implements `IAbstractProviderConfiguration`
4. **{System}ProviderExtensions.cs** — DI registration:
   ```csharp
   AddXyzProvider<TMessage, TConfig>(IConfigurationRoot, string)
   ```

### DI Registration Pattern
```csharp
// Feeder registration (message consumption)
services.AddKafkaFeeder<MyChannel, MyKafkaMessage, MyKafkaConfig>(
    configuration, "Messaging:Kafka");

// Provider registration (message publishing)
services.AddKafkaProvider<MyKafkaMessage, MyKafkaConfig>(
    configuration, "Messaging:Kafka");
```

## Build System & Versioning

### Multi-Targeting & Platforms
- **Frameworks**: .NET 8, 9, 10 (`TargetFrameworks` in [Directory.Build.props](../Directory.Build.props))
- **Platforms**: AnyCPU, x86, x64, ARM64
- **Central Package Management**: Version-controlled via [Directory.Packages.props](../Directory.Packages.props)
  - Framework-specific versions: `Condition="'$(TargetFramework)' == 'net9.0'"`
  - ThunderPropagator dependencies use dynamic PackageId: `$(ThunderPropagatorPackageId)` and `$(BuildingBlocksPackageId)`

### Package Naming Convention
Packages include configuration and platform suffixes:
- **Debug**: `{ProjectName}.Debug.{Platform}` (e.g., `ThunderPropagator.Feeders.Kafka.Debug.x64`)
- **Release**: `{ProjectName}.{Platform}` (AnyCPU omits platform suffix)
- Controlled by: `PackageIdConfigurationSuffix` and `PackageIdPlatformSuffix` in Directory.Build.props

### Version Management
Version: `1.0.1-beta.2` ([Directory.Build.props](../Directory.Build.props#L3))
- Update manually in Directory.Build.props (`<Version>` property)
- Version flows to all projects automatically
- ThunderPropagator dependency versions: Separate in Directory.Packages.props (`ThunderPropagatorVersion`, `BuildingBlocksVersion`)

## Development Workflows

### Building
```powershell
dotnet build ThunderPropagator.Feeviders.sln -c Release -p:Platform=AnyCPU
```

### Testing
- **Framework**: xUnit with NSubstitute (mocking) and Bogus (fake data)
- **Run**: `dotnet test` or via Visual Studio Test Explorer
- **Structure**: Tests in `Tests/` directory (UnitTests, ArchTests, LoadTests, DotNetClientTests)

### Package Publishing
Uses GitHub Packages. See [nuget.config](../nuget.config) for feed configuration.

```powershell
# Pack all platforms
dotnet pack -c Release -p:Platform=x64
dotnet pack -c Release -p:Platform=ARM64
dotnet pack -c Release -p:Platform=AnyCPU

# Publish to GitHub Packages
dotnet nuget push "bin/Release/*.nupkg" --source github --api-key $env:GH_TOKEN
```

## Code Conventions

### Conditional Compilation
- **DEBUG vs RELEASE**: Classes are `internal` and non-sealed in DEBUG for testability:
  ```csharp
  internal
  #if !DEBUG
      sealed
  #endif
      class KafkaFeeder<TChannel, TMessage, TConfig> : IterativeFeeder<...>
  ```

### Documentation & Warnings
- XML documentation enabled (`GenerateDocumentationFile`)
- Suppressed warnings: `CS1591` (missing XML comments), `CS0067` (unused events)
- Unsafe blocks allowed globally (`AllowUnsafeBlocks`)

### Nullability
- Nullable reference types enabled: `<Nullable>enable</Nullable>`
- Implicit usings enabled: `<ImplicitUsings>enable</ImplicitUsings>`

### Telemetry & Observability
All feeders include:
- **Activity tracing**: OpenTelemetry integration with `Activity.Current`
- **Health monitoring**: Set `HealthName` and `HealthTags` properties
  - Format: `feeder_{System}_{Identifier}` (e.g., `feeder_Kafka_my-group_topic1_topic2`)
  - Tags include system name and relevant identifiers (topics, queues, etc.)
- **Logging**: Use inherited `Logger` property from base classes

## Project Organization

```
Feeviders/
├── ActiveMQ/              # Apache ActiveMQ JMS messaging
├── Kafka/                 # Apache Kafka event streaming
├── Mqtt/                  # MQTT IoT protocol
├── NATS/                  # NATS cloud-native messaging
├── Pulsar/                # Apache Pulsar multi-tenant messaging
├── RabbitMQ/              # RabbitMQ AMQP broker
├── RedisPubSub/           # Redis Pub/Sub in-memory messaging
├── SharedKernel/          # Core abstractions (Feeders & Providers)
├── TcpSocket/             # TCP socket protocol
├── UdpClient/             # UDP datagram protocol
├── WebApi/                # HTTP/REST API
└── WebSocket/             # WebSocket real-time web

Each system has 2-3 projects:
  - ThunderPropagator.Feeders.{System}           # Message consumer
  - ThunderPropagator.Providers.DotNet.{System}  # Message publisher
  - ThunderPropagator.Feeviders.{System}.SharedKernel  # Shared utilities (optional)

Tests/
├── DotNetClientTests/     # .NET client integration tests
├── ThunderPropagator.ArchTests/          # Architecture validation (NetArchTest)
├── ThunderPropagator.UnitTests/          # Core unit tests
└── ThunderPropagator.Web.LoadTests/      # Load/performance tests

docs/
├── README.md              # Framework overview and quick start
├── SharedKernel/          # Core abstractions documentation
├── Kafka/                 # Kafka-specific docs
├── RabbitMQ/              # RabbitMQ-specific docs
└── [other systems]/       # Per-system documentation
```

## Key Implementation Patterns

### Feeder Types
1. **IterativeFeeder** — Pull-based consumption (Kafka, NATS, Pulsar)
   - Override `ReceiveAsync()` returning `IAsyncEnumerable<FeederReceivedMessage<T>>`
   - Use `[EnumeratorCancellation]` on cancellation token parameter
2. **DelegativeFeeder** — Push-based consumption (WebSocket, WebApi, MQTT)
   - Implement `EnqueueAsync(byte[], CancellationToken)` or `EnqueueAsync(string, CancellationToken)`
   - Delegates message handling to internal queue

### Configuration Pattern
All configurations extend base classes with system-specific properties:
```csharp
// Feeder configuration
public abstract class KafkaFeederConfiguration : ConsumerConfig, IAbstractFeederConfiguration
{
    public Guid Id { get; set; }
    public SerializerType SerializerType { get; set; }
    public string? EnrichmentScript { get; set; }
    public string[]? MetadataReferences { get; set; }
}

// Provider configuration
public abstract class KafkaProviderConfiguration : ProducerConfig, IAbstractProviderConfiguration
{
    public SerializerType SerializerType { get; set; }
}
```

### Serialization Support
Most systems support multiple serialization formats via `SerializerType` enum:
- `Json` — System.Text.Json
- `NJson` — Newtonsoft.Json
- `NetJson` — NetJSON (high-performance)
- **Kafka-specific**: `SchemaJson`, `Avro` (Confluent Schema Registry)

## External Dependencies
- **Core**: ThunderPropagator framework (GitHub Packages)
  - ThunderPropagator.BuildingBlocks — Common utilities and abstractions
  - ThunderPropagator — Core streaming framework
- **Testing**: xUnit, NSubstitute, coverlet, NetArchTest.Rules
- **Messaging**: Confluent.Kafka, RabbitMQ.Client, MQTTnet, NATS.Net, DotPulsar, Apache.NMS.ActiveMQ, StackExchange.Redis
- **Infrastructure**: Microsoft.Extensions.* (DI, caching, HTTP), OpenTelemetry.Api
- **Utilities**: Bogus (fake data), NodaTime (timezones), JetBrains.Annotations, NJsonSchema

## Key Files
- [Directory.Build.props](../Directory.Build.props) — Global MSBuild properties & versioning
- [Directory.Packages.props](../Directory.Packages.props) — Centralized package versions with framework conditions
- [nuget.config](../nuget.config) — NuGet feed configuration (GitHub Packages with credentials)
- [global.json](../global.json) — .NET SDK version pinning (10.0.0)
- [docs/README.md](../docs/README.md) — Complete framework documentation with performance comparison
- [Generate-Changelog.ps1](../Generate-Changelog.ps1) — Conventional Commits changelog generator
- [Generate-ReleaseNotes.ps1](../Generate-ReleaseNotes.ps1) — Release notes generator
