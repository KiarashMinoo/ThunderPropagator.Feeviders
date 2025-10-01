# SharedKernel

## Contents

- [Overview](#overview)
- [Files](#files)
- [Types & Members](#types--members)
- [Providers SharedKernel](#providers-sharedkernel)
  - [IProvider](#iprovider)
  - [AbstractProvider](#abstractprovider)
  - [AbstractProviderConfiguration](#abstractproviderconfiguration)
  - [IFeederMessageSerializer](#ifeederMessageserializer)
  - [FeederMessageSerializer](#feederMessageserializer)
- [Feeders SharedKernel](#feeders-sharedkernel)
  - [Extensions](#extensions)
- [RapidStreamer Dependencies](#rapidstreamer-dependencies)
- [See Also](#see-also)

## Overview

The SharedKernel provides foundational interfaces, abstract classes, and utilities for the RapidStreamer Feeviders framework. It establishes common contracts for providers (outbound message publishing) and feeders (inbound message consumption), enabling consistent behavior across all messaging system implementations.

## Files

| File | Primary type(s) | LOC (approx) | Responsibility |
|------|-----------------|--------------|----------------|
| `IProvider.cs` | IProvider, IProvider<TFeederMessage> | 12 | Core provider interfaces |
| `AbstractProvider.cs` | AbstractProvider<TFeederMessage, TProviderConfiguration> | 35 | Base provider implementation with serialization |
| `AbstractProviderConfiguration.cs` | IAbstractProviderConfiguration, AbstractProviderConfiguration | 20 | Provider configuration base classes |
| `IFeederMessageSerializer.cs` | IFeederMessageSerializer<TFeederMessage, TProviderConfiguration> | 12 | Message serialization interface |
| `FeederMessageSerializer.cs` | FeederMessageSerializer<TFeederMessage, TProviderConfiguration> | 45 | Default serialization implementation |
| `Extensions.cs` (Feeders) | Extensions | 15 | DI container registration helpers for feeders |
| `RapidStreamerExtensions.cs` | RapidStreamerExtensions | 20 | DI container registration helpers for providers |

## Types & Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|------|------|---------|-------------------|-------------|
| IProvider | Interface | Base provider interface for resource management | IDisposable | - |
| IProvider<TFeederMessage> | Interface | Generic provider interface for message publishing | IProvider | ExecuteAsync |
| AbstractProvider<TFeederMessage, TProviderConfiguration> | Abstract Class | Base implementation for all message providers | DisposableObject, IProvider<TFeederMessage> | ExecuteAsync, InternalExecuteAsync |
| IAbstractProviderConfiguration | Interface | Provider configuration contract | IServiceConfiguration | SerializerType |
| AbstractProviderConfiguration | Abstract Class | Base provider configuration implementation | ServiceConfiguration, IAbstractProviderConfiguration | SerializerType |
| IFeederMessageSerializer<TFeederMessage, TProviderConfiguration> | Interface | Message serialization contract | - | Serialize, SerializeToBytes |
| FeederMessageSerializer<TFeederMessage, TProviderConfiguration> | Class | Default message serialization implementation | IFeederMessageSerializer | Serialize, SerializeToBytes |

## Providers SharedKernel

### IProvider

**Kind**: Interface  
**Namespace**: RapidStreamer.Providers.DotNet.SharedKernel  
**Inherits**: IDisposable

Base interface for all providers, ensuring proper resource management and disposal patterns.

**Key Members**:
- _None (marker interface)_

**Thread Safety**: Implementation-dependent  
**Serialization Notes**: Not applicable  

#### Usage Recipe

```csharp
public class CustomProvider : IProvider
{
    public void Dispose()
    {
        // Cleanup resources
    }
}
```

### IProvider<TFeederMessage>

**Kind**: Interface  
**Namespace**: RapidStreamer.Providers.DotNet.SharedKernel  
**Inherits**: IProvider  
**Constraints**: TFeederMessage : FeederMessage

Generic provider interface for publishing messages to external systems.

**Key Members**:
- `ExecuteAsync(TFeederMessage feederMessage, CancellationToken cancellationToken = default)` — Publishes a message asynchronously

**Thread Safety**: Implementation-dependent  
**Serialization Notes**: Message serialization handled by implementation  

#### Usage Recipe

```csharp
public class OrderProvider : IProvider<OrderMessage>
{
    public async Task ExecuteAsync(OrderMessage message, CancellationToken cancellationToken = default)
    {
        // Publish message to external system
        await PublishToExternalSystemAsync(message, cancellationToken);
    }
    
    public void Dispose() => /* cleanup */;
}
```

### AbstractProvider

**Kind**: Abstract Class  
**Namespace**: RapidStreamer.Providers.DotNet.SharedKernel  
**Inherits**: DisposableObject  
**Implements**: IProvider<TFeederMessage>  
**Constraints**: TFeederMessage : FeederMessage, TProviderConfiguration : class, IAbstractProviderConfiguration

Base implementation providing common functionality for all message providers including automatic serialization, logging, and message enrichment.

**Attributes**:
- Conditional compilation for DEBUG/Release modes

**Key Properties**:
- `Logger : ILogger` — Logger instance for the provider

**Key Methods**:
- `ExecuteAsync(TFeederMessage feederMessage, CancellationToken cancellationToken = default) : Task` — Public execution method with message enrichment
- `InternalExecuteAsync(TFeederMessage feederMessage, CancellationToken cancellationToken = default) : Task` — Protected virtual method for custom implementation
- `InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default) : Task` — Protected abstract method for byte-level implementation

**Constructors**:
- `AbstractProvider(IServiceProvider serviceProvider)` — Initializes logger and message serializer

**Thread Safety**: Base implementation is thread-safe; derived classes must ensure thread safety  
**Serialization Notes**: Automatic serialization via IFeederMessageSerializer  
**Validation Notes**: Adds PublishedDateTime to messages automatically  

#### Usage Recipe

```csharp
public class MyProvider : AbstractProvider<MyMessage, MyConfiguration>
{
    private readonly MyConfiguration _config;
    
    public MyProvider(MyConfiguration config, IServiceProvider serviceProvider) 
        : base(serviceProvider)
    {
        _config = config;
    }
    
    protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
    {
        // Implement provider-specific logic
        await PublishBytesToExternalSystemAsync(bytes, cancellationToken);
    }
}
```

### AbstractProviderConfiguration

**Kind**: Abstract Class  
**Namespace**: RapidStreamer.Providers.DotNet.SharedKernel  
**Inherits**: ServiceConfiguration  
**Implements**: IAbstractProviderConfiguration

Base configuration class for all providers with built-in serialization settings.

**Key Properties**:
- `SerializerType : SerializerType` — Message serialization format (Json, NJson, NetJson, Avro, SchemaJson)

**Thread Safety**: Configuration objects should be treated as immutable after initialization  
**Serialization Notes**: Supports multiple serialization formats  

#### Usage Recipe

```csharp
public class MyProviderConfiguration : AbstractProviderConfiguration
{
    public string ConnectionString
    {
        get => Get<string>()!;
        set => Set(value);
    }
    
    public int Timeout
    {
        get => Get(30);
        set => Set(value);
    }
}
```

### IFeederMessageSerializer

**Kind**: Interface  
**Namespace**: RapidStreamer.Providers.DotNet.SharedKernel  
**Constraints**: TFeederMessage : FeederMessage, TProviderConfiguration : class, IAbstractProviderConfiguration

Interface for message serialization providing both string and byte array outputs.

**Key Methods**:
- `Serialize(TFeederMessage feederMessage, CancellationToken cancellationToken = default) : string` — Serializes message to string
- `SerializeToBytes(TFeederMessage feederMessage, CancellationToken cancellationToken = default) : byte[]` — Serializes message to byte array

**Thread Safety**: Implementation-dependent  
**Performance Notes**: Byte array serialization typically more efficient for network operations  

#### Usage Recipe

```csharp
public class CustomSerializer : IFeederMessageSerializer<MyMessage, MyConfiguration>
{
    public string Serialize(MyMessage message, CancellationToken cancellationToken = default)
    {
        return JsonSerializer.Serialize(message);
    }
    
    public byte[] SerializeToBytes(MyMessage message, CancellationToken cancellationToken = default)
    {
        return Encoding.UTF8.GetBytes(Serialize(message, cancellationToken));
    }
}
```

### FeederMessageSerializer

**Kind**: Class  
**Namespace**: RapidStreamer.Providers.DotNet.SharedKernel  
**Implements**: IFeederMessageSerializer<TFeederMessage, TProviderConfiguration>  
**Constraints**: TFeederMessage : FeederMessage, TProviderConfiguration : class, IAbstractProviderConfiguration

Default implementation of message serialization supporting multiple formats based on configuration.

**Attributes**:
- Conditional compilation for DEBUG/Release modes
- Internal visibility

**Supported Formats**:
- **Json**: Standard JSON serialization
- **NJson**: Newtonsoft.Json with type name handling
- **NetJson**: High-performance JSON serialization

**Key Methods**:
- `Serialize(TFeederMessage feederMessage, CancellationToken cancellationToken = default) : string` — String serialization
- `SerializeToBytes(TFeederMessage feederMessage, CancellationToken cancellationToken = default) : byte[]` — Byte array serialization

**Constructors**:
- `FeederMessageSerializer(TProviderConfiguration feederConfiguration)` — Initializes with configuration

**Thread Safety**: Thread-safe for read operations  
**Performance Notes**: NetJson offers best performance for high-throughput scenarios  

## Feeders SharedKernel

### Extensions

**Kind**: Static Class  
**Namespace**: RapidStreamer.Feeders.SharedKernel  

Extension methods for registering feeder components in dependency injection containers.

**Key Methods**:
- `AddChannelFeederResolver<TChannel, TFeeder, TFeederMessage, TFeederConfiguration>()` — Registers feeder resolver with factory function

**Thread Safety**: Registration methods are not thread-safe and should be called during application startup  

#### Usage Recipe

```csharp
services.AddChannelFeederResolver<MyChannel, MyFeeder, MyMessage, MyConfiguration>(
    (serviceProvider, channel, config, handler) => 
        new MyFeeder(channel, config, handler, serviceProvider));
```

## RapidStreamer Dependencies

| Package | Version | Description | Links |
|---------|---------|-------------|-------|
| RapidStreamer.BuildingBlocks.Application | 1.0.76+ | Core application building blocks | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| RapidStreamer.Application.Channels | 1.0.76+ | Channel abstractions for message routing | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| RapidStreamer.Application.Feeders | 1.0.76+ | Feeder abstractions and contracts | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |

The SharedKernel serves as the foundational layer that all messaging system implementations build upon, providing consistent interfaces and behavior across the entire RapidStreamer Feeviders ecosystem.

## See Also

- [../ActiveMQ/README.md](../ActiveMQ/README.md) - ActiveMQ messaging implementation
- [../Kafka/README.md](../Kafka/README.md) - Apache Kafka implementation  
- [../RabbitMQ/README.md](../RabbitMQ/README.md) - RabbitMQ messaging implementation
- [../WebSocket/README.md](../WebSocket/README.md) - WebSocket real-time communication
- [../WebApi/README.md](../WebApi/README.md) - HTTP REST API integration

[↑ Back to top](#contents)