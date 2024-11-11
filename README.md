# RapidStreamer NuGet Packages

RapidStreamer is a cutting-edge software solution designed to redefine real-time data streaming. Our mission is to provide effortless, blazingly fast, and cloud-native streaming capabilities for maximum impact. The library is organized into `Feeders`, `Feeviders` (combined Feeders and Providers), and `Providers`, offering flexible integration with messaging systems including ActiveMQ, Kafka, RabbitMQ, RedisPubSub, TCP, UDP, Web API, and WebSocket.

## Table of Contents
1. [Overview](#overview)
2. [Packages](#packages)
   - [ActiveMQ Packages](#activemq-packages) 
   - [Kafka Packages](#kafka-packages)
   - [RabbitMQ Packages](#rabbitmq-packages)
   - [RedisPubSub Packages](#redispubsub-packages)
   - [Shared Kernel Packages](#shared-kernel-packages)
   - [TcpSocket Packages](#tcpsocket-packages)
   - [UdpClient Packages](#udpclient-packages)
   - [Web API Packages](#web-api-packages)
   - [WebSocket Packages](#websocket-packages)
3. [Installation](#installation)
   - [NuGet CLI](#nuget-cli)
   - [Package Manager Console in Visual Studio](#package-manager-console-in-visual-studio)
4. [Usage](#usage)
   - [ActiveMQ Feeder Dependency Injection Integration](#activemq-feeder-dependency-injection-integration)
   - [ActiveMQ Provider Dependency Injection Integration](#activemq-provider-dependency-injection-integration)
   - [Kafka Feeder Dependency Injection Integration](#kafka-feeder-dependency-injection-integration)
   - [Kafka Provider Dependency Injection Integration](#kafka-provider-dependency-injection-integration)
   - [RabbitMQ Feeder Dependency Injection Integration](#rabbitmq-feeder-dependency-injection-integration)
   - [RabbitMQ Provider Dependency Injection Integration](#rabbitmq-provider-dependency-injection-integration)
   - [RedisPubSub Feeder Dependency Injection Integration](#redispubsub-feeder-dependency-injection-integration)
   - [RedisPubSub Provider Dependency Injection Integration](#redispubsub-provider-dependency-injection-integration)
   - [TcpSocket Feeder Dependency Injection Integration](#tcpsocket-feeder-dependency-injection-integration)
   - [TcpSocket Provider Dependency Injection Integration](#tcpsocket-provider-dependency-injection-integration)
   - [UdpClient Feeder Dependency Injection Integration](#udpclient-feeder-dependency-injection-integration)
   - [UdpClient Provider Dependency Injection Integration](#udpclient-provider-dependency-injection-integration)
   - [Web API Feeder Dependency Injection Integration](#web-api-feeder-dependency-injection-integration)
   - [Web API Provider Dependency Injection Integration](#web-api-provider-dependency-injection-integration)
   - [WebSocket Feeder Dependency Injection Integration](#websocket-feeder-dependency-injection-integration)
   - [WebSocket Provider Dependency Injection Integration](#websocket-provider-dependency-injection-integration)
5. [License](#license)

## Overview

RapidStreamer packages are categorized by functionality to provide specific integration points with different message brokers and transport protocols. Each package can work independently or integrate seamlessly with other RapidStreamer packages.

In RapidStreamer:
- **Feeders** are designed to consume messages.
- **Providers** produce messages for feeders.

## Packages

### ActiveMQ Packages
- **RapidStreamer.Feeders.ActiveMQ**: Consumes messages from ActiveMQ systems.
- **RapidStreamer.Feeviders.ActiveMQ.SharedKernel**: Provides shared utilities for integrating ActiveMQ feeders and providers.
- **RapidStreamer.Providers.DotNet.ActiveMQ**: Produces messages for ActiveMQ feeders.

### Kafka Packages
- **RapidStreamer.Feeders.Kafka**: Consumes messages from Kafka topics.
- **RapidStreamer.Providers.DotNet.Kafka**: Produces messages for Kafka feeders.

### RabbitMQ Packages
- **RapidStreamer.Feeders.RabbitMQ**: Consumes data streams from RabbitMQ.
- **RapidStreamer.Feeviders.RabbitMQ.SharedKernel**: Provides shared tools and utilities for RabbitMQ-based integrations.
- **RapidStreamer.Providers.DotNet.RabbitMQ**: Produces messages for RabbitMQ feeders.

### RedisPubSub Packages
- **RapidStreamer.Feeders.RedisPubSub**: Consumes messages from RedisPubSub channels.
- **RapidStreamer.Providers.DotNet.RedisPubSub**: Produces messages for RedisPubSub feeders.

### Shared Kernel Packages
- **RapidStreamer.Feeders.SharedKernel**: Common utilities and shared components for feeders in messaging applications.
- **RapidStreamer.Providers.DotNet.SharedKernel**: Common utilities for providers in .NET-based messaging applications.

### TcpSocket Packages
- **RapidStreamer.Feeders.TcpSocket**: Consumes data through TCP sockets.
- **RapidStreamer.Feeviders.TcpSocket.SharedKernel**: Shared utilities for TCP-based feeder and provider integrations.
- **RapidStreamer.Providers.DotNet.TcpSocket**: Produces messages for TCP socket feeders.

### UdpClient Packages
- **RapidStreamer.Feeders.UdpClient**: Consumes data over UDP connections.
- **RapidStreamer.Providers.DotNet.UdpClient**: Produces messages for UDP feeders.

### Web API Packages
- **RapidStreamer.Feeders.WebApi**: Consumes messages through Web API endpoints.
- **RapidStreamer.Providers.DotNet.WebApi**: Produces messages for Web API feeders.

### WebSocket Packages
- **RapidStreamer.Feeders.WebSocket**: Consumes data through WebSocket connections.
- **RapidStreamer.Providers.DotNet.WebSocket**: Produces messages for WebSocket feeders.

## Installation

The packages is hosted on GitHub Packages. To install it, add the GitHub package source configuration, then use the **NuGet Package Manager** in Visual Studio or the **dotnet CLI**.

### NuGet CLI

To install directly via the .NET CLI:
```bash
dotnet add package RapidStreamer.Feeders.ActiveMQ --version [Latest Version]
dotnet add package RapidStreamer.Feeviders.ActiveMQ.SharedKernel --version [Latest Version]
dotnet add package RapidStreamer.Providers.DotNet.ActiveMQ --version [Latest Version]
dotnet add package RapidStreamer.Feeders.Kafka --version [Latest Version]
dotnet add package RapidStreamer.Providers.DotNet.Kafka --version [Latest Version]
dotnet add package RapidStreamer.Feeders.RabbitMQ --version [Latest Version]
dotnet add package RapidStreamer.Feeviders.RabbitMQ.SharedKernel --version [Latest Version]
dotnet add package RapidStreamer.Providers.DotNet.RabbitMQ --version [Latest Version]
dotnet add package RapidStreamer.Feeders.RedisPubSub --version [Latest Version]
dotnet add package RapidStreamer.Providers.DotNet.RedisPubSub --version [Latest Version]
dotnet add package RapidStreamer.Feeders.SharedKernel --version [Latest Version]
dotnet add package RapidStreamer.Providers.DotNet.SharedKernel --version [Latest Version]
dotnet add package RapidStreamer.Feeders.TcpSocket --version [Latest Version]
dotnet add package RapidStreamer.Feeviders.TcpSocket.SharedKernel --version [Latest Version]
dotnet add package RapidStreamer.Providers.DotNet.TcpSocket --version [Latest Version]
dotnet add package RapidStreamer.Feeders.UdpClient --version [Latest Version]
dotnet add package RapidStreamer.Providers.DotNet.UdpClient --version [Latest Version]
dotnet add package RapidStreamer.Feeders.WebApi --version [Latest Version]
dotnet add package RapidStreamer.Providers.DotNet.WebApi --version [Latest Version]
dotnet add package RapidStreamer.Feeders.WebSocket --version [Latest Version]
dotnet add package RapidStreamer.Providers.DotNet.WebSocket --version [Latest Version]
```

### Package Manager Console in Visual Studio
```powershell
Install-Package RapidStreamer.Feeders.ActiveMQ --version [Latest Version]
Install-Package RapidStreamer.Feeviders.ActiveMQ.SharedKernel --version [Latest Version]
Install-Package RapidStreamer.Providers.DotNet.ActiveMQ --version [Latest Version]
Install-Package RapidStreamer.Feeders.Kafka --version [Latest Version]
Install-Package RapidStreamer.Providers.DotNet.Kafka --version [Latest Version]
Install-Package RapidStreamer.Feeders.RabbitMQ --version [Latest Version]
Install-Package RapidStreamer.Feeviders.RabbitMQ.SharedKernel --version [Latest Version]
Install-Package RapidStreamer.Providers.DotNet.RabbitMQ --version [Latest Version]
Install-Package RapidStreamer.Feeders.RedisPubSub --version [Latest Version]
Install-Package RapidStreamer.Providers.DotNet.RedisPubSub --version [Latest Version]
Install-Package RapidStreamer.Feeders.SharedKernel --version [Latest Version]
Install-Package RapidStreamer.Providers.DotNet.SharedKernel --version [Latest Version]
Install-Package RapidStreamer.Feeders.TcpSocket --version [Latest Version]
Install-Package RapidStreamer.Feeviders.TcpSocket.SharedKernel --version [Latest Version]
Install-Package RapidStreamer.Providers.DotNet.TcpSocket --version [Latest Version]
Install-Package RapidStreamer.Feeders.UdpClient --version [Latest Version]
Install-Package RapidStreamer.Providers.DotNet.UdpClient --version [Latest Version]
Install-Package RapidStreamer.Feeders.WebApi --version [Latest Version]
Install-Package RapidStreamer.Providers.DotNet.WebApi --version [Latest Version]
Install-Package RapidStreamer.Feeders.WebSocket --version [Latest Version]
Install-Package RapidStreamer.Providers.DotNet.WebSocket --version [Latest Version]
```

## Configuring GitHub as a NuGet Package Source

The packages is available from GitHub Packages. To enable this GitHub source in your project, add the GitHub Packages URL to your NuGet configuration.

1. **Edit NuGet.config**:
   Add the following GitHub package source to your `NuGet.config` file:

   ```xml
   <configuration>
     <packageSources>
       <add key="GitHub-KAB-TEAM" value="https://nuget.pkg.github.com/KAB-TEAM/index.json" />
     </packageSources>
   </configuration>
   ```

2. **Authentication**:
   - GitHub Packages requires authentication to access private repositories. Use your GitHub personal access token (PAT) as your password and GitHub username as the username.
   - Set up a GitHub PAT in your NuGet configuration to access the GitHub source:

   ```bash
   dotnet nuget add source https://nuget.pkg.github.com/KAB-TEAM/index.json -n GitHub-KAB-TEAM -u USERNAME -p TOKEN --store-password-in-clear-text
   ```
   Replace `USERNAME` with your GitHub username and `TOKEN` with a GitHub PAT with `read:packages` and `repo` scope.

3. **Verify the Configuration**:
   After adding the source, confirm that the GitHub source is listed with:
   ```bash
   dotnet nuget list source
   ```
   
## Usage

Refer to each package’s documentation for setup and usage instructions. Each package contains platform-specific configurations and API references.

---
### ActiveMQ Feeder Dependency Injection Integration

To integrate an ActiveMQ feeder in your application using Dependency Injection, use the following extension methods:

#### 1. `AddActiveMQFeeder`

Registers the ActiveMQ feeder configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

```csharp
public static IServiceCollection AddActiveMQFeeder<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TChannel : class, IChannel
    where TActiveMQFeederMessage : ActiveMQFeederMessage
    where TActiveMQFeederConfiguration : ActiveMQFeederConfiguration, new();
```

- **Parameters**:
   - `IServiceCollection services`: The DI service collection.
   - `IConfigurationRoot configuration`: The application's configuration root.
   - `string sectionName`: The name of the configuration section containing the feeder settings. 

- **Type Constraints**:
   - `TChannel`: The specific implementation of `IChannel`.
   - `TActiveMQFeederMessage`: A type derived from `ActiveMQFeederMessage`.
   - `TActiveMQFeederConfiguration`: A custom configuration derived from `ActiveMQFeederConfiguration`.

#### 2. `AddActiveMQFeederResolver`

Registers an ActiveMQ feeder resolver in the DI service collection, allowing for runtime resolution of ActiveMQ feeder dependencies.

```csharp
public static IServiceCollection AddActiveMQFeederResolver<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>
    (this IServiceCollection services)
    where TChannel : class, IChannel
    where TActiveMQFeederMessage : ActiveMQFeederMessage
    where TActiveMQFeederConfiguration : ActiveMQFeederConfiguration, new();
```

- **Parameters**:
   - `IServiceCollection services`: The DI service collection.

- **Type Constraints**:
   - `TChannel`: The specific implementation of `IChannel`.
   - `TActiveMQFeederMessage`: A type derived from `ActiveMQFeederMessage`.
   - `TActiveMQFeederConfiguration`: A custom configuration derived from `ActiveMQFeederConfiguration`.

#### 3. `UseActiveMQFeederResolver`

Configures the application to use the ActiveMQ feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IApplicationBuilder UseActiveMQFeederResolver<TChannel, TActiveMQFeederMessage, TActiveMQFeederConfiguration>
    (this IApplicationBuilder app, Guid channelKey, TActiveMQFeederConfiguration activeMQFeederConfiguration)
    where TChannel : class, IChannel
    where TActiveMQFeederMessage : ActiveMQFeederMessage
    where TActiveMQFeederConfiguration : ActiveMQFeederConfiguration;
```

- **Parameters**:
   - `IApplicationBuilder app`: The application's builder for configuring middleware.
   - `Guid channelKey`: A unique key for identifying the feeder's channel.
   - `TActiveMQFeederConfiguration activeMQFeederConfiguration`: A unique key for identifying the feeder's channel.

- **Type Constraints**:
   - `TChannel`: The specific implementation of `IChannel`.
   - `TActiveMQFeederMessage`: A type derived from `ActiveMQFeederMessage`.
   - `TActiveMQFeederConfiguration`: The feeder configuration instance.

### ActiveMQ Provider Dependency Injection Integration

Registers the ActiveMQ provider configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

Configures the application to use the ActiveMQ feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IServiceCollection AddActiveMQProvider<TActiveMQProviderMessage, TActiveMQProviderConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TActiveMQProviderMessage : ActiveMQProviderMessage
    where TActiveMQProviderConfiguration : ActiveMQProviderConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the provider settings.

- **Type Constraints**:
    - `TActiveMQProviderMessage`: A type derived from `ActiveMQProviderMessage`.
    - `TActiveMQProviderConfiguration`: A custom configuration derived from `ActiveMQProviderConfiguration`.

---
### Kafka Feeder Dependency Injection Integration

To integrate an Kafka feeder in your application using Dependency Injection, use the following extension methods:

#### 1. `AddKafkaFeeder`

Registers the Kafka feeder configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

```csharp
public static IServiceCollection AddKafkaFeeder<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TChannel : class, IChannel
    where TKafkaFeederMessage : KafkaFeederMessage
    where TKafkaFeederConfiguration : KafkaFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the feeder settings.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TKafkaFeederMessage`: A type derived from `KafkaFeederMessage`.
    - `TKafkaFeederConfiguration`: A custom configuration derived from `KafkaFeederConfiguration`.

#### 2. `AddKafkaFeederResolver`

Registers an Kafka feeder resolver in the DI service collection, allowing for runtime resolution of Kafka feeder dependencies.

```csharp
public static IServiceCollection AddKafkaFeederResolver<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>
    (this IServiceCollection services)
    where TChannel : class, IChannel
    where TKafkaFeederMessage : KafkaFeederMessage
    where TKafkaFeederConfiguration : KafkaFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TKafkaFeederMessage`: A type derived from `KafkaFeederMessage`.
    - `TKafkaFeederConfiguration`: A custom configuration derived from `KafkaFeederConfiguration`.

#### 3. `UseKafkaFeederResolver`

Configures the application to use the Kafka feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IApplicationBuilder UseKafkaFeederResolver<TChannel, TKafkaFeederMessage, TKafkaFeederConfiguration>
    (this IApplicationBuilder app, Guid channelKey, TKafkaFeederConfiguration kafkaFeederConfiguration)
    where TChannel : class, IChannel
    where TKafkaFeederMessage : KafkaFeederMessage
    where TKafkaFeederConfiguration : KafkaFeederConfiguration;
```

- **Parameters**:
    - `IApplicationBuilder app`: The application's builder for configuring middleware.
    - `Guid channelKey`: A unique key for identifying the feeder's channel.
    - `TKafkaFeederConfiguration kafkaFeederConfiguration`: A unique key for identifying the feeder's channel.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TKafkaFeederMessage`: A type derived from `KafkaFeederMessage`.
    - `TKafkaFeederConfiguration`: The feeder configuration instance.

### Kafka Provider Dependency Injection Integration

Registers the Kafka provider configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

Configures the application to use the Kafka feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IServiceCollection AddKafkaProvider<TKafkaProviderMessage, TKafkaProviderConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TKafkaProviderMessage : KafkaProviderMessage
    where TKafkaProviderConfiguration : KafkaProviderConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the provider settings.

- **Type Constraints**:
    - `TKafkaProviderMessage`: A type derived from `KafkaProviderMessage`.
    - `TKafkaProviderConfiguration`: A custom configuration derived from `KafkaProviderConfiguration`.

---
### RabbitMQ Feeder Dependency Injection Integration

To integrate an RabbitMQ feeder in your application using Dependency Injection, use the following extension methods:

#### 1. `AddRabbitMQFeeder`

Registers the RabbitMQ feeder configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

```csharp
public static IServiceCollection AddRabbitMQFeeder<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TChannel : class, IChannel
    where TRabbitMQFeederMessage : RabbitMQFeederMessage
    where TRabbitMQFeederConfiguration : RabbitMQFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the feeder settings.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TRabbitMQFeederMessage`: A type derived from `RabbitMQFeederMessage`.
    - `TRabbitMQFeederConfiguration`: A custom configuration derived from `RabbitMQFeederConfiguration`.

#### 2. `AddRabbitMQFeederResolver`

Registers an RabbitMQ feeder resolver in the DI service collection, allowing for runtime resolution of RabbitMQ feeder dependencies.

```csharp
public static IServiceCollection AddRabbitMQFeederResolver<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>
    (this IServiceCollection services)
    where TChannel : class, IChannel
    where TRabbitMQFeederMessage : RabbitMQFeederMessage
    where TRabbitMQFeederConfiguration : RabbitMQFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TRabbitMQFeederMessage`: A type derived from `RabbitMQFeederMessage`.
    - `TRabbitMQFeederConfiguration`: A custom configuration derived from `RabbitMQFeederConfiguration`.

#### 3. `UseRabbitMQFeederResolver`

Configures the application to use the RabbitMQ feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IApplicationBuilder UseRabbitMQFeederResolver<TChannel, TRabbitMQFeederMessage, TRabbitMQFeederConfiguration>
    (this IApplicationBuilder app, Guid channelKey, TRabbitMQFeederConfiguration rabbitMQFeederConfiguration)
    where TChannel : class, IChannel
    where TRabbitMQFeederMessage : RabbitMQFeederMessage
    where TRabbitMQFeederConfiguration : RabbitMQFeederConfiguration;
```

- **Parameters**:
    - `IApplicationBuilder app`: The application's builder for configuring middleware.
    - `Guid channelKey`: A unique key for identifying the feeder's channel.
    - `TRabbitMQFeederConfiguration rabbitMQFeederConfiguration`: A unique key for identifying the feeder's channel.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TRabbitMQFeederMessage`: A type derived from `RabbitMQFeederMessage`.
    - `TRabbitMQFeederConfiguration`: The feeder configuration instance.

### RabbitMQ Provider Dependency Injection Integration

Registers the RabbitMQ provider configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

Configures the application to use the RabbitMQ feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IServiceCollection AddRabbitMQProvider<TRabbitMQProviderMessage, TRabbitMQProviderConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TRabbitMQProviderMessage : RabbitMQProviderMessage
    where TRabbitMQProviderConfiguration : RabbitMQProviderConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the provider settings.

- **Type Constraints**:
    - `TRabbitMQProviderMessage`: A type derived from `RabbitMQProviderMessage`.
    - `TRabbitMQProviderConfiguration`: A custom configuration derived from `RabbitMQProviderConfiguration`.

---
### RedisPubSub Feeder Dependency Injection Integration

To integrate an RedisPubSub feeder in your application using Dependency Injection, use the following extension methods:

#### 1. `AddRedisPubSubFeeder`

Registers the RedisPubSub feeder configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

```csharp
public static IServiceCollection AddRedisPubSubFeeder<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TChannel : class, IChannel
    where TRedisPubSubFeederMessage : RedisPubSubFeederMessage
    where TRedisPubSubFeederConfiguration : RedisPubSubFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the feeder settings.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TRedisPubSubFeederMessage`: A type derived from `RedisPubSubFeederMessage`.
    - `TRedisPubSubFeederConfiguration`: A custom configuration derived from `RedisPubSubFeederConfiguration`.

#### 2. `AddRedisPubSubFeederResolver`

Registers an RedisPubSub feeder resolver in the DI service collection, allowing for runtime resolution of RedisPubSub feeder dependencies.

```csharp
public static IServiceCollection AddRedisPubSubFeederResolver<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>
    (this IServiceCollection services)
    where TChannel : class, IChannel
    where TRedisPubSubFeederMessage : RedisPubSubFeederMessage
    where TRedisPubSubFeederConfiguration : RedisPubSubFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TRedisPubSubFeederMessage`: A type derived from `RedisPubSubFeederMessage`.
    - `TRedisPubSubFeederConfiguration`: A custom configuration derived from `RedisPubSubFeederConfiguration`.

#### 3. `UseRedisPubSubFeederResolver`

Configures the application to use the RedisPubSub feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IApplicationBuilder UseRedisPubSubFeederResolver<TChannel, TRedisPubSubFeederMessage, TRedisPubSubFeederConfiguration>
    (this IApplicationBuilder app, Guid channelKey, TRedisPubSubFeederConfiguration redisPubSubFeederConfiguration)
    where TChannel : class, IChannel
    where TRedisPubSubFeederMessage : RedisPubSubFeederMessage
    where TRedisPubSubFeederConfiguration : RedisPubSubFeederConfiguration;
```

- **Parameters**:
    - `IApplicationBuilder app`: The application's builder for configuring middleware.
    - `Guid channelKey`: A unique key for identifying the feeder's channel.
    - `TRedisPubSubFeederConfiguration redisPubSubFeederConfiguration`: A unique key for identifying the feeder's channel.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TRedisPubSubFeederMessage`: A type derived from `RedisPubSubFeederMessage`.
    - `TRedisPubSubFeederConfiguration`: The feeder configuration instance.

### RedisPubSub Provider Dependency Injection Integration

Registers the RedisPubSub provider configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

Configures the application to use the RedisPubSub feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IServiceCollection AddRedisPubSubProvider<TRedisPubSubProviderMessage, TRedisPubSubProviderConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TRedisPubSubProviderMessage : RedisPubSubProviderMessage
    where TRedisPubSubProviderConfiguration : RedisPubSubProviderConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the provider settings.

- **Type Constraints**:
    - `TRedisPubSubProviderMessage`: A type derived from `RedisPubSubProviderMessage`.
    - `TRedisPubSubProviderConfiguration`: A custom configuration derived from `RedisPubSubProviderConfiguration`.


---
### TcpSocket Feeder Dependency Injection Integration

To integrate an TcpSocket feeder in your application using Dependency Injection, use the following extension methods:

#### 1. `AddTcpSocketFeeder`

Registers the TcpSocket feeder configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

```csharp
public static IServiceCollection AddTcpSocketFeeder<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TChannel : class, IChannel
    where TTcpSocketFeederMessage : TcpSocketFeederMessage
    where TTcpSocketFeederConfiguration : TcpSocketFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the feeder settings.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TTcpSocketFeederMessage`: A type derived from `TcpSocketFeederMessage`.
    - `TTcpSocketFeederConfiguration`: A custom configuration derived from `TcpSocketFeederConfiguration`.

#### 2. `AddTcpSocketFeederResolver`

Registers an TcpSocket feeder resolver in the DI service collection, allowing for runtime resolution of TcpSocket feeder dependencies.

```csharp
public static IServiceCollection AddTcpSocketFeederResolver<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>
    (this IServiceCollection services)
    where TChannel : class, IChannel
    where TTcpSocketFeederMessage : TcpSocketFeederMessage
    where TTcpSocketFeederConfiguration : TcpSocketFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TTcpSocketFeederMessage`: A type derived from `TcpSocketFeederMessage`.
    - `TTcpSocketFeederConfiguration`: A custom configuration derived from `TcpSocketFeederConfiguration`.

#### 3. `UseTcpSocketFeederResolver`

Configures the application to use the TcpSocket feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IApplicationBuilder UseTcpSocketFeederResolver<TChannel, TTcpSocketFeederMessage, TTcpSocketFeederConfiguration>
    (this IApplicationBuilder app, Guid channelKey, TTcpSocketFeederConfiguration tcpSocketFeederConfiguration)
    where TChannel : class, IChannel
    where TTcpSocketFeederMessage : TcpSocketFeederMessage
    where TTcpSocketFeederConfiguration : TcpSocketFeederConfiguration;
```

- **Parameters**:
    - `IApplicationBuilder app`: The application's builder for configuring middleware.
    - `Guid channelKey`: A unique key for identifying the feeder's channel.
    - `TTcpSocketFeederConfiguration tcpSocketFeederConfiguration`: A unique key for identifying the feeder's channel.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TTcpSocketFeederMessage`: A type derived from `TcpSocketFeederMessage`.
    - `TTcpSocketFeederConfiguration`: The feeder configuration instance.

### TcpSocket Provider Dependency Injection Integration

Registers the TcpSocket provider configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

Configures the application to use the TcpSocket feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IServiceCollection AddTcpSocketProvider<TTcpSocketProviderMessage, TTcpSocketProviderConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TTcpSocketProviderMessage : TcpSocketProviderMessage
    where TTcpSocketProviderConfiguration : TcpSocketProviderConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the provider settings.

- **Type Constraints**:
    - `TTcpSocketProviderMessage`: A type derived from `TcpSocketProviderMessage`.
    - `TTcpSocketProviderConfiguration`: A custom configuration derived from `TcpSocketProviderConfiguration`.

---
### UdpClient Feeder Dependency Injection Integration

To integrate an UdpClient feeder in your application using Dependency Injection, use the following extension methods:

#### 1. `AddUdpClientFeeder`

Registers the UdpClient feeder configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

```csharp
public static IServiceCollection AddUdpClientFeeder<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TChannel : class, IChannel
    where TUdpClientFeederMessage : UdpClientFeederMessage
    where TUdpClientFeederConfiguration : UdpClientFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the feeder settings.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TUdpClientFeederMessage`: A type derived from `UdpClientFeederMessage`.
    - `TUdpClientFeederConfiguration`: A custom configuration derived from `UdpClientFeederConfiguration`.

#### 2. `AddUdpClientFeederResolver`

Registers an UdpClient feeder resolver in the DI service collection, allowing for runtime resolution of UdpClient feeder dependencies.

```csharp
public static IServiceCollection AddUdpClientFeederResolver<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>
    (this IServiceCollection services)
    where TChannel : class, IChannel
    where TUdpClientFeederMessage : UdpClientFeederMessage
    where TUdpClientFeederConfiguration : UdpClientFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TUdpClientFeederMessage`: A type derived from `UdpClientFeederMessage`.
    - `TUdpClientFeederConfiguration`: A custom configuration derived from `UdpClientFeederConfiguration`.

#### 3. `UseUdpClientFeederResolver`

Configures the application to use the UdpClient feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IApplicationBuilder UseUdpClientFeederResolver<TChannel, TUdpClientFeederMessage, TUdpClientFeederConfiguration>
    (this IApplicationBuilder app, Guid channelKey, TUdpClientFeederConfiguration udpClientFeederConfiguration)
    where TChannel : class, IChannel
    where TUdpClientFeederMessage : UdpClientFeederMessage
    where TUdpClientFeederConfiguration : UdpClientFeederConfiguration;
```

- **Parameters**:
    - `IApplicationBuilder app`: The application's builder for configuring middleware.
    - `Guid channelKey`: A unique key for identifying the feeder's channel.
    - `TUdpClientFeederConfiguration udpClientFeederConfiguration`: A unique key for identifying the feeder's channel.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TUdpClientFeederMessage`: A type derived from `UdpClientFeederMessage`.
    - `TUdpClientFeederConfiguration`: The feeder configuration instance.

### UdpClient Provider Dependency Injection Integration

Registers the UdpClient provider configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

Configures the application to use the UdpClient feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IServiceCollection AddUdpClientProvider<TUdpClientProviderMessage, TUdpClientProviderConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TUdpClientProviderMessage : UdpClientProviderMessage
    where TUdpClientProviderConfiguration : UdpClientProviderConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the provider settings.

- **Type Constraints**:
    - `TUdpClientProviderMessage`: A type derived from `UdpClientProviderMessage`.
    - `TUdpClientProviderConfiguration`: A custom configuration derived from `UdpClientProviderConfiguration`.


---
### Web API Feeder Dependency Injection Integration

To integrate an Web API feeder in your application using Dependency Injection, use the following extension methods:

#### 1. `AddWeb APIFeeder`

Registers the Web API feeder configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

```csharp
public static IServiceCollection AddWeb APIFeeder<TChannel, TWeb APIFeederMessage, TWeb APIFeederConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TChannel : class, IChannel
    where TWeb APIFeederMessage : Web APIFeederMessage
    where TWeb APIFeederConfiguration : Web APIFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the feeder settings.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TWeb APIFeederMessage`: A type derived from `Web APIFeederMessage`.
    - `TWeb APIFeederConfiguration`: A custom configuration derived from `Web APIFeederConfiguration`.

#### 2. `AddWeb APIFeederResolver`

Registers an Web API feeder resolver in the DI service collection, allowing for runtime resolution of Web API feeder dependencies.

```csharp
public static IServiceCollection AddWeb APIFeederResolver<TChannel, TWeb APIFeederMessage, TWeb APIFeederConfiguration>
    (this IServiceCollection services)
    where TChannel : class, IChannel
    where TWeb APIFeederMessage : Web APIFeederMessage
    where TWeb APIFeederConfiguration : Web APIFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TWeb APIFeederMessage`: A type derived from `Web APIFeederMessage`.
    - `TWeb APIFeederConfiguration`: A custom configuration derived from `Web APIFeederConfiguration`.

#### 3. `UseWeb APIFeederResolver`

Configures the application to use the Web API feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IApplicationBuilder UseWeb APIFeederResolver<TChannel, TWeb APIFeederMessage, TWeb APIFeederConfiguration>
    (this IApplicationBuilder app, Guid channelKey, TWeb APIFeederConfiguration web APIFeederConfiguration)
    where TChannel : class, IChannel
    where TWeb APIFeederMessage : Web APIFeederMessage
    where TWeb APIFeederConfiguration : Web APIFeederConfiguration;
```

- **Parameters**:
    - `IApplicationBuilder app`: The application's builder for configuring middleware.
    - `Guid channelKey`: A unique key for identifying the feeder's channel.
    - `TWeb APIFeederConfiguration web APIFeederConfiguration`: A unique key for identifying the feeder's channel.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TWeb APIFeederMessage`: A type derived from `Web APIFeederMessage`.
    - `TWeb APIFeederConfiguration`: The feeder configuration instance.

### Web API Provider Dependency Injection Integration

Registers the Web API provider configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

Configures the application to use the Web API feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IServiceCollection AddWeb APIProvider<TWeb APIProviderMessage, TWeb APIProviderConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TWeb APIProviderMessage : Web APIProviderMessage
    where TWeb APIProviderConfiguration : Web APIProviderConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the provider settings.

- **Type Constraints**:
    - `TWeb APIProviderMessage`: A type derived from `Web APIProviderMessage`.
    - `TWeb APIProviderConfiguration`: A custom configuration derived from `Web APIProviderConfiguration`.

---
### WebSocket Feeder Dependency Injection Integration

To integrate an WebSocket feeder in your application using Dependency Injection, use the following extension methods:

#### 1. `AddWebSocketFeeder`

Registers the WebSocket feeder configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

```csharp
public static IServiceCollection AddWebSocketFeeder<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TChannel : class, IChannel
    where TWebSocketFeederMessage : WebSocketFeederMessage
    where TWebSocketFeederConfiguration : WebSocketFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the feeder settings.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TWebSocketFeederMessage`: A type derived from `WebSocketFeederMessage`.
    - `TWebSocketFeederConfiguration`: A custom configuration derived from `WebSocketFeederConfiguration`.

#### 2. `AddWebSocketFeederResolver`

Registers an WebSocket feeder resolver in the DI service collection, allowing for runtime resolution of WebSocket feeder dependencies.

```csharp
public static IServiceCollection AddWebSocketFeederResolver<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>
    (this IServiceCollection services)
    where TChannel : class, IChannel
    where TWebSocketFeederMessage : WebSocketFeederMessage
    where TWebSocketFeederConfiguration : WebSocketFeederConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TWebSocketFeederMessage`: A type derived from `WebSocketFeederMessage`.
    - `TWebSocketFeederConfiguration`: A custom configuration derived from `WebSocketFeederConfiguration`.

#### 3. `UseWebSocketFeederResolver`

Configures the application to use the WebSocket feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IApplicationBuilder UseWebSocketFeederResolver<TChannel, TWebSocketFeederMessage, TWebSocketFeederConfiguration>
    (this IApplicationBuilder app, Guid channelKey, TWebSocketFeederConfiguration webSocketFeederConfiguration)
    where TChannel : class, IChannel
    where TWebSocketFeederMessage : WebSocketFeederMessage
    where TWebSocketFeederConfiguration : WebSocketFeederConfiguration;
```

- **Parameters**:
    - `IApplicationBuilder app`: The application's builder for configuring middleware.
    - `Guid channelKey`: A unique key for identifying the feeder's channel.
    - `TWebSocketFeederConfiguration webSocketFeederConfiguration`: A unique key for identifying the feeder's channel.

- **Type Constraints**:
    - `TChannel`: The specific implementation of `IChannel`.
    - `TWebSocketFeederMessage`: A type derived from `WebSocketFeederMessage`.
    - `TWebSocketFeederConfiguration`: The feeder configuration instance.

### WebSocket Provider Dependency Injection Integration

Registers the WebSocket provider configuration in the service collection. This configuration reads from a specific section in the `IConfigurationRoot` for streamlined setup.

Configures the application to use the WebSocket feeder resolver, enabling it to resolve dependencies and manage the feeder's lifecycle.

```csharp
public static IServiceCollection AddWebSocketProvider<TWebSocketProviderMessage, TWebSocketProviderConfiguration>
    (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
    where TWebSocketProviderMessage : WebSocketProviderMessage
    where TWebSocketProviderConfiguration : WebSocketProviderConfiguration, new();
```

- **Parameters**:
    - `IServiceCollection services`: The DI service collection.
    - `IConfigurationRoot configuration`: The application's configuration root.
    - `string sectionName`: The name of the configuration section containing the provider settings.

- **Type Constraints**:
    - `TWebSocketProviderMessage`: A type derived from `WebSocketProviderMessage`.
    - `TWebSocketProviderConfiguration`: A custom configuration derived from `WebSocketProviderConfiguration`.


## License

This project is licensed under the MIT License.

---

© 2024 RapidStreamer. All rights reserved.

