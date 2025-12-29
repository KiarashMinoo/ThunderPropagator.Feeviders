# ThunderPropagator Feeviders

**ThunderPropagator** is a cutting-edge software solution designed to redefine real-time data streaming. Our mission is to provide **effortless, blazingly fast, and cloud-native streaming capabilities** for maximum impact. This repository contains the foundational libraries for both **Feeders** (message consumption) and **Providers** (message publishing) across multiple messaging systems, which empower developers to build scalable, high-performance streaming applications with ease.

These libraries support **.NET 9** and **.NET 8**, and are configured to work across multiple platforms, including **ARM64**, **x64**, **x86**, and **AnyCPU**. They are available as **NuGet packages** from **GitHub Packages**.

---

## Table of Contents

- [Overview](#overview)
- [Documentation](#documentation)
- [Features](#features)
- [Supported Platforms](#supported-platforms)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [License](#license)

---

## Overview

ThunderPropagator is designed to revolutionize real-time data streaming by providing:

- **Effortless Integration**: Simple and intuitive APIs for seamless integration into your applications.
- **Blazingly Fast Performance**: Optimized for low-latency, high-throughput streaming.
- **Cloud-Native Architecture**: Built for modern cloud environments, enabling scalability and resilience.
- **Cross-Platform Support**: Compatible with ARM64, x64, x86, and AnyCPU platforms.
- **Multiple Messaging Systems**: Support for 12+ messaging systems including Kafka, RabbitMQ, WebSocket, and more.

Whether you're building real-time analytics, live event processing, or IoT data pipelines, ThunderPropagator empowers you to deliver maximum impact with minimal effort.

---

## Documentation

📖 **[Complete Documentation](docs/README.md)** - Comprehensive framework documentation with API references, diagrams, examples, and best practices.

### Documentation Catalog

This repository publishes generated documentation under [`/docs`](docs/README.md). The catalog below links to messaging systems and key components.

#### SharedKernel `Types:15` `Files:25` `Diagrams:✓`
Core abstractions and base implementations for feeders and providers.
- [Feeders.SharedKernel](docs/SharedKernel/Feeders.SharedKernel/README.md) `Types:8` `Files:12` `Diagrams:✓`
- [Providers.DotNet.SharedKernel](docs/SharedKernel/Providers.DotNet.SharedKernel/README.md) `Types:7` `Files:13` `Diagrams:✓`

#### Kafka `Types:12` `Files:18` `Diagrams:✓`
High-throughput event streaming with Confluent.Kafka, Schema Registry, and Avro support.
- [Feeders.Kafka](docs/Kafka/Feeders.Kafka/README.md) `Types:6` `Files:9` `Diagrams:✓`
- [Providers.DotNet.Kafka](docs/Kafka/Providers.DotNet.Kafka/README.md) `Types:6` `Files:9` `Diagrams:✓`

#### RabbitMQ `Types:15` `Files:24` `Diagrams:✓`
AMQP-based messaging with complex routing, exchanges, and queues.
- [Feeders.RabbitMQ](docs/RabbitMQ/Feeders.RabbitMQ/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Providers.DotNet.RabbitMQ](docs/RabbitMQ/Providers.DotNet.RabbitMQ/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Feeviders.RabbitMQ.SharedKernel](docs/RabbitMQ/Feeviders.RabbitMQ.SharedKernel/README.md) `Types:5` `Files:8` `Diagrams:✓`

#### NATS `Types:15` `Files:24` `Diagrams:✓`
Cloud-native messaging with JetStream persistence and key-value stores.
- [Feeders.NATS](docs/NATS/Feeders.NATS/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Providers.DotNet.NATS](docs/NATS/Providers.DotNet.NATS/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Feeviders.NATS.SharedKernel](docs/NATS/Feeviders.NATS.SharedKernel/README.md) `Types:5` `Files:8` `Diagrams:✓`

#### Pulsar `Types:15` `Files:24` `Diagrams:✓`
Multi-tenant pub-sub with geo-replication and tiered storage.
- [Feeders.Pulsar](docs/Pulsar/Feeders.Pulsar/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Providers.DotNet.Pulsar](docs/Pulsar/Providers.DotNet.Pulsar/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Feeviders.Pulsar.SharedKernel](docs/Pulsar/Feeviders.Pulsar.SharedKernel/README.md) `Types:5` `Files:8` `Diagrams:✓`

#### MQTT `Types:15` `Files:24` `Diagrams:✓`
Lightweight IoT messaging protocol with Quality of Service guarantees.
- [Feeders.Mqtt](docs/Mqtt/Feeders.Mqtt/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Providers.DotNet.Mqtt](docs/Mqtt/Providers.DotNet.Mqtt/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Feeviders.Mqtt.SharedKernel](docs/Mqtt/Feeviders.Mqtt.SharedKernel/README.md) `Types:5` `Files:8` `Diagrams:✓`

#### ActiveMQ `Types:15` `Files:24` `Diagrams:✓`
Apache ActiveMQ JMS messaging with enterprise integration patterns.
- [Feeders.ActiveMQ](docs/ActiveMQ/Feeders.ActiveMQ/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Providers.DotNet.ActiveMQ](docs/ActiveMQ/Providers.DotNet.ActiveMQ/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Feeviders.ActiveMQ.SharedKernel](docs/ActiveMQ/Feeviders.ActiveMQ.SharedKernel/README.md) `Types:5` `Files:8` `Diagrams:✓`

#### RedisPubSub `Types:10` `Files:14` `Diagrams:✓`
In-memory pub/sub for low-latency messaging and caching.
- [Feeders.RedisPubSub](docs/RedisPubSub/Feeders.RedisPubSub/README.md) `Types:5` `Files:7` `Diagrams:✓`
- [Providers.DotNet.RedisPubSub](docs/RedisPubSub/Providers.DotNet.RedisPubSub/README.md) `Types:5` `Files:7` `Diagrams:✓`

#### WebSocket `Types:10` `Files:14` `Diagrams:✓`
Real-time bidirectional web communication over persistent connections.
- [Feeders.WebSocket](docs/WebSocket/Feeders.WebSocket/README.md) `Types:5` `Files:7` `Diagrams:✓`
- [Providers.DotNet.WebSocket](docs/WebSocket/Providers.DotNet.WebSocket/README.md) `Types:5` `Files:7` `Diagrams:✓`

#### WebApi `Types:10` `Files:14` `Diagrams:✓`
HTTP/REST API consumption and publishing with resilience patterns.
- [Feeders.WebApi](docs/WebApi/Feeders.WebApi/README.md) `Types:5` `Files:7` `Diagrams:✓`
- [Providers.DotNet.WebApi](docs/WebApi/Providers.DotNet.WebApi/README.md) `Types:5` `Files:7` `Diagrams:✓`

#### TcpSocket `Types:15` `Files:24` `Diagrams:✓`
Low-level TCP socket protocol with custom framing and binary protocols.
- [Feeders.TcpSocket](docs/TcpSocket/Feeders.TcpSocket/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Providers.DotNet.TcpSocket](docs/TcpSocket/Providers.DotNet.TcpSocket/README.md) `Types:5` `Files:8` `Diagrams:✓`
- [Feeviders.TcpSocket.SharedKernel](docs/TcpSocket/Feeviders.TcpSocket.SharedKernel/README.md) `Types:5` `Files:8` `Diagrams:✓`

#### UdpClient `Types:10` `Files:14` `Diagrams:✓`
UDP datagram protocol for fire-and-forget messaging.
- [Feeders.UdpClient](docs/UdpClient/Feeders.UdpClient/README.md) `Types:5` `Files:7` `Diagrams:✓`
- [Providers.DotNet.UdpClient](docs/UdpClient/Providers.DotNet.UdpClient/README.md) `Types:5` `Files:7` `Diagrams:✓`

---

**Total**: 12 Systems | 30 Projects | 167+ Types | 270+ Files | 33+ Diagrams  
**Last generated**: December 29, 2025

### Quick Links

- [Architecture Overview](docs/README.md#architecture) - Framework architecture and components
- [Getting Started Guide](docs/README.md#quick-start) - Installation and basic usage
- [Performance Notes](docs/README.md#performance-notes) - Optimization recommendations

---

## Features

- **Cross-Platform Support**: Works seamlessly on ARM64, x64, x86, and AnyCPU platforms.
- **.NET Compatibility**: Fully compatible with .NET 9 and .NET 8.
- **Debug and Release Configurations**: Pre-configured for both debug and release builds.
- **High Performance**: Optimized for low-latency, high-throughput streaming.
- **Cloud-Native**: Designed for modern cloud environments with built-in scalability and resilience.
- **NuGet Packages**: Easily installable via a custom NuGet repository.

---

## Supported Platforms

The projects support the following platforms:

- **ARM64**
- **x64**
- **x86**
- **AnyCPU**

Both **Debug** and **Release** configurations are available for all platforms.

---

## Installation

### Step 1: Add the Custom NuGet Repository
To install the libraries as NuGet packages, you need to add the custom NuGet repository to your NuGet configuration.

#### Using Visual Studio:
1. Open Visual Studio.
2. Go to **Tools** > **NuGet Package Manager** > **Package Manager Settings**.
3. Under **Package Sources**, click the **+** button to add a new source.
4. Enter the following details:
  - **Name**: `ThunderPropagator`
  - **Source**: `https://nuget.thunderpropagator.com/v3/index.json`
5. Click **Update** and then **OK**.

#### Using the Command Line:
Add the NuGet source using the following command:
```bash
dotnet nuget add source --name ThunderPropagator --source https://nuget.thunderpropagator.com/v3/index.json
```

#### Create or Update `nuget.config`
If you don’t already have a `nuget.config` file in your project or solution directory, create one. If you do, update it to include the custom repository.

Here’s an example of what the `nuget.config` file should look like:
```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <!-- Add the official NuGet.org source -->
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
    <!-- Add the ThunderPropagator GitHub Packages repository -->
    <add key="ThunderPropagator" value="https://nuget.pkg.github.com/KiarashMinoo/index.json" />
  </packageSources>
</configuration>
```

Place the `nuget.config` file in the root of your solution or project directory. This ensures that all projects in the solution can access the GitHub Packages repository.

### Step 2: Verify the Configuration

To verify that the GitHub Packages repository is correctly configured, you can use the following command:
```bash
dotnet nuget list source
```

You should see output similar to:
```text
Registered Sources:
  1.  nuget.org [Enabled]
      https://api.nuget.org/v3/index.json
  2.  ThunderPropagator [Enabled]
      https://nuget.pkg.github.com/KiarashMinoo/index.json
```

### Step 3: Build and Restore Packages

After configuring the NuGet sources, restore and build your project:
```bash
dotnet restore
dotnet build -c Release
```

### Step 4: Install ThunderPropagator Packages

You can now install the messaging system packages you need. Examples:

**High-throughput streaming (Kafka)**:
```bash
dotnet add package ThunderPropagator.Feeders.Kafka
dotnet add package ThunderPropagator.Providers.DotNet.Kafka
```

**Reliable messaging (RabbitMQ)**:
```bash
dotnet add package ThunderPropagator.Feeders.RabbitMQ
dotnet add package ThunderPropagator.Providers.DotNet.RabbitMQ
```

**Real-time web communication (WebSocket)**:
```bash
dotnet add package ThunderPropagator.Feeders.WebSocket
dotnet add package ThunderPropagator.Providers.DotNet.WebSocket
```

**See [Documentation](docs/README.md#nuget-packages) for complete package listing.**

---

## Quick Start

### 1. Basic Message Producer

```csharp
// Define your message
public class OrderEvent : RabbitMQProviderMessage
{
    public string OrderId { get; set; }
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

// Define configuration
public class OrderConfig : RabbitMQProviderConfiguration { }

// Register in DI container
services.AddRabbitMQProvider<OrderEvent, OrderConfig>(
    configuration, "Messaging:RabbitMQ");

// Use in your service
public class OrderService
{
    private readonly IProvider<OrderEvent> _provider;
    
    public OrderService(IProvider<OrderEvent> provider)
    {
        _provider = provider;
    }
    
    public async Task ProcessOrderAsync(Order order)
    {
        await _provider.ExecuteAsync(new OrderEvent
        {
            OrderId = order.Id,
            Amount = order.Total,
            OrderDate = DateTime.UtcNow
        });
    }
}
```

### 2. Configuration Example

```json
{
  "Messaging": {
    "RabbitMQ": {
      "HostName": "localhost",
      "Port": 5672,
      "UserName": "guest",
      "Password": "guest",
      "ExchangeName": "orders",
      "QueueName": "order-processing",
      "RoutingKey": "order.created"
    }
  }
}
```

**📖 [View Complete Documentation](docs/README.md) for detailed examples, configuration options, and best practices.**

---

## License

This project is licensed under the **Apache-2.0 License**.

© 2024 ThunderPropagator Corporation. All rights reserved.