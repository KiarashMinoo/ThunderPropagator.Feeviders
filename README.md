# RapidStreamer Feeviders

**RapidStreamer** is a cutting-edge software solution designed to redefine real-time data streaming. Our mission is to provide **effortless, blazingly fast, and cloud-native streaming capabilities** for maximum impact. This repository contains the foundational libraries for both **Feeders** (message consumption) and **Providers** (message publishing) across multiple messaging systems, which empower developers to build scalable, high-performance streaming applications with ease.

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

RapidStreamer is designed to revolutionize real-time data streaming by providing:

- **Effortless Integration**: Simple and intuitive APIs for seamless integration into your applications.
- **Blazingly Fast Performance**: Optimized for low-latency, high-throughput streaming.
- **Cloud-Native Architecture**: Built for modern cloud environments, enabling scalability and resilience.
- **Cross-Platform Support**: Compatible with ARM64, x64, x86, and AnyCPU platforms.
- **Multiple Messaging Systems**: Support for 12+ messaging systems including Kafka, RabbitMQ, WebSocket, and more.

Whether you're building real-time analytics, live event processing, or IoT data pipelines, RapidStreamer empowers you to deliver maximum impact with minimal effort.

---

## Documentation

📖 **[Complete Documentation](docs/README.md)** - Comprehensive framework documentation with API references, examples, and best practices.

### Quick Links

- **[Architecture Overview](docs/README.md#architecture-components)** - Framework architecture and components
- **[SharedKernel](docs/SharedKernel/README.md)** - Core interfaces and base classes
- **[RabbitMQ Integration](docs/RabbitMQ/README.md)** - AMQP messaging with complex routing
- **[Kafka Integration](docs/Kafka/README.md)** - High-throughput streaming and event sourcing
- **[WebSocket Integration](docs/WebSocket/README.md)** - Real-time web communication
- **[Getting Started Guide](docs/README.md#getting-started)** - Installation and basic usage
- **[Performance Notes](docs/README.md#performance-notes)** - Optimization recommendations

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
  - **Name**: `RapidStreamer`
  - **Source**: `https://nuget.rapidstreamer.com/v3/index.json`
5. Click **Update** and then **OK**.

#### Using the Command Line:
Add the NuGet source using the following command:
```bash
dotnet nuget add source --name RapidStreamer --source https://nuget.rapidstreamer.com/v3/index.json
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
    <!-- Add the RapidStreamer GitHub Packages repository -->
    <add key="RapidStreamer" value="https://nuget.pkg.github.com/KiarashMinoo/index.json" />
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
  2.  RapidStreamer [Enabled]
      https://nuget.pkg.github.com/KiarashMinoo/index.json
```

### Step 3: Build and Restore Packages

After configuring the NuGet sources, restore and build your project:
```bash
dotnet restore
dotnet build -c Release
```

### Step 4: Install RapidStreamer Packages

You can now install the messaging system packages you need. Examples:

**High-throughput streaming (Kafka)**:
```bash
dotnet add package RapidStreamer.Feeders.Kafka
dotnet add package RapidStreamer.Providers.DotNet.Kafka
```

**Reliable messaging (RabbitMQ)**:
```bash
dotnet add package RapidStreamer.Feeders.RabbitMQ
dotnet add package RapidStreamer.Providers.DotNet.RabbitMQ
```

**Real-time web communication (WebSocket)**:
```bash
dotnet add package RapidStreamer.Feeders.WebSocket
dotnet add package RapidStreamer.Providers.DotNet.WebSocket
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

© 2024 RapidStreamer Corporation. All rights reserved.