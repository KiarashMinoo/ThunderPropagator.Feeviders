# RapidStreamer.Providers

**RapidStreamer** is a cutting-edge software solution designed to redefine real-time data streaming. Our mission is to provide **effortless, blazingly fast, and cloud-native streaming capabilities** for maximum impact. This repository contains the foundational libraries, **RapidStreamer.Providers.DotNet.ActiveMQ**, **RapidStreamer.Providers.DotNet.Kafka**, **RapidStreamer.Providers.DotNet.Mqtt**, **RapidStreamer.Providers.DotNet.NATS**, **RapidStreamer.Providers.DotNet.Pulsar**, **RapidStreamer.Providers.DotNet.RabbitMQ**, **RapidStreamer.Providers.DotNet.RedisPubSub**, **RapidStreamer.Providers.DotNet.TcpSocket**, **RapidStreamer.Providers.DotNet.UdpClient**, **RapidStreamer.Providers.DotNet.WebApi** and **RapidStreamer.Providers.DotNet.WebSocket**, which empower developers to build scalable, high-performance streaming applications with ease.

These libraries support **.NET 9** and **.NET 8**, and are configured to work across multiple platforms, including **ARM64**, **x64**, **x86**, and **AnyCPU**. They are available as **NuGet packages** and can be installed from the custom NuGet repository:
**`https://nuget.rapidstreamer.com/v3/index.json`**.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Supported Platforms](#supported-platforms)
- [Installation](#installation)
- [License](#license)

---

## Overview

RapidStreamer is designed to revolutionize real-time data streaming by providing:

- **Effortless Integration**: Simple and intuitive APIs for seamless integration into your applications.
- **Blazingly Fast Performance**: Optimized for low-latency, high-throughput streaming.
- **Cloud-Native Architecture**: Built for modern cloud environments, enabling scalability and resilience.
- **Cross-Platform Support**: Compatible with ARM64, x64, x86, and AnyCPU platforms.

Whether you're building real-time analytics, live event processing, or IoT data pipelines, RapidStreamer empowers you to deliver maximum impact with minimal effort.

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
    <!-- Add the custom RapidStreamer NuGet repository -->
    <add key="RapidStreamer" value="https://nuget.rapidstreamer.com/v3/index.json" />
  </packageSources>
</configuration>
```

Place the `nuget.config` file in the root of your solution or project directory. This ensures that all projects in the solution can access the custom NuGet repository.

### Step 2: Verify the Configuration

To verify that the custom repository is correctly configured, you can use the following command in the terminal:
```bash
dotnet nuget list source
```
This will list all configured NuGet sources. You should see something like this in the output:
```text
Registered Sources:
  1.  nuget.org [Enabled]
      https://api.nuget.org/v3/index.json
  2.  RapidStreamer [Enabled]
      https://nuget.rapidstreamer.com/v3/index.json
```

### Step 3: Install the NuGet Packages
You can now install the packages using the following commands:

For `RapidStreamer.Providers.DotNet.ActiveMQ`:
```bash
dotnet add package RapidStreamer.Providers.DotNet.ActiveMQ
```

For `RapidStreamer.Providers.DotNet.Kafka`:
```bash
dotnet add package RapidStreamer.Providers.DotNet.Kafka
```

For `RapidStreamer.Providers.DotNet.Mqtt`:
```bash
dotnet add package RapidStreamer.Providers.DotNet.Mqtt
```

For `RapidStreamer.Providers.DotNet.NATS`:
```bash
dotnet add package RapidStreamer.Providers.DotNet.NATS
```

For `RapidStreamer.Providers.DotNet.Pulsar`:
```bash
dotnet add package RapidStreamer.Providers.DotNet.Pulsar
```

For `RapidStreamer.Providers.DotNet.RabbitMQ`:
```bash
dotnet add package RapidStreamer.Providers.DotNet.RabbitMQ
```

For `RapidStreamer.Providers.DotNet.RedisPubSub`:
```bash
dotnet add package RapidStreamer.Providers.DotNet.RedisPubSub
```

For `RapidStreamer.Providers.DotNet.TcpSocket`:
```bash
dotnet add package RapidStreamer.Providers.DotNet.TcpSocket
```

For `RapidStreamer.Providers.DotNet.UdpClient`:
```bash
dotnet add package RapidStreamer.Providers.DotNet.UdpClient
```

For `RapidStreamer.Providers.DotNet.WebApi`:
```bash
dotnet add package RapidStreamer.Providers.DotNet.WebApi
```

For `RapidStreamer.Providers.DotNet.WebSocket`:
```bash
dotnet add package RapidStreamer.Providers.DotNet.WebSocket
```

Alternatively, you can install the packages via the NuGet Package Manager in Visual Studio.

## License
This project is licensed under the **MIT License**.

© 2024 RapidStreamer. All rights reserved.