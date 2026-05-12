# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Restore (also downloads shared build props from ThunderPropagator.SharedBuild on GitHub)
dotnet restore

# Build
dotnet build
dotnet build -c Release

# Test (all test projects)
dotnet test

# Run a single test project
dotnet test Tests/UnitTests/ThunderPropagator.UnitTests/ThunderPropagator.UnitTests.csproj
dotnet test Tests/ThunderPropagator.ArchTests/ThunderPropagator.ArchTests.csproj

# Run a single test by name
dotnet test --filter "FullyQualifiedName~MyTestMethod"

# Clean (also removes .shared-props/ so next restore fetches fresh copies)
dotnet clean
```

## Shared Build Infrastructure

`Directory.Build.props` automatically downloads `Shared.Build.props` and `Shared.Nuget.props` from the [`ThunderPropagator.SharedBuild`](https://github.com/KiarashMinoo/ThunderPropagator.SharedBuild) repo on every `dotnet restore` or `dotnet build`. These files live in `.shared-props/` (gitignored). If the download fails (3 retries, 3s apart), the build errors with a clear message. `dotnet clean` removes `.shared-props/` entirely.

Package versions are managed centrally in `Directory.Packages.props` with `ManagePackageVersionsCentrally=true`. `Microsoft.Extensions.*` versions float per target framework (`8.*`, `9.*`, `10.*`). Never specify `Version=` on individual `<PackageReference>` items.

## Architecture

**Feeviders** = **Feed**ers + Prov**iders** — reusable .NET libraries for real-time data streaming across 11 messaging systems.

### Naming Convention

- `ThunderPropagator.Feeders.*` — message consumption (subscribe/receive side)
- `ThunderPropagator.Providers.DotNet.*` — message publishing (send side)
- `ThunderPropagator.Feeviders.*.SharedKernel` — shared base implementations for that transport

### Project Layout

```
Feeviders/
  SharedKernel/           # Cross-transport abstractions (IFeeder, IProvider, IChannel, FeederMessage)
  Kafka/                  # Feeders + Providers (Confluent.Kafka, Schema Registry, Avro/JSON serdes)
  RabbitMQ/               # Feeders + Providers + SharedKernel (AMQP)
  NATS/                   # Feeders + Providers + SharedKernel (JetStream)
  Pulsar/                 # Feeders + Providers + SharedKernel (DotPulsar)
  Mqtt/                   # Feeders + Providers + SharedKernel (MQTTnet v5)
  ActiveMQ/               # Feeders + Providers + SharedKernel (Apache.NMS)
  RedisPubSub/            # Feeders + Providers (StackExchange.Redis)
  WebSocket/              # Feeders + Providers
  WebApi/                 # Feeders + Providers (HTTP/REST)
  TcpSocket/              # Feeders + Providers + SharedKernel
  UdpClient/              # Feeders + Providers
Tests/
  ThunderPropagator.UnitTests/     # xunit + NSubstitute + Bogus; references all Feeviders projects
  ThunderPropagator.ArchTests/     # NetArchTest.Rules — enforces namespace/layer rules
  ThunderPropagator.Web.LoadTests/ # Load/performance tests
  DotNetClientTests/               # net10.0 console integration tests
```

### Core Abstractions (in `Feeviders.SharedKernel`)

- `IFeeder<TChannel>` — consumer abstraction
- `IProvider<T>` — publisher abstraction
- `IChannel` — channel abstraction
- `FeederMessage` — base message type

Each transport's SharedKernel builds on these; the Feeder/Provider projects in each transport folder depend on their SharedKernel.

### Multi-targeting

All library projects target `net8.0;net9.0;net10.0`. Solution configurations include `AnyCPU`, `ARM64`, `x64`, and `x86`.

## Testing Stack

- **xunit** v2.9.3 with `xunit.runner.visualstudio`
- **NSubstitute** v5.3.0 for mocking
- **Bogus** v35.6.5 for test data generation
- **NetArchTest.Rules** v1.3.2 for architecture enforcement
- **coverlet.collector** for coverage

## CI/CD

`.github/workflows/ci.yml` delegates to reusable workflows in `KiarashMinoo/.github`:
- `develop` branch → `reusable-beta-ci.yml`
- `release/**` branches → `reusable-release-ci.yml`

Required secrets: `GH_TOKEN`, `NUGET_API_KEY`.

## Code Style

Enforced via `.editorconfig`: 4-space indentation (2 for XML/JSON/YAML), LF line endings, UTF-8, `var` for obvious types, braces required, private fields prefixed `_camelCase`.