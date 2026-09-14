# CLAUDE.md

## Commands

```bash
dotnet restore    # fetches shared build config on first use
dotnet build
dotnet build -c Release
dotnet test
dotnet test <TestProject>
dotnet test --filter "FullyQualifiedName~<Name>"
dotnet clean      # also clears the downloaded shared build cache
```

## Architecture

Real-time data streaming: message consumption ("feeders") and publishing ("providers") across many external messaging systems, each behind the same pair of abstractions.

- Shared-kernel area: cross-transport feeder/provider/channel interfaces + dictionary-backed message base.
- Every transport is a sibling area with its own feeder project, provider project, and (if needed) its own transport-scoped shared kernel building on the top-level one.

Consumption/publish-side projects are named to make direction obvious; a transport's own shared kernel is named after that transport plus a shared-kernel suffix.

## Conventions

- All library projects multi-target the same three frameworks; solution configs cover architecture-neutral and architecture-specific platforms.
- Package versions centrally managed; `Microsoft.Extensions.*` floats per TFM — never pin per-reference.
- Private fields `_camelCase`; telemetry activity names `{ClassName}_{MethodName}`; braces required even for single-line blocks.

## Adding a Transport

New area under the transport root → feeder project (implements feeder interface) → provider project (implements provider interface) → transport-scoped shared kernel only if connection/serialization plumbing is needed → unit tests → architecture-test row asserting namespace isolation from every sibling transport.

## Testing

xUnit + NSubstitute + a fake-data generator library. Architecture-test project enforces namespace/layer isolation between transports and the shared kernel. A load-test project and a small integration-test console project also exist; the unit-test project is referenced by every transport.

## Build & Versioning

Version/TFMs centralized; CI bumps automatically. Restore fetches shared build config into a local, gitignored cache — `dotnet clean` removes it, next restore refetches.

CI: beta channel bumps + publishes a prerelease on every push; release channel finalizes the version and publishes a stable release.
