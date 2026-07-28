# CLAUDE.md

Guidance for working in this repository.

## Commands

```bash
dotnet restore    # fetches shared build configuration on first use
dotnet build
dotnet build -c Release
dotnet test
dotnet test <TestProject>
dotnet test --filter "FullyQualifiedName~<Name>"
dotnet clean      # also clears the downloaded shared build cache
```

## Architecture

Reusable libraries for real-time data streaming: message consumption ("feeders") and message publishing ("providers") across many external messaging systems, each behind the same pair of abstractions.

- A shared-kernel area defines the cross-transport abstractions: a feeder interface, a provider interface, a channel interface, and a dictionary-backed message base.
- Every transport is a sibling area with its own feeder project, provider project, and (where the client library needs shared plumbing) its own transport-scoped shared kernel building on the top-level one.

Consumption-side and publish-side projects are named to make the direction obvious at a glance; a transport's own shared kernel is named after that transport plus a shared-kernel suffix.

## Conventions

- All library projects multi-target the same three frameworks; solution configurations cover both architecture-neutral and architecture-specific platforms.
- Package versions are centrally managed; `Microsoft.Extensions.*` versions float per target framework — never pin a version on an individual package reference.
- Private fields `_camelCase`; telemetry activity names `{ClassName}_{MethodName}`; 4-space indent (2 for structured-data formats), LF endings, UTF-8, braces required.

## Adding a transport

New area under the transport root → its own feeder project implementing the feeder interface → its own provider project implementing the provider interface → a transport-scoped shared kernel only if the client library needs shared connection/serialization plumbing → unit tests referencing the new projects → an architecture-test row asserting the new transport's namespace stays isolated from every sibling transport.

## Testing

xUnit + NSubstitute + a fake-data generator library for test data. A separate architecture-test project enforces namespace/layer isolation between transports and between the shared kernel and any one transport. A load-test project and a small integration-test console project also exist; the unit-test project is the one referenced by every transport.

## Build & versioning

Version and target frameworks are centralized; CI bumps automatically. Restore fetches shared build configuration into a local, gitignored cache — a clean removes it, the next restore refetches it.

CI publishes on two branch patterns: a beta channel that bumps and publishes a prerelease on every push, and a release channel that finalizes the version and publishes a stable release.
