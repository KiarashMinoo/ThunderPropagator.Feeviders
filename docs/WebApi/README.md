# WebApi Messaging System

## Contents

- [Overview](#overview)
- [Files](#files)
- [Types & Members](#types--members)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Resilience & Reliability](#resilience--reliability)
- [Performance Notes](#performance-notes)
- [Usage Examples](#usage-examples)
- [RapidStreamer Dependencies](#rapidstreamer-dependencies)
- [See Also](#see-also)

[↑ Back to top](#contents)

## Overview

The WebApi messaging system provides HTTP/REST-based communication capabilities for the RapidStreamer framework. It enables reliable web-based messaging through standard HTTP protocols, supporting both server-side (Feeder) for receiving HTTP requests and client-side (Provider) for sending HTTP requests with comprehensive resilience patterns.

The implementation features enterprise-grade reliability with circuit breakers, retry policies, timeout handling, and automatic decompression. It's ideal for microservices communication, webhook processing, RESTful API integration, and scenarios requiring HTTP-based messaging with strong resilience guarantees.

**Key Features:**
- RESTful HTTP/HTTPS communication with standard verbs
- Comprehensive resilience patterns (Circuit Breaker, Retry, Timeout)
- Automatic request/response compression (GZip, Deflate)
- Configurable retry strategies with exponential backoff
- Health monitoring and OpenTelemetry integration
- ASP.NET Core endpoint integration for feeders
- HttpClient factory integration for providers
- JSON/XML content type support

[↑ Back to top](#contents)

## Files

| File | Primary Type(s) | LOC (approx) | Responsibility |
|------|----------------|--------------|----------------|
| **Feeder Components** |
| `WebApiFeeder.cs` | `WebApiFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>` | 35 | HTTP endpoint for receiving REST API calls |
| `WebApiFeederConfiguration.cs` | `WebApiFeederConfiguration` | 12 | Server-side configuration with endpoint path |
| `WebApiFeederMessage.cs` | `WebApiFeederMessage` | 5 | Base message type for HTTP feeder messages |
| `WebApiFeederExtensions.cs` | `WebApiFeederExtensions` | 80 | DI registration and ASP.NET Core integration |
| **Provider Components** |
| `WebApiProvider.cs` | `WebApiProvider<TWebApiProviderMessage, TWebApiProviderConfiguration>` | 45 | HTTP client for outbound REST API calls |
| `WebApiProviderConfiguration.cs` | `WebApiProviderConfiguration` | 70 | Client-side configuration with resilience settings |
| `WebApiProviderMessage.cs` | `WebApiProviderMessage` | 5 | Base message type for HTTP provider messages |
| `WebApiProviderExtensions.cs` | `WebApiProviderExtensions` | 65 | DI registration with HttpClient factory |

[↑ Back to top](#contents)

## Types & Members

| Type | Kind | Summary | Inherits/Implements | Key Members |
|------|------|---------|---------------------|-------------|
| `WebApiFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>` | Class | HTTP endpoint handler for receiving API calls | `DelegativeFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>`, `IFeature` | EnqueueAsync |
| `WebApiFeederConfiguration` | Abstract Class | Server-side HTTP endpoint configuration | `AbstractFeederConfiguration` | Path |
| `WebApiFeederMessage` | Abstract Class | Base message type for HTTP feeders | `FeederMessage` | Inherited message properties |
| `WebApiProvider<TWebApiProviderMessage, TWebApiProviderConfiguration>` | Class | HTTP client for outbound API calls | `AbstractProvider<TWebApiProviderMessage, TWebApiProviderConfiguration>` | InternalExecuteAsync |
| `WebApiProviderConfiguration` | Abstract Class | Client-side HTTP configuration with resilience | `AbstractProviderConfiguration` | BaseAddress, Path, retry settings |
| `WebApiProviderMessage` | Abstract Class | Base message type for HTTP providers | `FeederMessage` | Inherited message properties |
| `WebApiFeederExtensions` | Static Class | DI registration and ASP.NET Core integration | N/A | AddWebApiFeeder, UseWebApiFeeder |
| `WebApiProviderExtensions` | Static Class | DI registration with HttpClient factory | N/A | AddWebApiProvider |

[↑ Back to top](#contents)

## API Reference

### WebApiFeeder&lt;TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration&gt;

HTTP endpoint handler that integrates with ASP.NET Core to receive and process HTTP requests.

**Namespace:** `RapidStreamer.Feeders.WebApi`  
**Inherits:** `DelegativeFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>`  
**Implements:** `IFeature`  
**Attributes:** `[IsAvailableOnDemo]`

#### Key Properties
- `HealthName : string` — Health check identifier based on endpoint path
- `HealthTags : string[]` — Health monitoring tags including "WebApi" and path

#### Key Methods
- `EnqueueAsync(string rawMessage, CancellationToken) : ValueTask` — Processes incoming HTTP request body

#### Constructors
```csharp
public WebApiFeeder(
    TChannel channel,
    TWebApiFeederConfiguration webApiFeederConfiguration,
    IFeederHandler<TChannel, TWebApiFeederMessage> feederHandler,
    IServiceProvider serviceProvider)
```

#### ASP.NET Core Integration
- Integrates with ASP.NET Core routing system
- Automatically maps HTTP POST endpoints
- Supports JSON request body deserialization
- Built-in health monitoring integration

#### Thread Safety
- Thread-safe for concurrent HTTP request processing
- Uses ASP.NET Core's built-in request handling pipeline
- Proper exception handling and logging

#### Usage Recipe
```csharp
// Configure WebApi feeder for HTTP endpoints
services.AddWebApiFeeder<MyChannel, MyWebApiMessage, MyWebApiConfig>(
    configuration, "WebApiServer");

// Map HTTP endpoint in ASP.NET Core
app.UseWebApiFeeder<MyChannel, MyWebApiMessage, MyWebApiConfig>();
```

[↑ Back to top](#contents)

### WebApiFeederConfiguration

Server-side configuration for HTTP endpoint registration and request handling.

**Namespace:** `RapidStreamer.Feeders.WebApi`  
**Inherits:** `AbstractFeederConfiguration`

#### Key Properties
- `Path : string` — HTTP endpoint path for receiving requests (required)

#### Endpoint Registration
- Path supports ASP.NET Core route patterns
- Automatically registers as HTTP POST endpoint
- Integrates with ASP.NET Core middleware pipeline
- Supports standard HTTP status codes and responses

#### Content Types
- Accepts JSON request bodies by default
- Automatic content type negotiation
- Support for custom serialization formats
- Handles standard HTTP headers

#### Usage Recipe
```json
{
  "WebApiServer": {
    "Path": "/api/messages",
    "SerializerType": "Json"
  }
}
```

[↑ Back to top](#contents)

### WebApiProvider&lt;TWebApiProviderMessage, TWebApiProviderConfiguration&gt;

HTTP client implementation for sending REST API requests with comprehensive resilience patterns.

**Namespace:** `RapidStreamer.Providers.DotNet.WebApi`  
**Inherits:** `AbstractProvider<TWebApiProviderMessage, TWebApiProviderConfiguration>`

#### Key Properties
- `_httpClient : HttpClient` — Configured HttpClient with resilience policies
- `_webApiProviderConfiguration : TWebApiProviderConfiguration` — Configuration instance

#### Key Methods
- `InternalExecuteAsync(byte[] bytes, CancellationToken) : Task` — Sends HTTP POST request with byte payload
- `DisposeManagedResources() : void` — Properly disposes HttpClient resources

#### Constructors
```csharp
public WebApiProvider(
    HttpClient httpClient,
    TWebApiProviderConfiguration webApiProviderConfiguration,
    IServiceProvider serviceProvider)
```

#### HTTP Request Details
- Uses HTTP POST method for message sending
- Sets content as `ByteArrayContent`
- Automatic compression/decompression (GZip, Deflate)
- Configurable request timeout and retry policies

#### Resilience Features
- Circuit breaker pattern implementation
- Exponential backoff retry strategy
- Timeout handling for slow responses
- Automatic failure detection and recovery

#### Thread Safety
- Thread-safe for concurrent HTTP requests
- HttpClient factory managed lifecycle
- Proper resource disposal and cleanup

#### Usage Recipe
```csharp
// Configure WebApi provider for outbound requests
services.AddWebApiProvider<MyWebApiMessage, MyWebApiConfig>(
    configuration, "WebApiClient");

// Send HTTP request via provider
await webApiProvider.ExecuteAsync(myMessage, cancellationToken);
```

[↑ Back to top](#contents)

### WebApiProviderConfiguration

Client-side configuration for HTTP requests with comprehensive resilience and retry settings.

**Namespace:** `RapidStreamer.Providers.DotNet.WebApi`  
**Inherits:** `AbstractProviderConfiguration`

#### Key Properties
- `BaseAddress : string` — Target server base URL (required)
- `Path : string` — API endpoint path (required)

#### Retry Configuration
- `BackoffType : DelayBackoffType` — Backoff strategy (default: Exponential)
- `MaxRetryAttempts : int` — Maximum retry attempts (default: 3)
- `MaxDelay : int` — Maximum delay between retries in milliseconds (default: 3)
- `UseJitter : bool` — Enable jitter in retry delays (default: true)

#### Circuit Breaker Configuration
- `SamplingDuration : int` — Sampling period in seconds (default: 10)
- `FailureRatio : double` — Failure ratio threshold (default: 0.2)
- `MinimumThroughput : int` — Minimum requests for circuit breaker activation (default: 3)
- `CircuitBreakerRetryCount : int` — Circuit breaker retry attempts (default: 3)
- `CircuitBreakerDurationOfBreak : int` — Circuit breaker open duration (default: 3)

#### Timeout Configuration
- `RequestTimeout : int` — Individual request timeout in seconds (default: 20)

#### Validation Notes
- BaseAddress must be valid HTTP/HTTPS URL
- Path should start with "/" for proper URL composition
- Retry and circuit breaker settings affect fault tolerance
- Timeout values in seconds for configuration simplicity

#### Usage Recipe
```json
{
  "WebApiClient": {
    "BaseAddress": "https://api.example.com",
    "Path": "/api/v1/messages",
    "MaxRetryAttempts": 5,
    "MaxDelay": 10000,
    "BackoffType": "Exponential",
    "UseJitter": true,
    "FailureRatio": 0.3,
    "RequestTimeout": 30,
    "SerializerType": "Json"
  }
}
```

[↑ Back to top](#contents)

## Configuration

### Server Configuration (Feeder)

```json
{
  "WebApiServer": {
    "Path": "/api/webhook/messages",
    "SerializerType": "Json"
  }
}
```

### Client Configuration (Provider)

```json
{
  "WebApiClient": {
    "BaseAddress": "https://api.partner.com",
    "Path": "/api/v2/events",
    "MaxRetryAttempts": 3,
    "MaxDelay": 5000,
    "BackoffType": "Exponential",
    "UseJitter": true,
    "SamplingDuration": 30,
    "FailureRatio": 0.2,
    "MinimumThroughput": 5,
    "RequestTimeout": 20,
    "SerializerType": "Json"
  }
}
```

### High Availability Configuration

```json
{
  "WebApiHA": {
    "BaseAddress": "https://ha-api.example.com",
    "Path": "/api/critical/data",
    "MaxRetryAttempts": 10,
    "MaxDelay": 30000,
    "BackoffType": "Linear",
    "UseJitter": false,
    "SamplingDuration": 60,
    "FailureRatio": 0.1,
    "MinimumThroughput": 10,
    "CircuitBreakerRetryCount": 5,
    "CircuitBreakerDurationOfBreak": 60,
    "RequestTimeout": 60,
    "SerializerType": "NJson"
  }
}
```

### Development Configuration

```json
{
  "WebApiDev": {
    "BaseAddress": "http://localhost:5000",
    "Path": "/api/dev/test",
    "MaxRetryAttempts": 1,
    "MaxDelay": 1000,
    "BackoffType": "Constant",
    "UseJitter": false,
    "RequestTimeout": 5,
    "SerializerType": "Json"
  }
}
```

[↑ Back to top](#contents)

## Resilience & Reliability

### Retry Strategies

The WebApi provider implements sophisticated retry mechanisms:

**Exponential Backoff (Default):**
```json
{
  "BackoffType": "Exponential",
  "MaxRetryAttempts": 3,
  "MaxDelay": 10000,
  "UseJitter": true
}
```

**Linear Backoff:**
```json
{
  "BackoffType": "Linear",
  "MaxRetryAttempts": 5,
  "MaxDelay": 5000,
  "UseJitter": false
}
```

**Constant Delay:**
```json
{
  "BackoffType": "Constant",
  "MaxRetryAttempts": 2,
  "MaxDelay": 2000
}
```

### Circuit Breaker Pattern

Prevents cascade failures and allows systems to recover:

```csharp
// Circuit breaker configuration
builder.AddCircuitBreaker(new HttpCircuitBreakerStrategyOptions
{
    SamplingDuration = TimeSpan.FromSeconds(30),
    FailureRatio = 0.2,           // 20% failure rate triggers open
    MinimumThroughput = 5,        // Minimum requests to evaluate
    BreakDuration = TimeSpan.FromMinutes(1)
});
```

**Circuit Breaker States:**
- **Closed:** Normal operation, requests flow through
- **Open:** Circuit breaker active, requests fail fast
- **Half-Open:** Testing if service has recovered

### Timeout Handling

Multiple timeout layers for comprehensive protection:

```csharp
// Request-level timeout
builder.AddTimeout(TimeSpan.FromSeconds(20));

// HttpClient timeout (handled automatically)
client.Timeout = TimeSpan.FromSeconds(30);
```

### Error Handling

Automatic retry for specific HTTP status codes:

```csharp
ShouldHandle = static args => ValueTask.FromResult(args is
{
    Outcome.Result.StatusCode: 
        HttpStatusCode.RequestTimeout or 
        HttpStatusCode.TooManyRequests or
        HttpStatusCode.InternalServerError or
        HttpStatusCode.BadGateway or
        HttpStatusCode.ServiceUnavailable or
        HttpStatusCode.GatewayTimeout
});
```

[↑ Back to top](#contents)

## Performance Notes

### Throughput Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| **Peak Throughput** | 10K+ req/s | Depends on payload size and latency |
| **Latency** | 5-50ms | Network dependent, varies with distance |
| **Connection Limit** | 100+ concurrent | HttpClient pooling handles scaling |
| **Memory Usage** | ~2KB per request | Plus message payload size |

### Optimization Strategies

**HttpClient Configuration:**
```csharp
services.AddHttpClient<WebApiProvider>(client =>
{
    client.BaseAddress = new Uri(baseAddress);
    client.DefaultRequestHeaders.Add("User-Agent", "RapidStreamer/1.0");
    client.Timeout = TimeSpan.FromSeconds(30);
})
.ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
{
    AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
    MaxConnectionsPerServer = 100
});
```

**Compression Benefits:**
- GZip compression reduces payload by 60-80%
- Deflate provides alternative compression format
- Automatic content negotiation with server
- Improved bandwidth utilization

**Connection Pooling:**
- HttpClient factory manages connection reuse
- Automatic DNS refresh and connection lifecycle
- Reduced connection establishment overhead
- Better resource utilization

### Monitoring & Health Checks

```csharp
// Health check registration
services.AddHealthChecks()
    .AddCheck<WebApiFeederHealthCheck>("webapi_feeder")
    .AddHttpMessageHandler<WebApiProvider>("webapi_provider");

// Circuit breaker metrics
public class CircuitBreakerMetrics
{
    public int TotalRequests { get; set; }
    public int FailedRequests { get; set; }
    public double FailureRate => TotalRequests > 0 ? (double)FailedRequests / TotalRequests : 0;
    public CircuitBreakerState State { get; set; }
}
```

### Performance Comparison

| Pattern | Latency | Reliability | Complexity | Use Case |
|---------|---------|-------------|------------|----------|
| **Direct HTTP** | Low | Low | Simple | Development, trusted networks |
| **Retry Only** | Medium | Medium | Medium | Temporary network issues |
| **Circuit Breaker** | Variable | High | High | Fault-tolerant systems |
| **Full Resilience** | Variable | Very High | Complex | Mission-critical applications |

[↑ Back to top](#contents)

## Usage Examples

### Basic WebApi Server Setup

```csharp
// Message definition
public class MyWebApiMessage : WebApiFeederMessage
{
    public string Content { get; set; }
    public DateTime Timestamp { get; set; }
    public string Source { get; set; }
}

// Configuration
public class MyWebApiServerConfig : WebApiFeederConfiguration
{
    // Configuration loaded from appsettings.json
}

// Service registration
services.AddWebApiFeeder<MyChannel, MyWebApiMessage, MyWebApiServerConfig>(
    configuration, "WebApiServer");

// Endpoint mapping in ASP.NET Core
app.UseWebApiFeeder<MyChannel, MyWebApiMessage, MyWebApiServerConfig>();
```

### WebApi Client (Provider) Setup

```csharp
// Message definition
public class MyWebApiProviderMessage : WebApiProviderMessage
{
    public string EventType { get; set; }
    public object Data { get; set; }
    public string CorrelationId { get; set; }
}

// Configuration
public class MyWebApiClientConfig : WebApiProviderConfiguration
{
    // Configuration loaded from appsettings.json
}

// Service registration
services.AddWebApiProvider<MyWebApiProviderMessage, MyWebApiClientConfig>(
    configuration, "WebApiClient");

// Usage in service
public class MyApiService
{
    private readonly IProvider<MyWebApiProviderMessage> _webApiProvider;

    public async Task SendEventAsync(string eventType, object data)
    {
        var message = new MyWebApiProviderMessage
        {
            EventType = eventType,
            Data = data,
            CorrelationId = Guid.NewGuid().ToString()
        };
        
        await _webApiProvider.ExecuteAsync(message);
    }
}
```

### Webhook Processing

```csharp
// Webhook message with validation
public class WebhookMessage : WebApiFeederMessage
{
    public string WebhookId { get; set; }
    public string EventType { get; set; }
    public DateTime EventTime { get; set; }
    public string Signature { get; set; }
    public JObject Payload { get; set; }
}

// Webhook configuration
public class WebhookConfig : WebApiFeederConfiguration
{
    public override string Path => "/webhooks/github";
}

// Webhook handler with validation
public class WebhookHandler : IFeederHandler<WebhookChannel, WebhookMessage>
{
    private readonly ILogger<WebhookHandler> _logger;
    private readonly IWebhookValidator _validator;

    public async Task<bool> HandleAsync(WebhookChannel channel, WebhookMessage message, CancellationToken cancellationToken = default)
    {
        // Validate webhook signature
        if (!_validator.ValidateSignature(message.Signature, message.Payload))
        {
            _logger.LogWarning("Invalid webhook signature for {WebhookId}", message.WebhookId);
            return false;
        }

        // Process webhook event
        await ProcessWebhookEventAsync(message.EventType, message.Payload);
        return true;
    }
}
```

### Microservices Communication

```csharp
// Service-to-service message
public class ServiceMessage : WebApiProviderMessage
{
    public string ServiceName { get; set; }
    public string Operation { get; set; }
    public Dictionary<string, object> Parameters { get; set; }
    public string RequestId { get; set; }
}

// High-reliability configuration
public class MicroserviceApiConfig : WebApiProviderConfiguration
{
    public override string BaseAddress => "https://service-mesh.internal";
    public override string Path => "/api/v1/operations";
    public override int MaxRetryAttempts => 5;
    public override DelayBackoffType BackoffType => DelayBackoffType.Exponential;
    public override bool UseJitter => true;
    public override double FailureRatio => 0.1; // Very sensitive to failures
    public override int RequestTimeout => 10;   // Fast timeout for responsiveness
}

// Service client with error handling
public class MicroserviceClient
{
    private readonly IProvider<ServiceMessage> _serviceProvider;
    private readonly ILogger<MicroserviceClient> _logger;

    public async Task<bool> CallServiceAsync(string operation, Dictionary<string, object> parameters)
    {
        try
        {
            var message = new ServiceMessage
            {
                ServiceName = "OrderProcessing",
                Operation = operation,
                Parameters = parameters,
                RequestId = Activity.Current?.Id ?? Guid.NewGuid().ToString()
            };

            await _serviceProvider.ExecuteAsync(message);
            return true;
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError(ex, "Failed to call microservice operation {Operation}", operation);
            return false;
        }
    }
}
```

### API Gateway Integration

```csharp
// API Gateway message with routing
public class GatewayMessage : WebApiProviderMessage
{
    public string TargetService { get; set; }
    public string Route { get; set; }
    public string Method { get; set; }
    public Dictionary<string, string> Headers { get; set; }
    public object Body { get; set; }
}

// Gateway configuration with load balancing
public class ApiGatewayConfig : WebApiProviderConfiguration
{
    public override string BaseAddress => "https://api-gateway.company.com";
    public override string Path => "/gateway/route";
    public override int MaxRetryAttempts => 3;
    public override int SamplingDuration => 60;
    public override int MinimumThroughput => 20;
    public override double FailureRatio => 0.15;
}

// Gateway service with routing logic
public class ApiGatewayService
{
    private readonly IProvider<GatewayMessage> _gatewayProvider;

    public async Task RouteRequestAsync(string service, string route, object payload)
    {
        var message = new GatewayMessage
        {
            TargetService = service,
            Route = route,
            Method = "POST",
            Headers = new Dictionary<string, string>
            {
                ["Content-Type"] = "application/json",
                ["X-Request-Source"] = "RapidStreamer",
                ["X-Timestamp"] = DateTimeOffset.UtcNow.ToString("O")
            },
            Body = payload
        };

        await _gatewayProvider.ExecuteAsync(message);
    }
}
```

### Custom ASP.NET Core Integration

```csharp
// Custom endpoint configuration
public class CustomWebApiConfig : WebApiFeederConfiguration
{
    public override string Path => "/api/custom/{id:int}";
}

// Extended endpoint mapping with custom logic
public static class CustomWebApiExtensions
{
    public static IEndpointRouteBuilder UseCustomWebApiFeeder<TChannel, TMessage, TConfig>(
        this IEndpointRouteBuilder builder)
        where TChannel : class, IChannel
        where TMessage : WebApiFeederMessage
        where TConfig : WebApiFeederConfiguration
    {
        builder.MapPost("/api/custom/{id:int}", async (
            [FromRoute] int id,
            [FromBody] string rawMessage,
            [FromServices] WebApiFeeder<TChannel, TMessage, TConfig> feeder,
            [FromServices] ILogger<WebApiFeeder<TChannel, TMessage, TConfig>> logger,
            CancellationToken cancellationToken) =>
        {
            logger.LogInformation("Received message for ID {Id}", id);
            
            // Custom processing logic
            var enrichedMessage = $"{{\"id\":{id},\"data\":{rawMessage}}}";
            
            await feeder.EnqueueAsync(enrichedMessage, cancellationToken);
            
            return Results.Ok(new { Status = "Processed", Id = id });
        });

        return builder;
    }
}
```

[↑ Back to top](#contents)

## RapidStreamer Dependencies

| Package | Version | Description | Links |
|---------|---------|-------------|-------|
| **RapidStreamer.Feeders.SharedKernel** | 1.0.78 | Core feeder abstractions and utilities | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| **RapidStreamer.Providers.DotNet.SharedKernel** | 1.0.78 | Core provider abstractions and utilities | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| **RapidStreamer.BuildingBlocks.Application** | 1.0.78 | Application building blocks and base classes | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |
| **RapidStreamer.Infrastructure.Extensions** | 1.0.78 | Infrastructure and hosting extensions | [GitHub Packages](https://nuget.pkg.github.com/KiarashMinoo/index.json) |

### Installation

```bash
# Add GitHub Packages source
dotnet nuget add source "https://nuget.pkg.github.com/KiarashMinoo/index.json" \
  --name "GitHub" --username YOUR_USERNAME --password YOUR_TOKEN

# Install WebApi packages
dotnet add package RapidStreamer.Feeders.WebApi
dotnet add package RapidStreamer.Providers.DotNet.WebApi
```

### Framework Dependencies

- **.NET 8.0** or **.NET 9.0**
- **Microsoft.AspNetCore.App** (for hosting and routing)
- **Microsoft.Extensions.Http** (HttpClient factory)
- **Microsoft.Extensions.Http.Resilience** (resilience patterns)
- **Polly** (circuit breaker and retry policies)
- **System.Net.Http** (HTTP communication)

### ASP.NET Core Integration

The WebApi feeder integrates seamlessly with ASP.NET Core:

```csharp
// Program.cs
var builder = WebApplication.CreateBuilder(args);

// Add services
builder.Services.AddWebApiFeeder<MyChannel, MyMessage, MyConfig>(
    builder.Configuration, "WebApi");

var app = builder.Build();

// Configure pipeline
app.UseRouting();
app.UseWebApiFeeder<MyChannel, MyMessage, MyConfig>();

app.Run();
```

[↑ Back to top](#contents)

## See Also

- **[SharedKernel](../SharedKernel/README.md)** - Core abstractions and base classes
- **[WebSocket](../WebSocket/README.md)** - WebSocket-based real-time communication
- **[RabbitMQ](../RabbitMQ/README.md)** - AMQP message broker integration
- **[Main Documentation](../README.md)** - Complete framework overview

---

**Framework Integration:** WebApi messaging system provides HTTP/REST-based communication within the RapidStreamer ecosystem, offering enterprise-grade reliability patterns for web-based messaging scenarios.

[↑ Back to top](#contents)