# ThunderPropagator WebApi System

## Overview

The **ThunderPropagator WebApi** system provides comprehensive HTTP/REST API integration capabilities for .NET applications, enabling both message consumption (Feeders) and publishing (Providers) via RESTful web services. Built on the robust .NET HttpClient infrastructure with Polly resilience policies, this system offers enterprise-grade reliability for distributed HTTP communication.

WebApi implements RESTful architectural patterns, supporting all standard HTTP methods (GET, POST, PUT, DELETE, PATCH) with advanced features including:
- **Polly Resilience Policies**: Retry with exponential backoff, circuit breaker, timeout, bulkhead isolation, and hedging
- **Authentication**: Bearer tokens (JWT), Basic authentication, OAuth2 flows, API keys
- **Content Negotiation**: JSON, XML, form-urlencoded, multipart/form-data
- **Compression**: gzip, deflate, brotli for efficient payload transfer
- **HTTP/2 Support**: Multiplexing, header compression, server push

### Key Features

#### 🌐 HTTP Protocol Support
- **HTTP Methods**: GET (retrieve), POST (create), PUT (replace), DELETE (remove), PATCH (partial update)
- **HTTP Versions**: HTTP/1.1, HTTP/2, HTTP/3 (QUIC)
- **Content Types**: application/json, application/xml, application/x-www-form-urlencoded, multipart/form-data
- **Compression**: gzip (CPU-efficient), deflate, brotli (best compression ratio)

#### 🛡️ Polly Resilience Policies
- **Retry**: Exponential backoff with jitter for transient failures (500-503 errors, network timeouts)
- **Circuit Breaker**: Prevent cascading failures, open circuit after failure threshold
- **Timeout**: Per-request and total timeout policies (fail fast)
- **Bulkhead Isolation**: Limit concurrent requests to prevent resource exhaustion
- **Hedging**: Parallel requests for latency-sensitive operations

#### 🔐 Authentication Strategies
- **Bearer Tokens**: JWT (JSON Web Tokens) in Authorization header
- **Basic Authentication**: Base64-encoded username:password
- **OAuth2 Flows**: Client credentials, password grant, authorization code
- **API Keys**: Header-based or query parameter authentication

#### ⚡ Performance Optimizations
- **Connection Pooling**: SocketsHttpHandler reuses TCP connections
- **HTTP/2 Multiplexing**: Multiple requests over single connection
- **Keep-Alive**: Persistent connections reduce handshake overhead
- **Compression**: Reduce payload size (30-70% reduction typical)

## Architecture

The WebApi system follows a layered architecture with resilience built-in at every level:

```mermaid
graph TB
    subgraph Application["Application Layer"]
        App[Your Application]
        Feeder[WebApiFeeder<br/>Pull-based Polling]
        Provider[WebApiProvider<br/>Push-based Sending]
    end
    
    subgraph ResilienceLayer["Polly Resilience Layer"]
        Retry[Retry Policy<br/>Exponential Backoff + Jitter]
        CircuitBreaker[Circuit Breaker<br/>Failure Threshold]
        Timeout[Timeout Policy<br/>Per-Request / Total]
        Hedging[Hedging Policy<br/>Parallel Requests]
    end
    
    subgraph HttpLayer["HTTP Layer"]
        HttpClient[HttpClient<br/>SocketsHttpHandler]
        ConnPool[Connection Pool<br/>Keep-Alive]
        Compression[Compression<br/>gzip/deflate/brotli]
    end
    
    subgraph ExternalLayer["External Systems"]
        RestApi[REST API<br/>HTTP/1.1 or HTTP/2]
    end
    
    App -->|Consume Messages| Feeder
    App -->|Publish Messages| Provider
    
    Feeder --> Retry
    Provider --> Retry
    
    Retry --> CircuitBreaker
    CircuitBreaker --> Timeout
    Timeout --> Hedging
    
    Hedging --> HttpClient
    HttpClient --> ConnPool
    ConnPool --> Compression
    
    Compression -->|HTTP Request| RestApi
    RestApi -->|HTTP Response| Compression
    
    style App fill:#e1f5ff
    style Feeder fill:#fff4e1
    style Provider fill:#ffe1f5
    style Retry fill:#d4edda
    style CircuitBreaker fill:#f8d7da
    style Timeout fill:#fff3cd
    style Hedging fill:#d1ecf1
    style HttpClient fill:#e2e3e5
    style RestApi fill:#cfe2ff
```

### Component Responsibilities

#### WebApiFeeder (Message Consumer)
- **Pull-based polling** of REST APIs at configurable intervals
- **IterativeFeeder** pattern: Returns `IAsyncEnumerable<FeederReceivedMessage<T>>`
- **Polling strategies**: Fixed interval, exponential backoff, adaptive intervals
- **Pagination support**: Offset-based, cursor-based, Link header (RFC 5988)
- **Rate limiting**: Respect 429 responses and Retry-After headers
- **ETag caching**: Conditional requests (If-None-Match, 304 Not Modified)

#### WebApiProvider (Message Publisher)
- **Push-based HTTP requests** (POST, PUT, PATCH, DELETE)
- **AbstractProvider** pattern: Automatic serialization and resilience
- **Idempotency keys**: Prevent duplicate operations (Idempotency-Key header)
- **Content types**: JSON, XML, form-urlencoded, multipart (file uploads)
- **Request/response handling**: Status code validation, error mapping

#### Polly Resilience Pipeline
1. **Retry Policy**: Exponential backoff (2^n seconds) with jitter (±25% randomness)
2. **Circuit Breaker**: Open after N failures in M seconds, half-open test after recovery duration
3. **Timeout Policy**: Per-request timeout (e.g., 30s) or total operation timeout (e.g., 5 min)
4. **Hedging Policy**: Send parallel requests after primary delay, return first successful response

## Performance Characteristics

### Connection Pooling
- **SocketsHttpHandler**: Default handler with automatic connection pooling
- **Max Connections Per Server**: Configure via `MaxConnectionsPerServer` (default: unlimited)
- **Connection Lifetime**: Set `PooledConnectionLifetime` to force connection refresh
- **Connection Idle Timeout**: Set `PooledConnectionIdleTimeout` for cleanup

### HTTP/2 Multiplexing
- **Single Connection**: Multiple concurrent requests over one TCP connection
- **Header Compression**: HPACK compression reduces overhead (40-90% reduction)
- **Server Push**: Server proactively sends resources (reduce round trips)
- **Stream Prioritization**: Prioritize critical requests

### Compression
| Algorithm | Compression Ratio | CPU Cost | Speed | Use Case |
|-----------|-------------------|----------|-------|----------|
| **gzip** | ~60-70% | Medium | Fast | General purpose (default) |
| **deflate** | ~60-70% | Medium | Fast | Legacy systems |
| **brotli** | ~70-80% | High | Slower | Static content, CPU available |

### Latency Comparison
```
┌─────────────────────────────────────────────────────────┐
│  Protocol Latency Comparison (ms)                       │
├─────────────────────────────────────────────────────────┤
│  REST (HTTP/1.1 Keep-Alive)    │████████████ 45ms       │
│  REST (HTTP/2)                 │████████ 30ms            │
│  GraphQL (Batching)            │██████ 25ms              │
│  gRPC (HTTP/2 + Protobuf)      │███ 15ms                 │
│  WebSocket (Persistent)        │█ 5ms                    │
└─────────────────────────────────────────────────────────┘
```

### Throughput Benchmarks
- **HTTP/1.1 Keep-Alive**: ~5,000 req/s (single connection limited)
- **HTTP/2 Multiplexing**: ~20,000 req/s (single connection, 100 streams)
- **Connection Pooling**: ~50,000 req/s (10 connections × 5,000 req/s)
- **Compression**: +20-30% throughput (network-bound scenarios)

## REST vs GraphQL vs gRPC

| Feature | REST | GraphQL | gRPC |
|---------|------|---------|------|
| **Protocol** | HTTP/1.1, HTTP/2 | HTTP/1.1, HTTP/2 | HTTP/2 |
| **Data Format** | JSON, XML | JSON | Protobuf (binary) |
| **Schema** | OpenAPI/Swagger | GraphQL SDL | Protobuf .proto |
| **Request Model** | Multiple endpoints | Single endpoint | Service methods |
| **Over-fetching** | Yes (fixed responses) | No (client selects fields) | No (specific messages) |
| **Under-fetching** | Yes (multiple requests) | No (nested queries) | No (streaming) |
| **Caching** | HTTP caching (ETag, Cache-Control) | Complex (query-based) | No built-in |
| **Streaming** | No (SSE separate) | Subscriptions | Bidirectional |
| **Latency** | Medium | Medium | Low (binary, HTTP/2) |
| **Browser Support** | Native | Native | Requires gRPC-Web |
| **Best For** | Public APIs, CRUD | Complex data requirements | Microservices, low latency |

### When to Use REST
✅ **Public APIs**: Wide compatibility, HTTP caching, CDN support  
✅ **CRUD Operations**: Resource-based URLs map naturally  
✅ **Simple Requirements**: Standard HTTP tools, no code generation  
✅ **HTTP Semantics**: Leverage status codes, caching, authentication  

❌ **Avoid When**: Complex data relationships (N+1 queries), real-time bidirectional communication

## Project Catalog

### Feeders (Message Consumers)
- **[ThunderPropagator.Feeders.WebApi](Feeders.WebApi/README.md)**: Pull-based HTTP polling for REST APIs
  - Iterative feeder with configurable polling intervals
  - Support for GET, POST requests
  - Pagination, rate limiting, ETag caching
  - OAuth2, Bearer, Basic authentication

### Providers (Message Publishers)
- **[ThunderPropagator.Providers.DotNet.WebApi](Providers.DotNet.WebApi/README.md)**: Push-based HTTP requests
  - POST, PUT, PATCH, DELETE operations
  - JSON, XML, form-urlencoded, multipart content types
  - Idempotency keys, retry policies
  - Distributed tracing with OpenTelemetry

## Quick Start

### Install Package
```bash
dotnet add package ThunderPropagator.Feeders.WebApi
dotnet add package ThunderPropagator.Providers.DotNet.WebApi
```

### Configuration (appsettings.json)
```json
{
  "WebApi": {
    "Feeder": {
      "Id": "weather-api-feeder",
      "BaseUrl": "https://api.weather.com/v1",
      "Endpoint": "/current",
      "HttpMethod": "GET",
      "PollingInterval": "00:01:00",
      "Headers": {
        "User-Agent": "ThunderPropagator/1.0",
        "X-API-Key": "${API_KEY}"
      },
      "Authentication": {
        "Type": "Bearer",
        "Token": "${BEARER_TOKEN}"
      },
      "RetryPolicy": {
        "MaxAttempts": 3,
        "BackoffMultiplier": 2.0,
        "UseJitter": true
      },
      "CircuitBreaker": {
        "FailureThreshold": 5,
        "BreakDuration": "00:00:30",
        "SamplingDuration": "00:00:30"
      },
      "SerializerType": "Json"
    },
    "Provider": {
      "BaseUrl": "https://api.myservice.com/v1",
      "Endpoint": "/events",
      "HttpMethod": "POST",
      "Headers": {
        "Content-Type": "application/json"
      },
      "Authentication": {
        "Type": "Bearer",
        "Token": "${BEARER_TOKEN}"
      },
      "RetryPolicy": {
        "MaxAttempts": 3,
        "BackoffMultiplier": 2.0
      },
      "Timeout": "00:00:30",
      "SerializerType": "Json"
    }
  }
}
```

### Register Services
```csharp
using ThunderPropagator.Feeders.WebApi;
using ThunderPropagator.Providers.DotNet.WebApi;

var builder = WebApplication.CreateBuilder(args);

// Register WebApi Feeder (polling REST API)
builder.Services.AddWebApiFeeder<WeatherChannel, WeatherMessage, WeatherFeederConfig>(
    builder.Configuration,
    "WebApi:Feeder"
);

// Register WebApi Provider (sending HTTP requests)
builder.Services.AddWebApiProvider<EventMessage, EventProviderConfig>(
    builder.Configuration,
    "WebApi:Provider"
);

var app = builder.Build();
```

### Define Message Classes
```csharp
// Feeder message (incoming from API)
public sealed class WeatherMessage : WebApiFeederMessage
{
    public required string City { get; init; }
    public required double Temperature { get; init; }
    public required string Condition { get; init; }
    public required DateTime Timestamp { get; init; }
}

// Feeder configuration
public sealed class WeatherFeederConfig : WebApiFeederConfiguration
{
    // Inherits BaseUrl, Endpoint, Headers, Authentication, etc.
}

// Provider message (outgoing to API)
public sealed class EventMessage : WebApiProviderMessage
{
    public required string EventType { get; init; }
    public required string Source { get; init; }
    public required object Payload { get; init; }
    public required DateTime OccurredAt { get; init; }
}

// Provider configuration
public sealed class EventProviderConfig : WebApiProviderConfiguration
{
    // Inherits BaseUrl, Endpoint, Headers, Authentication, etc.
}
```

### Consume Messages (Feeder)
```csharp
public class WeatherProcessor : BackgroundService
{
    private readonly IFeeder<WeatherChannel, WeatherMessage, WeatherFeederConfig> _feeder;
    private readonly ILogger<WeatherProcessor> _logger;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var message in _feeder.ReceiveAsync(stoppingToken))
        {
            _logger.LogInformation(
                "Weather: {City} = {Temp}°C ({Condition})",
                message.Message.City,
                message.Message.Temperature,
                message.Message.Condition
            );
            
            // Process weather data
            await ProcessWeatherAsync(message.Message);
            
            // Acknowledge (auto-handled for HTTP)
            await message.AcknowledgeAsync();
        }
    }
}
```

### Publish Messages (Provider)
```csharp
public class EventPublisher
{
    private readonly IProvider<EventMessage, EventProviderConfig> _provider;

    public async Task PublishEventAsync(string eventType, object payload)
    {
        var message = new EventMessage
        {
            EventType = eventType,
            Source = "MyApplication",
            Payload = payload,
            OccurredAt = DateTime.UtcNow
        };

        await _provider.ExecuteAsync(message);
        // Provider handles serialization, retry, circuit breaker automatically
    }
}
```

## REST Concepts

### Resource-Based URLs
RESTful APIs organize around **resources** (nouns), not actions (verbs):

✅ **Good** (Resource-based):
```
GET    /users           # List users
GET    /users/123       # Get user 123
POST   /users           # Create user
PUT    /users/123       # Replace user 123
PATCH  /users/123       # Update user 123
DELETE /users/123       # Delete user 123
```

❌ **Bad** (RPC-style):
```
POST /getUsers
POST /createUser
POST /updateUser
POST /deleteUser
```

### HTTP Verbs and Idempotency

| Verb | Purpose | Idempotent | Safe | Request Body | Response Body |
|------|---------|------------|------|--------------|---------------|
| **GET** | Retrieve resource | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes |
| **POST** | Create resource | ❌ No | ❌ No | ✅ Yes | ✅ Yes (created) |
| **PUT** | Replace resource | ✅ Yes | ❌ No | ✅ Yes | ✅ Optional |
| **PATCH** | Partial update | ❌ No* | ❌ No | ✅ Yes | ✅ Optional |
| **DELETE** | Remove resource | ✅ Yes | ❌ No | ❌ Optional | ✅ Optional |

**Idempotent**: Same request can be repeated safely (same outcome)  
**Safe**: No side effects (read-only)  
*PATCH can be idempotent if designed carefully

### HTTP Status Codes

#### 2xx Success
- **200 OK**: Request succeeded (GET, PUT, PATCH)
- **201 Created**: Resource created (POST), includes Location header
- **202 Accepted**: Async processing started
- **204 No Content**: Success, no response body (DELETE, PUT)

#### 4xx Client Errors
- **400 Bad Request**: Invalid request syntax or validation failure
- **401 Unauthorized**: Authentication required or failed
- **403 Forbidden**: Authenticated but insufficient permissions
- **404 Not Found**: Resource does not exist
- **409 Conflict**: Request conflicts with current state (e.g., duplicate)
- **422 Unprocessable Entity**: Validation errors (semantic issues)
- **429 Too Many Requests**: Rate limit exceeded, check Retry-After header

#### 5xx Server Errors
- **500 Internal Server Error**: Generic server failure
- **502 Bad Gateway**: Upstream service error (proxy/gateway)
- **503 Service Unavailable**: Temporary overload or maintenance
- **504 Gateway Timeout**: Upstream service timeout

### Content Negotiation

Clients request format via **Accept** header, servers respond with **Content-Type**:

```http
# Request JSON
GET /users/123
Accept: application/json

# Response
HTTP/1.1 200 OK
Content-Type: application/json

{"id": 123, "name": "Alice"}
```

```http
# Request XML
GET /users/123
Accept: application/xml

# Response
HTTP/1.1 200 OK
Content-Type: application/xml

<user><id>123</id><name>Alice</name></user>
```

### HTTP Caching

#### ETag (Entity Tag)
Server provides hash of response, client sends in subsequent requests:

```http
# Initial request
GET /users/123

HTTP/1.1 200 OK
ETag: "abc123"
{"id": 123, "name": "Alice"}

# Subsequent request
GET /users/123
If-None-Match: "abc123"

HTTP/1.1 304 Not Modified
# No body, use cached version
```

#### Cache-Control
```http
Cache-Control: max-age=3600        # Cache 1 hour
Cache-Control: no-cache            # Revalidate every time
Cache-Control: no-store            # Don't cache (sensitive data)
Cache-Control: private             # User-specific (not CDN)
Cache-Control: public, max-age=86400  # CDN cacheable, 24 hours
```

### Authentication Patterns

#### Bearer Token (JWT)
```http
GET /api/protected
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

**JWT Structure**: `header.payload.signature` (base64url-encoded)
```json
// Header
{"alg": "HS256", "typ": "JWT"}

// Payload
{"sub": "user123", "exp": 1735488000, "role": "admin"}

// Signature: HMAC-SHA256(header + payload, secret)
```

#### Basic Authentication
```http
GET /api/protected
Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=
```

**Format**: `base64(username:password)`

#### API Key
```http
# Header-based
GET /api/data
X-API-Key: sk_live_abc123...

# Query parameter (less secure)
GET /api/data?api_key=sk_live_abc123
```

#### OAuth2 Client Credentials Flow
```http
# 1. Request token
POST /oauth/token
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials&client_id=abc&client_secret=xyz

# 2. Response
{
  "access_token": "eyJhbG...",
  "token_type": "Bearer",
  "expires_in": 3600
}

# 3. Use token
GET /api/protected
Authorization: Bearer eyJhbG...
```

## Best Practices

### ✅ Do
- **Use appropriate HTTP methods**: GET for reads, POST for creates, PUT/PATCH for updates, DELETE for removes
- **Implement Polly policies**: Combine retry + circuit breaker + timeout for resilience
- **Version your API**: Use URL versioning (/v1/users) or header versioning (Accept: application/vnd.api+json; version=1)
- **Paginate large responses**: Implement offset-based or cursor-based pagination
- **Use ETag caching**: Reduce bandwidth and server load for unchanged resources
- **Respect rate limits**: Handle 429 responses, use Retry-After header
- **Implement idempotency keys**: Prevent duplicate POST operations (Idempotency-Key header)
- **Enable compression**: gzip for general purpose, brotli for static content
- **Use HTTP/2**: Multiplexing, header compression, lower latency
- **Secure with TLS**: HTTPS only for production APIs
- **Log distributed traces**: Propagate trace context (W3C Trace Context, OpenTelemetry)

### ❌ Don't
- **Don't ignore retry policies**: Transient failures are common (network timeouts, 5xx errors)
- **Don't use GET with side effects**: GET must be safe (read-only)
- **Don't ignore circuit breakers**: Prevent cascading failures to downstream services
- **Don't expose sensitive data in URLs**: Use POST body for credentials, tokens
- **Don't hardcode timeouts**: Configure per-endpoint (fast queries vs. slow reports)
- **Don't skip authentication**: Even internal APIs should authenticate
- **Don't over-poll**: Balance freshness vs. load (adaptive polling intervals)
- **Don't ignore pagination**: Loading 100K records in one request kills performance
- **Don't skip input validation**: Validate early (400 Bad Request for invalid input)

## Health Monitoring

WebApi feeders and providers automatically register health checks:

```csharp
// Feeder health check
builder.Services.AddHealthChecks()
    .AddCheck<WebApiFeederHealthCheck>("webapi_feeder_weather");

// Provider health check
builder.Services.AddHealthChecks()
    .AddCheck<WebApiProviderHealthCheck>("webapi_provider_events");
```

**Health Tags**:
- `feeder_webapi_{id}_{endpoint}` (e.g., `feeder_webapi_weather-api_current`)
- `provider_webapi_{endpoint}` (e.g., `provider_webapi_events`)

**Metrics Tracked**:
- Request success rate (2xx responses)
- Average response time (p50, p95, p99)
- Error rate (4xx, 5xx)
- Circuit breaker state (Closed, Open, HalfOpen)
- Retry attempts per request
- Connection pool utilization

## Troubleshooting

### Issue: 429 Too Many Requests
**Cause**: Exceeding API rate limits  
**Solution**: 
- Check `Retry-After` header, wait before retrying
- Reduce `PollingInterval` (Feeder)
- Implement exponential backoff
- Request rate limit increase from API provider

### Issue: Timeout Errors
**Cause**: Slow API responses, network latency  
**Solution**:
- Increase `Timeout` configuration (e.g., 30s → 60s)
- Enable `Hedging` policy (parallel requests)
- Check API performance, query optimization
- Use connection pooling (reduce handshake overhead)

### Issue: 401 Unauthorized
**Cause**: Invalid or expired authentication credentials  
**Solution**:
- Verify `Authentication.Token` or `Authentication.Credentials`
- Implement token refresh logic (OAuth2)
- Check token expiration (`exp` claim in JWT)
- Validate API key/credentials with provider

### Issue: Circuit Breaker Open
**Cause**: Excessive failures triggered circuit breaker  
**Solution**:
- Check API health, recent deployments
- Review `FailureThreshold` and `SamplingDuration` configuration
- Wait for `BreakDuration` to expire (auto half-open test)
- Investigate root cause (API bugs, network issues)

### Issue: High Memory Usage
**Cause**: Large responses, no pagination, connection leaks  
**Solution**:
- Implement pagination (offset/cursor-based)
- Configure `MaxFrameSize` limit
- Enable compression (gzip/brotli)
- Set `PooledConnectionLifetime` to force connection refresh
- Review response payload size, optimize API queries

## Related Documentation

- **[Feeders.WebApi](Feeders.WebApi/README.md)**: Detailed feeder configuration, examples, advanced patterns
- **[Providers.DotNet.WebApi](Providers.DotNet.WebApi/README.md)**: Provider configuration, examples, performance tuning
- **[SharedKernel Documentation](../SharedKernel/README.md)**: Core abstractions, base classes, utilities
- **[ThunderPropagator Framework](../README.md)**: Overall framework architecture, getting started

## References

- **RFC 7230-7235**: HTTP/1.1 Specification
- **RFC 7540**: HTTP/2 Specification
- **RFC 9114**: HTTP/3 (QUIC) Specification
- **RFC 7519**: JSON Web Token (JWT)
- **RFC 6749**: OAuth 2.0 Authorization Framework
- **RFC 5988**: Web Linking (Link header for pagination)
- **OpenAPI 3.0**: REST API specification standard
- **Polly**: .NET resilience and transient-fault-handling library

---

**Version**: 1.0.1-beta.2  
**Last Updated**: December 2025  
**Feedback**: Report issues via GitHub Issues
