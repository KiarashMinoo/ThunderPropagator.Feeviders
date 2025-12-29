# ThunderPropagator.Feeders.WebApi

## Overview

**ThunderPropagator.Feeders.WebApi** provides a pull-based HTTP polling mechanism for consuming messages from REST APIs. Built on the `IterativeFeeder` pattern, it continuously polls configured endpoints at specified intervals, deserializes responses, and yields messages through an asynchronous enumerable stream.

This feeder is ideal for:
- **Polling REST APIs** that don't support webhooks or push notifications
- **Periodic data synchronization** from external systems
- **Monitoring endpoints** that expose status or metrics
- **Consuming paginated datasets** from APIs
- **Rate-limited API consumption** with configurable polling intervals

### Key Features

#### 🔄 Pull-Based Polling
- **Configurable intervals**: Fixed interval, exponential backoff, or adaptive polling
- **HTTP methods**: GET (primary), POST (for complex queries)
- **Async enumeration**: `IAsyncEnumerable<FeederReceivedMessage<T>>` for efficient streaming
- **Cancellation support**: Graceful shutdown on `CancellationToken` cancellation

#### 🛡️ Polly Resilience Integration
- **Retry with exponential backoff**: Handle transient failures (500-503, network timeouts)
- **Circuit breaker**: Prevent cascade failures after threshold exceeded
- **Timeout policies**: Per-request timeout (fail fast), total timeout (operations)
- **Jitter**: Randomize retry delays (±25%) to prevent thundering herd

#### 🔐 Authentication Support
- **Bearer tokens**: JWT in `Authorization: Bearer {token}` header
- **Basic authentication**: Base64-encoded `username:password`
- **OAuth2 flows**: Client credentials, password grant, authorization code
- **API keys**: Header-based (`X-API-Key`) or query parameter (`?api_key=...`)

#### 📄 Pagination Strategies
- **Offset-based**: `?offset=0&limit=100`, increment offset per page
- **Cursor-based**: `?cursor=abc123`, use next cursor from response
- **Link header**: RFC 5988 `Link: <url>; rel="next"`, parse for next page
- **Page number**: `?page=1&size=100`, increment page number

#### ⚡ Performance Optimizations
- **Connection pooling**: Reuse HTTP connections via `SocketsHttpHandler`
- **HTTP/2 support**: Multiplexing, header compression
- **Compression**: gzip, deflate, brotli for response bodies
- **Keep-alive**: Persistent connections reduce handshake overhead
- **ETag caching**: Conditional requests (`If-None-Match`), handle 304 Not Modified

## Architecture

The WebApiFeeder follows a **pull-based polling architecture** with resilience policies applied to every HTTP request:

```mermaid
sequenceDiagram
    participant App as Your Application
    participant Feeder as WebApiFeeder
    participant Timer as Polling Timer
    participant Polly as Polly Policies
    participant Http as HttpClient
    participant Api as REST API
    
    App->>Feeder: ReceiveAsync(CancellationToken)
    activate Feeder
    
    loop Polling Loop
        Feeder->>Timer: Wait for Interval
        activate Timer
        Timer-->>Feeder: Interval Elapsed
        deactivate Timer
        
        Feeder->>Polly: ExecuteAsync(httpRequest)
        activate Polly
        
        alt Retry Logic
            Polly->>Http: SendAsync(GET /endpoint)
            activate Http
            Http->>Api: HTTP GET Request
            activate Api
            
            alt Success (2xx)
                Api-->>Http: HTTP 200 OK + JSON
                Http-->>Polly: Response
                deactivate Api
                Polly-->>Feeder: Successful Response
            else Transient Error (5xx, timeout)
                Api-->>Http: HTTP 503 Service Unavailable
                deactivate Api
                Http-->>Polly: Error Response
                deactivate Http
                
                Polly->>Polly: Wait (Exponential Backoff + Jitter)
                Polly->>Http: Retry SendAsync
                activate Http
                Http->>Api: HTTP GET Retry
                activate Api
                Api-->>Http: HTTP 200 OK
                Http-->>Polly: Success
                deactivate Api
                deactivate Http
                Polly-->>Feeder: Response After Retry
            else Circuit Breaker Open
                Polly-->>Feeder: BrokenCircuitException
            end
        end
        
        deactivate Polly
        
        Feeder->>Feeder: Deserialize Response (JSON/XML)
        Feeder->>Feeder: Create FeederReceivedMessage
        
        Feeder-->>App: yield return message
        
        alt Check for More Pages (Pagination)
            Feeder->>Feeder: Parse Next Page URL/Cursor
            Feeder->>Polly: Fetch Next Page
        end
    end
    
    deactivate Feeder
```

### Polling Flow

1. **Initialize**: Load configuration (base URL, endpoint, headers, auth, polling interval)
2. **Wait Interval**: Sleep for configured interval (e.g., 60 seconds)
3. **Build Request**: Construct HTTP request (method, URL, headers, query params)
4. **Apply Auth**: Add authentication headers (Bearer, Basic, OAuth2 token)
5. **Execute with Resilience**: Polly policies wrap request (retry, circuit breaker, timeout)
6. **Handle Response**:
   - **2xx Success**: Deserialize body, yield message
   - **304 Not Modified**: Skip (cached data unchanged)
   - **4xx Client Error**: Log error, skip iteration (or throw based on config)
   - **429 Rate Limit**: Respect `Retry-After` header, wait before next attempt
   - **5xx Server Error**: Trigger retry policy (transient failure)
7. **Pagination**: If configured, parse next page URL/cursor, repeat step 3-7
8. **Repeat**: Loop until cancellation token triggered

## Project Files

| File | Purpose |
|------|---------|
| **WebApiFeeder.cs** | Core feeder class, inherits `IterativeFeeder<TChannel, TMessage, TConfig>` |
| **WebApiFeederMessage.cs** | Abstract base class for feeder messages |
| **WebApiFeederConfiguration.cs** | Configuration class with HTTP-specific properties |
| **WebApiFeederExtensions.cs** | DI registration extension methods |
| **Pagination/OffsetPaginationStrategy.cs** | Offset-based pagination logic |
| **Pagination/CursorPaginationStrategy.cs** | Cursor-based pagination logic |
| **Pagination/LinkHeaderPaginationStrategy.cs** | RFC 5988 Link header parsing |
| **Authentication/BearerAuthHandler.cs** | Bearer token authentication |
| **Authentication/BasicAuthHandler.cs** | Basic authentication (username:password) |
| **Authentication/OAuth2AuthHandler.cs** | OAuth2 token acquisition and refresh |

## Dependencies

```xml
<PackageReference Include="ThunderPropagator" Version="1.0.1-beta.2" />
<PackageReference Include="ThunderPropagator.BuildingBlocks" Version="1.0.1-beta.2" />
<PackageReference Include="Microsoft.Extensions.Http" Version="9.0.0" />
<PackageReference Include="Microsoft.Extensions.Http.Polly" Version="9.0.0" />
<PackageReference Include="Microsoft.Extensions.Http.Resilience" Version="9.0.0" />
<PackageReference Include="System.Net.Http" Version="9.0.0" />
<PackageReference Include="System.Net.Http.Json" Version="9.0.0" />
```

**Key Dependencies**:
- **ThunderPropagator**: Core feeder abstractions (`IterativeFeeder`, `FeederReceivedMessage`)
- **Microsoft.Extensions.Http**: HttpClient factory, typed clients
- **Microsoft.Extensions.Http.Polly**: Polly integration for resilience
- **Microsoft.Extensions.Http.Resilience**: Standard resilience pipelines
- **System.Net.Http.Json**: JSON serialization extensions (`GetFromJsonAsync`, `PostAsJsonAsync`)

## Configuration

### WebApiFeederConfiguration Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| **Id** | `Guid` | Required | Unique feeder identifier |
| **BaseUrl** | `string` | Required | Base URL of REST API (e.g., `https://api.example.com`) |
| **Endpoint** | `string` | Required | Endpoint path (e.g., `/v1/events`, `/users`) |
| **HttpMethod** | `HttpMethod` | `GET` | HTTP method (GET, POST) |
| **PollingInterval** | `TimeSpan` | `00:01:00` | Time between polling iterations |
| **Headers** | `Dictionary<string, string>` | `{}` | Custom HTTP headers (User-Agent, Accept, etc.) |
| **QueryParameters** | `Dictionary<string, string>` | `{}` | Query string parameters |
| **Authentication** | `AuthConfig` | `null` | Authentication configuration |
| **RetryPolicy** | `RetryPolicyConfig` | Enabled | Retry policy settings |
| **CircuitBreaker** | `CircuitBreakerConfig` | Enabled | Circuit breaker settings |
| **Timeout** | `TimeoutConfig` | `00:00:30` | Timeout policy settings |
| **HttpVersion** | `Version` | `2.0` | HTTP version (1.1, 2.0, 3.0) |
| **Compression** | `DecompressionMethods` | `GZip` | Compression algorithms (GZip, Deflate, Brotli) |
| **MaxConnectionsPerServer** | `int` | `null` | Max concurrent connections per server (null = unlimited) |
| **KeepAlive** | `bool` | `true` | Enable persistent connections |
| **PooledConnectionLifetime** | `TimeSpan?` | `null` | Force connection refresh after duration |
| **PooledConnectionIdleTimeout** | `TimeSpan` | `00:01:30` | Close idle connections after timeout |
| **UseCookies** | `bool` | `false` | Enable cookie container |
| **AllowAutoRedirect** | `bool` | `true` | Follow 3xx redirects |
| **MaxAutomaticRedirections** | `int` | `50` | Max redirect hops |
| **TlsVersion** | `SslProtocols` | `Tls12 \| Tls13` | TLS versions (Tls12, Tls13) |
| **ServerCertificateValidation** | `Func<HttpRequestMessage, X509Certificate2, X509Chain, SslPolicyErrors, bool>` | `null` | Custom cert validation callback |
| **ClientCertificates** | `X509CertificateCollection` | `null` | Client certificates for mutual TLS |
| **Pagination** | `PaginationConfig` | `null` | Pagination settings |
| **ETagCaching** | `bool` | `false` | Enable ETag caching (If-None-Match header) |
| **RateLimiting** | `RateLimitConfig` | `null` | Rate limiting settings (respect 429 responses) |
| **SerializerType** | `SerializerType` | `Json` | Serialization format (Json, NJson, NetJson, Xml) |
| **EnrichmentScript** | `string?` | `null` | C# script for message enrichment |
| **MetadataReferences** | `string[]?` | `null` | Assemblies for enrichment script |

### AuthConfig Properties

| Property | Type | Description |
|----------|------|-------------|
| **Type** | `AuthenticationType` | Authentication type (Bearer, Basic, OAuth2, ApiKey) |
| **Token** | `string?` | Bearer token or API key value |
| **Username** | `string?` | Username (Basic auth) |
| **Password** | `string?` | Password (Basic auth) |
| **OAuth2** | `OAuth2Config?` | OAuth2 configuration |
| **ApiKeyLocation** | `ApiKeyLocation?` | API key location (Header, QueryParameter) |
| **ApiKeyName** | `string?` | Header name or query param name for API key |

### RetryPolicyConfig Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| **Enabled** | `bool` | `true` | Enable retry policy |
| **MaxAttempts** | `int` | `3` | Maximum retry attempts |
| **BackoffMultiplier** | `double` | `2.0` | Exponential backoff multiplier (2^n) |
| **InitialDelay** | `TimeSpan` | `00:00:01` | Initial delay before first retry |
| **MaxDelay** | `TimeSpan` | `00:00:30` | Maximum delay between retries |
| **UseJitter** | `bool` | `true` | Add randomness (±25%) to delays |
| **RetryableStatusCodes** | `int[]` | `[500, 502, 503, 504]` | HTTP status codes triggering retry |
| **RetryableExceptions** | `Type[]` | `[HttpRequestException, TaskCanceledException]` | Exception types triggering retry |

### CircuitBreakerConfig Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| **Enabled** | `bool` | `true` | Enable circuit breaker |
| **FailureThreshold** | `int` | `5` | Failures before opening circuit |
| **SamplingDuration** | `TimeSpan` | `00:00:30` | Time window for failure count |
| **BreakDuration** | `TimeSpan` | `00:00:30` | Duration circuit stays open |
| **MinimumThroughput** | `int` | `10` | Minimum requests before evaluating |

### PaginationConfig Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| **Strategy** | `PaginationStrategy` | `None` | Pagination strategy (None, Offset, Cursor, LinkHeader) |
| **PageSize** | `int` | `100` | Records per page |
| **OffsetParamName** | `string` | `offset` | Query param name for offset |
| **LimitParamName** | `string` | `limit` | Query param name for page size |
| **CursorParamName** | `string` | `cursor` | Query param name for cursor |
| **CursorJsonPath** | `string` | `$.next_cursor` | JSON path to next cursor in response |
| **MaxPages** | `int?` | `null` | Maximum pages to fetch (null = unlimited) |

## API Reference

### WebApiFeeder<TChannel, TMessage, TConfig>

**Namespace**: `ThunderPropagator.Feeders.WebApi`

```csharp
internal
#if !DEBUG
    sealed
#endif
    class WebApiFeeder<TChannel, TMessage, TConfig> : 
        IterativeFeeder<TChannel, TMessage, TConfig>
    where TMessage : WebApiFeederMessage
    where TConfig : WebApiFeederConfiguration
{
    public override string HealthName { get; }
    public override string[] HealthTags { get; }
    
    public override IAsyncEnumerable<FeederReceivedMessage<TMessage>> ReceiveAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default);
}
```

**Key Methods**:
- `ReceiveAsync()`: Polls configured endpoint, yields deserialized messages
- `BuildRequestAsync()`: Constructs HTTP request with auth, headers, query params
- `HandlePaginationAsync()`: Iterates through paginated results
- `HandleRateLimitAsync()`: Respects `Retry-After` header on 429 responses

### WebApiFeederMessage

**Namespace**: `ThunderPropagator.Feeders.WebApi`

```csharp
public abstract class WebApiFeederMessage : FeederMessage
{
    public string? ResponseHeaders { get; init; }
    public int StatusCode { get; init; }
    public string? ETag { get; init; }
}
```

**Properties**:
- `ResponseHeaders`: Serialized response headers (JSON)
- `StatusCode`: HTTP status code (200, 304, etc.)
- `ETag`: ETag value from response (for caching)

### WebApiFeederConfiguration

**Namespace**: `ThunderPropagator.Feeders.WebApi`

```csharp
public abstract class WebApiFeederConfiguration : IAbstractFeederConfiguration
{
    public Guid Id { get; set; }
    public required string BaseUrl { get; set; }
    public required string Endpoint { get; set; }
    public HttpMethod HttpMethod { get; set; } = HttpMethod.Get;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMinutes(1);
    public Dictionary<string, string> Headers { get; set; } = new();
    public AuthConfig? Authentication { get; set; }
    public RetryPolicyConfig RetryPolicy { get; set; } = new();
    public CircuitBreakerConfig CircuitBreaker { get; set; } = new();
    // ... (see Configuration section for full list)
}
```

### Extension Methods

**Namespace**: `Microsoft.Extensions.DependencyInjection`

```csharp
public static class WebApiFeederExtensions
{
    // Register feeder with configuration from IConfiguration
    public static IServiceCollection AddWebApiFeeder<TChannel, TMessage, TConfig>(
        this IServiceCollection services,
        IConfigurationRoot configuration,
        string configSection)
        where TMessage : WebApiFeederMessage
        where TConfig : WebApiFeederConfiguration;
    
    // Register feeder resolver (multi-tenancy)
    public static IServiceCollection AddWebApiFeederResolver<TChannel, TMessage, TConfig>(
        this IServiceCollection services)
        where TMessage : WebApiFeederMessage
        where TConfig : WebApiFeederConfiguration;
    
    // Use specific feeder configuration (multi-tenancy)
    public static void UseWebApiFeederResolver<TChannel, TMessage, TConfig>(
        this IServiceProvider services,
        Guid feederId,
        TConfig configuration)
        where TMessage : WebApiFeederMessage
        where TConfig : WebApiFeederConfiguration;
}
```

## Examples

### Example 1: Basic GET Polling (Weather API)

Poll a weather API every 60 seconds to retrieve current conditions.

**Configuration (appsettings.json)**:
```json
{
  "WeatherApi": {
    "Id": "550e8400-e29b-41d4-a716-446655440001",
    "BaseUrl": "https://api.weather.com",
    "Endpoint": "/v1/current",
    "HttpMethod": "GET",
    "PollingInterval": "00:01:00",
    "Headers": {
      "User-Agent": "ThunderPropagator/1.0",
      "Accept": "application/json"
    },
    "QueryParameters": {
      "city": "New York",
      "units": "metric"
    },
    "RetryPolicy": {
      "Enabled": true,
      "MaxAttempts": 3,
      "BackoffMultiplier": 2.0,
      "UseJitter": true
    },
    "SerializerType": "Json"
  }
}
```

**Message Class**:
```csharp
public sealed class WeatherMessage : WebApiFeederMessage
{
    public required string City { get; init; }
    public required double Temperature { get; init; }
    public required double Humidity { get; init; }
    public required string Condition { get; init; }
    public required DateTime Timestamp { get; init; }
}

public sealed class WeatherFeederConfig : WebApiFeederConfiguration
{
    // Inherits all HTTP configuration properties
}
```

**Registration**:
```csharp
services.AddWebApiFeeder<WeatherChannel, WeatherMessage, WeatherFeederConfig>(
    configuration,
    "WeatherApi"
);
```

**Consumption**:
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
                "Weather in {City}: {Temp}°C, {Humidity}% humidity, {Condition}",
                message.Message.City,
                message.Message.Temperature,
                message.Message.Humidity,
                message.Message.Condition
            );
            
            await message.AcknowledgeAsync();
        }
    }
}
```

### Example 2: OAuth2 Authentication (Client Credentials Flow)

Use OAuth2 to obtain an access token before polling a protected API.

**Configuration**:
```json
{
  "ProtectedApi": {
    "Id": "550e8400-e29b-41d4-a716-446655440002",
    "BaseUrl": "https://api.protected.com",
    "Endpoint": "/v1/data",
    "PollingInterval": "00:05:00",
    "Authentication": {
      "Type": "OAuth2",
      "OAuth2": {
        "TokenEndpoint": "https://auth.protected.com/oauth/token",
        "ClientId": "my-client-id",
        "ClientSecret": "${OAUTH_CLIENT_SECRET}",
        "GrantType": "client_credentials",
        "Scope": "read:data",
        "TokenCacheKey": "protected-api-token"
      }
    },
    "SerializerType": "Json"
  }
}
```

**OAuth2Config**:
```csharp
public class OAuth2Config
{
    public required string TokenEndpoint { get; set; }
    public required string ClientId { get; set; }
    public required string ClientSecret { get; set; }
    public string GrantType { get; set; } = "client_credentials";
    public string? Scope { get; set; }
    public string? TokenCacheKey { get; set; }
    public TimeSpan TokenCacheDuration { get; set; } = TimeSpan.FromMinutes(55); // Refresh before 1-hour expiry
}
```

**Token Acquisition** (automatic):
```csharp
// OAuth2AuthHandler automatically:
// 1. POST to token endpoint with client credentials
// 2. Parse access_token from response
// 3. Cache token with expiry
// 4. Refresh token when expired
// 5. Add Authorization: Bearer {token} header to requests
```

### Example 3: Exponential Backoff Retry (Transient Errors)

Handle transient errors (500-503, network timeouts) with exponential backoff and jitter.

**Configuration**:
```json
{
  "UnreliableApi": {
    "BaseUrl": "https://api.unreliable.com",
    "Endpoint": "/v1/events",
    "PollingInterval": "00:02:00",
    "RetryPolicy": {
      "Enabled": true,
      "MaxAttempts": 5,
      "BackoffMultiplier": 2.0,
      "InitialDelay": "00:00:01",
      "MaxDelay": "00:01:00",
      "UseJitter": true,
      "RetryableStatusCodes": [500, 502, 503, 504],
      "RetryableExceptions": ["HttpRequestException", "TaskCanceledException"]
    },
    "Timeout": {
      "PerRequestTimeout": "00:00:10"
    }
  }
}
```

**Retry Behavior**:
```
Attempt 1: Immediate (0s delay)
  └─ Fails with 503 Service Unavailable

Attempt 2: Wait 1s × 2^0 = 1s (±25% jitter → 0.75-1.25s)
  └─ Fails with timeout

Attempt 3: Wait 1s × 2^1 = 2s (±25% jitter → 1.5-2.5s)
  └─ Fails with 500 Internal Server Error

Attempt 4: Wait 1s × 2^2 = 4s (±25% jitter → 3-5s)
  └─ Fails with network error

Attempt 5: Wait 1s × 2^3 = 8s (±25% jitter → 6-10s)
  └─ Success (200 OK)
```

**Why Jitter?**
Prevents **thundering herd** problem: If 1000 clients retry at exact same time (2s, 4s, 8s), they overload server simultaneously. Jitter spreads retries over time.

### Example 4: Circuit Breaker (Prevent Cascade Failures)

Open circuit after repeated failures, preventing wasted requests to unhealthy API.

**Configuration**:
```json
{
  "HealthCheckApi": {
    "BaseUrl": "https://api.health.com",
    "Endpoint": "/status",
    "PollingInterval": "00:00:10",
    "CircuitBreaker": {
      "Enabled": true,
      "FailureThreshold": 5,
      "SamplingDuration": "00:00:30",
      "BreakDuration": "00:01:00",
      "MinimumThroughput": 10
    }
  }
}
```

**Circuit States**:

1. **Closed (Normal)**: Requests pass through normally
   - Monitor failure rate over `SamplingDuration` (30s)
   - If ≥5 failures out of ≥10 requests → Open circuit

2. **Open (Blocked)**: All requests fail immediately with `BrokenCircuitException`
   - No requests reach API (fast fail)
   - Wait `BreakDuration` (60s) → Half-Open

3. **Half-Open (Testing)**: Allow single test request
   - If success → Close circuit (resume normal)
   - If failure → Open circuit (wait another 60s)

**Visualization**:
```
Time: 0s - Closed
├─ 10 requests, 2 failures (20% failure rate) → Stay Closed

Time: 30s - Closed
├─ 10 requests, 6 failures (60% failure rate) → OPEN CIRCUIT

Time: 31s-90s - Open
├─ All requests fail immediately (BrokenCircuitException)
├─ No load on failing API (prevent cascade)

Time: 90s - Half-Open
├─ Test request → Success → CLOSE CIRCUIT

Time: 91s+ - Closed (Recovered)
```

### Example 5: Custom Headers (User-Agent, API Key)

Add custom headers for API identification and authentication.

**Configuration**:
```json
{
  "ApiKeyApi": {
    "BaseUrl": "https://api.example.com",
    "Endpoint": "/v2/data",
    "PollingInterval": "00:03:00",
    "Headers": {
      "User-Agent": "MyApp/1.0 (support@myapp.com)",
      "Accept": "application/json",
      "Accept-Language": "en-US",
      "X-Request-ID": "{{guid}}"
    },
    "Authentication": {
      "Type": "ApiKey",
      "ApiKeyLocation": "Header",
      "ApiKeyName": "X-API-Key",
      "Token": "${API_KEY}"
    }
  }
}
```

**Resulting HTTP Request**:
```http
GET /v2/data HTTP/1.1
Host: api.example.com
User-Agent: MyApp/1.0 (support@myapp.com)
Accept: application/json
Accept-Language: en-US
X-Request-ID: 550e8400-e29b-41d4-a716-446655440000
X-API-Key: sk_live_abc123xyz789...
```

**Dynamic Header Values**:
- `{{guid}}`: Generate unique GUID per request (tracking)
- `{{timestamp}}`: Current UTC timestamp (ISO 8601)
- `${ENV_VAR}`: Environment variable substitution

### Example 6: Pagination (Offset-Based, Cursor-Based)

Fetch large datasets across multiple pages.

#### Offset-Based Pagination

**Configuration**:
```json
{
  "UserApi": {
    "BaseUrl": "https://api.users.com",
    "Endpoint": "/v1/users",
    "PollingInterval": "01:00:00",
    "Pagination": {
      "Strategy": "Offset",
      "PageSize": 100,
      "OffsetParamName": "offset",
      "LimitParamName": "limit",
      "MaxPages": 10
    }
  }
}
```

**HTTP Requests**:
```http
GET /v1/users?offset=0&limit=100    # Page 1 (users 0-99)
GET /v1/users?offset=100&limit=100  # Page 2 (users 100-199)
GET /v1/users?offset=200&limit=100  # Page 3 (users 200-299)
...
GET /v1/users?offset=900&limit=100  # Page 10 (users 900-999)
```

**Response** (Page 1):
```json
{
  "users": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
    ...
  ],
  "total": 1543,
  "offset": 0,
  "limit": 100
}
```

#### Cursor-Based Pagination

**Configuration**:
```json
{
  "EventApi": {
    "BaseUrl": "https://api.events.com",
    "Endpoint": "/v1/events",
    "PollingInterval": "00:30:00",
    "Pagination": {
      "Strategy": "Cursor",
      "PageSize": 50,
      "CursorParamName": "cursor",
      "CursorJsonPath": "$.pagination.next_cursor",
      "MaxPages": null
    }
  }
}
```

**HTTP Requests**:
```http
GET /v1/events?limit=50                    # Page 1 (no cursor)
GET /v1/events?cursor=abc123&limit=50      # Page 2
GET /v1/events?cursor=def456&limit=50      # Page 3
```

**Response** (Page 1):
```json
{
  "events": [...],
  "pagination": {
    "next_cursor": "abc123",
    "has_more": true
  }
}
```

**Stop Condition**: `next_cursor` is `null` or `has_more` is `false`

#### Link Header Pagination (RFC 5988)

**Response Headers**:
```http
HTTP/1.1 200 OK
Link: <https://api.example.com/users?page=2>; rel="next",
      <https://api.example.com/users?page=5>; rel="last",
      <https://api.example.com/users?page=1>; rel="first"
```

**Configuration**:
```json
{
  "Pagination": {
    "Strategy": "LinkHeader"
  }
}
```

Feeder parses `Link` header, follows `rel="next"` until no next link exists.

## Advanced Patterns

### Pattern 1: Polling Strategies

Choose polling strategy based on data freshness requirements and API load.

#### Fixed Interval
**Use Case**: Predictable load, consistent data update frequency  
**Configuration**:
```json
{
  "PollingInterval": "00:05:00"
}
```
**Behavior**: Poll every 5 minutes, regardless of API response (200, 304, 5xx)

#### Exponential Backoff
**Use Case**: API rate limiting, reduce load during failures  
**Implementation**:
```csharp
private TimeSpan _currentInterval = TimeSpan.FromSeconds(10);

private async Task<TimeSpan> CalculateNextIntervalAsync(HttpResponseMessage response)
{
    if (response.IsSuccessStatusCode)
    {
        // Success: Reset to base interval
        _currentInterval = Configuration.PollingInterval;
    }
    else if ((int)response.StatusCode >= 500)
    {
        // Server error: Exponential backoff
        _currentInterval = TimeSpan.FromSeconds(
            Math.Min(_currentInterval.TotalSeconds * 2, 3600) // Max 1 hour
        );
    }
    
    return _currentInterval;
}
```
**Behavior**: 10s → 20s → 40s → 80s → ... (doubles on failure, resets on success)

#### Adaptive Polling
**Use Case**: Balance freshness vs. load, adjust based on data change rate  
**Implementation**:
```csharp
private async Task<TimeSpan> CalculateAdaptiveIntervalAsync(HttpResponseMessage response)
{
    if (response.StatusCode == HttpStatusCode.NotModified) // 304
    {
        // No changes: Increase interval (poll less frequently)
        return TimeSpan.FromSeconds(
            Math.Min(_currentInterval.TotalSeconds * 1.5, 600) // Max 10 min
        );
    }
    else if (response.IsSuccessStatusCode && HasChanges(response))
    {
        // Changes detected: Decrease interval (poll more frequently)
        return TimeSpan.FromSeconds(
            Math.Max(_currentInterval.TotalSeconds * 0.75, 30) // Min 30s
        );
    }
    
    return _currentInterval;
}
```

### Pattern 2: Polly Resilience Composition

Combine multiple Polly policies for comprehensive resilience.

**Standard Pipeline**:
```
Request → Retry → Circuit Breaker → Timeout → HttpClient → API
```

**Configuration**:
```csharp
services.AddHttpClient<WebApiFeeder<TChannel, TMessage, TConfig>>()
    .AddPolicyHandler(GetRetryPolicy())        // Outer: Retry transient failures
    .AddPolicyHandler(GetCircuitBreakerPolicy()) // Middle: Prevent cascade
    .AddPolicyHandler(GetTimeoutPolicy());       // Inner: Fail fast on slow requests
```

**Policy Definitions**:
```csharp
private static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
{
    return HttpPolicyExtensions
        .HandleTransientHttpError() // 5xx or network failure
        .OrResult(r => (int)r.StatusCode == 429) // Rate limit
        .WaitAndRetryAsync(
            retryCount: 3,
            sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt))
                + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 500)), // Jitter
            onRetry: (outcome, timespan, retryAttempt, context) =>
            {
                Log.Warning("Retry {Attempt} after {Delay}ms due to {StatusCode}",
                    retryAttempt, timespan.TotalMilliseconds, outcome.Result?.StatusCode);
            }
        );
}

private static IAsyncPolicy<HttpResponseMessage> GetCircuitBreakerPolicy()
{
    return HttpPolicyExtensions
        .HandleTransientHttpError()
        .CircuitBreakerAsync(
            handledEventsAllowedBeforeBreaking: 5,
            durationOfBreak: TimeSpan.FromSeconds(30),
            onBreak: (outcome, duration) =>
            {
                Log.Error("Circuit OPEN for {Duration}s due to {Failures}",
                    duration.TotalSeconds, 5);
            },
            onReset: () => Log.Information("Circuit CLOSED (recovered)"),
            onHalfOpen: () => Log.Information("Circuit HALF-OPEN (testing)")
        );
}

private static IAsyncPolicy<HttpResponseMessage> GetTimeoutPolicy()
{
    return Policy.TimeoutAsync<HttpResponseMessage>(
        timeout: TimeSpan.FromSeconds(30),
        onTimeoutAsync: (context, timespan, task) =>
        {
            Log.Warning("Request timed out after {Timeout}s", timespan.TotalSeconds);
            return Task.CompletedTask;
        }
    );
}
```

**Execution Flow**:
```
1. Retry Policy wraps request
   └─ Attempt 1 → Circuit Breaker → Timeout → HTTP (fails: 503)
   └─ Wait 2s (exponential backoff + jitter)
   └─ Attempt 2 → Circuit Breaker → Timeout → HTTP (fails: timeout)
   └─ Wait 4s
   └─ Attempt 3 → Circuit Breaker → Timeout → HTTP (success: 200)
   └─ Return response

2. If 5 consecutive failures → Circuit Breaker opens
   └─ Subsequent requests fail immediately (BrokenCircuitException)
   └─ After 30s → Half-open (test request)
   └─ Test success → Close circuit (resume normal)
```

### Pattern 3: Authentication Token Refresh

Implement automatic token refresh for OAuth2 and JWT.

**OAuth2 Token Caching**:
```csharp
public class OAuth2AuthHandler : DelegatingHandler
{
    private readonly IMemoryCache _cache;
    private readonly OAuth2Config _config;
    private readonly SemaphoreSlim _refreshLock = new(1, 1);

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        var token = await GetAccessTokenAsync(cancellationToken);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        
        var response = await base.SendAsync(request, cancellationToken);
        
        if (response.StatusCode == HttpStatusCode.Unauthorized)
        {
            // Token expired: Refresh and retry
            _cache.Remove(_config.TokenCacheKey);
            token = await GetAccessTokenAsync(cancellationToken);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            response = await base.SendAsync(request, cancellationToken);
        }
        
        return response;
    }

    private async Task<string> GetAccessTokenAsync(CancellationToken cancellationToken)
    {
        // Check cache first
        if (_cache.TryGetValue(_config.TokenCacheKey, out string cachedToken))
            return cachedToken;
        
        // Prevent thundering herd (multiple threads refreshing simultaneously)
        await _refreshLock.WaitAsync(cancellationToken);
        try
        {
            // Double-check after acquiring lock
            if (_cache.TryGetValue(_config.TokenCacheKey, out cachedToken))
                return cachedToken;
            
            // Request new token
            var tokenResponse = await RequestTokenAsync(cancellationToken);
            
            // Cache with expiry (token lifetime - 5min buffer)
            _cache.Set(
                _config.TokenCacheKey,
                tokenResponse.AccessToken,
                TimeSpan.FromSeconds(tokenResponse.ExpiresIn - 300)
            );
            
            return tokenResponse.AccessToken;
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    private async Task<TokenResponse> RequestTokenAsync(CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, _config.TokenEndpoint);
        request.Content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = _config.GrantType,
            ["client_id"] = _config.ClientId,
            ["client_secret"] = _config.ClientSecret,
            ["scope"] = _config.Scope
        });
        
        var response = await _httpClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();
        
        return await response.Content.ReadFromJsonAsync<TokenResponse>(cancellationToken);
    }
}
```

### Pattern 4: Pagination Abstraction

Implement reusable pagination strategies.

**Interface**:
```csharp
public interface IPaginationStrategy
{
    HttpRequestMessage BuildInitialRequest(string baseUrl, string endpoint);
    HttpRequestMessage? BuildNextRequest(HttpResponseMessage previousResponse);
    bool HasMorePages(HttpResponseMessage response);
}
```

**Offset-Based Implementation**:
```csharp
public class OffsetPaginationStrategy : IPaginationStrategy
{
    private int _currentOffset = 0;
    private readonly int _pageSize;
    private readonly int? _maxPages;
    private int _pageCount = 0;

    public HttpRequestMessage BuildInitialRequest(string baseUrl, string endpoint)
    {
        _currentOffset = 0;
        _pageCount = 0;
        return new HttpRequestMessage(HttpMethod.Get, 
            $"{baseUrl}{endpoint}?offset={_currentOffset}&limit={_pageSize}");
    }

    public HttpRequestMessage? BuildNextRequest(HttpResponseMessage previousResponse)
    {
        _currentOffset += _pageSize;
        _pageCount++;
        
        if (_maxPages.HasValue && _pageCount >= _maxPages.Value)
            return null;
        
        return new HttpRequestMessage(HttpMethod.Get,
            $"{previousResponse.RequestMessage.RequestUri.GetLeftPart(UriPartial.Path)}" +
            $"?offset={_currentOffset}&limit={_pageSize}");
    }

    public bool HasMorePages(HttpResponseMessage response)
    {
        var content = await response.Content.ReadFromJsonAsync<PaginatedResponse>();
        return content.Total > _currentOffset + _pageSize;
    }
}
```

**Usage in Feeder**:
```csharp
public override async IAsyncEnumerable<FeederReceivedMessage<TMessage>> ReceiveAsync(
    [EnumeratorCancellation] CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        await Task.Delay(Configuration.PollingInterval, cancellationToken);
        
        var paginationStrategy = CreatePaginationStrategy();
        var request = paginationStrategy.BuildInitialRequest(Configuration.BaseUrl, Configuration.Endpoint);
        
        do
        {
            var response = await _httpClient.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();
            
            var messages = await DeserializeMessagesAsync(response);
            foreach (var message in messages)
            {
                yield return new FeederReceivedMessage<TMessage>(message);
            }
            
            if (!paginationStrategy.HasMorePages(response))
                break;
            
            request = paginationStrategy.BuildNextRequest(response);
        } while (request != null);
    }
}
```

### Pattern 5: Rate Limiting (Respect 429 Responses)

Handle API rate limits gracefully by respecting `Retry-After` headers.

**Configuration**:
```json
{
  "RateLimiting": {
    "Enabled": true,
    "MaxRequestsPerMinute": 100,
    "BurstSize": 20,
    "WaitOnRateLimit": true
  }
}
```

**Implementation**:
```csharp
private async Task<HttpResponseMessage> SendWithRateLimitAsync(
    HttpRequestMessage request,
    CancellationToken cancellationToken)
{
    var response = await _httpClient.SendAsync(request, cancellationToken);
    
    if (response.StatusCode == HttpStatusCode.TooManyRequests) // 429
    {
        if (Configuration.RateLimiting?.WaitOnRateLimit == true)
        {
            var retryAfter = GetRetryAfterDelay(response);
            Logger.LogWarning(
                "Rate limit exceeded, waiting {Delay}s before retry",
                retryAfter.TotalSeconds
            );
            
            await Task.Delay(retryAfter, cancellationToken);
            
            // Retry request after waiting
            response = await _httpClient.SendAsync(request, cancellationToken);
        }
        else
        {
            throw new RateLimitExceededException(
                $"Rate limit exceeded. Retry after {GetRetryAfterDelay(response).TotalSeconds}s"
            );
        }
    }
    
    return response;
}

private TimeSpan GetRetryAfterDelay(HttpResponseMessage response)
{
    // Check Retry-After header (seconds or HTTP date)
    if (response.Headers.TryGetValues("Retry-After", out var values))
    {
        var retryAfter = values.FirstOrDefault();
        
        // Retry-After: 120 (seconds)
        if (int.TryParse(retryAfter, out var seconds))
            return TimeSpan.FromSeconds(seconds);
        
        // Retry-After: Wed, 29 Dec 2025 12:00:00 GMT
        if (DateTimeOffset.TryParse(retryAfter, out var date))
            return date - DateTimeOffset.UtcNow;
    }
    
    // Fallback: Default delay
    return TimeSpan.FromMinutes(1);
}
```

**Rate Limit Headers** (vary by API):
```http
HTTP/1.1 429 Too Many Requests
X-RateLimit-Limit: 100           # Max requests per window
X-RateLimit-Remaining: 0         # Requests remaining
X-RateLimit-Reset: 1735488000    # Unix timestamp when limit resets
Retry-After: 120                 # Seconds to wait
```

### Pattern 6: ETag Caching (Conditional Requests)

Reduce bandwidth and server load using HTTP ETag caching.

**Configuration**:
```json
{
  "ETagCaching": {
    "Enabled": true,
    "CacheSize": 1000
  }
}
```

**Implementation**:
```csharp
private readonly IMemoryCache _etagCache;

public async IAsyncEnumerable<FeederReceivedMessage<TMessage>> ReceiveAsync(
    [EnumeratorCancellation] CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        await Task.Delay(Configuration.PollingInterval, cancellationToken);
        
        var request = new HttpRequestMessage(HttpMethod.Get, GetFullUrl());
        
        // Add If-None-Match header if we have cached ETag
        var cacheKey = GetFullUrl();
        if (_etagCache.TryGetValue(cacheKey, out string cachedETag))
        {
            request.Headers.IfNoneMatch.Add(new EntityTagHeaderValue($"\"{cachedETag}\""));
        }
        
        var response = await _httpClient.SendAsync(request, cancellationToken);
        
        if (response.StatusCode == HttpStatusCode.NotModified) // 304
        {
            Logger.LogDebug("Resource not modified (304), skipping processing");
            continue; // Skip to next polling iteration
        }
        
        response.EnsureSuccessStatusCode();
        
        // Cache new ETag for future requests
        if (response.Headers.ETag != null)
        {
            _etagCache.Set(cacheKey, response.Headers.ETag.Tag.Trim('"'),
                TimeSpan.FromHours(1));
        }
        
        var messages = await DeserializeMessagesAsync(response);
        foreach (var message in messages)
        {
            yield return new FeederReceivedMessage<TMessage>(message);
        }
    }
}
```

**HTTP Flow**:
```http
# Initial request
GET /api/users HTTP/1.1

HTTP/1.1 200 OK
ETag: "abc123"
Content-Length: 1024
[user data]

# Subsequent request (cached ETag)
GET /api/users HTTP/1.1
If-None-Match: "abc123"

HTTP/1.1 304 Not Modified
# No body, saves bandwidth

# Later request (data changed)
GET /api/users HTTP/1.1
If-None-Match: "abc123"

HTTP/1.1 200 OK
ETag: "def456"
Content-Length: 1024
[updated user data]
```

### Pattern 7: Health Monitoring (Response Time, Error Rate)

Track feeder health metrics for observability.

**Metrics**:
```csharp
public class WebApiFeederMetrics
{
    public required string FeederId { get; init; }
    public required string Endpoint { get; init; }
    public long TotalRequests { get; set; }
    public long SuccessfulRequests { get; set; }
    public long FailedRequests { get; set; }
    public double ErrorRate => TotalRequests > 0 
        ? (double)FailedRequests / TotalRequests 
        : 0;
    public TimeSpan AverageResponseTime { get; set; }
    public TimeSpan P95ResponseTime { get; set; }
    public TimeSpan P99ResponseTime { get; set; }
    public CircuitState CircuitBreakerState { get; set; }
    public int RetryCount { get; set; }
}
```

**Tracking**:
```csharp
private readonly ConcurrentQueue<TimeSpan> _responseTimes = new();

private async Task<HttpResponseMessage> SendWithMetricsAsync(
    HttpRequestMessage request,
    CancellationToken cancellationToken)
{
    var stopwatch = Stopwatch.StartNew();
    
    try
    {
        var response = await _httpClient.SendAsync(request, cancellationToken);
        stopwatch.Stop();
        
        _responseTimes.Enqueue(stopwatch.Elapsed);
        if (_responseTimes.Count > 1000) // Keep last 1000 samples
            _responseTimes.TryDequeue(out _);
        
        Metrics.TotalRequests++;
        
        if (response.IsSuccessStatusCode)
            Metrics.SuccessfulRequests++;
        else
            Metrics.FailedRequests++;
        
        return response;
    }
    catch (Exception ex)
    {
        stopwatch.Stop();
        Metrics.TotalRequests++;
        Metrics.FailedRequests++;
        throw;
    }
}

private void CalculatePercentiles()
{
    var sortedTimes = _responseTimes.OrderBy(t => t).ToArray();
    if (sortedTimes.Length == 0) return;
    
    Metrics.AverageResponseTime = TimeSpan.FromMilliseconds(
        sortedTimes.Average(t => t.TotalMilliseconds)
    );
    Metrics.P95ResponseTime = sortedTimes[(int)(sortedTimes.Length * 0.95)];
    Metrics.P99ResponseTime = sortedTimes[(int)(sortedTimes.Length * 0.99)];
}
```

**Health Check**:
```csharp
public class WebApiFeederHealthCheck : IHealthCheck
{
    private readonly WebApiFeederMetrics _metrics;

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken)
    {
        var data = new Dictionary<string, object>
        {
            ["endpoint"] = _metrics.Endpoint,
            ["total_requests"] = _metrics.TotalRequests,
            ["error_rate"] = $"{_metrics.ErrorRate:P2}",
            ["avg_response_time_ms"] = _metrics.AverageResponseTime.TotalMilliseconds,
            ["p95_response_time_ms"] = _metrics.P95ResponseTime.TotalMilliseconds,
            ["circuit_breaker_state"] = _metrics.CircuitBreakerState.ToString()
        };
        
        // Unhealthy if error rate > 50% or circuit breaker open
        if (_metrics.ErrorRate > 0.5 || _metrics.CircuitBreakerState == CircuitState.Open)
            return HealthCheckResult.Unhealthy("High error rate or circuit breaker open", data: data);
        
        // Degraded if error rate > 10% or p95 latency > 5s
        if (_metrics.ErrorRate > 0.1 || _metrics.P95ResponseTime > TimeSpan.FromSeconds(5))
            return HealthCheckResult.Degraded("Elevated error rate or latency", data: data);
        
        return HealthCheckResult.Healthy("Feeder operating normally", data: data);
    }
}
```

## Best Practices

### ✅ Do
- **Tune polling intervals**: Balance data freshness vs. API load (adaptive polling for dynamic scenarios)
- **Combine Polly policies**: Retry + Circuit Breaker + Timeout for comprehensive resilience
- **Implement token refresh**: Automatic OAuth2/JWT refresh before expiration
- **Use pagination**: Fetch large datasets incrementally, avoid memory issues
- **Respect rate limits**: Handle 429 responses, use `Retry-After` header
- **Enable ETag caching**: Reduce bandwidth, skip processing for unchanged data
- **Monitor metrics**: Track error rate, response time percentiles, circuit breaker state
- **Configure timeouts**: Per-request timeout (fail fast), total timeout (long operations)
- **Use connection pooling**: Reuse TCP connections, reduce handshake overhead
- **Enable compression**: gzip/brotli for large payloads
- **Secure credentials**: Use environment variables, Azure Key Vault, AWS Secrets Manager
- **Log distributed traces**: Propagate W3C Trace Context, correlate requests across services

### ❌ Don't
- **Don't ignore circuit breakers**: Open circuit prevents wasted requests to unhealthy APIs
- **Don't poll too frequently**: Respect API rate limits, use webhooks if available
- **Don't skip retry policies**: Transient failures are common (network timeouts, 5xx errors)
- **Don't hardcode credentials**: Use configuration, environment variables, secret management
- **Don't ignore pagination**: Loading 100K records in one request causes memory/timeout issues
- **Don't skip authentication refresh**: Expired tokens cause 401 errors, implement automatic refresh
- **Don't over-configure**: Start with defaults (retry 3× exponential backoff, circuit breaker 5 failures), tune based on metrics
- **Don't ignore 429 responses**: Retry immediately without backoff gets you banned (respect `Retry-After`)
- **Don't poll during maintenance**: Check API status endpoints, skip polling if unhealthy
- **Don't expose secrets in logs**: Sanitize Authorization headers, API keys, passwords

## Troubleshooting

### Issue: High Error Rate (5xx Responses)
**Symptoms**: Frequent 500/503 errors, circuit breaker opening  
**Causes**:
- API overload or maintenance
- Database connection issues (API backend)
- Timeout too short (slow queries)

**Solutions**:
1. Check API status page, recent deployments
2. Increase retry `MaxDelay` (e.g., 30s → 60s)
3. Increase `BreakDuration` (e.g., 30s → 5min) to give API recovery time
4. Reduce polling frequency during peak hours
5. Contact API provider if persistent

### Issue: 429 Too Many Requests
**Symptoms**: Rate limit errors, `RateLimitExceededException`  
**Causes**:
- Polling too frequently
- Multiple feeder instances (total exceeds limit)
- Burst traffic (page load spikes)

**Solutions**:
1. Check `X-RateLimit-Limit` and `X-RateLimit-Remaining` headers
2. Increase `PollingInterval` (e.g., 1min → 5min)
3. Implement adaptive polling (less frequent when no changes)
4. Use shared rate limiter across instances (Redis-based)
5. Request rate limit increase from API provider

### Issue: Authentication Failures (401 Unauthorized)
**Symptoms**: 401 errors after initial success  
**Causes**:
- Token expired (JWT, OAuth2)
- Refresh token expired
- API key rotated

**Solutions**:
1. Check token expiration (`exp` claim in JWT)
2. Implement automatic token refresh (OAuth2AuthHandler)
3. Set `TokenCacheDuration` to < token lifetime (e.g., 55min for 1-hour tokens)
4. Verify credentials in configuration
5. Monitor API provider communications (key rotation announcements)

### Issue: High Memory Usage
**Symptoms**: OutOfMemoryException, GC pressure, slow performance  
**Causes**:
- Large responses (no pagination)
- ETag cache unbounded growth
- Connection pool leaks

**Solutions**:
1. Implement pagination (fetch 100 records/page vs. 100K at once)
2. Limit ETag cache size (`IMemoryCache` with size limit)
3. Set `PooledConnectionLifetime` (force connection refresh, prevent leaks)
4. Use streaming deserialization (`JsonSerializer.DeserializeAsyncEnumerable`)
5. Monitor memory metrics, heap dumps

### Issue: Slow Response Times
**Symptoms**: Timeouts, high p95/p99 latency, sluggish processing  
**Causes**:
- Slow API queries (no indexing, large joins)
- Network latency (geographic distance)
- Compression overhead (brotli on large payloads)

**Solutions**:
1. Enable HTTP/2 (multiplexing, header compression)
2. Use connection pooling (reuse sockets)
3. Switch compression (brotli → gzip for speed)
4. Implement hedging (parallel requests, use first response)
5. Work with API provider to optimize queries, add indexes
6. Use regional endpoints (reduce network hops)

### Issue: Circuit Breaker Stuck Open
**Symptoms**: All requests fail immediately, `BrokenCircuitException`  
**Causes**:
- API still unhealthy after `BreakDuration`
- Failure threshold too sensitive
- Half-open test request failing

**Solutions**:
1. Check API health (status page, monitoring)
2. Increase `BreakDuration` (e.g., 30s → 5min) for slow recovery
3. Increase `FailureThreshold` (e.g., 5 → 10) for tolerance
4. Increase `MinimumThroughput` (e.g., 10 → 20) to avoid premature opening
5. Manual circuit reset (restart feeder, clear state)
6. Review half-open test request failure reason (logs, traces)

## Related Documentation

- **[Providers.DotNet.WebApi](../Providers.DotNet.WebApi/README.md)**: Provider for HTTP POST/PUT/PATCH/DELETE operations
- **[WebApi System Overview](../README.md)**: REST concepts, architecture, comparison to GraphQL/gRPC
- **[SharedKernel Feeders](../../SharedKernel/Feeders.SharedKernel/README.md)**: `IterativeFeeder` base class, `FeederReceivedMessage`
- **[ThunderPropagator Framework](../../README.md)**: Core framework documentation, getting started

## References

- **Microsoft.Extensions.Http**: [Typed HttpClient](https://learn.microsoft.com/en-us/dotnet/core/extensions/httpclient-factory)
- **Polly**: [Resilience Policies](https://www.pollydocs.org/)
- **RFC 7230-7235**: HTTP/1.1 Specification
- **RFC 7540**: HTTP/2 Specification
- **RFC 5988**: Web Linking (Link header pagination)
- **RFC 7519**: JSON Web Token (JWT)
- **RFC 6749**: OAuth 2.0 Authorization Framework

---

**Version**: 1.0.1-beta.2  
**Last Updated**: December 2025  
**Maintainer**: ThunderPropagator Team
