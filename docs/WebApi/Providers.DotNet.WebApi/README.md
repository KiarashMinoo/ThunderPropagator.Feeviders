# ThunderPropagator.Providers.DotNet.WebApi

## Overview

**ThunderPropagator.Providers.DotNet.WebApi** enables push-based HTTP communication for publishing messages to REST APIs. Built on the `AbstractProvider` pattern, it handles serialization, resilience policies, and HTTP request execution automatically, supporting POST, PUT, PATCH, and DELETE operations.

This provider is ideal for:
- **Creating resources** via POST requests (create orders, users, events)
- **Updating resources** via PUT (full replacement) or PATCH (partial updates)
- **Deleting resources** via DELETE requests
- **Triggering webhooks** or external API callbacks
- **Publishing events** to HTTP-based event systems

### Key Features

#### 📤 HTTP Method Support
- **POST**: Create new resources (non-idempotent)
- **PUT**: Replace existing resources (idempotent)
- **PATCH**: Partial updates (conditional idempotency)
- **DELETE**: Remove resources (idempotent)

#### 🛡️ Polly Resilience Integration
- **Retry with exponential backoff**: Handle transient failures (500-503, network timeouts)
- **Circuit breaker**: Prevent cascade failures to downstream APIs
- **Timeout policies**: Per-request timeout (fail fast)
- **Jitter**: Randomize retry delays (prevent thundering herd)

#### 🔐 Authentication Support
- **Bearer tokens**: JWT in `Authorization: Bearer {token}` header
- **Basic authentication**: Base64-encoded credentials
- **OAuth2 flows**: Client credentials, password grant
- **API keys**: Header-based or query parameter

#### 📄 Content Type Support
- **application/json**: JSON serialization (System.Text.Json, Newtonsoft.Json)
- **application/xml**: XML serialization (System.Xml.Serialization)
- **application/x-www-form-urlencoded**: Form data (key-value pairs)
- **multipart/form-data**: File uploads with metadata

#### ⚡ Performance Optimizations
- **Connection pooling**: Reuse HTTP connections via `SocketsHttpHandler`
- **HTTP/2 support**: Multiplexing, header compression
- **Compression**: gzip, deflate, brotli for request/response bodies
- **Keep-alive**: Persistent connections reduce handshake overhead

## Architecture

The WebApiProvider follows a **push-based architecture** where application code triggers HTTP requests:

```mermaid
sequenceDiagram
    participant App as Your Application
    participant Provider as WebApiProvider
    participant Serializer as Message Serializer
    participant Polly as Polly Policies
    participant Http as HttpClient
    participant Api as REST API
    
    App->>Provider: ExecuteAsync(message)
    activate Provider
    
    Provider->>Serializer: Serialize(message)
    activate Serializer
    Serializer-->>Provider: JSON/XML/Form bytes
    deactivate Serializer
    
    Provider->>Provider: Build HTTP Request (method, headers, auth)
    
    Provider->>Polly: ExecuteAsync(httpRequest)
    activate Polly
    
    alt Retry Logic
        Polly->>Http: SendAsync(POST /endpoint)
        activate Http
        Http->>Api: HTTP POST Request
        activate Api
        
        alt Success (2xx)
            Api-->>Http: HTTP 201 Created + Location
            Http-->>Polly: Response
            deactivate Api
            Polly-->>Provider: Successful Response
        else Transient Error (5xx)
            Api-->>Http: HTTP 503 Service Unavailable
            deactivate Api
            Http-->>Polly: Error Response
            deactivate Http
            
            Polly->>Polly: Wait (Exponential Backoff + Jitter)
            Polly->>Http: Retry SendAsync
            activate Http
            Http->>Api: HTTP POST Retry
            activate Api
            Api-->>Http: HTTP 201 Created
            Http-->>Polly: Success
            deactivate Api
            deactivate Http
            Polly-->>Provider: Response After Retry
        else Circuit Breaker Open
            Polly-->>Provider: BrokenCircuitException
        end
    end
    
    deactivate Polly
    
    Provider->>Provider: Validate Response (2xx, 4xx, 5xx)
    Provider-->>App: Success (or throw exception)
    
    deactivate Provider
```

### Request Flow

1. **Initialize**: Application calls `provider.ExecuteAsync(message)`
2. **Serialize**: Convert message object to JSON/XML/form data
3. **Build Request**: Construct HTTP request (method, URL, headers, body)
4. **Apply Auth**: Add authentication headers (Bearer, Basic, OAuth2 token)
5. **Execute with Resilience**: Polly policies wrap request (retry, circuit breaker, timeout)
6. **Handle Response**:
   - **2xx Success**: Return (optionally parse response body)
   - **4xx Client Error**: Throw `HttpRequestException` (invalid request)
   - **5xx Server Error**: Trigger retry policy (transient failure)
7. **Complete**: Return to application

## Project Files

| File | Purpose |
|------|---------|
| **WebApiProvider.cs** | Core provider class, inherits `AbstractProvider<TMessage, TConfig>` |
| **WebApiProviderMessage.cs** | Abstract base class for provider messages |
| **WebApiProviderConfiguration.cs** | Configuration class with HTTP-specific properties |
| **WebApiProviderExtensions.cs** | DI registration extension methods |
| **ContentType/JsonContentSerializer.cs** | JSON serialization (System.Text.Json) |
| **ContentType/XmlContentSerializer.cs** | XML serialization |
| **ContentType/FormContentSerializer.cs** | URL-encoded form serialization |
| **ContentType/MultipartContentSerializer.cs** | Multipart/form-data serialization |
| **Authentication/BearerAuthHandler.cs** | Bearer token authentication |
| **Authentication/BasicAuthHandler.cs** | Basic authentication |
| **Authentication/OAuth2AuthHandler.cs** | OAuth2 token acquisition and refresh |

## Dependencies

```xml
<PackageReference Include="ThunderPropagator" Version="1.0.1-beta.2" />
<PackageReference Include="ThunderPropagator.BuildingBlocks" Version="1.0.1-beta.2" />
<PackageReference Include="Microsoft.Extensions.Http" Version="9.0.0" />
<PackageReference Include="Microsoft.Extensions.Http.Polly" Version="9.0.0" />
<PackageReference Include="System.Net.Http" Version="9.0.0" />
<PackageReference Include="System.Net.Http.Json" Version="9.0.0" />
```

**Key Dependencies**:
- **ThunderPropagator**: Core provider abstractions (`AbstractProvider`)
- **Microsoft.Extensions.Http**: HttpClient factory, typed clients
- **Microsoft.Extensions.Http.Polly**: Polly integration for resilience
- **System.Net.Http.Json**: JSON serialization extensions

## Configuration

### WebApiProviderConfiguration Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| **BaseUrl** | `string` | Required | Base URL of REST API (e.g., `https://api.example.com`) |
| **Endpoint** | `string` | Required | Endpoint path (e.g., `/v1/orders`, `/users`) |
| **HttpMethod** | `HttpMethod` | `POST` | HTTP method (POST, PUT, PATCH, DELETE) |
| **Headers** | `Dictionary<string, string>` | `{}` | Custom HTTP headers |
| **Authentication** | `AuthConfig` | `null` | Authentication configuration |
| **RetryPolicy** | `RetryPolicyConfig` | Enabled | Retry policy settings |
| **CircuitBreaker** | `CircuitBreakerConfig` | Enabled | Circuit breaker settings |
| **Timeout** | `TimeoutConfig` | `00:00:30` | Timeout policy settings |
| **ContentType** | `string` | `application/json` | Content-Type header (json, xml, form-urlencoded, multipart) |
| **HttpVersion** | `Version` | `2.0` | HTTP version (1.1, 2.0, 3.0) |
| **Compression** | `DecompressionMethods` | `GZip` | Compression algorithms (GZip, Deflate, Brotli) |
| **MaxConnectionsPerServer** | `int` | `null` | Max concurrent connections per server (null = unlimited) |
| **KeepAlive** | `bool` | `true` | Enable persistent connections |
| **PooledConnectionLifetime** | `TimeSpan?` | `null` | Force connection refresh after duration |
| **PooledConnectionIdleTimeout** | `TimeSpan` | `00:01:30` | Close idle connections after timeout |
| **UseCookies** | `bool` | `false` | Enable cookie container |
| **AllowAutoRedirect** | `bool` | `false` | Follow 3xx redirects (usually false for APIs) |
| **TlsVersion** | `SslProtocols` | `Tls12 \| Tls13` | TLS versions |
| **ServerCertificateValidation** | `Func<...>` | `null` | Custom cert validation callback |
| **ClientCertificates** | `X509CertificateCollection` | `null` | Client certificates for mutual TLS |
| **IdempotencyKey** | `bool` | `false` | Generate Idempotency-Key header (POST only) |
| **IdempotencyKeyHeader** | `string` | `Idempotency-Key` | Header name for idempotency key |
| **ExpectedStatusCodes** | `int[]` | `[200, 201, 204]` | Valid success status codes |
| **SerializerType** | `SerializerType` | `Json` | Serialization format (Json, NJson, NetJson, Xml) |

### AuthConfig Properties

| Property | Type | Description |
|----------|------|-------------|
| **Type** | `AuthenticationType` | Authentication type (Bearer, Basic, OAuth2, ApiKey) |
| **Token** | `string?` | Bearer token or API key value |
| **Username** | `string?` | Username (Basic auth) |
| **Password** | `string?` | Password (Basic auth) |
| **OAuth2** | `OAuth2Config?` | OAuth2 configuration |

### RetryPolicyConfig Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| **Enabled** | `bool` | `true` | Enable retry policy |
| **MaxAttempts** | `int` | `3` | Maximum retry attempts |
| **BackoffMultiplier** | `double` | `2.0` | Exponential backoff multiplier |
| **InitialDelay** | `TimeSpan` | `00:00:01` | Initial delay before first retry |
| **MaxDelay** | `TimeSpan` | `00:00:30` | Maximum delay between retries |
| **UseJitter** | `bool` | `true` | Add randomness to delays |
| **RetryableStatusCodes** | `int[]` | `[500, 502, 503, 504]` | Status codes triggering retry |

## API Reference

### WebApiProvider<TMessage, TConfig>

**Namespace**: `ThunderPropagator.Providers.DotNet.WebApi`

```csharp
internal
#if !DEBUG
    sealed
#endif
    class WebApiProvider<TMessage, TConfig> : AbstractProvider<TMessage, TConfig>
    where TMessage : WebApiProviderMessage
    where TConfig : WebApiProviderConfiguration
{
    protected override async Task InternalExecuteAsync(
        TMessage message,
        CancellationToken cancellationToken);
}
```

**Key Methods**:
- `InternalExecuteAsync()`: Serializes message, sends HTTP request, validates response
- `BuildRequestAsync()`: Constructs HTTP request with auth, headers
- `SerializeMessageAsync()`: Converts message to JSON/XML/form content
- `ValidateResponseAsync()`: Checks status code, throws on error

### WebApiProviderMessage

**Namespace**: `ThunderPropagator.Providers.DotNet.WebApi`

```csharp
public abstract class WebApiProviderMessage : ProviderMessage
{
    public string? IdempotencyKey { get; init; }
    public Dictionary<string, string>? CustomHeaders { get; init; }
}
```

**Properties**:
- `IdempotencyKey`: Client-provided idempotency key (prevents duplicate POST)
- `CustomHeaders`: Message-specific headers (override configuration)

### WebApiProviderConfiguration

**Namespace**: `ThunderPropagator.Providers.DotNet.WebApi`

```csharp
public abstract class WebApiProviderConfiguration : IAbstractProviderConfiguration
{
    public required string BaseUrl { get; set; }
    public required string Endpoint { get; set; }
    public HttpMethod HttpMethod { get; set; } = HttpMethod.Post;
    public string ContentType { get; set; } = "application/json";
    public Dictionary<string, string> Headers { get; set; } = new();
    public AuthConfig? Authentication { get; set; }
    public RetryPolicyConfig RetryPolicy { get; set; } = new();
    // ... (see Configuration section for full list)
}
```

### Extension Methods

**Namespace**: `Microsoft.Extensions.DependencyInjection`

```csharp
public static class WebApiProviderExtensions
{
    // Register provider with configuration from IConfiguration
    public static IServiceCollection AddWebApiProvider<TMessage, TConfig>(
        this IServiceCollection services,
        IConfigurationRoot configuration,
        string configSection)
        where TMessage : WebApiProviderMessage
        where TConfig : WebApiProviderConfiguration;
}
```

## Examples

### Example 1: POST JSON (Create Order)

Send a POST request to create a new order with JSON body.

**Configuration (appsettings.json)**:
```json
{
  "OrderApi": {
    "BaseUrl": "https://api.ecommerce.com",
    "Endpoint": "/v1/orders",
    "HttpMethod": "POST",
    "ContentType": "application/json",
    "Headers": {
      "User-Agent": "ThunderPropagator/1.0"
    },
    "Authentication": {
      "Type": "Bearer",
      "Token": "${BEARER_TOKEN}"
    },
    "RetryPolicy": {
      "Enabled": true,
      "MaxAttempts": 3,
      "BackoffMultiplier": 2.0
    },
    "ExpectedStatusCodes": [201],
    "SerializerType": "Json"
  }
}
```

**Message Class**:
```csharp
public sealed class CreateOrderMessage : WebApiProviderMessage
{
    public required string CustomerId { get; init; }
    public required List<OrderItem> Items { get; init; }
    public required decimal TotalAmount { get; init; }
    public required string Currency { get; init; }
}

public sealed class OrderItem
{
    public required string ProductId { get; init; }
    public required int Quantity { get; init; }
    public required decimal Price { get; init; }
}

public sealed class OrderProviderConfig : WebApiProviderConfiguration
{
    // Inherits all HTTP configuration properties
}
```

**Registration**:
```csharp
services.AddWebApiProvider<CreateOrderMessage, OrderProviderConfig>(
    configuration,
    "OrderApi"
);
```

**Usage**:
```csharp
public class OrderService
{
    private readonly IProvider<CreateOrderMessage, OrderProviderConfig> _provider;

    public async Task CreateOrderAsync(string customerId, List<OrderItem> items)
    {
        var message = new CreateOrderMessage
        {
            CustomerId = customerId,
            Items = items,
            TotalAmount = items.Sum(i => i.Price * i.Quantity),
            Currency = "USD"
        };

        await _provider.ExecuteAsync(message);
        // Provider handles serialization, retry, circuit breaker automatically
    }
}
```

**HTTP Request**:
```http
POST /v1/orders HTTP/1.1
Host: api.ecommerce.com
Content-Type: application/json
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
User-Agent: ThunderPropagator/1.0

{
  "customerId": "cust_123",
  "items": [
    {"productId": "prod_456", "quantity": 2, "price": 29.99},
    {"productId": "prod_789", "quantity": 1, "price": 49.99}
  ],
  "totalAmount": 109.97,
  "currency": "USD"
}
```

**Expected Response**:
```http
HTTP/1.1 201 Created
Location: /v1/orders/ord_abc123
Content-Type: application/json

{
  "orderId": "ord_abc123",
  "status": "pending",
  "createdAt": "2025-12-29T12:00:00Z"
}
```

### Example 2: PUT with Retry (Update Entity)

Use PUT to replace an existing resource, with retry for transient errors.

**Configuration**:
```json
{
  "UserApi": {
    "BaseUrl": "https://api.users.com",
    "Endpoint": "/v1/users/{userId}",
    "HttpMethod": "PUT",
    "ContentType": "application/json",
    "RetryPolicy": {
      "Enabled": true,
      "MaxAttempts": 5,
      "BackoffMultiplier": 2.0,
      "InitialDelay": "00:00:01",
      "MaxDelay": "00:00:30",
      "UseJitter": true,
      "RetryableStatusCodes": [500, 502, 503, 504, 408]
    },
    "Timeout": {
      "PerRequestTimeout": "00:00:15"
    },
    "ExpectedStatusCodes": [200, 204]
  }
}
```

**Message Class**:
```csharp
public sealed class UpdateUserMessage : WebApiProviderMessage
{
    public required string UserId { get; init; }
    public required string Name { get; init; }
    public required string Email { get; init; }
    public required string Phone { get; init; }
    public required UserSettings Settings { get; init; }
}
```

**Usage**:
```csharp
var message = new UpdateUserMessage
{
    UserId = "user_123",
    Name = "Alice Smith",
    Email = "alice@example.com",
    Phone = "+1-555-0100",
    Settings = new UserSettings { Theme = "dark", Notifications = true }
};

// Provider substitutes {userId} in endpoint URL
await _provider.ExecuteAsync(message);
```

**HTTP Request**:
```http
PUT /v1/users/user_123 HTTP/1.1
Host: api.users.com
Content-Type: application/json

{
  "name": "Alice Smith",
  "email": "alice@example.com",
  "phone": "+1-555-0100",
  "settings": {
    "theme": "dark",
    "notifications": true
  }
}
```

**PUT Idempotency**: Multiple identical PUT requests produce same result (safe to retry).

### Example 3: PATCH (Partial Update)

Update only specific fields of a resource using PATCH.

**Configuration**:
```json
{
  "ProductApi": {
    "BaseUrl": "https://api.catalog.com",
    "Endpoint": "/v1/products/{productId}",
    "HttpMethod": "PATCH",
    "ContentType": "application/json",
    "ExpectedStatusCodes": [200]
  }
}
```

**Message Class**:
```csharp
public sealed class UpdateProductPriceMessage : WebApiProviderMessage
{
    public required string ProductId { get; init; }
    public decimal? Price { get; init; }
    public decimal? DiscountPercent { get; init; }
}
```

**Usage**:
```csharp
// Update only price (leave other fields unchanged)
var message = new UpdateProductPriceMessage
{
    ProductId = "prod_456",
    Price = 39.99m,
    DiscountPercent = null // Don't update discount
};

await _provider.ExecuteAsync(message);
```

**HTTP Request** (JSON Merge Patch):
```http
PATCH /v1/products/prod_456 HTTP/1.1
Host: api.catalog.com
Content-Type: application/merge-patch+json

{
  "price": 39.99
}
```

**Alternative**: JSON Patch (RFC 6902) for more control:
```http
PATCH /v1/products/prod_456 HTTP/1.1
Content-Type: application/json-patch+json

[
  {"op": "replace", "path": "/price", "value": 39.99}
]
```

### Example 4: Bearer Authentication (JWT)

Use JWT bearer tokens for authentication.

**Configuration**:
```json
{
  "ProtectedApi": {
    "BaseUrl": "https://api.protected.com",
    "Endpoint": "/v1/data",
    "HttpMethod": "POST",
    "Authentication": {
      "Type": "Bearer",
      "Token": "${JWT_TOKEN}"
    }
  }
}
```

**HTTP Request**:
```http
POST /v1/data HTTP/1.1
Host: api.protected.com
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIiwiZXhwIjoxNzM1NDg4MDAwfQ.abc123...
Content-Type: application/json

{"data": "payload"}
```

**JWT Structure**:
```
Header:  {"alg": "HS256", "typ": "JWT"}
Payload: {"sub": "user123", "exp": 1735488000, "role": "admin"}
Signature: HMAC-SHA256(header + payload, secret)
```

**Token Refresh** (automatic with OAuth2AuthHandler):
```csharp
// Handler checks token expiration, refreshes if needed
// 1. Parse exp claim from JWT
// 2. If exp < now + 5min, request new token
// 3. Cache new token
// 4. Add Authorization header
```

### Example 5: Multipart/Form-Data (File Upload)

Upload files with metadata using multipart/form-data.

**Configuration**:
```json
{
  "FileUploadApi": {
    "BaseUrl": "https://api.storage.com",
    "Endpoint": "/v1/files",
    "HttpMethod": "POST",
    "ContentType": "multipart/form-data"
  }
}
```

**Message Class**:
```csharp
public sealed class UploadFileMessage : WebApiProviderMessage
{
    public required Stream FileStream { get; init; }
    public required string FileName { get; init; }
    public required string ContentType { get; init; }
    public Dictionary<string, string>? Metadata { get; init; }
}
```

**Usage**:
```csharp
using var fileStream = File.OpenRead("document.pdf");

var message = new UploadFileMessage
{
    FileStream = fileStream,
    FileName = "document.pdf",
    ContentType = "application/pdf",
    Metadata = new Dictionary<string, string>
    {
        ["description"] = "Q4 Report",
        ["department"] = "Finance"
    }
};

await _provider.ExecuteAsync(message);
```

**HTTP Request**:
```http
POST /v1/files HTTP/1.1
Host: api.storage.com
Content-Type: multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW

------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="file"; filename="document.pdf"
Content-Type: application/pdf

<binary file data>
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="description"

Q4 Report
------WebKitFormBoundary7MA4YWxkTrZu0gW
Content-Disposition: form-data; name="department"

Finance
------WebKitFormBoundary7MA4YWxkTrZu0gW--
```

### Example 6: OpenTelemetry Distributed Tracing

Propagate trace context for distributed tracing across services.

**Configuration**:
```json
{
  "DownstreamApi": {
    "BaseUrl": "https://api.downstream.com",
    "Endpoint": "/v1/events",
    "HttpMethod": "POST"
  }
}
```

**Tracing Integration** (automatic):
```csharp
protected override async Task InternalExecuteAsync(
    TMessage message,
    CancellationToken cancellationToken)
{
    using var activity = ActivitySource.StartActivity("WebApiProvider.SendRequest");
    activity?.SetTag("http.method", Configuration.HttpMethod.Method);
    activity?.SetTag("http.url", GetFullUrl());
    activity?.SetTag("http.flavor", "2.0");
    
    var request = await BuildRequestAsync(message);
    
    // Inject W3C Trace Context headers
    var propagator = new TraceContextPropagator();
    propagator.Inject(new PropagationContext(activity?.Context ?? default, Baggage.Current),
        request, (r, key, value) => r.Headers.Add(key, value));
    
    var response = await _httpClient.SendAsync(request, cancellationToken);
    
    activity?.SetTag("http.status_code", (int)response.StatusCode);
    
    if (!response.IsSuccessStatusCode)
        activity?.SetStatus(ActivityStatusCode.Error, $"HTTP {(int)response.StatusCode}");
}
```

**HTTP Request** (with trace headers):
```http
POST /v1/events HTTP/1.1
Host: api.downstream.com
traceparent: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
tracestate: congo=t61rcWkgMzE
Content-Type: application/json

{"eventType": "order.created", "orderId": "ord_123"}
```

**W3C Trace Context**:
- `traceparent`: `00-{trace-id}-{parent-id}-{flags}`
  - `00`: Version
  - `0af7651916cd43dd8448eb211c80319c`: Trace ID (128-bit)
  - `b7ad6b7169203331`: Parent Span ID (64-bit)
  - `01`: Flags (sampled)
- `tracestate`: Vendor-specific data (optional)

## Advanced Patterns

### Pattern 1: HTTP Method Selection

Choose HTTP method based on operation semantics.

| Method | Use Case | Idempotent | Safe | Request Body | Success Codes |
|--------|----------|------------|------|--------------|---------------|
| **POST** | Create resource (non-idempotent) | ❌ No | ❌ No | ✅ Yes | 201, 202 |
| **PUT** | Replace resource (full update) | ✅ Yes | ❌ No | ✅ Yes | 200, 204 |
| **PATCH** | Partial update | ⚠️ Conditional | ❌ No | ✅ Yes | 200 |
| **DELETE** | Remove resource | ✅ Yes | ❌ No | ❌ Optional | 200, 204 |

**Examples**:
```csharp
// POST: Create new order (each POST creates new order, not idempotent)
var createMessage = new CreateOrderMessage { Items = [...] };
await _provider.ExecuteAsync(createMessage); // 201 Created, Location: /orders/123

// PUT: Replace entire user (same PUT = same result, idempotent)
var replaceMessage = new ReplaceUserMessage { UserId = "123", Name = "Alice", Email = "..." };
await _provider.ExecuteAsync(replaceMessage); // 200 OK or 204 No Content

// PATCH: Update only price (idempotent if designed carefully)
var updateMessage = new UpdatePriceMessage { ProductId = "456", Price = 29.99m };
await _provider.ExecuteAsync(updateMessage); // 200 OK

// DELETE: Remove product (same DELETE = same result, idempotent)
var deleteMessage = new DeleteProductMessage { ProductId = "789" };
await _provider.ExecuteAsync(deleteMessage); // 204 No Content
```

**POST vs PUT**:
- **POST `/users`**: Create new user (generates ID server-side)
- **PUT `/users/123`**: Replace user 123 (client specifies ID)

### Pattern 2: Content Type Selection

Choose serialization format based on API requirements.

#### JSON (application/json)
**Best For**: Modern REST APIs, JavaScript clients, human-readable  
**Pros**: Widely supported, compact, easy to debug  
**Cons**: No schema enforcement (use JSON Schema)

```csharp
public sealed class JsonConfig : WebApiProviderConfiguration
{
    public override string ContentType => "application/json";
    public override SerializerType SerializerType => SerializerType.Json;
}
```

#### XML (application/xml)
**Best For**: Legacy systems, SOAP interop, strong schema (XSD)  
**Pros**: Schema validation, namespaces, attributes  
**Cons**: Verbose, slower parsing, harder to debug

```csharp
public sealed class XmlConfig : WebApiProviderConfiguration
{
    public override string ContentType => "application/xml";
    public override SerializerType SerializerType => SerializerType.Xml;
}
```

#### Form-Urlencoded (application/x-www-form-urlencoded)
**Best For**: Simple key-value data, HTML forms  
**Pros**: Universal support, simple  
**Cons**: No nested objects, URL encoding overhead

```csharp
public sealed class FormMessage : WebApiProviderMessage
{
    public string Username { get; init; }
    public string Password { get; init; }
}

// Serializes to: username=alice&password=secret123
```

#### Multipart/Form-Data
**Best For**: File uploads with metadata  
**Pros**: Binary files, mixed content types  
**Cons**: Complex parsing, larger payload

```csharp
public sealed class MultipartConfig : WebApiProviderConfiguration
{
    public override string ContentType => "multipart/form-data";
}
```

### Pattern 3: Polly Resilience Strategies

Layer policies for comprehensive resilience.

**Policy Stack**:
```
Request
  │
  ├─ Retry Policy (outer)
  │   └─ Attempts: 3
  │   └─ Backoff: 2^n seconds + jitter
  │   └─ Conditions: 5xx, timeout
  │
  ├─ Circuit Breaker (middle)
  │   └─ Threshold: 5 failures in 30s
  │   └─ Break Duration: 30s
  │   └─ Half-Open Test: Single request
  │
  ├─ Timeout Policy (inner)
  │   └─ Per-Request: 30s
  │   └─ Fail Fast: TaskCanceledException
  │
  └─ HttpClient
      └─ SendAsync(request)
```

**Configuration**:
```json
{
  "RetryPolicy": {
    "Enabled": true,
    "MaxAttempts": 3,
    "BackoffMultiplier": 2.0,
    "UseJitter": true,
    "RetryableStatusCodes": [500, 502, 503, 504]
  },
  "CircuitBreaker": {
    "Enabled": true,
    "FailureThreshold": 5,
    "SamplingDuration": "00:00:30",
    "BreakDuration": "00:00:30"
  },
  "Timeout": {
    "PerRequestTimeout": "00:00:30"
  }
}
```

**Execution Flow**:
```
Attempt 1: Retry wraps Circuit Breaker wraps Timeout wraps HTTP
  └─ Timeout (30s) → Circuit Breaker (closed) → HTTP → 503 Error
  └─ Retry waits 2s (2^1 + jitter)

Attempt 2:
  └─ Timeout → Circuit Breaker (4/5 failures) → HTTP → Timeout (30s exceeded)
  └─ Retry waits 4s (2^2 + jitter)

Attempt 3:
  └─ Timeout → Circuit Breaker (5/5 failures, OPEN) → BrokenCircuitException
  └─ Retry gives up (max attempts)

Next Request (within 30s):
  └─ Circuit Breaker OPEN → Immediate failure (no HTTP call)

After 30s Break:
  └─ Circuit Breaker HALF-OPEN → Test request
  └─ If success → CLOSED (resume normal)
  └─ If failure → OPEN again (wait 30s)
```

### Pattern 4: Authentication Strategies

Implement various authentication patterns.

#### Bearer Token (Static)
```json
{
  "Authentication": {
    "Type": "Bearer",
    "Token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
  }
}
```

#### Bearer Token (Environment Variable)
```json
{
  "Authentication": {
    "Type": "Bearer",
    "Token": "${JWT_TOKEN}"
  }
}
```

#### Basic Authentication
```json
{
  "Authentication": {
    "Type": "Basic",
    "Username": "admin",
    "Password": "${API_PASSWORD}"
  }
}
```
**HTTP**: `Authorization: Basic YWRtaW46cGFzc3dvcmQ=` (base64 of `admin:password`)

#### OAuth2 Client Credentials
```json
{
  "Authentication": {
    "Type": "OAuth2",
    "OAuth2": {
      "TokenEndpoint": "https://auth.example.com/oauth/token",
      "ClientId": "my-client-id",
      "ClientSecret": "${OAUTH_SECRET}",
      "GrantType": "client_credentials",
      "Scope": "write:orders"
    }
  }
}
```

#### API Key (Header)
```json
{
  "Authentication": {
    "Type": "ApiKey",
    "ApiKeyLocation": "Header",
    "ApiKeyName": "X-API-Key",
    "Token": "${API_KEY}"
  }
}
```
**HTTP**: `X-API-Key: sk_live_abc123...`

#### API Key (Query Parameter)
```json
{
  "Authentication": {
    "Type": "ApiKey",
    "ApiKeyLocation": "QueryParameter",
    "ApiKeyName": "api_key",
    "Token": "${API_KEY}"
  }
}
```
**HTTP**: `POST /v1/data?api_key=sk_live_abc123`

### Pattern 5: Compression (Request/Response)

Enable compression to reduce payload size and network usage.

**Configuration**:
```json
{
  "Compression": "GZip",
  "Headers": {
    "Accept-Encoding": "gzip, deflate, br"
  }
}
```

**Request Compression** (send compressed body):
```csharp
private async Task<HttpRequestMessage> BuildCompressedRequestAsync(TMessage message)
{
    var json = JsonSerializer.Serialize(message);
    var bytes = Encoding.UTF8.GetBytes(json);
    
    using var inputStream = new MemoryStream(bytes);
    using var outputStream = new MemoryStream();
    using (var gzip = new GZipStream(outputStream, CompressionMode.Compress))
    {
        await inputStream.CopyToAsync(gzip);
    }
    
    var request = new HttpRequestMessage(Configuration.HttpMethod, GetFullUrl());
    request.Content = new ByteArrayContent(outputStream.ToArray());
    request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
    request.Content.Headers.ContentEncoding.Add("gzip");
    
    return request;
}
```

**HTTP Request**:
```http
POST /v1/events HTTP/1.1
Content-Type: application/json
Content-Encoding: gzip
Content-Length: 324

<gzip compressed binary data>
```

**Response Compression** (automatic with `DecompressionMethods`):
```csharp
var handler = new SocketsHttpHandler
{
    AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate | DecompressionMethods.Brotli
};

// HttpClient automatically decompresses responses
```

**Compression Comparison**:
| Algorithm | Ratio | CPU | Speed | Use Case |
|-----------|-------|-----|-------|----------|
| **gzip** | 60-70% | Medium | Fast | General (default) |
| **deflate** | 60-70% | Medium | Fast | Legacy |
| **brotli** | 70-80% | High | Slower | Static content, CPU available |

### Pattern 6: Idempotency Keys (Prevent Duplicate POST)

Use idempotency keys to safely retry POST requests without creating duplicates.

**Configuration**:
```json
{
  "IdempotencyKey": true,
  "IdempotencyKeyHeader": "Idempotency-Key"
}
```

**Implementation**:
```csharp
public sealed class CreatePaymentMessage : WebApiProviderMessage
{
    public string IdempotencyKey { get; init; } = Guid.NewGuid().ToString();
    public required decimal Amount { get; init; }
    public required string Currency { get; init; }
}
```

**Usage**:
```csharp
var message = new CreatePaymentMessage
{
    IdempotencyKey = "payment_abc123", // Client-generated unique ID
    Amount = 100.00m,
    Currency = "USD"
};

// First request
await _provider.ExecuteAsync(message); // 201 Created, payment processed

// Retry (network timeout, unclear if first succeeded)
await _provider.ExecuteAsync(message); // 200 OK, returns existing payment (not duplicate)
```

**HTTP Requests**:
```http
# First request
POST /v1/payments HTTP/1.1
Idempotency-Key: payment_abc123
Content-Type: application/json

{"amount": 100.00, "currency": "USD"}

# Response
HTTP/1.1 201 Created
{"paymentId": "pay_xyz789", "status": "processed"}

# Retry with same idempotency key
POST /v1/payments HTTP/1.1
Idempotency-Key: payment_abc123
Content-Type: application/json

{"amount": 100.00, "currency": "USD"}

# Response (same payment, not duplicate)
HTTP/1.1 200 OK
{"paymentId": "pay_xyz789", "status": "processed"}
```

**Server Logic**:
1. Check if `Idempotency-Key` exists in cache/database
2. If exists: Return cached response (200 OK)
3. If not exists: Process request, cache response, return 201 Created

**Best Practices**:
- Generate key client-side (GUID, UUID)
- Use semantic keys: `order_{orderId}`, `payment_{transactionId}`
- Server caches key + response for 24 hours
- Only for non-idempotent operations (POST, not PUT/DELETE)

### Pattern 7: Response Handling (Status Codes)

Handle various HTTP response scenarios.

**Success (2xx)**:
```csharp
private async Task ValidateResponseAsync(HttpResponseMessage response)
{
    if (response.IsSuccessStatusCode)
    {
        Logger.LogInformation(
            "Request succeeded: {Method} {Url} → {StatusCode}",
            Configuration.HttpMethod,
            GetFullUrl(),
            (int)response.StatusCode
        );
        
        // Optionally parse response body
        if (response.Content.Headers.ContentLength > 0)
        {
            var body = await response.Content.ReadAsStringAsync();
            Logger.LogDebug("Response body: {Body}", body);
        }
        
        return;
    }
    
    // Handle errors (see below)
}
```

**Client Errors (4xx)**: Don't retry, throw exception
```csharp
if ((int)response.StatusCode >= 400 && (int)response.StatusCode < 500)
{
    var body = await response.Content.ReadAsStringAsync();
    
    throw (int)response.StatusCode switch
    {
        400 => new BadRequestException($"Bad Request: {body}"),
        401 => new UnauthorizedException("Authentication failed"),
        403 => new ForbiddenException("Insufficient permissions"),
        404 => new NotFoundException($"Resource not found: {GetFullUrl()}"),
        409 => new ConflictException($"Resource conflict: {body}"),
        422 => new ValidationException($"Validation failed: {body}"),
        429 => new RateLimitExceededException("Rate limit exceeded", GetRetryAfter(response)),
        _ => new HttpRequestException($"Client error {(int)response.StatusCode}: {body}")
    };
}
```

**Server Errors (5xx)**: Retry with backoff
```csharp
if ((int)response.StatusCode >= 500)
{
    // Polly retry policy handles this automatically
    // Log and throw, Polly will catch and retry
    Logger.LogWarning(
        "Server error {StatusCode}, will retry",
        (int)response.StatusCode
    );
    
    throw new HttpRequestException(
        $"Server error {(int)response.StatusCode}: {await response.Content.ReadAsStringAsync()}"
    );
}
```

## Performance

### Connection Pooling
**SocketsHttpHandler** reuses TCP connections:
```csharp
var handler = new SocketsHttpHandler
{
    MaxConnectionsPerServer = 20,            // Max concurrent connections
    PooledConnectionLifetime = TimeSpan.FromMinutes(10), // Force refresh
    PooledConnectionIdleTimeout = TimeSpan.FromSeconds(90) // Close idle
};
```

**Benefits**:
- Avoid TCP handshake (3-way: SYN, SYN-ACK, ACK) = ~50ms saved
- Reuse TLS session (skip handshake) = ~100ms saved
- Lower server load (fewer connections)

**Metrics**:
- Without pooling: ~150ms overhead per request
- With pooling: ~5ms overhead (reuse existing connection)

### HTTP/2 Multiplexing
Multiple requests over single TCP connection:
```csharp
var handler = new SocketsHttpHandler
{
    EnableMultipleHttp2Connections = true // Allow multiple HTTP/2 connections per server
};

var client = new HttpClient(handler)
{
    DefaultRequestVersion = new Version(2, 0),
    DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
};
```

**Benefits**:
- **Header Compression**: HPACK reduces header size (40-90% reduction)
- **Stream Multiplexing**: 100+ concurrent requests on 1 connection
- **Server Push**: Proactive resource delivery

**Performance**:
- HTTP/1.1 (6 connections): 6 concurrent requests
- HTTP/2 (1 connection): 100+ concurrent requests

### Compression
Reduce payload size:
```csharp
// Request compression (manual)
request.Content = new GZipContent(jsonContent);

// Response compression (automatic)
var handler = new SocketsHttpHandler
{
    AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Brotli
};
```

**Typical Compression Ratios**:
- JSON: 60-70% reduction (1 MB → 300-400 KB)
- XML: 70-80% reduction (1 MB → 200-300 KB)
- Binary (Protobuf): Already compact, minimal benefit

**Trade-off**: CPU (compression/decompression) vs Network (bandwidth/latency)

### Keep-Alive
Persistent HTTP connections:
```http
Connection: keep-alive
Keep-Alive: timeout=60, max=100
```

**Benefits**:
- Reuse TCP connection (avoid handshake)
- Reduce latency (50-100ms saved per request)

**Configuration**:
```csharp
request.Headers.Connection.Add("keep-alive");
request.Headers.Add("Keep-Alive", "timeout=60, max=100");
```

## Best Practices

### ✅ Do
- **Choose correct HTTP method**: POST (create), PUT (replace), PATCH (update), DELETE (remove)
- **Implement retry policies**: Transient failures are common (500-503, timeouts)
- **Use circuit breakers**: Prevent cascade failures to unhealthy APIs
- **Configure timeouts**: Fail fast on slow requests (30s typical)
- **Enable compression**: Reduce payload size (gzip/brotli)
- **Use connection pooling**: Reuse TCP connections (lower latency)
- **Implement idempotency keys**: Safely retry POST without duplicates
- **Propagate trace context**: Distributed tracing (W3C Trace Context, OpenTelemetry)
- **Validate responses**: Check status codes, parse error bodies
- **Secure credentials**: Environment variables, secret management (Azure Key Vault, AWS Secrets Manager)
- **Log requests/responses**: Debugging, auditing (sanitize sensitive data)

### ❌ Don't
- **Don't ignore retry policies**: Network failures happen (configure exponential backoff)
- **Don't use GET for mutations**: Use POST/PUT/PATCH/DELETE (GET must be safe, read-only)
- **Don't skip circuit breakers**: Wasted requests to unhealthy APIs cause cascade failures
- **Don't hardcode URLs**: Use configuration (different environments: dev, staging, prod)
- **Don't expose secrets in logs**: Sanitize Authorization headers, API keys, passwords
- **Don't ignore 4xx errors**: Client errors don't benefit from retry (fix request)
- **Don't retry POST without idempotency keys**: Risk duplicate operations (payments, orders)
- **Don't skip input validation**: Validate before sending (avoid 400 Bad Request)
- **Don't ignore rate limits**: Respect 429 responses, use `Retry-After` header
- **Don't over-configure**: Start with defaults, tune based on metrics

## Troubleshooting

### Issue: 401 Unauthorized (Authentication Failed)
**Symptoms**: 401 errors, "Invalid token" messages  
**Causes**: Expired token, invalid credentials, wrong scope  
**Solutions**:
1. Verify `Authentication.Token` or credentials in configuration
2. Check token expiration (`exp` claim in JWT)
3. Implement automatic token refresh (OAuth2AuthHandler)
4. Validate scopes/permissions required by API
5. Test credentials manually (curl, Postman)

### Issue: 4xx Client Errors (Bad Request, Not Found)
**Symptoms**: 400/404 errors, validation messages  
**Causes**: Invalid request data, wrong endpoint URL, missing required fields  
**Solutions**:
1. Validate request data before sending
2. Check endpoint URL (typos, version, path parameters)
3. Review API documentation (required fields, data types, constraints)
4. Log request body, inspect what was sent
5. Test with API provider's examples (curl, Postman)

### Issue: 5xx Server Errors (High Failure Rate)
**Symptoms**: Frequent 500/503 errors, circuit breaker opening  
**Causes**: API overload, database issues, recent deployment  
**Solutions**:
1. Check API status page, recent deployments
2. Increase retry delays (MaxDelay: 30s → 60s)
3. Increase circuit breaker threshold (FailureThreshold: 5 → 10)
4. Reduce request rate (backoff during peak hours)
5. Contact API provider if persistent

### Issue: Timeout Errors (TaskCanceledException)
**Symptoms**: Requests timing out, "Operation canceled" exceptions  
**Causes**: Slow API queries, network latency, timeout too short  
**Solutions**:
1. Increase timeout (PerRequestTimeout: 30s → 60s)
2. Optimize API queries (indexes, caching, pagination)
3. Check network latency (traceroute, ping)
4. Use regional endpoints (reduce distance)
5. Implement hedging (parallel requests, use first response)

### Issue: High Memory Usage (OutOfMemoryException)
**Symptoms**: Memory growth, GC pressure, slow performance  
**Causes**: Large request bodies, connection leaks, no connection recycling  
**Solutions**:
1. Limit request body size (paginate, split large payloads)
2. Set `PooledConnectionLifetime` (force connection refresh every 10min)
3. Dispose HttpClient properly (use DI, not manual creation)
4. Monitor connection pool metrics (active, idle connections)
5. Use streaming for large files (avoid loading into memory)

### Issue: Duplicate Operations (Double POST)
**Symptoms**: Duplicate orders, payments, records  
**Causes**: Retry without idempotency key, network timeout ambiguity  
**Solutions**:
1. Implement idempotency keys (client-generated unique IDs)
2. Configure server to cache idempotency key + response (24 hours)
3. Use PUT instead of POST when possible (idempotent by nature)
4. Check operation status before retry (GET /orders/{id})
5. Log request IDs, correlate duplicates

## Related Documentation

- **[Feeders.WebApi](../Feeders.WebApi/README.md)**: Feeder for HTTP polling (GET requests)
- **[WebApi System Overview](../README.md)**: REST concepts, architecture, Polly policies
- **[SharedKernel Providers](../../SharedKernel/Providers.DotNet.SharedKernel/README.md)**: `AbstractProvider` base class
- **[ThunderPropagator Framework](../../README.md)**: Core framework documentation

## References

- **Microsoft.Extensions.Http**: [HttpClient Factory](https://learn.microsoft.com/en-us/dotnet/core/extensions/httpclient-factory)
- **Polly**: [Resilience Policies](https://www.pollydocs.org/)
- **RFC 7230-7235**: HTTP/1.1 Specification
- **RFC 7540**: HTTP/2 Specification
- **RFC 7519**: JSON Web Token (JWT)
- **RFC 6749**: OAuth 2.0 Authorization Framework
- **RFC 5789**: PATCH Method for HTTP
- **RFC 6902**: JSON Patch (application/json-patch+json)

---

**Version**: 1.0.1-beta.2  
**Last Updated**: December 2025  
**Maintainer**: ThunderPropagator Team
