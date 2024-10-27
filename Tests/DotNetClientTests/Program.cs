using DotNetClientTests;
using Newtonsoft.Json;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using RapidStreamer.BuildingBlocks.Application.Objects;
using RapidStreamer.Clients.DotNet.Clients;
using RapidStreamer.Clients.DotNet.Connections.WebSocket;
using RapidStreamer.Clients.DotNet.Infrastructure.Loggers;
using RapidStreamer.Clients.DotNet.Models.Enums;

var test = new TestUsr();
test.UserSeens = Enumerable.Range(0, 10)
    .Select(i => new UserSeen()
    {
        UserId = i.ToString(),
        SeenType = (UserMessageSeenType)Random.Shared.Next(1, 2)
    })
    .ToList();

test.UserSeens.Add(new UserSeen() { UserId = 0.ToString(), SeenType = UserMessageSeenType.Seen });

var xxx = JsonConvert.SerializeObject(test);
var yyy = JsonConvert.DeserializeObject(xxx);

//JArray

var xxxx = System.Text.Json.JsonSerializer.Serialize(test);
var yyyy = System.Text.Json.JsonSerializer.Deserialize(xxxx, typeof(object));

CancellationTokenSource cancellationTokenSource = new();

//((System.Text.Json.JsonElement)System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(xxxx)["UserSeens"]).EnumerateArray()

RapidStreamerWebSocketClient client = new(new RapidStreamerWebSocketConnectionConfiguration { Uri = "ws://127.0.0.1:8080/rapidStreamer" }, new LoggerProvider());
//RapidStreamerInfiniteDataStreamClient client = new(new RapidStreamerInfiniteDataStreamConnectionConfiguration { Uri = "http://192.168.1.141:8080/rapidStreamer" }, new LoggerProvider());
// RapidStreamerWebSocketClient client = new(new RapidStreamerWebSocketConnectionConfiguration { Uri = "ws://192.168.1.141:8080/rapidStreamer" }, new LoggerProvider());
await client.ConnectAsync(cancellationTokenSource.Token);
var channel = await client.CreateChannelAsync("ClockChannel", cancellationToken: cancellationTokenSource.Token);
var subscription = channel.CreateSubscription(new Dictionary<string, string> { { "Key", "UtcNow" } }, ["Date", "Time", "DateTime"], RapidStreamerSubscriptionMode.Modified);
// subscription.FieldUpdated += (sender, item, token) =>
// {
//     Console.WriteLine($"{item.FieldName} => {item.FieldValue}");
//     return Task.CompletedTask;
// };
subscription.TableUpdated += (sender, table, row, items, recordStatus, token) =>
{
    Console.WriteLine($"{table} => {row ?? "Not Defined"}, {items.ToNJson()}");
    return Task.CompletedTask;
};
await subscription.SubscribeAsync(cancellationTokenSource.Token);

Console.ReadKey();

await cancellationTokenSource.CancelAsync();

namespace DotNetClientTests
{
    public class LoggerProvider : DisposableObject,
        ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName)
        {
            return new Logger();
        }
    }

    public class Logger : ILogger
    {
        public void Log(LogLevel logLevel, Exception? exception, string? message, params object?[] args)
        {
            Console.WriteLine(exception?.Message ?? message);
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
        {
            return null;
        }
    }

    enum UserMessageSeenType
    {
        ToastSeen = 1,
        Seen = 2
    }

    class UserSeen
    {
        public string UserId { get; set; } = null!;
        public UserMessageSeenType SeenType { get; set; }
    }

    class TestUsr
    {
        public List<UserSeen> UserSeens { get; set; } = [];
    }
}