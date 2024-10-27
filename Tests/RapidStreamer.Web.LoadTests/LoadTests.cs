using System.Net.WebSockets;
using System.Text;
using Microsoft.AspNetCore.Mvc.Testing;
using Newtonsoft.Json;
using RapidStreamer.BuildingBlocks.Application.Collections;
using RapidStreamer.BuildingBlocks.Application.Helpers;
using Xunit.Abstractions;

namespace RapidStreamer.Web.LoadTests
{
    public
#if !DEBUG
        sealed
#endif
        class LoadTests : IClassFixture<WebApplicationFactory<Program>>
    {
        private static readonly object Mutex = new();
        private static RapidStreamerApplication? _rapidStreamerApplication;

        private static RapidStreamerApplication RapidStreamerApplication
        {
            get
            {
                lock (Mutex)
                    return _rapidStreamerApplication ??= new RapidStreamerApplication();
            }
        }

        private readonly ITestOutputHelper _testOutputHelper;

        public LoadTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;

            GetServerInformation().Wait();
        }

        private async Task GetServerInformation()
        {
            var httpClient = RapidStreamerApplication.CreateClient();

            var uri = new UriBuilder(RapidStreamerApplication.Server.BaseAddress)
            {
                Path = "rapidStreamer/serverInformationModel"
            }.Uri;

            var httpResponseMessage = await httpClient.GetAsync(uri, CancellationToken.None);
            var exception = Record.Exception(() => httpResponseMessage.EnsureSuccessStatusCode());
            Assert.Null(exception);

            var content = await httpResponseMessage.Content.ReadAsStringAsync();
            Assert.NotNull(content);

            _testOutputHelper.WriteLine(content);
            _testOutputHelper.WriteLine("---");
            _testOutputHelper.WriteLine("Server warmed-up");
            _testOutputHelper.WriteLine(string.Empty);
        }

        private static async Task<int> GetConnectionsCount()
        {
            var uri = new UriBuilder(RapidStreamerApplication.Server.BaseAddress)
            {
                Path = "connections/websocket/count"
            }.Uri;

            using var httpClient = RapidStreamerApplication.CreateClient();
            var httpResponseMessage = await httpClient.GetAsync(uri, CancellationToken.None);
            var exception = Record.Exception(() => httpResponseMessage.EnsureSuccessStatusCode());
            Assert.Null(exception);

            var content = await httpResponseMessage.Content.ReadAsStringAsync();
            Assert.NotNull(content);

            var result = 0;
            exception = Record.Exception(() => result = int.Parse(content));
            Assert.Null(exception);

            return result;
        }

        private static async Task Connections_Must_Be_Removed_OnServer()
        {
            var connectionsCount = await GetConnectionsCount();
            Assert.Equal(0, connectionsCount);
        }

        private static async Task<TWebSocketResponse> InternalConnectAsync<TWebSocketResponse>()
            where TWebSocketResponse : WebSocketConnectionResponse, new()
        {
            TWebSocketResponse webSocketConnectionResponse = new();

            try
            {
                var webSocketUri = new UriBuilder(RapidStreamerApplication.Server.BaseAddress)
                {
                    Scheme = "ws",
                    Path = "ws"
                }.Uri;

                var webSocketClient = RapidStreamerApplication.Server.CreateWebSocketClient();

                webSocketConnectionResponse.StartConnectingDateTime = DateTime.UtcNow;
                webSocketConnectionResponse.WebSocket = await webSocketClient.ConnectAsync(webSocketUri, CancellationToken.None);
                webSocketConnectionResponse.EndConnectingDateTime = DateTime.UtcNow;
            }
            catch (Exception exception)
            {
                webSocketConnectionResponse.Error = exception;
            }

            return webSocketConnectionResponse;
        }

        private async Task Check_Values(WebSocketConnectionResponse[] webSocketConnectionResponses, int maxAllowedErrors, Func<Task>? beforeDisposing = null)
        {
            var min = webSocketConnectionResponses.Min(x => x.DiffConnectingDateTime);
            var max = webSocketConnectionResponses.Max(x => x.DiffConnectingDateTime);
            var avg = (long)webSocketConnectionResponses.Average(x => x.DiffConnectingDateTime);
            var errors = webSocketConnectionResponses.Where(x => x.Error is not null || x.State != WebSocketState.Open).GroupBy(x => x.Error!.Message).ToArray();

            _testOutputHelper.WriteLine($"The Minimum: {TimeSpan.FromTicks(min)}");
            _testOutputHelper.WriteLine($"The Maximum: {TimeSpan.FromTicks(max)}");
            _testOutputHelper.WriteLine($"The Average: {TimeSpan.FromTicks(avg)}");
            _testOutputHelper.WriteLine($"The Errors count: {errors.Length}");

            if (errors.Length != 0)
                errors.ForEach(error => _testOutputHelper.WriteLine($"{error.Count()} errors occured on {error.Key}"));

            var connectionsCount = await GetConnectionsCount();
            _testOutputHelper.WriteLine($"The Connections count: {connectionsCount}");

            if (beforeDisposing is not null)
                await beforeDisposing.Invoke();

            await Parallel.ForEachAsync(webSocketConnectionResponses,
                async (webSocketConnectionResponse, _) =>
                {
                    try
                    {
                        await webSocketConnectionResponse.WebSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Disposing WebSocket", CancellationToken.None);
                    }
                    catch
                    {
                        // ignored
                    }
                });

            Assert.True(errors.Length <= maxAllowedErrors, $"The total number of errors allowed while connecting to the server must be less than {maxAllowedErrors}");
            await Connections_Must_Be_Removed_OnServer();
        }

        private void WaitForNSeconds(int seconds)
        {
            ManualResetEvent manualResetEvent = new(false);

            _ = Task.Factory.StartNew(() =>
            {
                Thread.Sleep(TimeSpan.FromSeconds(seconds));
                manualResetEvent.Set();
            });

            manualResetEvent.WaitOne();
        }

        [Theory]
        [InlineData(1, 0, 1)]
        [InlineData(5, 0, 1)]
        [InlineData(10, 0, 1)]
        [InlineData(100, 0, 1)]
        [InlineData(1000, 0, 1)]
        [InlineData(1, 0, 300)]
        [InlineData(5, 0, 300)]
        [InlineData(10, 0, 300)]
        [InlineData(100, 0, 300)]
        [InlineData(1000, 0, 300)]
        public async Task SerialConnectAsync(int connectionCount, int maxAllowedErrors, int waiting)
        {
            List<WebSocketConnectionResponse> webSocketConnectionResponses = [];

            for (var i = 0; i < connectionCount; i++)
                webSocketConnectionResponses.Add(await InternalConnectAsync<WebSocketConnectionResponse>());

            WaitForNSeconds(waiting);

            await Check_Values(webSocketConnectionResponses.ToArray(), maxAllowedErrors);
        }

        [Theory]
        [InlineData(5, 0, 1)]
        [InlineData(10, 0, 1)]
        [InlineData(100, 0, 1)]
        [InlineData(1000, 0, 1)]
        [InlineData(5, 0, 300)]
        [InlineData(10, 0, 300)]
        [InlineData(100, 0, 300)]
        [InlineData(1000, 0, 300)]
        public async Task ParallelConnectAsync(int connectionCount, int maxAllowedErrors, int waiting)
        {
            BindingDictionary<int, WebSocketConnectionResponse> webSocketConnectionResponses = new(true);

            await Parallel.ForEachAsync(Enumerable.Range(0, connectionCount),
                async (index, _) => webSocketConnectionResponses.TryAdd<int, WebSocketConnectionResponse>(index, await InternalConnectAsync<WebSocketConnectionResponse>()));

            WaitForNSeconds(waiting);

            await Check_Values(webSocketConnectionResponses.Values.ToArray(), maxAllowedErrors);
        }

        private static async Task<WebSocketInteractiveResponse> InternalInteractiveAsync()
        {
            var webSocketInteractiveResponse = await InternalConnectAsync<WebSocketInteractiveResponse>();
            webSocketInteractiveResponse.RequestId = DateTime.UtcNow.ToString("O");

            var subscriptionRequest = new
            {
                requestId = webSocketInteractiveResponse.RequestId,
                route = new
                {
                    channel = "ClockChannel",
                    requestType = "Subscribe"
                },
                pushKeys = false,
                subscriptionMode = "Full",
                subscribingKeys = new[] { new Dictionary<string, string> { { "Key", "UtcNow" } } },
                subscribingFields = new[] { "Date", "Time", "DateTime" }
            };
            var requestString = JsonConvert.SerializeObject(subscriptionRequest);
            var bytes = Encoding.UTF8.GetBytes(requestString);
            await webSocketInteractiveResponse.WebSocket.SendAsync(bytes, WebSocketMessageType.Text, WebSocketMessageFlags.EndOfMessage, CancellationToken.None);

            return webSocketInteractiveResponse;
        }

        private static async Task UnsubscribeCLockChannelAsync(WebSocketConnectionResponse webSocketInteractiveResponse)
        {
            var unsubscribeRequest = new
            {
                requestId = DateTime.UtcNow.ToString("O"),
                route = new
                {
                    channel = "ClockChannel",
                    requestType = "Unsubscribe"
                }
            };
            var requestString = JsonConvert.SerializeObject(unsubscribeRequest);
            var bytes = Encoding.UTF8.GetBytes(requestString);
            await webSocketInteractiveResponse.WebSocket.SendAsync(bytes, WebSocketMessageType.Text, WebSocketMessageFlags.EndOfMessage, CancellationToken.None);
        }

        private static void Subscription_Should_Received_Messages_Between_9_And_11(IEnumerable<WebSocketInteractiveResponse> webSocketInteractiveResponses)
        {
            var receivedMessages = webSocketInteractiveResponses.SelectMany(x =>
            {
                var receivedMessages = x.ReceivedMessage.GroupBy(r => new TimeSpan(0, r.Key.Hour, r.Key.Minute, r.Key.Second)).Select(r => new
                {
                    Time = r.Key,
                    Count = r.Count(),
                    First = r.First().Key.TimeOfDay,
                    Last = r.Last().Key.TimeOfDay,
                    Average = TimeSpan.FromTicks((long)r.Average(z => z.Key.TimeOfDay.Ticks)),
                    Messages = r.Select(z => z.Value).ToArray()
                }).OrderBy(r => r.Time).ToArray();

                return receivedMessages.Skip(1).Take(receivedMessages.Length - 2).ToArray();
            }).OrderBy(x => x.Time).ToArray();

            Assert.DoesNotContain(receivedMessages, x => x.Count is < 9 or > 11);
        }

        [Theory]
        [InlineData(1, 0, 10)]
        [InlineData(5, 0, 10)]
        [InlineData(10, 0, 10)]
        [InlineData(100, 0, 10)]
        [InlineData(1000, 0, 10)]
        [InlineData(1, 0, 300)]
        [InlineData(5, 0, 300)]
        [InlineData(10, 0, 300)]
        [InlineData(100, 0, 300)]
        [InlineData(1000, 0, 300)]
        public async Task SerialInteractiveAsync(int connectionCount, int maxAllowedErrors, int waiting)
        {
            List<WebSocketInteractiveResponse> webSocketInteractiveResponses = [];

            for (var i = 0; i < connectionCount; i++)
                webSocketInteractiveResponses.Add(await InternalInteractiveAsync());

            WaitForNSeconds(waiting);

            await Check_Values(webSocketInteractiveResponses.Cast<WebSocketConnectionResponse>().ToArray(), maxAllowedErrors, async () =>
            {
                await Parallel.ForEachAsync(webSocketInteractiveResponses,
                    async (webSocketInteractiveResponse, _) => await UnsubscribeCLockChannelAsync(webSocketInteractiveResponse));
            });

            Subscription_Should_Received_Messages_Between_9_And_11(webSocketInteractiveResponses);
        }

        [Theory]
        [InlineData(5, 0, 10)]
        [InlineData(10, 0, 10)]
        [InlineData(100, 0, 10)]
        [InlineData(1000, 0, 10)]
        [InlineData(5, 0, 300)]
        [InlineData(10, 0, 300)]
        [InlineData(100, 0, 300)]
        [InlineData(1000, 0, 300)]
        public async Task ParallelInteractiveAsync(int connectionCount, int maxAllowedErrors, int waiting)
        {
            BindingDictionary<int, WebSocketInteractiveResponse> webSocketInteractiveResponses = new(true);

            await Parallel.ForEachAsync(Enumerable.Range(0, connectionCount),
                async (index, _) => webSocketInteractiveResponses.TryAdd<int, WebSocketInteractiveResponse>(index, await InternalInteractiveAsync()));

            WaitForNSeconds(waiting);

            await Check_Values(webSocketInteractiveResponses.Values.Cast<WebSocketConnectionResponse>().ToArray(), maxAllowedErrors, async () =>
            {
                await Parallel.ForEachAsync(webSocketInteractiveResponses.Values,
                    async (webSocketInteractiveResponse, _) => await UnsubscribeCLockChannelAsync(webSocketInteractiveResponse));
            });

            Subscription_Should_Received_Messages_Between_9_And_11(webSocketInteractiveResponses.Values);
        }
    }
}