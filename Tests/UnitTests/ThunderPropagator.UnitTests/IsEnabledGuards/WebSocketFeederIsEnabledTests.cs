using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Feeders.WebSocket;

namespace ThunderPropagator.UnitTests.IsEnabledGuards
{
    public class WebSocketFeederIsEnabledTests
    {
        private static TestWebSocketFeederConfiguration CreateConfiguration(bool isEnabled) => new()
        {
            IsEnabled = isEnabled,
            Path = "/ws"
        };

        private static (RequestDelegate Pipeline, HttpContext Context) BuildPipeline(TestWebSocketFeederConfiguration configuration)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton(configuration);
            var serviceProvider = services.BuildServiceProvider();

            var applicationBuilder = new ApplicationBuilder(serviceProvider);
            applicationBuilder.UseWebSocketFeeder<IChannel, TestWebSocketFeederMessage, TestWebSocketFeederConfiguration>();
            var pipeline = applicationBuilder.Build();

            var context = new DefaultHttpContext { RequestServices = serviceProvider };
            context.Request.Path = "/ws";

            return (pipeline, context);
        }

        [Fact]
        public async Task Middleware_ShouldRejectWithServiceUnavailable_WhenDisabled()
        {
            var (pipeline, context) = BuildPipeline(CreateConfiguration(isEnabled: false));

            await pipeline(context);

            Assert.Equal(StatusCodes.Status503ServiceUnavailable, context.Response.StatusCode);
        }

        [Fact]
        public async Task Middleware_ShouldProceedPastGuard_WhenEnabled()
        {
            var (pipeline, context) = BuildPipeline(CreateConfiguration(isEnabled: true));

            await pipeline(context);

            // No real IHttpWebSocketFeature is registered on this bare HttpContext, so
            // IsWebSocketRequest is false and the middleware falls through to its existing
            // "not a websocket request" branch (400) — proving the IsEnabled guard did not
            // short-circuit the request, unlike the disabled case (503) above.
            Assert.Equal(StatusCodes.Status400BadRequest, context.Response.StatusCode);
        }

        internal sealed class TestWebSocketFeederMessage : WebSocketFeederMessage;

        internal sealed class TestWebSocketFeederConfiguration : WebSocketFeederConfiguration;
    }
}
