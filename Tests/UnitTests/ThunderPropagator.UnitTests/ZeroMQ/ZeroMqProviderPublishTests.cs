using Microsoft.Extensions.DependencyInjection;
using NetMQ;
using NSubstitute;
using ThunderPropagator.Feeviders.ZeroMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using ThunderPropagator.Providers.DotNet.ZeroMQ;

namespace ThunderPropagator.UnitTests.ZeroMQ
{
    public class ZeroMqProviderPublishTests
    {
        internal sealed class TestZeroMqProviderMessage : ZeroMqProviderMessage;

        internal sealed class TestZeroMqProviderConfiguration : ZeroMqProviderConfiguration;

        // A hand-written IOutgoingSocket test double: IOutgoingSocket.TrySend takes its Msg by ref,
        // which NSubstitute's argument matchers can't stand in for at a call site, so a real
        // implementation is used instead of a mock.
        private sealed class RecordingOutgoingSocket : IOutgoingSocket
        {
            private readonly Exception? _exceptionToThrow;

            public RecordingOutgoingSocket(Exception? exceptionToThrow = null)
            {
                _exceptionToThrow = exceptionToThrow;
            }

            public List<byte[]> Frames { get; } = [];

            public bool TrySend(ref Msg msg, TimeSpan timeout, bool more)
            {
                if (_exceptionToThrow is not null)
                    throw _exceptionToThrow;

                Frames.Add(msg.ToArray());
                return true;
            }
        }

        private static IServiceProvider CreateServiceProvider(byte[] serializedBytes)
        {
            var services = new ServiceCollection();
            services.AddLogging();

            var serializer = Substitute.For<IFeederMessageSerializer<TestZeroMqProviderMessage, TestZeroMqProviderConfiguration>>();
            serializer.SerializeToBytes(Arg.Any<TestZeroMqProviderMessage>(), Arg.Any<CancellationToken>()).Returns(serializedBytes);
            services.AddSingleton(serializer);

            return services.BuildServiceProvider();
        }

        [Theory]
        [InlineData(ZeroMqSocketPattern.PubSub)]
        [InlineData(ZeroMqSocketPattern.PushPull)]
        public async Task ExecuteAsync_ShouldPublishSerializedPayloadThroughTheOutgoingSocket(ZeroMqSocketPattern pattern)
        {
            var payload = "message-bytes"u8.ToArray();
            var serviceProvider = CreateServiceProvider(payload);

            var outgoingSocket = new RecordingOutgoingSocket();

            var configuration = new TestZeroMqProviderConfiguration { SocketPattern = pattern, Topic = "orders", Endpoint = "tcp://localhost:5555" };
            var provider = new ZeroMqProvider<TestZeroMqProviderMessage, TestZeroMqProviderConfiguration>(configuration, serviceProvider, outgoingSocket);

            await provider.ExecuteAsync(new TestZeroMqProviderMessage());

            var expectedFrameCount = pattern == ZeroMqSocketPattern.PubSub ? 4 : 3;
            Assert.Equal(expectedFrameCount, outgoingSocket.Frames.Count);
            Assert.Equal(payload, outgoingSocket.Frames[^1]);

            if (pattern == ZeroMqSocketPattern.PubSub)
                Assert.Equal("orders", System.Text.Encoding.UTF8.GetString(outgoingSocket.Frames[0]));
        }

        [Fact]
        public async Task ExecuteAsync_ShouldRethrowAndNotSwallowPublishFailures()
        {
            var serviceProvider = CreateServiceProvider([]);

            var outgoingSocket = new RecordingOutgoingSocket(new InvalidOperationException("socket closed"));

            var configuration = new TestZeroMqProviderConfiguration { SocketPattern = ZeroMqSocketPattern.PushPull, Endpoint = "tcp://localhost:5555" };
            var provider = new ZeroMqProvider<TestZeroMqProviderMessage, TestZeroMqProviderConfiguration>(configuration, serviceProvider, outgoingSocket);

            await Assert.ThrowsAsync<InvalidOperationException>(() => provider.ExecuteAsync(new TestZeroMqProviderMessage()));
        }
    }
}
