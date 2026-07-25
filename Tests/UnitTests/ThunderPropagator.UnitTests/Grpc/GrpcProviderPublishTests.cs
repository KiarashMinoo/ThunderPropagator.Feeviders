using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using ThunderPropagator.Feeviders.Grpc.SharedKernel.Protos;
using ThunderPropagator.Providers.DotNet.Grpc;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.UnitTests.Grpc
{
    public class GrpcProviderPublishTests
    {
        internal sealed class TestGrpcProviderMessage : GrpcProviderMessage;

        internal sealed class TestGrpcProviderConfiguration : GrpcProviderConfiguration;

        private static IServiceProvider CreateServiceProvider(byte[] serializedBytes)
        {
            var services = new ServiceCollection();
            services.AddLogging();

            var serializer = Substitute.For<IFeederMessageSerializer<TestGrpcProviderMessage, TestGrpcProviderConfiguration>>();
            serializer.SerializeToBytes(Arg.Any<TestGrpcProviderMessage>(), Arg.Any<CancellationToken>()).Returns(serializedBytes);
            services.AddSingleton(serializer);

            return services.BuildServiceProvider();
        }

        private static AsyncUnaryCall<GrpcPublishAck> CreateSuccessfulCall()
        {
            return new AsyncUnaryCall<GrpcPublishAck>(
                Task.FromResult(new GrpcPublishAck { Success = true }),
                Task.FromResult(new Metadata()),
                () => Status.DefaultSuccess,
                () => new Metadata(),
                () => { });
        }

        [Fact]
        public async Task ExecuteAsync_ShouldPublishSerializedPayloadToConfiguredTopic()
        {
            var payload = "message-bytes"u8.ToArray();
            var serviceProvider = CreateServiceProvider(payload);

            var client = Substitute.For<GrpcFeeviderService.GrpcFeeviderServiceClient>();
            client.PublishAsync(Arg.Any<GrpcEnvelope>(), Arg.Any<Metadata>(), Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
                .Returns(CreateSuccessfulCall());

            var configuration = new TestGrpcProviderConfiguration { Topic = "orders" };
            var provider = new GrpcProvider<TestGrpcProviderMessage, TestGrpcProviderConfiguration>(configuration, serviceProvider, client);

            await provider.ExecuteAsync(new TestGrpcProviderMessage());

            _ = client.Received(1).PublishAsync(
                Arg.Is<GrpcEnvelope>(envelope => envelope.Topic == "orders" && envelope.Payload.ToByteArray().SequenceEqual(payload)),
                Arg.Any<Metadata>(),
                Arg.Any<DateTime?>(),
                Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task ExecuteAsync_ShouldRethrowAndNotSwallowPublishFailures()
        {
            var serviceProvider = CreateServiceProvider([]);

            var client = Substitute.For<GrpcFeeviderService.GrpcFeeviderServiceClient>();
            client.PublishAsync(Arg.Any<GrpcEnvelope>(), Arg.Any<Metadata>(), Arg.Any<DateTime?>(), Arg.Any<CancellationToken>())
                .Throws(new RpcException(new Status(StatusCode.Unavailable, "broker down")));

            var configuration = new TestGrpcProviderConfiguration { Topic = "orders" };
            var provider = new GrpcProvider<TestGrpcProviderMessage, TestGrpcProviderConfiguration>(configuration, serviceProvider, client);

            await Assert.ThrowsAsync<RpcException>(() => provider.ExecuteAsync(new TestGrpcProviderMessage()));
        }
    }
}
