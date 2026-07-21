using NSubstitute;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.UnitTests
{
    public class FeederMessageSerializerTests
    {
        private static readonly SerializerType UnregisteredSerializerType = new(int.MaxValue);

        public static TheoryData<SerializerType> SerializerTypes
            => new(JsonFormatSerializer.Json, NJsonFormatSerializer.NJson);

        [Theory]
        [MemberData(nameof(SerializerTypes))]
        public void Constructor_ShouldResolveEverySerializerTypeFromRegistry(SerializerType serializerType)
        {
            var registry = Substitute.For<IFormatSerializerRegistry>();
            registry.GetSerializer(serializerType).Returns(new RecordingFormatSerializer(serializerType));

            _ = new FeederMessageSerializer<TestProviderMessage, TestProviderConfiguration>(
                new TestProviderConfiguration { SerializerType = serializerType },
                registry);

            registry.Received(1).GetSerializer(serializerType);
        }

        [Fact]
        public void Serialize_ShouldDelegateToResolvedFormatSerializer()
        {
            var formatSerializer = new RecordingFormatSerializer(JsonFormatSerializer.Json);
            var serializer = CreateSerializer(formatSerializer);
            var message = new TestProviderMessage();

            var result = serializer.Serialize(message);

            Assert.Equal(RecordingFormatSerializer.SerializedText, result);
            Assert.Same(message, formatSerializer.LastInstance);
        }

        [Fact]
        public void SerializeToBytes_ShouldDelegateToResolvedFormatSerializer()
        {
            var formatSerializer = new RecordingFormatSerializer(NJsonFormatSerializer.NJson);
            var serializer = CreateSerializer(formatSerializer);
            var message = new TestProviderMessage();

            var result = serializer.SerializeToBytes(message);

            Assert.Equal(RecordingFormatSerializer.SerializedBytes, result);
            Assert.Same(message, formatSerializer.LastInstance);
        }

        [Fact]
        public void Constructor_ShouldDescribeMissingSerializerRegistration()
        {
            var registry = Substitute.For<IFormatSerializerRegistry>();
            registry.GetSerializer(UnregisteredSerializerType)
                .Returns(_ => throw new InvalidOperationException("Serializer is missing."));

            var exception = Assert.Throws<InvalidOperationException>(() =>
                new FeederMessageSerializer<TestProviderMessage, TestProviderConfiguration>(
                    new TestProviderConfiguration { SerializerType = UnregisteredSerializerType },
                    registry));

            Assert.Contains(UnregisteredSerializerType.ToString(), exception.Message);
            Assert.Contains(nameof(TestProviderConfiguration), exception.Message);
            Assert.IsType<InvalidOperationException>(exception.InnerException);
        }

        [Fact]
        public void AddChannelProvider_ShouldValidateSerializerWhenHostedServicesAreResolved()
        {
            var registry = Substitute.For<IFormatSerializerRegistry>();
            registry.GetSerializer(UnregisteredSerializerType)
                .Returns(_ => throw new InvalidOperationException("Serializer is missing."));
            var services = new ServiceCollection();
            services.AddSingleton<IFormatSerializerRegistry>(registry);
            services.AddSingleton(new TestProviderConfiguration { SerializerType = UnregisteredSerializerType });
            services.AddChannelProvider<TestProvider, TestProviderMessage, TestProviderConfiguration>();

            using var serviceProvider = services.BuildServiceProvider();

            var exception = Assert.Throws<InvalidOperationException>(() =>
                serviceProvider.GetServices<IHostedService>().ToArray());
            Assert.Contains(UnregisteredSerializerType.ToString(), exception.Message);
            Assert.Contains(nameof(TestProviderConfiguration), exception.Message);
        }

        private static FeederMessageSerializer<TestProviderMessage, TestProviderConfiguration> CreateSerializer(
            RecordingFormatSerializer formatSerializer)
        {
            var registry = Substitute.For<IFormatSerializerRegistry>();
            registry.GetSerializer(formatSerializer.SerializerType).Returns(formatSerializer);
            return new FeederMessageSerializer<TestProviderMessage, TestProviderConfiguration>(
                new TestProviderConfiguration { SerializerType = formatSerializer.SerializerType },
                registry);
        }

        private sealed class RecordingFormatSerializer(SerializerType serializerType) : IFormatSerializer
        {
            public const string SerializedText = "serialized";
            public static readonly byte[] SerializedBytes = [1, 2, 3];

            public SerializerType SerializerType { get; } = serializerType;
            public string MediaType => "application/test";
            public object? LastInstance { get; private set; }

            public string Serialize<T>(T instance)
            {
                LastInstance = instance;
                return SerializedText;
            }

            public byte[] SerializeToBytes<T>(T instance)
            {
                LastInstance = instance;
                return SerializedBytes;
            }
        }

        internal sealed class TestProviderMessage : FeederMessage;

        private sealed class TestProviderConfiguration : AbstractProviderConfiguration;

        private sealed class TestProvider(IServiceProvider serviceProvider)
            : AbstractProvider<TestProviderMessage, TestProviderConfiguration>(serviceProvider)
        {
            protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
                => Task.CompletedTask;
        }
    }
}
