using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Channels.Metadata;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application;

namespace ThunderPropagator.UnitTests.IsEnabledGuards
{
    internal static class FeederTestHarness
    {
        public static IChannel CreateChannel(string channelName = "test-channel")
        {
            var metadata = Substitute.For<IChannelMetadata>();
            metadata.ChannelName.Returns(channelName);

            var channel = Substitute.For<IChannel>();
            channel.Metadata.Returns(metadata);

            return channel;
        }

        public static IFeederHandler<TChannel, TFeederMessage> CreateHandler<TChannel, TFeederMessage>()
            where TChannel : class, IChannel
            where TFeederMessage : FeederMessage
            => Substitute.For<IFeederHandler<TChannel, TFeederMessage>>();

        public static IServiceProvider CreateServiceProvider<TFeederMessage, TFeederConfiguration>(bool includeHostApplicationLifetime = false)
            where TFeederMessage : FeederMessage
            where TFeederConfiguration : class, IAbstractFeederConfiguration
        {
            var services = new ServiceCollection();
            services.AddLogging();

            FeederMessageDeserializerResolver<TFeederMessage, TFeederConfiguration> resolver =
                _ => Substitute.For<IFeederMessageDeserializer<TFeederMessage, TFeederConfiguration>>();
            services.AddSingleton(resolver);

            if (includeHostApplicationLifetime)
                services.AddSingleton(Substitute.For<IHostApplicationLifetime>());

            return services.BuildServiceProvider();
        }
    }
}
