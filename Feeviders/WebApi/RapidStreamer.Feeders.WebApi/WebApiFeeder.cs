using RapidStreamer.Application.Channels;
using RapidStreamer.Application.Feeders;
using System.Reflection;
using RapidStreamer.Application;

namespace RapidStreamer.Feeders.WebApi
{
    internal
#if !DEBUG
        sealed
#endif
        class WebApiFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration> : DelegativeFeeder<TChannel, TWebApiFeederMessage, TWebApiFeederConfiguration>, IFeature
        where TChannel : class, IChannel
        where TWebApiFeederMessage : WebApiFeederMessage
        where TWebApiFeederConfiguration : WebApiFeederConfiguration
    {
        public WebApiFeeder(TChannel channel,
            TWebApiFeederConfiguration webApiFeederConfiguration,
            IFeederHandler<TChannel, TWebApiFeederMessage> feederHandler,
            IServiceProvider serviceProvider)
            : base(channel, webApiFeederConfiguration, feederHandler, serviceProvider)
        {
            Logger.LogInformation("{Name}/{ChannelName} on Endpoint {Endpoint} has configured.", GetType().GetTypeInfo().Name, channel.Metadata.ChannelName,
                webApiFeederConfiguration.Path);

            HealthName = $"feeder_{nameof(WebApi)}_{webApiFeederConfiguration.Path.Replace("/", "_")}";
            HealthTags = [.. HealthTags, nameof(WebApi), webApiFeederConfiguration.Path.Replace("/", "_")];
        }

        internal ValueTask EnqueueAsync(string rawMessage, CancellationToken cancellationToken = default) => ReceiveAsync(rawMessage, cancellationToken: cancellationToken);
    }
}