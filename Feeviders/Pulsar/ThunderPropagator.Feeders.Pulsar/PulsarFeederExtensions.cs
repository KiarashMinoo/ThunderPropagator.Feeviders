using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Feeviders.Pulsar.SharedKernel;
using ThunderPropagator.Infrastructure.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Feeders.Pulsar
{
    public static class PulsarFeederExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.pulsar");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.pulsar");
        internal static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>("thunderpropagator.feeviders.pulsar.messages.received");
        internal static readonly Counter<long> MessagesReceiveFailed = Meter.CreateCounter<long>("thunderpropagator.feeviders.pulsar.messages.receive.failed");
        internal static readonly Histogram<double> ReceiveDuration = Meter.CreateHistogram<double>("thunderpropagator.feeviders.pulsar.receive.duration", unit: "ms");

        public static IServiceCollection AddPulsarFeeder<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TPulsarFeederMessage : PulsarFeederMessage
            where TPulsarFeederConfiguration : PulsarFeederConfiguration, new()
        {
            TPulsarFeederConfiguration pulsarFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(pulsarFeederConfiguration);
            services.TryAddSingleton(pulsarFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                PulsarFeeder<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>,
                TPulsarFeederMessage,
                TPulsarFeederConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IServiceCollection AddPulsarFeederResolver<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TPulsarFeederMessage : PulsarFeederMessage
            where TPulsarFeederConfiguration : PulsarFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                PulsarFeeder<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>,
                TPulsarFeederMessage,
                TPulsarFeederConfiguration>(services, (serviceProvider, channel, pulsarFeederConfiguration, feederHandler) =>
                new PulsarFeeder<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>(channel, pulsarFeederConfiguration, feederHandler, serviceProvider));
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IApplicationBuilder UsePulsarFeederResolver<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TPulsarFeederConfiguration pulsarFeederConfiguration)
            where TChannel : class, IChannel
            where TPulsarFeederMessage : PulsarFeederMessage
            where TPulsarFeederConfiguration : PulsarFeederConfiguration
        {
            var pulsarFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TPulsarFeederMessage, TPulsarFeederConfiguration>>();

            pulsarFeederManager.UseFeeder(channelKey, pulsarFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}