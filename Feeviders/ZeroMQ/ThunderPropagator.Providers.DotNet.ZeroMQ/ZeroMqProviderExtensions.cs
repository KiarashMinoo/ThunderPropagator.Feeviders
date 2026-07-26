using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.ZeroMQ
{
    public static class ZeroMqProviderExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.zeromq");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.zeromq");
        internal static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>("thunderpropagator.feeviders.zeromq.messages.published");
        internal static readonly Counter<long> MessagesPublishFailed = Meter.CreateCounter<long>("thunderpropagator.feeviders.zeromq.messages.publish.failed");
        internal static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>("thunderpropagator.feeviders.zeromq.publish.duration", unit: "ms");

        public static IServiceCollection AddZeroMqProvider<TZeroMqProviderMessage, TZeroMqProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TZeroMqProviderMessage : ZeroMqProviderMessage
            where TZeroMqProviderConfiguration : ZeroMqProviderConfiguration, new()
        {
            TZeroMqProviderConfiguration zeroMqProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(zeroMqProviderConfiguration);
            services.TryAddSingleton(zeroMqProviderConfiguration);

            services.AddChannelProvider<ZeroMqProvider<TZeroMqProviderMessage, TZeroMqProviderConfiguration>, TZeroMqProviderMessage, TZeroMqProviderConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }
    }
}
