using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.Grpc
{
    public static class GrpcProviderExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.grpc");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.grpc");
        internal static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>("thunderpropagator.feeviders.grpc.messages.published");
        internal static readonly Counter<long> MessagesPublishFailed = Meter.CreateCounter<long>("thunderpropagator.feeviders.grpc.messages.publish.failed");
        internal static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>("thunderpropagator.feeviders.grpc.publish.duration", unit: "ms");

        public static IServiceCollection AddGrpcProvider<TGrpcProviderMessage, TGrpcProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TGrpcProviderMessage : GrpcProviderMessage
            where TGrpcProviderConfiguration : GrpcProviderConfiguration, new()
        {
            TGrpcProviderConfiguration grpcProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(grpcProviderConfiguration);
            services.TryAddSingleton(grpcProviderConfiguration);

            services.AddChannelProvider<GrpcProvider<TGrpcProviderMessage, TGrpcProviderConfiguration>, TGrpcProviderMessage, TGrpcProviderConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }
    }
}
