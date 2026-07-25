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
using ThunderPropagator.Feeviders.Grpc.SharedKernel;
using ThunderPropagator.Infrastructure.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Feeders.Grpc
{
    public static class GrpcFeederExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.grpc");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.grpc");
        internal static readonly Counter<long> MessagesReceived = Meter.CreateCounter<long>("thunderpropagator.feeviders.grpc.messages.received");
        internal static readonly Counter<long> MessagesReceiveFailed = Meter.CreateCounter<long>("thunderpropagator.feeviders.grpc.messages.receive.failed");
        internal static readonly Histogram<double> ReceiveDuration = Meter.CreateHistogram<double>("thunderpropagator.feeviders.grpc.receive.duration", unit: "ms");

        public static IServiceCollection AddGrpcFeeder<TChannel, TGrpcFeederMessage, TGrpcFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TGrpcFeederMessage : GrpcFeederMessage
            where TGrpcFeederConfiguration : GrpcFeederConfiguration, new()
        {
            TGrpcFeederConfiguration grpcFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(grpcFeederConfiguration);
            services.TryAddSingleton(grpcFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                GrpcFeeder<TChannel, TGrpcFeederMessage, TGrpcFeederConfiguration>,
                TGrpcFeederMessage,
                TGrpcFeederConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IServiceCollection AddGrpcFeederResolver<TChannel, TGrpcFeederMessage, TGrpcFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TGrpcFeederMessage : GrpcFeederMessage
            where TGrpcFeederConfiguration : GrpcFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                GrpcFeeder<TChannel, TGrpcFeederMessage, TGrpcFeederConfiguration>,
                TGrpcFeederMessage,
                TGrpcFeederConfiguration>(services, (serviceProvider, channel, grpcFeederConfiguration, feederHandler) =>
                new GrpcFeeder<TChannel, TGrpcFeederMessage, TGrpcFeederConfiguration>(channel, grpcFeederConfiguration, feederHandler, serviceProvider));
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IApplicationBuilder UseGrpcFeederResolver<TChannel, TGrpcFeederMessage, TGrpcFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TGrpcFeederConfiguration grpcFeederConfiguration)
            where TChannel : class, IChannel
            where TGrpcFeederMessage : GrpcFeederMessage
            where TGrpcFeederConfiguration : GrpcFeederConfiguration
        {
            var grpcFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TGrpcFeederMessage, TGrpcFeederConfiguration>>();

            grpcFeederManager.UseFeeder(channelKey, grpcFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}
