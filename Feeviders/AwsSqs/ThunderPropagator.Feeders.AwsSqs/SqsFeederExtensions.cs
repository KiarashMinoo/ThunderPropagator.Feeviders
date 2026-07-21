using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using ThunderPropagator.Application.Channels;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Infrastructure.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Feeders.AwsSqs
{
    public static class SqsFeederExtensions
    {
        public static IServiceCollection AddSqsFeeder<TChannel, TSqsFeederMessage, TSqsFeederConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TChannel : class, IChannel
            where TSqsFeederMessage : SqsFeederMessage
            where TSqsFeederConfiguration : SqsFeederConfiguration, new()
        {
            TSqsFeederConfiguration sqsFeederConfiguration = new();
            configuration.GetSection(sectionName).Bind(sqsFeederConfiguration);
            services.TryAddSingleton(sqsFeederConfiguration);

            services.AddChannelFeeder<TChannel,
                SqsFeeder<TChannel, TSqsFeederMessage, TSqsFeederConfiguration>,
                TSqsFeederMessage,
                TSqsFeederConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IServiceCollection AddSqsFeederResolver<TChannel, TSqsFeederMessage, TSqsFeederConfiguration>
            (this IServiceCollection services)
            where TChannel : class, IChannel
            where TSqsFeederMessage : SqsFeederMessage
            where TSqsFeederConfiguration : SqsFeederConfiguration, new()
        {
            SharedKernel.Extensions.AddChannelFeederResolver<TChannel,
                SqsFeeder<TChannel, TSqsFeederMessage, TSqsFeederConfiguration>,
                TSqsFeederMessage,
                TSqsFeederConfiguration>(services, (serviceProvider, channel, sqsFeederConfiguration, feederHandler) =>
                new SqsFeeder<TChannel, TSqsFeederMessage, TSqsFeederConfiguration>(channel, sqsFeederConfiguration, feederHandler, serviceProvider));
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }

        public static IApplicationBuilder UseSqsFeederResolver<TChannel, TSqsFeederMessage, TSqsFeederConfiguration>
            (this IApplicationBuilder app, Guid channelKey, TSqsFeederConfiguration sqsFeederConfiguration)
            where TChannel : class, IChannel
            where TSqsFeederMessage : SqsFeederMessage
            where TSqsFeederConfiguration : SqsFeederConfiguration
        {
            var sqsFeederManager = app.ApplicationServices.GetRequiredService<IFeederManager<TChannel, TSqsFeederMessage, TSqsFeederConfiguration>>();

            sqsFeederManager.UseFeeder(channelKey, sqsFeederConfiguration, app.ApplicationServices.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping);

            return app;
        }
    }
}
