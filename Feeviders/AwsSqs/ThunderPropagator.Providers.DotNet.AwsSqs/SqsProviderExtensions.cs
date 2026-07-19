using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.AwsSqs
{
    public static class SqsProviderExtensions
    {
        public static IServiceCollection AddSqsProvider<TSqsProviderMessage, TSqsProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TSqsProviderMessage : SqsProviderMessage
            where TSqsProviderConfiguration : SqsProviderConfiguration, new()
        {
            TSqsProviderConfiguration sqsProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(sqsProviderConfiguration);
            services.TryAddSingleton(sqsProviderConfiguration);

            services.AddChannelProvider<SqsProvider<TSqsProviderMessage, TSqsProviderConfiguration>, TSqsProviderMessage, TSqsProviderConfiguration>();

            return services;
        }
    }
}
