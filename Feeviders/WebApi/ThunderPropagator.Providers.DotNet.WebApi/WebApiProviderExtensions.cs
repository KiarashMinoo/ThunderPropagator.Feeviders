using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Http.Resilience;
using Polly;
using ThunderPropagator.Feeders.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel.Extensions;

namespace ThunderPropagator.Providers.DotNet.WebApi
{
    public static class WebApiProviderExtensions
    {
        internal static readonly ActivitySource ActivitySource = new("thunderpropagator.feeviders.webapi");
        internal static readonly Meter Meter = new("thunderpropagator.feeviders.webapi");

        internal static readonly Counter<long> MessagesPublished = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.webapi.messages.published", "{message}", "Total messages published via WebApi");
        internal static readonly Counter<long> MessagesPublishFailed = Meter.CreateCounter<long>(
            "thunderpropagator.feeviders.webapi.messages.publish.failed", "{message}", "Total WebApi publish failures");
        internal static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
            "thunderpropagator.feeviders.webapi.publish.duration", "ms", "WebApi message publish latency");

        public static IServiceCollection AddWebApiProvider<TWebApiProviderMessage, TWebApiProviderConfiguration>
            (this IServiceCollection services, IConfigurationRoot configuration, string sectionName)
            where TWebApiProviderMessage : WebApiProviderMessage
            where TWebApiProviderConfiguration : WebApiProviderConfiguration, new()
        {
            TWebApiProviderConfiguration webApiProviderConfiguration = new();
            configuration.GetSection(sectionName).Bind(webApiProviderConfiguration);
            services.TryAddSingleton(webApiProviderConfiguration);

            services.AddHttpClient<WebApiProvider<TWebApiProviderMessage, TWebApiProviderConfiguration>>(
                    client => client.BaseAddress = new Uri(webApiProviderConfiguration.BaseAddress))
                .ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler { AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate }).AddResilienceHandler(
                    nameof(WebApiProvider<TWebApiProviderMessage, TWebApiProviderConfiguration>), builder =>
                    {
                        builder.AddRetry(new HttpRetryStrategyOptions
                        {
                            BackoffType = webApiProviderConfiguration.BackoffType,
                            MaxRetryAttempts = webApiProviderConfiguration.MaxRetryAttempts,
                            MaxDelay = TimeSpan.FromMilliseconds(webApiProviderConfiguration.MaxDelay),
                            UseJitter = webApiProviderConfiguration.UseJitter,
                        });

                        builder.AddCircuitBreaker(new HttpCircuitBreakerStrategyOptions
                        {
                            SamplingDuration = TimeSpan.FromSeconds(webApiProviderConfiguration.SamplingDuration),
                            FailureRatio = webApiProviderConfiguration.FailureRatio,
                            MinimumThroughput = webApiProviderConfiguration.MinimumThroughput,
                            ShouldHandle = static args => ValueTask.FromResult(args is
                            {
                                Outcome.Result.StatusCode: HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests
                            })
                        });

                        builder.AddTimeout(TimeSpan.FromSeconds(webApiProviderConfiguration.RequestTimeout));
                    });

            services.AddChannelProvider<WebApiProvider<TWebApiProviderMessage, TWebApiProviderConfiguration>, TWebApiProviderMessage, TWebApiProviderConfiguration>();
            services.AddFormatSerializerInvoker();
            services.AddFormatDeserializerInvoker();

            return services;
        }
    }
}