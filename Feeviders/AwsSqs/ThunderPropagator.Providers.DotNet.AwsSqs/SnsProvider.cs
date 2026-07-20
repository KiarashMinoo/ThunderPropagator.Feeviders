using System.Diagnostics;
using System.Text;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using ThunderPropagator.Feeviders.AwsSqs.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.AwsSqs
{
    internal
#if !DEBUG
        sealed
#endif
        partial class SnsProvider<TSnsProviderMessage, TSnsProviderConfiguration> : AbstractProvider<TSnsProviderMessage, TSnsProviderConfiguration>
        where TSnsProviderMessage : SnsProviderMessage
        where TSnsProviderConfiguration : SnsProviderConfiguration
    {
        private static partial class Log
        {
            [LoggerMessage(EventId = 5302, Level = LogLevel.Error, Message = "error has occured while publishing message to topic {TopicArn}.")]
            public static partial void PublishException(ILogger logger, Exception exception, string topicArn);
        }

        private readonly TSnsProviderConfiguration _snsProviderConfiguration;
        private readonly IAmazonSimpleNotificationService _client;

        public SnsProvider(TSnsProviderConfiguration snsProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _snsProviderConfiguration = snsProviderConfiguration;
            _client = AwsSqsFeeviderConnectionFactory.CreateSnsClient(snsProviderConfiguration);
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            try
            {
                var request = new PublishRequest
                {
                    TopicArn = _snsProviderConfiguration.TopicArn,
                    Message = Encoding.UTF8.GetString(bytes),
                    MessageAttributes = SnsMessageAttributeBuilder.Build(Activity.Current?.Context, Baggage.Current)
                };

                await _client.PublishAsync(request, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Log.PublishException(Logger, exception, _snsProviderConfiguration.TopicArn);
                throw;
            }
        }

        protected override ValueTask DisposeManagedResourcesAsync()
        {
            _client.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
