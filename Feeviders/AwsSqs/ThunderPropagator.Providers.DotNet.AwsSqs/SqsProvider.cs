using System.Diagnostics;
using System.Text;
using Amazon.SQS;
using Amazon.SQS.Model;
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
        class SqsProvider<TSqsProviderMessage, TSqsProviderConfiguration> : AbstractProvider<TSqsProviderMessage, TSqsProviderConfiguration>
        where TSqsProviderMessage : SqsProviderMessage
        where TSqsProviderConfiguration : SqsProviderConfiguration
    {
        private readonly TSqsProviderConfiguration _sqsProviderConfiguration;
        private readonly IAmazonSQS _client;

        public SqsProvider(TSqsProviderConfiguration sqsProviderConfiguration, IServiceProvider serviceProvider)
            : base(serviceProvider)
        {
            _sqsProviderConfiguration = sqsProviderConfiguration;
            _client = AwsSqsFeeviderConnectionFactory.CreateSqsClient(sqsProviderConfiguration);
        }

        protected override async Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
        {
            try
            {
                var request = new SendMessageRequest
                {
                    QueueUrl = _sqsProviderConfiguration.QueueUrl,
                    MessageBody = Encoding.UTF8.GetString(bytes),
                    MessageAttributes = SqsMessageAttributeBuilder.Build(Activity.Current?.Context, Baggage.Current)
                };

                await _client.SendMessageAsync(request, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                Logger.LogError(exception, "error has occured while producing message to queue {QueueUrl}.", _sqsProviderConfiguration.QueueUrl);
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
