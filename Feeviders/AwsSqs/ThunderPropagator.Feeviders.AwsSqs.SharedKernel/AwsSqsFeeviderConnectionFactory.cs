using Amazon;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;

namespace ThunderPropagator.Feeviders.AwsSqs.SharedKernel
{
    internal
#if !DEBUG
        sealed
#endif
        class AwsSqsFeeviderConnectionFactory
    {
        public static IAmazonSQS CreateSqsClient(IAwsFeeviderConfiguration configuration)
        {
            var config = new AmazonSQSConfig();
            Configure(config, configuration);

            var credentials = CreateCredentials(configuration);
            return credentials is not null
                ? new AmazonSQSClient(credentials, config)
                : new AmazonSQSClient(config);
        }

        public static IAmazonSimpleNotificationService CreateSnsClient(IAwsFeeviderConfiguration configuration)
        {
            var config = new AmazonSimpleNotificationServiceConfig();
            Configure(config, configuration);

            var credentials = CreateCredentials(configuration);
            return credentials is not null
                ? new AmazonSimpleNotificationServiceClient(credentials, config)
                : new AmazonSimpleNotificationServiceClient(config);
        }

        private static void Configure(ClientConfig config, IAwsFeeviderConfiguration configuration)
        {
            if (!string.IsNullOrWhiteSpace(configuration.RegionSystemName))
                config.RegionEndpoint = RegionEndpoint.GetBySystemName(configuration.RegionSystemName);

            if (!string.IsNullOrWhiteSpace(configuration.ServiceUrl))
                config.ServiceURL = configuration.ServiceUrl;
        }

        private static AWSCredentials? CreateCredentials(IAwsFeeviderConfiguration configuration)
        {
            return !string.IsNullOrWhiteSpace(configuration.AccessKey) && !string.IsNullOrWhiteSpace(configuration.SecretKey)
                ? new BasicAWSCredentials(configuration.AccessKey, configuration.SecretKey)
                : null;
        }
    }
}
