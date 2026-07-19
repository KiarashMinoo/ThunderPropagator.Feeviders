using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.AwsSqs.SharedKernel;

namespace ThunderPropagator.Feeders.AwsSqs
{
    public abstract class SqsFeederConfiguration : AbstractFeederConfiguration, IAwsFeeviderConfiguration
    {
        public string QueueUrl
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public int MaxNumberOfMessages
        {
            get => Get(10);
            set => Set(value);
        }

        public int WaitTimeSeconds
        {
            get => Get(20);
            set => Set(value);
        }

        public int? VisibilityTimeout
        {
            get => Get<int>();
            set => Set(value);
        }

        public string? RegionSystemName
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? ServiceUrl
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? AccessKey
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? SecretKey
        {
            get => Get<string>();
            set => Set(value);
        }
    }
}
