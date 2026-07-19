using ThunderPropagator.Feeviders.AwsSqs.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.AwsSqs
{
    public abstract class SnsProviderConfiguration : AbstractProviderConfiguration, IAwsFeeviderConfiguration
    {
        public string TopicArn
        {
            get => Get<string>()!;
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
