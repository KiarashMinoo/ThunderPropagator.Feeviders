using ThunderPropagator.Feeviders.GcpPubSub.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.GcpPubSub;

public abstract class PubSubProviderConfiguration : AbstractProviderConfiguration, IGcpPubSubFeeviderConfiguration
{
    public string ProjectId { get => Get<string>()!; set => Set(value); }
    public string TopicId { get => Get<string>()!; set => Set(value); }
    public string? OrderingKey { get => Get<string>(); set => Set(value); }
    public string? ServiceAccountKeyPath { get => Get<string>(); set => Set(value); }
}
