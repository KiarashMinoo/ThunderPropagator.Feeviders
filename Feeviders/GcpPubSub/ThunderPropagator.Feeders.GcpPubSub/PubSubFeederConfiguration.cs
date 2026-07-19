using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.GcpPubSub.SharedKernel;

namespace ThunderPropagator.Feeders.GcpPubSub;

public abstract class PubSubFeederConfiguration : AbstractFeederConfiguration, IGcpPubSubFeeviderConfiguration
{
    public string ProjectId { get => Get<string>()!; set => Set(value); }
    public string SubscriptionId { get => Get<string>()!; set => Set(value); }
    public int MaxOutstandingMessages { get => Get(1000); set => Set(value); }
    public bool ExactlyOnceDelivery { get => Get(false); set => Set(value); }
    public string? ServiceAccountKeyPath { get => Get<string>(); set => Set(value); }
}
