namespace ThunderPropagator.Feeviders.GcpPubSub.SharedKernel;

public interface IGcpPubSubFeeviderConfiguration
{
    string? ServiceAccountKeyPath { get; set; }
}
