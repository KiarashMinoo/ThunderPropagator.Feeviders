using ThunderPropagator.Feeviders.ZeroMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.ZeroMQ
{
    public abstract class ZeroMqProviderConfiguration : AbstractZeroMqFeevidersConfiguration, IAbstractProviderConfiguration
    {
        /// <summary>Topic prefix published in front of the payload. Only meaningful when <see cref="AbstractZeroMqFeevidersConfiguration.SocketPattern"/> is <see cref="ZeroMqSocketPattern.PubSub"/>; ignored for PushPull.</summary>
        public string? Topic { get => Get<string>(); set => Set(value); }

        public Guid Id { get => Get(Guid.NewGuid()); set => Set(value); }

        /// <summary>Opt-in transactional Outbox configuration for this Provider. Disabled by default.</summary>
        public OutboxOptions Outbox { get => Get(new OutboxOptions()); set => Set(value); }
    }
}
