using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using ThunderPropagator.Feeviders.NATS.SharedKernel;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.NATS
{
    public abstract class NatsProviderConfiguration : AbstractNatsFeevidersConfiguration, IAbstractProviderConfiguration
    {
        public Guid Id
        {
            get => Get(Guid.NewGuid());
            set => Set(value);
        }

        /// <summary>Opt-in transactional Outbox configuration for this Provider. Disabled by default.</summary>
        public OutboxOptions Outbox
        {
            get => Get(new OutboxOptions());
            set => Set(value);
        }

        public string Subject
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public string? ReplyTo
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public StreamConfig? StreamConfig
        {
            get => Get<StreamConfig>();
            set => Set(value);
        }

        public NatsJSPubOpts? NatsJSPubOpts
        {
            get => Get<NatsJSPubOpts>();
            set => Set(value);
        }
    }
}