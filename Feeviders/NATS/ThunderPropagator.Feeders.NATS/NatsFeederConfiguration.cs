using NATS.Client.Core;
using NATS.Client.JetStream.Models;
using NATS.Client.Services;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.NATS.SharedKernel;

namespace ThunderPropagator.Feeders.NATS
{
    public abstract class NatsFeederConfiguration : AbstractNatsFeevidersConfiguration, IAbstractFeederConfiguration
    {
        public Guid Id
        {
            get => Get(Guid.NewGuid());
            set => Set(value);
        }

        public string? EnrichmentScript
        {
            get => Get<string>();
            set => Set(value);
        }

        public string[]? MetadataReferences
        {
            get => Get<string[]>();
            set => Set(value);
        }

        //Consumer
        public string Subject
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public string? QueueGroup
        {
            get => Get<string>();
            set => Set(value);
        }

        public int? MaxMsgs
        {
            get => Get<int>();
            set => Set(value);
        }

        public TimeSpan? Timeout
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public TimeSpan? StartUpTimeout
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public TimeSpan? IdleTimeout
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public bool? StopOnEmptyMsg
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? ThrowIfNoResponders
        {
            get => Get<bool>();
            set => Set(value);
        }

        public NatsSubChannelOpts? ChannelOpts
        {
            get => Get<NatsSubChannelOpts>();
            set => Set(value);
        }

        public string? StreamName
        {
            get => Get<string>();
            set => Set(value);
        }

        public ConsumerConfig? ConsumerConfig
        {
            get => Get<ConsumerConfig>();
            set => Set(value);
        }

        public NatsSvcConfig? NatsSvcConfig
        {
            get => Get<NatsSvcConfig>();
            set => Set(value);
        }
    }
}