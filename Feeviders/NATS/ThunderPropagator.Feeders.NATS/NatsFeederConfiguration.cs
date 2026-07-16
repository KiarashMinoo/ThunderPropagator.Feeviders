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

        public TimeSpan ReceiveTimeout { get => Get(TimeSpan.FromHours(1)); set => Set(value); }
        public TimeSpan StartupTimeout { get => Get(TimeSpan.FromSeconds(30)); set => Set(value); }
        public TimeSpan MessageHandlerTimeout { get => Get(TimeSpan.FromHours(1)); set => Set(value); }
        public double MemoryPressurePauseThreshold { get => Get(0.0); set => Set(value); }
        public double MemoryPressureResumeThreshold { get => Get(0.70); set => Set(value); }
        public TimeSpan MemoryPressurePollingInterval { get => Get(TimeSpan.FromSeconds(5)); set => Set(value); }

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
