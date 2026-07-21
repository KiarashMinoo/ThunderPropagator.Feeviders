using DotPulsar;
using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;
using ThunderPropagator.Feeviders.Pulsar.SharedKernel;

namespace ThunderPropagator.Feeders.Pulsar
{
    public abstract class PulsarFeederConfiguration : AbstractPulsarFeevidersConfiguration, IAbstractFeederConfiguration
    {
        public Guid Id
        {
            get => Get(Guid.NewGuid());
            set => Set(value);
        }

        public SerializerType SerializerType
        {
            get => Get(JsonFormatSerializer.Json);
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
        public string? ConsumerName
        {
            get => Get<string>();
            set => Set(value);
        }

        public SubscriptionInitialPosition? InitialPosition
        {
            get => Get<SubscriptionInitialPosition>();
            set => Set(value);
        }

        public uint? MessagePrefetchCount
        {
            get => Get<uint>();
            set => Set(value);
        }

        public int? PriorityLevel
        {
            get => Get<int>();
            set => Set(value);
        }

        public bool? ReadCompacted
        {
            get => Get<bool>();
            set => Set(value);
        }

        public string SubscriptionName
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public SubscriptionType? SubscriptionType
        {
            get => Get<SubscriptionType>();
            set => Set(value);
        }

        public string Topic
        {
            get => Get<string>()!;
            set => Set(value);
        }
    }
}
