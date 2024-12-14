using DotPulsar;
using RapidStreamer.Application.Feeders;
using RapidStreamer.BuildingBlocks.Application.Serializations;
using RapidStreamer.Feeviders.Pulsar.SharedKernel;

namespace RapidStreamer.Feeders.Pulsar
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
            get => Get(SerializerType.Json);
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