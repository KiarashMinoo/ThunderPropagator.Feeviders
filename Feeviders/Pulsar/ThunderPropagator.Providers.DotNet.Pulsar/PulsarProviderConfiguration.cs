using DotPulsar;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;
using ThunderPropagator.Feeviders.Pulsar.SharedKernel;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.Pulsar
{
    public abstract class PulsarProviderConfiguration : AbstractPulsarFeevidersConfiguration, IAbstractProviderConfiguration
    {
        public SerializerType SerializerType
        {
            get => Get(JsonFormatSerializer.Json);
            set => Set(value);
        }

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

        public bool? AttachTraceInfoToMessages
        {
            get => Get<bool>();
            set => Set(value);
        }

        public CompressionType? CompressionType
        {
            get => Get<CompressionType>();
            set => Set(value);
        }

        public ulong? InitialSequenceId
        {
            get => Get<ulong>();
            set => Set(value);
        }

        public ProducerAccessMode? ProducerAccessMode
        {
            get => Get<ProducerAccessMode>();
            set => Set(value);
        }

        public string? ProducerName
        {
            get => Get<string>();
            set => Set(value);
        }

        public string Topic
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public uint? MaxPendingMessages
        {
            get => Get<uint>();
            set => Set(value);
        }

        public Dictionary<string, string>? ProducerProperties
        {
            get => Get<Dictionary<string, string>>();
            set => Set(value);
        }
    }
}
