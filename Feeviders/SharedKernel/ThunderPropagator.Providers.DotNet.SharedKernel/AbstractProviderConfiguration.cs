using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    public interface IAbstractProviderConfiguration : IServiceConfiguration
    {
        SerializerType SerializerType { get; set; }

        /// <summary>Stable identifier for this Provider instance - used as <see cref="OutboxMessage.ProviderKey"/> when the Outbox is enabled.</summary>
        Guid Id { get; set; }

        /// <summary>Opt-in transactional Outbox configuration for this Provider. Disabled by default.</summary>
        OutboxOptions Outbox { get; set; }
    }

    public abstract class AbstractProviderConfiguration : ServiceConfiguration,
        IAbstractProviderConfiguration
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

        public OutboxOptions Outbox
        {
            get => Get(new OutboxOptions());
            set => Set(value);
        }
    }
}
