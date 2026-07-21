using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    public interface IAbstractProviderConfiguration : IServiceConfiguration
    {
        SerializerType SerializerType { get; set; }
    }

    public abstract class AbstractProviderConfiguration : ServiceConfiguration,
        IAbstractProviderConfiguration
    {
        public SerializerType SerializerType
        {
            get => Get(JsonFormatSerializer.Json);
            set => Set(value);
        }
    }
}
