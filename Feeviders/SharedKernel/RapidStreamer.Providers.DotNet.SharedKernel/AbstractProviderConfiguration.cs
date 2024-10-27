using RapidStreamer.BuildingBlocks.Application;
using RapidStreamer.BuildingBlocks.Application.Serializations;

namespace RapidStreamer.Providers.DotNet.SharedKernel
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
            get => Get(SerializerType.Json);
            set => Set(value);
        }
    }
}