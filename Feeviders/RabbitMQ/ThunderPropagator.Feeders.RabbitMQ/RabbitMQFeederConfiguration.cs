using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.RabbitMQ.SharedKernel;
using ThunderPropagator.BuildingBlocks.Application.Serializations;

namespace ThunderPropagator.Feeders.RabbitMQ
{
    public abstract class RabbitMQFeederConfiguration : RabbitMQFeeviderConfiguration, IAbstractFeederConfiguration
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

        public TimeSpan ReceiveTimeout { get => Get(TimeSpan.FromHours(1)); set => Set(value); }
        public TimeSpan StartupTimeout { get => Get(TimeSpan.FromSeconds(30)); set => Set(value); }
        public TimeSpan MessageHandlerTimeout { get => Get(TimeSpan.FromHours(1)); set => Set(value); }
        public TimeSpan ReconnectInitialDelay { get => Get(TimeSpan.FromSeconds(1)); set => Set(value); }
        public TimeSpan ReconnectMaxDelay { get => Get(TimeSpan.FromSeconds(30)); set => Set(value); }
        public double MemoryPressurePauseThreshold { get => Get(0.0); set => Set(value); }
        public double MemoryPressureResumeThreshold { get => Get(0.70); set => Set(value); }
        public TimeSpan MemoryPressurePollingInterval { get => Get(TimeSpan.FromSeconds(5)); set => Set(value); }
    }
}
