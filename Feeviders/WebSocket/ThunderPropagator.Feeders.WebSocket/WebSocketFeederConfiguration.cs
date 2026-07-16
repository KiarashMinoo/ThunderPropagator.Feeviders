using ThunderPropagator.Application.Feeders;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Infrastructure.Protocols.WebSockets;

namespace ThunderPropagator.Feeders.WebSocket
{
    public abstract class WebSocketFeederConfiguration : WebSocketConfiguration,
        IAbstractFeederConfiguration
    {
        public bool IsEnabled
        {
            get => Get(false);
            set => Set(value);
        }

        public Guid Id
        {
            get => Get(Guid.NewGuid());
            set => Set(value);
        }

        public new SerializerType SerializerType
        {
            get => Get(SerializerType.NJson);
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
    }
}
