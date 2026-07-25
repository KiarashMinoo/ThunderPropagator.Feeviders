using ThunderPropagator.Application.Feeders;
using ThunderPropagator.Feeviders.Grpc.SharedKernel;

namespace ThunderPropagator.Feeders.Grpc
{
    public abstract class GrpcFeederConfiguration : AbstractGrpcFeevidersConfiguration, IAbstractFeederConfiguration
    {
        public Guid Id { get => Get(Guid.NewGuid()); set => Set(value); }
        public string? EnrichmentScript { get => Get<string>(); set => Set(value); }
        public string[]? MetadataReferences { get => Get<string[]>(); set => Set(value); }
        public TimeSpan ReceiveTimeout { get => Get(TimeSpan.FromHours(1)); set => Set(value); }
        public TimeSpan StartupTimeout { get => Get(TimeSpan.FromSeconds(30)); set => Set(value); }
        public TimeSpan MessageHandlerTimeout { get => Get(TimeSpan.FromHours(1)); set => Set(value); }
        public double MemoryPressurePauseThreshold { get => Get(0.0); set => Set(value); }
        public double MemoryPressureResumeThreshold { get => Get(0.70); set => Set(value); }
        public TimeSpan MemoryPressurePollingInterval { get => Get(TimeSpan.FromSeconds(5)); set => Set(value); }

        //Consumer
        public string Topic { get => Get<string>()!; set => Set(value); }
        public int MaxReconnectAttempts { get => Get(-1); set => Set(value); }
    }
}
