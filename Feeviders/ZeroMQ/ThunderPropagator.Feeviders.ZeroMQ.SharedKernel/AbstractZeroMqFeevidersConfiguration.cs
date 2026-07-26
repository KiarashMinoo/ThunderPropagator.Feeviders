using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;

namespace ThunderPropagator.Feeviders.ZeroMQ.SharedKernel
{
    public abstract class AbstractZeroMqFeevidersConfiguration : ServiceConfiguration
    {
        public bool IsEnabled { get => Get<bool>(); set => Set(value); }
        public string Endpoint { get => Get<string>()!; set => Set(value); }
        public ZeroMqSocketPattern SocketPattern { get => Get(ZeroMqSocketPattern.PubSub); set => Set(value); }
        public bool Bind { get => Get(false); set => Set(value); }
        public int HighWatermark { get => Get(1000); set => Set(value); }
        public TimeSpan Linger { get => Get(TimeSpan.Zero); set => Set(value); }
        public SerializerType SerializerType { get => Get(JsonFormatSerializer.Json); set => Set(value); }
    }
}
