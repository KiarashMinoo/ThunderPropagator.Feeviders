using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Certificate;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.BuildingBlocks.Application.Serializations.Json;

namespace ThunderPropagator.Feeviders.Grpc.SharedKernel
{
    public abstract class AbstractGrpcFeevidersConfiguration : ServiceConfiguration
    {
        public bool IsEnabled { get => Get<bool>(); set => Set(value); }
        public Uri Endpoint { get => Get<Uri>()!; set => Set(value); }
        public bool UseTls { get => Get(true); set => Set(value); }
        public CertificateModel? ClientCertificate { get => Get<CertificateModel>(); set => Set(value); }
        public TimeSpan KeepAliveInterval { get => Get(TimeSpan.FromSeconds(30)); set => Set(value); }
        public TimeSpan KeepAliveTimeout { get => Get(TimeSpan.FromSeconds(10)); set => Set(value); }
        public TimeSpan ReconnectInitialDelay { get => Get(TimeSpan.FromSeconds(1)); set => Set(value); }
        public TimeSpan ReconnectMaxDelay { get => Get(TimeSpan.FromSeconds(30)); set => Set(value); }
        public SerializerType SerializerType { get => Get(JsonFormatSerializer.Json); set => Set(value); }
    }
}
