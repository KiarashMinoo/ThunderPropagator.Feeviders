using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.WebSocket
{
    public abstract class WebSocketProviderConfiguration : AbstractProviderConfiguration
    {
        public required string Endpoint
        {
            get => Get<string>()!;
            set => Set(value);
        }
    }
}