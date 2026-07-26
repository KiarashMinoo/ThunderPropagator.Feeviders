using ThunderPropagator.Feeviders.ZeroMQ.SharedKernel;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.ZeroMQ
{
    public abstract class ZeroMqProviderConfiguration : AbstractZeroMqFeevidersConfiguration, IAbstractProviderConfiguration
    {
        /// <summary>Topic prefix published in front of the payload. Only meaningful when <see cref="AbstractZeroMqFeevidersConfiguration.SocketPattern"/> is <see cref="ZeroMqSocketPattern.PubSub"/>; ignored for PushPull.</summary>
        public string? Topic { get => Get<string>(); set => Set(value); }
    }
}
