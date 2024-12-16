using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using RapidStreamer.Feeviders.NATS.SharedKernel;
using RapidStreamer.Providers.DotNet.SharedKernel;

namespace RapidStreamer.Providers.DotNet.NATS
{
    public abstract class NatsProviderConfiguration : AbstractNatsFeevidersConfiguration, IAbstractProviderConfiguration
    {
        public string Subject
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public string? ReplyTo
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public StreamConfig? StreamConfig
        {
            get => Get<StreamConfig>();
            set => Set(value);
        }

        public NatsJSPubOpts? NatsJSPubOpts
        {
            get => Get<NatsJSPubOpts>();
            set => Set(value);
        }
    }
}