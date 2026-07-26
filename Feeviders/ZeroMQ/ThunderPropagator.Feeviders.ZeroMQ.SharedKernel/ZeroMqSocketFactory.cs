using NetMQ;
using NetMQ.Sockets;

namespace ThunderPropagator.Feeviders.ZeroMQ.SharedKernel
{
    internal
#if !DEBUG
        sealed
#endif
        class ZeroMqSocketFactory
    {
        public static NetMQSocket CreateFeederSocket(AbstractZeroMqFeevidersConfiguration configuration) =>
            configuration.SocketPattern switch
            {
                ZeroMqSocketPattern.PubSub => new SubscriberSocket(),
                ZeroMqSocketPattern.PushPull => new PullSocket(),
                _ => throw new ArgumentOutOfRangeException(nameof(configuration), configuration.SocketPattern, null)
            };

        public static NetMQSocket CreateProviderSocket(AbstractZeroMqFeevidersConfiguration configuration) =>
            configuration.SocketPattern switch
            {
                ZeroMqSocketPattern.PubSub => new PublisherSocket(),
                ZeroMqSocketPattern.PushPull => new PushSocket(),
                _ => throw new ArgumentOutOfRangeException(nameof(configuration), configuration.SocketPattern, null)
            };

        public static void ApplyOptionsAndConnect(NetMQSocket socket, AbstractZeroMqFeevidersConfiguration configuration)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(configuration.Endpoint);

            socket.Options.SendHighWatermark = configuration.HighWatermark;
            socket.Options.ReceiveHighWatermark = configuration.HighWatermark;
            socket.Options.Linger = configuration.Linger;

            if (configuration.Bind)
                socket.Bind(configuration.Endpoint);
            else
                socket.Connect(configuration.Endpoint);
        }
    }
}
