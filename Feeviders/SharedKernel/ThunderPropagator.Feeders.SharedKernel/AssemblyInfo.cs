using System.Runtime.CompilerServices;
using System.Runtime.Versioning;

[assembly: RequiresPreviewFeatures]

[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.ActiveMQ")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.Kafka")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.RabbitMQ")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.RedisPubSub")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.TcpSocket")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.UdpClient")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.WebApi")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.WebSocket")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.Pulsar")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.NATS")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.Mqtt")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.AwsSqs")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.AzureServiceBus")]
[assembly: InternalsVisibleTo("ThunderPropagator.Feeders.GcpPubSub")]
[assembly: InternalsVisibleTo("ThunderPropagator.UnitTests")]
