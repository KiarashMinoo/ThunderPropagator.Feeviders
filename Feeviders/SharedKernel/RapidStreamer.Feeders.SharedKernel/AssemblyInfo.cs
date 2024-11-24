using System.Runtime.CompilerServices;
using System.Runtime.Versioning;

[assembly: RequiresPreviewFeatures]

[assembly: InternalsVisibleTo("RapidStreamer.Feeders.ActiveMQ")]
[assembly: InternalsVisibleTo("RapidStreamer.Feeders.Kafka")]
[assembly: InternalsVisibleTo("RapidStreamer.Feeders.RabbitMQ")]
[assembly: InternalsVisibleTo("RapidStreamer.Feeders.RedisPubSub")]
[assembly: InternalsVisibleTo("RapidStreamer.Feeders.TcpSocket")]
[assembly: InternalsVisibleTo("RapidStreamer.Feeders.UdpClient")]
[assembly: InternalsVisibleTo("RapidStreamer.Feeders.WebApi")]
[assembly: InternalsVisibleTo("RapidStreamer.Feeders.WebSocket")]