using RapidStreamer.Application.Feeders;
using RapidStreamer.BuildingBlocks.Application.Serializations;
using RapidStreamer.Infrastructure.Protocols.WebSockets;

namespace RapidStreamer.Feeders.WebSocket
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

        public SerializerType SerializerType
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
    }
}