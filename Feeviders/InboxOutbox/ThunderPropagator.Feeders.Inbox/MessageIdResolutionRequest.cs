namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Everything an <see cref="IMessageIdResolver"/> needs to derive a dedup-ready message ID for one
    /// inbound message, independent of any specific broker client type.
    /// </summary>
    public sealed record MessageIdResolutionRequest
    {
        /// <summary>Broker/transport-native headers for this message, for <see cref="InboxMessageIdStrategy.BrokerHeader"/>.</summary>
        public IReadOnlyDictionary<string, string>? Headers { get; init; }

        /// <summary>The raw payload, for <see cref="InboxMessageIdStrategy.PayloadHash"/>.</summary>
        public byte[]? Payload { get; init; }

        /// <summary>
        /// A value the feeder already extracted from the message (e.g. a domain/business field), for
        /// <see cref="InboxMessageIdStrategy.FeederField"/>.
        /// </summary>
        public string? FeederFieldValue { get; init; }

        /// <summary>
        /// Additional scope folded into the resolved ID ahead of the strategy-derived value, so an
        /// identifier that is only unique within a partition/topic (not globally) cannot collide across
        /// different scopes - e.g. a Kafka (topic, partition) pair or a RabbitMQ exchange/routing key.
        /// Ignored for <see cref="InboxMessageIdStrategy.NewGuid"/>, which can never collide regardless.
        /// </summary>
        public IReadOnlyList<string>? ScopeSegments { get; init; }
    }
}
