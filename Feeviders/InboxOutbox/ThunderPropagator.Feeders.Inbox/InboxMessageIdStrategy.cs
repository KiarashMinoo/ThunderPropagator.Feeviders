namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>How an <see cref="IMessageIdResolver"/> derives <see cref="InboxMessage.MessageId"/> for one inbound message.</summary>
    public enum InboxMessageIdStrategy
    {
        /// <summary>Read a broker-native header value (see <see cref="MessageIdResolverOptions.HeaderName"/>).</summary>
        BrokerHeader,

        /// <summary>
        /// Hash the payload with SHA-256, after applying <see cref="MessageIdResolverOptions.PayloadCanonicalizer"/>
        /// when one is configured.
        /// </summary>
        PayloadHash,

        /// <summary>
        /// Use a value the feeder already extracted from the message (e.g. a domain/business field),
        /// supplied via <see cref="MessageIdResolutionRequest.FeederFieldValue"/>.
        /// </summary>
        FeederField,

        /// <summary>
        /// No dedup: mint a fresh GUID for every message, so every receive is treated as unique and
        /// can never collide with (or be deduplicated against) any other.
        /// </summary>
        NewGuid,
    }
}
