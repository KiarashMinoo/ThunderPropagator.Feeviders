namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Configures one <see cref="IMessageIdResolver"/> instance. Immutable once built.</summary>
    public sealed record MessageIdResolverOptions
    {
        /// <summary>Which strategy resolves <see cref="InboxMessage.MessageId"/>.</summary>
        public required InboxMessageIdStrategy Strategy { get; init; }

        /// <summary>
        /// Header key to read from <see cref="MessageIdResolutionRequest.Headers"/>. Required when
        /// <see cref="Strategy"/> is <see cref="InboxMessageIdStrategy.BrokerHeader"/>; ignored otherwise.
        /// </summary>
        public string? HeaderName { get; init; }

        /// <summary>
        /// Name of the domain/business field <see cref="MessageIdResolutionRequest.FeederFieldValue"/>
        /// is extracted from. Required when <see cref="Strategy"/> is <see cref="InboxMessageIdStrategy.FeederField"/>;
        /// ignored otherwise. Not read by <see cref="IMessageIdResolver.Resolve"/> itself - the feeder
        /// already extracted the value by the time it builds a <see cref="MessageIdResolutionRequest"/> -
        /// this only documents, and lets startup validation confirm, which field that extraction targets.
        /// </summary>
        public string? FieldName { get; init; }

        /// <summary>What to do when the value <see cref="Strategy"/> depends on is missing.</summary>
        public MessageIdMissingValueBehavior MissingValueBehavior { get; init; } = MessageIdMissingValueBehavior.Throw;

        /// <summary>
        /// Optional transform applied to <see cref="MessageIdResolutionRequest.Payload"/> before hashing,
        /// for <see cref="InboxMessageIdStrategy.PayloadHash"/> - e.g. normalizing JSON property order/
        /// whitespace so two serializer runs that produced the same logical payload hash identically.
        /// When <see langword="null"/>, the raw payload bytes are hashed as-is. Ignored for every other
        /// <see cref="Strategy"/>.
        /// </summary>
        public Func<byte[], byte[]>? PayloadCanonicalizer { get; init; }

        /// <summary>
        /// Throws <see cref="ArgumentException"/> if <see cref="Strategy"/> is
        /// <see cref="InboxMessageIdStrategy.BrokerHeader"/> without a configured <see cref="HeaderName"/>,
        /// or <see cref="InboxMessageIdStrategy.FeederField"/> without a configured <see cref="FieldName"/> -
        /// a misconfiguration that should fail at startup, not on the first message that hits it.
        /// </summary>
        public void Validate()
        {
            if (Strategy == InboxMessageIdStrategy.BrokerHeader && string.IsNullOrWhiteSpace(HeaderName))
                throw new ArgumentException($"{nameof(HeaderName)} is required when {nameof(Strategy)} is {InboxMessageIdStrategy.BrokerHeader}.", nameof(HeaderName));

            if (Strategy == InboxMessageIdStrategy.FeederField && string.IsNullOrWhiteSpace(FieldName))
                throw new ArgumentException($"{nameof(FieldName)} is required when {nameof(Strategy)} is {InboxMessageIdStrategy.FeederField}.", nameof(FieldName));
        }
    }
}
