namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Per-Feevider opt-in Inbox configuration: store selection, deduplication/retention windows,
    /// retry backoff, claim leasing, and message-ID resolution. Binds from configuration unchanged
    /// with every existing (pre-Inbox) config, since <see cref="InboxEnabled"/> defaults to
    /// <see langword="false"/> and every other member has a safe, always-valid default.
    /// </summary>
    /// <remarks>
    /// This type only declares and validates configuration - it does not itself resolve an
    /// <see cref="IInboxStore"/> or a connection. <see cref="StoreConnectionName"/> is a named/secret
    /// reference (e.g. a key into <c>IConfiguration</c> or a secret store), never a raw connection
    /// string, so a logged or serialized <see cref="InboxOptions"/> never exposes one - see
    /// <see cref="ToString"/>.
    /// </remarks>
    public sealed record InboxOptions
    {
        /// <summary>Whether the Inbox is active for this Feevider. Disabled by default - the existing direct receive path is otherwise unchanged.</summary>
        public bool InboxEnabled { get; init; }

        /// <summary>Which <see cref="IInboxStore"/> backend to use. Ignored while <see cref="InboxEnabled"/> is <see langword="false"/>.</summary>
        public InboxStoreType StoreType { get; init; } = InboxStoreType.InMemory;

        /// <summary>
        /// Named/secret reference to the store connection (e.g. a configuration key or secret name),
        /// resolved by the store factory - never a raw connection string. Required for every
        /// <see cref="StoreType"/> other than <see cref="InboxStoreType.InMemory"/>.
        /// </summary>
        public string? StoreConnectionName { get; init; }

        /// <summary>
        /// When <see langword="true"/>, asserts this Feevider requires a durable Inbox and fails
        /// <see cref="Validate"/> if <see cref="StoreType"/> is <see cref="InboxStoreType.InMemory"/> -
        /// so a deployment that accidentally selects the non-durable backend fails at startup instead
        /// of silently losing claimed/in-flight messages on restart.
        /// </summary>
        public bool RequireDurableStore { get; init; }

        /// <summary>
        /// How long a message that reached <see cref="InboxMessageStatus.Processed"/> keeps rejecting
        /// duplicate deliveries of the same dedup key before it becomes eligible for purge. Must not
        /// exceed <see cref="RetentionPeriod"/>.
        /// </summary>
        public TimeSpan DeduplicationWindow { get; init; } = TimeSpan.FromHours(24);

        /// <summary>
        /// How long terminal (<see cref="InboxMessageStatus.Processed"/>/<see cref="InboxMessageStatus.DeadLettered"/>)
        /// entries are kept before a retention worker purges them via <see cref="IInboxStore.PurgeAsync"/>.
        /// Must be at least <see cref="DeduplicationWindow"/>, so a dedup key can never age out of the
        /// store before its dedup guarantee said it would.
        /// </summary>
        public TimeSpan RetentionPeriod { get; init; } = TimeSpan.FromDays(7);

        /// <summary>
        /// Separate retention window for DeadLettered entries only, overriding <see cref="RetentionPeriod"/>
        /// for that one status. <see langword="null"/> (default) means DeadLettered entries follow
        /// <see cref="RetentionPeriod"/> exactly like Processed entries always have. Set this when dead
        /// letters need longer retention than successfully processed entries do, to leave time for
        /// manual triage/replay before <see cref="InboxPurgeWorker"/> removes them.
        /// </summary>
        public TimeSpan? DeadLetterRetentionPeriod { get; init; }

        /// <summary>How often <see cref="InboxPurgeWorker"/> runs a purge pass for this channel.</summary>
        public TimeSpan PurgePollingInterval { get; init; } = TimeSpan.FromHours(1);

        /// <summary>Maximum entries <see cref="InboxPurgeWorker"/> deletes per <see cref="IInboxStore.PurgeAsync"/> call. Bounded by <see cref="InboxMessageLimits.MaxPurgeBatchSize"/>.</summary>
        public int PurgeBatchSize { get; init; } = 500;

        /// <summary>
        /// Maximum consecutive purge batches <see cref="InboxPurgeWorker"/> runs per polling tick before
        /// waiting for the next <see cref="PurgePollingInterval"/>, bounding how long one purge pass can
        /// occupy the store even when far more eligible entries exist than fit in one
        /// <see cref="PurgeBatchSize"/> batch.
        /// </summary>
        public int MaxPurgeBatchesPerRun { get; init; } = 20;

        /// <summary>Maximum processing attempts before a message is dead-lettered. Zero means no retry - a single failure dead-letters immediately.</summary>
        public int MaxRetryAttempts { get; init; } = 5;

        /// <summary>Base delay of the exponential backoff applied between retry attempts. Must not exceed <see cref="RetryMaxDelay"/>.</summary>
        public TimeSpan RetryBaseDelay { get; init; } = TimeSpan.FromSeconds(1);

        /// <summary>Ceiling the exponential backoff between retry attempts never grows past.</summary>
        public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromMinutes(5);

        /// <summary>How often a retry worker polls <see cref="IInboxStore.QueryRetryableAsync"/> for elapsed retries.</summary>
        public TimeSpan RetryPollingInterval { get; init; } = TimeSpan.FromSeconds(30);

        /// <summary>Maximum entries a retry worker claims per <see cref="IInboxStore.QueryRetryableAsync"/> poll. Bounded by <see cref="InboxMessageLimits.MaxRetryBatchSize"/>.</summary>
        public int RetryBatchSize { get; init; } = 100;

        /// <summary>How long a processing lease (<see cref="InboxClaimRequest.LeaseDuration"/>) is held for before it becomes reclaimable by another worker.</summary>
        public TimeSpan ClaimLeaseDuration { get; init; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Maximum serialized payload size this Feevider's Inbox accepts, in bytes. Must not exceed
        /// the hard ceiling every <see cref="IInboxStore"/> implementation enforces,
        /// <see cref="InboxMessageLimits.MaxPayloadSizeBytes"/>.
        /// </summary>
        public int MaxPayloadSizeBytes { get; init; } = InboxMessageLimits.MaxPayloadSizeBytes;

        /// <summary>How <see cref="InboxMessage.MessageId"/> is derived for this Feevider's inbound messages.</summary>
        public MessageIdResolverOptions MessageIdResolution { get; init; } = new() { Strategy = InboxMessageIdStrategy.NewGuid };

        /// <summary>
        /// Validates every cross-field rule below, throwing <see cref="ArgumentException"/> on the
        /// first violation - a misconfiguration that should fail at startup, not on the first message
        /// that hits it. A no-op combination of defaults (Inbox disabled) always passes.
        /// </summary>
        public void Validate()
        {
            MessageIdResolution.Validate();

            if (RequireDurableStore && StoreType == InboxStoreType.InMemory)
                throw new ArgumentException($"{nameof(StoreType)} cannot be {InboxStoreType.InMemory} when {nameof(RequireDurableStore)} is true.", nameof(StoreType));

            if (StoreType != InboxStoreType.InMemory && string.IsNullOrWhiteSpace(StoreConnectionName))
                throw new ArgumentException($"{nameof(StoreConnectionName)} is required when {nameof(StoreType)} is {StoreType}.", nameof(StoreConnectionName));

            if (DeduplicationWindow <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(DeduplicationWindow)} must be positive.", nameof(DeduplicationWindow));

            if (RetentionPeriod < DeduplicationWindow)
                throw new ArgumentException($"{nameof(RetentionPeriod)} must be at least {nameof(DeduplicationWindow)}.", nameof(RetentionPeriod));

            if (DeadLetterRetentionPeriod is { } deadLetterRetentionPeriod && deadLetterRetentionPeriod < DeduplicationWindow)
                throw new ArgumentException($"{nameof(DeadLetterRetentionPeriod)} must be at least {nameof(DeduplicationWindow)}.", nameof(DeadLetterRetentionPeriod));

            if (PurgePollingInterval <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(PurgePollingInterval)} must be positive.", nameof(PurgePollingInterval));

            if (PurgeBatchSize is <= 0 or > InboxMessageLimits.MaxPurgeBatchSize)
                throw new ArgumentException($"{nameof(PurgeBatchSize)} must be between 1 and {InboxMessageLimits.MaxPurgeBatchSize}.", nameof(PurgeBatchSize));

            if (MaxPurgeBatchesPerRun <= 0)
                throw new ArgumentException($"{nameof(MaxPurgeBatchesPerRun)} must be at least 1.", nameof(MaxPurgeBatchesPerRun));

            if (MaxRetryAttempts < 0)
                throw new ArgumentException($"{nameof(MaxRetryAttempts)} cannot be negative.", nameof(MaxRetryAttempts));

            if (RetryBaseDelay <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(RetryBaseDelay)} must be positive.", nameof(RetryBaseDelay));

            if (RetryMaxDelay < RetryBaseDelay)
                throw new ArgumentException($"{nameof(RetryMaxDelay)} must be at least {nameof(RetryBaseDelay)}.", nameof(RetryMaxDelay));

            if (RetryPollingInterval <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(RetryPollingInterval)} must be positive.", nameof(RetryPollingInterval));

            if (RetryBatchSize is <= 0 or > InboxMessageLimits.MaxRetryBatchSize)
                throw new ArgumentException($"{nameof(RetryBatchSize)} must be between 1 and {InboxMessageLimits.MaxRetryBatchSize}.", nameof(RetryBatchSize));

            if (ClaimLeaseDuration <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(ClaimLeaseDuration)} must be positive.", nameof(ClaimLeaseDuration));

            if (MaxPayloadSizeBytes is <= 0 || MaxPayloadSizeBytes > InboxMessageLimits.MaxPayloadSizeBytes)
                throw new ArgumentException($"{nameof(MaxPayloadSizeBytes)} must be between 1 and {InboxMessageLimits.MaxPayloadSizeBytes}.", nameof(MaxPayloadSizeBytes));
        }

        /// <summary>Redacts <see cref="StoreConnectionName"/> so a logged/traced <see cref="InboxOptions"/> never exposes the connection reference, even though it is not itself a raw connection string.</summary>
        public override string ToString() =>
            $"{nameof(InboxOptions)} {{ {nameof(InboxEnabled)} = {InboxEnabled}, {nameof(StoreType)} = {StoreType}, "
            + $"{nameof(StoreConnectionName)} = {(string.IsNullOrEmpty(StoreConnectionName) ? "<none>" : "***")}, "
            + $"{nameof(RequireDurableStore)} = {RequireDurableStore}, {nameof(DeduplicationWindow)} = {DeduplicationWindow}, "
            + $"{nameof(RetentionPeriod)} = {RetentionPeriod}, {nameof(DeadLetterRetentionPeriod)} = {DeadLetterRetentionPeriod}, "
            + $"{nameof(PurgePollingInterval)} = {PurgePollingInterval}, {nameof(PurgeBatchSize)} = {PurgeBatchSize}, "
            + $"{nameof(MaxPurgeBatchesPerRun)} = {MaxPurgeBatchesPerRun}, {nameof(MaxRetryAttempts)} = {MaxRetryAttempts}, "
            + $"{nameof(RetryBaseDelay)} = {RetryBaseDelay}, {nameof(RetryMaxDelay)} = {RetryMaxDelay}, "
            + $"{nameof(RetryPollingInterval)} = {RetryPollingInterval}, {nameof(RetryBatchSize)} = {RetryBatchSize}, "
            + $"{nameof(ClaimLeaseDuration)} = {ClaimLeaseDuration}, {nameof(MaxPayloadSizeBytes)} = {MaxPayloadSizeBytes} }}";
    }
}
