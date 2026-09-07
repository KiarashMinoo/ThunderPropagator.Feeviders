namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Per-Provider opt-in Outbox configuration: store selection, relay polling/batching, retry backoff,
    /// retention, claim leasing, partitioning, and transactional enlistment. Binds from configuration
    /// unchanged with every existing (pre-Outbox) config, since <see cref="OutboxEnabled"/> defaults to
    /// <see langword="false"/> and every other member has a safe, always-valid default.
    /// </summary>
    /// <remarks>
    /// This type only declares and validates configuration - it does not itself resolve an
    /// <see cref="IOutboxStore"/> or a connection, and it does not itself enqueue or relay anything.
    /// <see cref="StoreConnectionName"/> is a named/secret reference (e.g. a key into <c>IConfiguration</c>
    /// or a secret store), never a raw connection string, so a logged or serialized <see cref="OutboxOptions"/>
    /// never exposes one - see <see cref="ToString"/>.
    /// </remarks>
    /// <remarks>
    /// <see cref="IOutboxStore.EnqueueAsync"/> succeeding only means the message was durably recorded
    /// Pending - publication itself is asynchronous, performed later by a relay worker polling
    /// <see cref="IOutboxStore.ClaimBatchAsync"/>. A caller must not infer the message has reached (or
    /// will imminently reach) the broker just because enqueue returned; observe
    /// <see cref="OutboxMessage.Status"/>/<see cref="OutboxMessage.PublishedAtUtc"/> for that. There is
    /// no separate "target metadata" option here - provider-specific destination detail (topic, routing
    /// key, exchange, etc.) travels on the message itself via <see cref="OutboxEnqueueRequest.Headers"/>,
    /// the same bounded header bag every backend already carries.
    /// </remarks>
    /// <remarks>
    /// <para>
    /// <b>Downstream deduplication.</b> This Outbox provides at-least-once delivery, never
    /// exactly-once: an ambiguous broker outcome (a publish call throws without the caller knowing
    /// whether the broker actually received it), or a relay worker crash between the broker's
    /// acknowledgement and its own <see cref="IOutboxStore.MarkPublishedAsync"/> call, both retry the
    /// same entry under the same <see cref="OutboxMessage.MessageId"/> - see
    /// <see cref="OutboxHeaderNames.MessageId"/>. A downstream consumer that cannot tolerate a duplicate
    /// delivery must deduplicate on that header itself; this Outbox has no way to guarantee a broker
    /// never redelivers or a relay worker never retries.
    /// </para>
    /// <para>
    /// <b>Poison-message policy.</b> An entry that fails every attempt up to <see cref="MaxRetryAttempts"/>
    /// is dead-lettered automatically and, from that point on, never blocks its partition (see
    /// <see cref="OutboxOrderingPolicy"/>) - dead-lettering is itself the unblock mechanism. Setting
    /// <see cref="MaxRetryAttempts"/> to zero dead-letters on the very first failure, for a Provider
    /// where a broker-rejected message should never occupy a retry slot at all. Setting
    /// <see cref="OrderingPolicy"/> to <see cref="OutboxOrderingPolicy.ContinueOnFailure"/> additionally
    /// keeps a partition moving *while* a poison entry is still mid-retry, at the cost of strict
    /// ordering for whatever publishes ahead of it in the meantime.
    /// </para>
    /// </remarks>
    public sealed record OutboxOptions
    {
        /// <summary>Whether the Outbox is active for this Provider. Disabled by default - the existing direct publish path is otherwise unchanged.</summary>
        public bool OutboxEnabled { get; init; }

        /// <summary>Which <see cref="IOutboxStore"/> backend to use. Ignored while <see cref="OutboxEnabled"/> is <see langword="false"/>.</summary>
        public OutboxStoreType StoreType { get; init; } = OutboxStoreType.InMemory;

        /// <summary>
        /// Named/secret reference to the store connection (e.g. a configuration key or secret name),
        /// resolved by the store factory - never a raw connection string. Required for every
        /// <see cref="StoreType"/> other than <see cref="OutboxStoreType.InMemory"/>.
        /// </summary>
        public string? StoreConnectionName { get; init; }

        /// <summary>
        /// When <see langword="true"/>, asserts this Provider requires a durable Outbox and fails
        /// <see cref="Validate"/> if <see cref="StoreType"/> is <see cref="OutboxStoreType.InMemory"/> -
        /// so a deployment that accidentally selects the non-durable backend fails at startup instead of
        /// silently losing enqueued messages on restart.
        /// </summary>
        public bool RequireDurableStore { get; init; }

        /// <summary>
        /// Whether <see cref="IOutboxStore.EnqueueAsync"/> shares a local transaction with business
        /// state. Defaults to <see cref="OutboxTransactionMode.NonTransactional"/> - the safe default
        /// that assumes no particular unit-of-work support. See <see cref="Validate"/> for the backend
        /// support <see cref="OutboxTransactionMode.Enlisted"/> requires.
        /// </summary>
        public OutboxTransactionMode TransactionMode { get; init; } = OutboxTransactionMode.NonTransactional;

        /// <summary>How this Provider's outgoing messages are assigned to a partition. See <see cref="OutboxPartitionStrategy"/>.</summary>
        public OutboxPartitionStrategy PartitionStrategy { get; init; } = OutboxPartitionStrategy.None;

        /// <summary>The shared partition key used when <see cref="PartitionStrategy"/> is <see cref="OutboxPartitionStrategy.Fixed"/>. Required in that case; ignored otherwise.</summary>
        public string? FixedPartitionKey { get; init; }

        /// <summary>
        /// What a relay worker does with the rest of a partition's claimed batch when one entry fails
        /// transiently. Defaults to <see cref="OutboxOrderingPolicy.StrictPerPartition"/> - see
        /// <see cref="OutboxOrderingPolicy"/> for the availability/ordering trade-off
        /// <see cref="OutboxOrderingPolicy.ContinueOnFailure"/> makes instead.
        /// </summary>
        public OutboxOrderingPolicy OrderingPolicy { get; init; } = OutboxOrderingPolicy.StrictPerPartition;

        /// <summary>How often a relay worker polls <see cref="IOutboxStore.ClaimBatchAsync"/> for claimable entries.</summary>
        public TimeSpan RelayPollingInterval { get; init; } = TimeSpan.FromSeconds(30);

        /// <summary>Maximum entries a relay worker claims per <see cref="IOutboxStore.ClaimBatchAsync"/> poll. Bounded by <see cref="OutboxMessageLimits.MaxRelayBatchSize"/>.</summary>
        public int RelayBatchSize { get; init; } = 100;

        /// <summary>Maximum publish attempts before a message is dead-lettered. Zero means no retry - a single failure dead-letters immediately.</summary>
        public int MaxRetryAttempts { get; init; } = 5;

        /// <summary>Base delay of the exponential backoff applied between publish retry attempts. Must not exceed <see cref="RetryMaxDelay"/>.</summary>
        public TimeSpan RetryBaseDelay { get; init; } = TimeSpan.FromSeconds(1);

        /// <summary>Ceiling the exponential backoff between publish retry attempts never grows past.</summary>
        public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromMinutes(5);

        /// <summary>How long terminal (<see cref="OutboxMessageStatus.Published"/>/<see cref="OutboxMessageStatus.DeadLettered"/>) entries are kept before a retention worker purges them via <see cref="IOutboxStore.PurgeAsync"/>.</summary>
        public TimeSpan RetentionPeriod { get; init; } = TimeSpan.FromDays(7);

        /// <summary>How long a publishing lease (<see cref="IOutboxStore.ClaimBatchAsync"/>) is held for before it becomes reclaimable by another relay worker.</summary>
        public TimeSpan ClaimLeaseDuration { get; init; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Maximum serialized payload size this Provider's Outbox accepts, in bytes. Must not exceed the
        /// hard ceiling every <see cref="IOutboxStore"/> implementation enforces,
        /// <see cref="OutboxMessageLimits.MaxPayloadSizeBytes"/>.
        /// </summary>
        public int MaxPayloadSizeBytes { get; init; } = OutboxMessageLimits.MaxPayloadSizeBytes;

        /// <summary>
        /// Validates every cross-field rule below, throwing <see cref="ArgumentException"/> on the first
        /// violation - a misconfiguration that should fail at startup, not on the first message that
        /// hits it. A no-op combination of defaults (Outbox disabled) always passes.
        /// </summary>
        public void Validate()
        {
            if (RequireDurableStore && StoreType == OutboxStoreType.InMemory)
                throw new ArgumentException($"{nameof(StoreType)} cannot be {OutboxStoreType.InMemory} when {nameof(RequireDurableStore)} is true.", nameof(StoreType));

            if (StoreType != OutboxStoreType.InMemory && string.IsNullOrWhiteSpace(StoreConnectionName))
                throw new ArgumentException($"{nameof(StoreConnectionName)} is required when {nameof(StoreType)} is {StoreType}.", nameof(StoreConnectionName));

            if (TransactionMode == OutboxTransactionMode.Enlisted && StoreType != OutboxStoreType.EFCore)
                throw new ArgumentException(
                    $"{nameof(TransactionMode)} cannot be {OutboxTransactionMode.Enlisted} when {nameof(StoreType)} is {StoreType} - only {OutboxStoreType.EFCore} supports sharing a local transaction with business state.",
                    nameof(TransactionMode));

            if (PartitionStrategy == OutboxPartitionStrategy.Fixed && string.IsNullOrWhiteSpace(FixedPartitionKey))
                throw new ArgumentException($"{nameof(FixedPartitionKey)} is required when {nameof(PartitionStrategy)} is {OutboxPartitionStrategy.Fixed}.", nameof(FixedPartitionKey));

            if (RelayPollingInterval <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(RelayPollingInterval)} must be positive.", nameof(RelayPollingInterval));

            if (RelayBatchSize is <= 0 or > OutboxMessageLimits.MaxRelayBatchSize)
                throw new ArgumentException($"{nameof(RelayBatchSize)} must be between 1 and {OutboxMessageLimits.MaxRelayBatchSize}.", nameof(RelayBatchSize));

            if (MaxRetryAttempts < 0)
                throw new ArgumentException($"{nameof(MaxRetryAttempts)} cannot be negative.", nameof(MaxRetryAttempts));

            if (RetryBaseDelay <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(RetryBaseDelay)} must be positive.", nameof(RetryBaseDelay));

            if (RetryMaxDelay < RetryBaseDelay)
                throw new ArgumentException($"{nameof(RetryMaxDelay)} must be at least {nameof(RetryBaseDelay)}.", nameof(RetryMaxDelay));

            if (RetentionPeriod <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(RetentionPeriod)} must be positive.", nameof(RetentionPeriod));

            if (ClaimLeaseDuration <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(ClaimLeaseDuration)} must be positive.", nameof(ClaimLeaseDuration));

            if (MaxPayloadSizeBytes is <= 0 || MaxPayloadSizeBytes > OutboxMessageLimits.MaxPayloadSizeBytes)
                throw new ArgumentException($"{nameof(MaxPayloadSizeBytes)} must be between 1 and {OutboxMessageLimits.MaxPayloadSizeBytes}.", nameof(MaxPayloadSizeBytes));
        }

        /// <summary>Redacts <see cref="StoreConnectionName"/> so a logged/traced <see cref="OutboxOptions"/> never exposes the connection reference, even though it is not itself a raw connection string.</summary>
        public override string ToString() =>
            $"{nameof(OutboxOptions)} {{ {nameof(OutboxEnabled)} = {OutboxEnabled}, {nameof(StoreType)} = {StoreType}, "
            + $"{nameof(StoreConnectionName)} = {(string.IsNullOrEmpty(StoreConnectionName) ? "<none>" : "***")}, "
            + $"{nameof(RequireDurableStore)} = {RequireDurableStore}, {nameof(TransactionMode)} = {TransactionMode}, "
            + $"{nameof(PartitionStrategy)} = {PartitionStrategy}, {nameof(OrderingPolicy)} = {OrderingPolicy}, {nameof(RelayPollingInterval)} = {RelayPollingInterval}, "
            + $"{nameof(RelayBatchSize)} = {RelayBatchSize}, {nameof(MaxRetryAttempts)} = {MaxRetryAttempts}, "
            + $"{nameof(RetryBaseDelay)} = {RetryBaseDelay}, {nameof(RetryMaxDelay)} = {RetryMaxDelay}, "
            + $"{nameof(RetentionPeriod)} = {RetentionPeriod}, {nameof(ClaimLeaseDuration)} = {ClaimLeaseDuration}, "
            + $"{nameof(MaxPayloadSizeBytes)} = {MaxPayloadSizeBytes} }}";
    }
}
