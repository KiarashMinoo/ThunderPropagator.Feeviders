namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>Outcome of one <see cref="IOutboxStoreInitializer.InitializeAsync"/> call.</summary>
    public sealed record StoreInitializationResult
    {
        /// <summary>What happened - see <see cref="StoreInitializationOutcome"/>.</summary>
        public required StoreInitializationOutcome Outcome { get; init; }

        /// <summary>The schema version this code requires.</summary>
        public required int RequiredSchemaVersion { get; init; }

        /// <summary>The schema version actually found persisted, or <see langword="null"/> if none was found (first-ever start).</summary>
        public int? PersistedSchemaVersion { get; init; }

        /// <summary>Human-readable detail - always safe to log, never includes a connection string/secret.</summary>
        public required string Message { get; init; }

        /// <summary>The underlying exception behind <see cref="StoreInitializationOutcome.InsufficientPermissions"/>, if any.</summary>
        public Exception? Error { get; init; }

        /// <summary>
        /// Whether a worker may safely start relaying against this store - <see langword="true"/> for
        /// <see cref="StoreInitializationOutcome.Ready"/>, <see cref="StoreInitializationOutcome.Upgraded"/>,
        /// and <see cref="StoreInitializationOutcome.VerifiedCompatible"/>; <see langword="false"/> otherwise.
        /// </summary>
        public bool IsReady => Outcome is StoreInitializationOutcome.Ready or StoreInitializationOutcome.Upgraded or StoreInitializationOutcome.VerifiedCompatible;
    }
}
