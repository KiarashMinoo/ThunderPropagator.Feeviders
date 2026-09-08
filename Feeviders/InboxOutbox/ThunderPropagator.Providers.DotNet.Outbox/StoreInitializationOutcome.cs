namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>Result of one <see cref="IOutboxStoreInitializer.InitializeAsync"/> call. See <see cref="StoreInitializationResult.IsReady"/> for which of these mean a worker may safely proceed.</summary>
    public enum StoreInitializationOutcome
    {
        /// <summary>The persisted schema already matched what this code requires - nothing was changed.</summary>
        Ready,

        /// <summary><see cref="StoreInitializationMode.Apply"/> created the schema (first-ever start) or upgraded it to the version this code requires.</summary>
        Upgraded,

        /// <summary><see cref="StoreInitializationMode.VerifyOnly"/> confirmed the persisted schema already matches what this code requires.</summary>
        VerifiedCompatible,

        /// <summary>
        /// The persisted schema version is newer than this code understands (a downgrade - this code is
        /// older than whatever last wrote the schema), or - under <see cref="StoreInitializationMode.VerifyOnly"/>
        /// only - older than what this code requires and nothing here is allowed to upgrade it. Never
        /// safe to proceed past this outcome.
        /// </summary>
        IncompatibleVersion,

        /// <summary>The backend rejected a read/write this initializer needed (e.g. missing CREATE TABLE/index or read permission). Never safe to proceed past this outcome.</summary>
        InsufficientPermissions,
    }
}
