namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>How <see cref="IOutboxStoreInitializer.InitializeAsync"/> is allowed to treat the persisted schema.</summary>
    public enum StoreInitializationMode
    {
        /// <summary>
        /// Create the schema/indexes if absent, or upgrade them if the persisted version is older than
        /// this code requires. The default - appropriate whenever this process itself is allowed to
        /// mutate the store's schema.
        /// </summary>
        Apply,

        /// <summary>
        /// "External migration mode": never creates or mutates anything, only checks whether the
        /// currently persisted schema is exactly what this code requires. For a restricted production
        /// environment where an operator (or a separate, more-privileged tool/pipeline) applies schema
        /// changes out of band, and application instances themselves must never attempt DDL/index
        /// creation - see <see cref="StoreInitializationOutcome.IncompatibleVersion"/> for what a
        /// not-yet-migrated or downgraded schema reports as.
        /// </summary>
        VerifyOnly,
    }
}
