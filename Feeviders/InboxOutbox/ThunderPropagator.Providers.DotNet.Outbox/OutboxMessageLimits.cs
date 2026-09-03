namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Size/count ceilings every <see cref="IOutboxStore"/> implementation must enforce on
    /// <see cref="OutboxMessage"/> construction, so that no single backend silently accepts a
    /// record another backend would reject.
    /// </summary>
    public static class OutboxMessageLimits
    {
        /// <summary>Maximum serialized payload size, in bytes.</summary>
        public const int MaxPayloadSizeBytes = 1024 * 1024;

        /// <summary>Maximum number of header entries.</summary>
        public const int MaxHeaderCount = 32;

        /// <summary>Maximum length of a single header key.</summary>
        public const int MaxHeaderKeyLength = 128;

        /// <summary>Maximum length of a single header value.</summary>
        public const int MaxHeaderValueLength = 2048;

        /// <summary>Maximum length of the stable idempotency/message id.</summary>
        public const int MaxMessageIdLength = 512;

        /// <summary>Maximum length of <see cref="OutboxMessage.PartitionKey"/>.</summary>
        public const int MaxPartitionKeyLength = 256;

        /// <summary>Maximum length of <see cref="OutboxMessage.ProviderKey"/>.</summary>
        public const int MaxProviderKeyLength = 256;

        /// <summary>
        /// Maximum length of <see cref="OutboxMessage.FailureReason"/>. Callers must sanitize
        /// (strip stack traces/secrets) before persisting - this bounds the sanitized text, not
        /// raw exception output.
        /// </summary>
        public const int MaxFailureReasonLength = 4096;

        /// <summary>Maximum length of a lease owner token.</summary>
        public const int MaxLeaseOwnerLength = 256;
    }
}
