namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Why an entry ended up <see cref="OutboxMessageStatus.DeadLettered"/> - see
    /// <see cref="OutboxFailureClassifier.Classify"/> for how a failure is assigned one of these.
    /// </summary>
    public enum OutboxDeadLetterFailureCategory
    {
        /// <summary>
        /// The failure looked environment-related (a timeout, a dropped connection, throttling) -
        /// ordinarily retryable, dead-lettered only because <see cref="OutboxOptions.MaxRetryAttempts"/>
        /// was exhausted before the environment recovered.
        /// </summary>
        Transient,

        /// <summary>
        /// The handler explicitly threw <see cref="OutboxNonRetryableException"/> - a declared "this can
        /// never succeed, do not retry" signal, dead-lettered on the very first attempt regardless of
        /// remaining attempts.
        /// </summary>
        Permanent,

        /// <summary>
        /// The payload itself could not be serialized/interpreted by the target - retrying would
        /// reproduce the identical failure every time, since nothing about the environment caused it.
        /// </summary>
        Serialization,

        /// <summary>The publish attempt could not authenticate/authorize against the target.</summary>
        Authorization,

        /// <summary>
        /// Every attempt failed for a reason that did not match any of the categories above - a message
        /// that repeatedly "poisons" its own publishing without a more specific, classifiable cause.
        /// </summary>
        Poison,
    }
}
