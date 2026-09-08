namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Why an entry ended up <see cref="InboxMessageStatus.DeadLettered"/> - see
    /// <see cref="InboxFailureClassifier.Classify"/> for how a failure is assigned one of these.
    /// </summary>
    public enum InboxDeadLetterFailureCategory
    {
        /// <summary>
        /// The failure looked environment-related (a timeout, a dropped connection, throttling) -
        /// ordinarily retryable, dead-lettered only because <see cref="InboxOptions.MaxRetryAttempts"/>
        /// was exhausted before the environment recovered.
        /// </summary>
        Transient,

        /// <summary>
        /// The handler explicitly threw <see cref="InboxNonRetryableException"/> - a declared "this can
        /// never succeed, do not retry" signal, dead-lettered on the very first attempt regardless of
        /// remaining attempts.
        /// </summary>
        Permanent,

        /// <summary>
        /// The payload itself could not be interpreted (a deserialization/format exception) - retrying
        /// would reproduce the identical failure every time, since nothing about the environment caused it.
        /// </summary>
        Serialization,

        /// <summary>The handler could not authenticate/authorize the operation the message required.</summary>
        Authorization,

        /// <summary>
        /// Every attempt failed for a reason that did not match any of the categories above - a message
        /// that repeatedly "poisons" its own processing without a more specific, classifiable cause.
        /// </summary>
        Poison,
    }
}
