namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Controls whether <see cref="InboxDeadLetterContext.Payload"/> carries the original payload bytes -
    /// the persisted <see cref="InboxMessage.Payload"/> itself is never affected by this policy, only what
    /// a dead-letter handler pipeline is handed. A dead letter is, by definition, retained data an
    /// operator may need to inspect to diagnose or replay it; this policy exists for deployments where
    /// even that in-memory exposure (e.g. to a third-party alerting handler) must be limited.
    /// </summary>
    /// <remarks>
    /// This is not encryption - it decides visibility to the in-process handler pipeline only. Encrypting
    /// payload bytes at rest is the chosen <see cref="IInboxStore"/> backend's own concern (e.g. database-
    /// or collection-level encryption), never something this library performs itself.
    /// </remarks>
    public enum InboxDeadLetterPayloadPolicy
    {
        /// <summary>The original payload is included as-is. The default - appropriate when every registered handler is already trusted with raw message content.</summary>
        Include,

        /// <summary>
        /// The payload is withheld (<see cref="InboxDeadLetterContext.Payload"/> is <see langword="null"/>),
        /// but <see cref="InboxDeadLetterContext.PayloadContentType"/> is still populated - a handler can
        /// still route/alert on shape without seeing content.
        /// </summary>
        Redact,

        /// <summary>Both the payload and its content type are withheld.</summary>
        Omit,
    }
}
