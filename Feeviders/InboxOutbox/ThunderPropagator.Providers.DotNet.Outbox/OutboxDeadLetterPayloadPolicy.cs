namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Controls whether <see cref="OutboxDeadLetterContext.Payload"/> carries the original payload bytes -
    /// the persisted <see cref="OutboxMessage.Payload"/> itself is never affected by this policy, only
    /// what a dead-letter handler pipeline is handed. See <c>InboxDeadLetterPayloadPolicy</c>'s remarks:
    /// this is not encryption, only in-process handler visibility.
    /// </summary>
    public enum OutboxDeadLetterPayloadPolicy
    {
        /// <summary>The original payload is included as-is. The default.</summary>
        Include,

        /// <summary>The payload is withheld, but <see cref="OutboxDeadLetterContext.PayloadContentType"/> is still populated.</summary>
        Redact,

        /// <summary>Both the payload and its content type are withheld.</summary>
        Omit,
    }
}
