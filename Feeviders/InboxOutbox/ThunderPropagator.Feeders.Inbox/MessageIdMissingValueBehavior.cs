namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// What an <see cref="IMessageIdResolver"/> does when the value its configured
    /// <see cref="InboxMessageIdStrategy"/> depends on is absent from a
    /// <see cref="MessageIdResolutionRequest"/> - a missing header, a missing payload, or a missing
    /// feeder field.
    /// </summary>
    public enum MessageIdMissingValueBehavior
    {
        /// <summary>Throw <see cref="MessageIdResolutionException"/>. The default - silent fallback hides a misconfiguration.</summary>
        Throw,

        /// <summary>
        /// Fall back to a fresh GUID, the same as <see cref="InboxMessageIdStrategy.NewGuid"/> - this
        /// message is treated as unique rather than rejected.
        /// </summary>
        FallBackToNewGuid,
    }
}
