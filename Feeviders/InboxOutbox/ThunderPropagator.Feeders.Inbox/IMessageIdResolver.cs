namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Derives the stable, configurable <see cref="InboxMessage.MessageId"/> a feeder should hand to
    /// <see cref="IInboxStore.TryClaimAsync"/> for one inbound message. Deduplication depends on this
    /// value being deterministic for the same logical message and distinct across different ones -
    /// see <see cref="InboxMessageIdStrategy"/> for the supported derivations.
    /// </summary>
    public interface IMessageIdResolver
    {
        /// <summary>
        /// Resolves a message ID from <paramref name="request"/> per the configured
        /// <see cref="InboxMessageIdStrategy"/>. Pure and synchronous - never touches the network or a store.
        /// </summary>
        /// <exception cref="MessageIdResolutionException">
        /// The configured strategy's required value is missing and the configured
        /// <see cref="MessageIdMissingValueBehavior"/> is <see cref="MessageIdMissingValueBehavior.Throw"/>,
        /// or the resolved ID exceeds <see cref="InboxMessageLimits.MaxMessageIdLength"/>.
        /// </exception>
        MessageIdResolutionResult Resolve(MessageIdResolutionRequest request);
    }
}
