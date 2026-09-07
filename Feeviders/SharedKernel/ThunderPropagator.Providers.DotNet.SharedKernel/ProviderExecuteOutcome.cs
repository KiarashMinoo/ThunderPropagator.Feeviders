namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    /// <summary>Result of one <see cref="AbstractProvider{TFeederMessage,TProviderConfiguration}.ExecuteWithResultAsync"/> call.</summary>
    public enum ProviderExecuteOutcome
    {
        /// <summary>The message was published directly - the Outbox was disabled for this Provider.</summary>
        Published,

        /// <summary>
        /// The message was durably enqueued to the Outbox - not yet published. A relay worker will
        /// publish it later via <see cref="IProvider.PublishDirectAsync"/>; do not infer the message has
        /// reached (or will imminently reach) the broker just because this call returned.
        /// </summary>
        Enqueued,
    }
}
