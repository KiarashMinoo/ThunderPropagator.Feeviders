using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.Providers.DotNet.SharedKernel
{
    /// <summary>Result of one <see cref="AbstractProvider{TFeederMessage,TProviderConfiguration}.ExecuteWithResultAsync"/> call.</summary>
    public sealed record ProviderExecuteResult
    {
        /// <summary>Whether the message was published directly or durably enqueued.</summary>
        public required ProviderExecuteOutcome Outcome { get; init; }

        /// <summary>The durably enqueued entry. Populated only when <see cref="Outcome"/> is <see cref="ProviderExecuteOutcome.Enqueued"/>.</summary>
        public OutboxMessage? EnqueuedMessage { get; init; }

        public static ProviderExecuteResult Published() => new() { Outcome = ProviderExecuteOutcome.Published };

        public static ProviderExecuteResult Enqueued(OutboxMessage message) => new() { Outcome = ProviderExecuteOutcome.Enqueued, EnqueuedMessage = message };
    }
}
