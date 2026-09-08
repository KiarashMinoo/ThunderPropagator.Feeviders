namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>Outcome of <see cref="IOutboxReplayAuthorizer.AuthorizeAsync"/>.</summary>
    public sealed record OutboxReplayAuthorizationResult
    {
        /// <summary>Whether the requested replay may proceed.</summary>
        public required bool IsAuthorized { get; init; }

        /// <summary>Human-readable reason for a denial. Ignored (and should be <see langword="null"/>) when <see cref="IsAuthorized"/> is <see langword="true"/>.</summary>
        public string? DenialReason { get; init; }

        public static OutboxReplayAuthorizationResult Allow() => new() { IsAuthorized = true };

        public static OutboxReplayAuthorizationResult Deny(string reason) => new() { IsAuthorized = false, DenialReason = reason };
    }
}
