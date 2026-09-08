namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Outcome of <see cref="IInboxReplayAuthorizer.AuthorizeAsync"/>.</summary>
    public sealed record InboxReplayAuthorizationResult
    {
        /// <summary>Whether the requested replay may proceed.</summary>
        public required bool IsAuthorized { get; init; }

        /// <summary>Human-readable reason for a denial. Ignored (and should be <see langword="null"/>) when <see cref="IsAuthorized"/> is <see langword="true"/>.</summary>
        public string? DenialReason { get; init; }

        public static InboxReplayAuthorizationResult Allow() => new() { IsAuthorized = true };

        public static InboxReplayAuthorizationResult Deny(string reason) => new() { IsAuthorized = false, DenialReason = reason };
    }
}
