namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Assigns a <see cref="OutboxDeadLetterFailureCategory"/> to the exception that caused an entry to
    /// be dead-lettered. A best-effort heuristic based only on the exception's type - see
    /// <c>InboxFailureClassifier</c>'s remarks for why classification must happen while the exception is
    /// still in hand, before it is reduced to a sanitized reason string.
    /// </summary>
    public static class OutboxFailureClassifier
    {
        /// <param name="exception">The exception that caused this publish attempt to fail.</param>
        /// <param name="attemptsExhausted">Whether this dead-letter is happening because <see cref="OutboxOptions.MaxRetryAttempts"/> was reached, as opposed to an immediate <see cref="OutboxNonRetryableException"/>.</param>
        public static OutboxDeadLetterFailureCategory Classify(Exception exception, bool attemptsExhausted)
        {
            ArgumentNullException.ThrowIfNull(exception);

            if (exception is OutboxNonRetryableException)
                return OutboxDeadLetterFailureCategory.Permanent;

            if (IsAuthorizationFailure(exception))
                return OutboxDeadLetterFailureCategory.Authorization;

            if (IsSerializationFailure(exception))
                return OutboxDeadLetterFailureCategory.Serialization;

            if (!attemptsExhausted)
                return OutboxDeadLetterFailureCategory.Permanent;

            return IsTransientFailure(exception) ? OutboxDeadLetterFailureCategory.Transient : OutboxDeadLetterFailureCategory.Poison;
        }

        private static bool IsAuthorizationFailure(Exception exception) =>
            exception is UnauthorizedAccessException or System.Security.SecurityException;

        private static bool IsSerializationFailure(Exception exception) =>
            exception is FormatException
                or System.Runtime.Serialization.SerializationException
                or System.Text.Json.JsonException;

        private static bool IsTransientFailure(Exception exception) =>
            exception is TimeoutException
                or OperationCanceledException
                or System.Net.Http.HttpRequestException
                or System.Net.Sockets.SocketException
                or IOException;
    }
}
