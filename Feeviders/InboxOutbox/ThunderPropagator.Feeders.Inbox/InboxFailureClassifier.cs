namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Assigns a <see cref="InboxDeadLetterFailureCategory"/> to the exception that caused an entry to be
    /// dead-lettered. A best-effort heuristic based only on the exception's type - this store never
    /// retains the original exception instance (only its sanitized reason string, per
    /// <see cref="InboxMessageLimits.MaxFailureReasonLength"/>), so classification must happen at the
    /// moment the exception is still in hand, before it is reduced to text.
    /// </summary>
    public static class InboxFailureClassifier
    {
        /// <param name="exception">The exception that caused this attempt to fail.</param>
        /// <param name="attemptsExhausted">Whether this dead-letter is happening because <see cref="InboxOptions.MaxRetryAttempts"/> was reached, as opposed to an immediate <see cref="InboxNonRetryableException"/>.</param>
        public static InboxDeadLetterFailureCategory Classify(Exception exception, bool attemptsExhausted)
        {
            ArgumentNullException.ThrowIfNull(exception);

            if (exception is InboxNonRetryableException)
                return InboxDeadLetterFailureCategory.Permanent;

            if (IsAuthorizationFailure(exception))
                return InboxDeadLetterFailureCategory.Authorization;

            if (IsSerializationFailure(exception))
                return InboxDeadLetterFailureCategory.Serialization;

            if (!attemptsExhausted)
                return InboxDeadLetterFailureCategory.Permanent;

            return IsTransientFailure(exception) ? InboxDeadLetterFailureCategory.Transient : InboxDeadLetterFailureCategory.Poison;
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
