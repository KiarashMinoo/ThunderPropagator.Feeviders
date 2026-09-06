using System.Security.Cryptography;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Default, backend-independent <see cref="IMessageIdResolver"/> implementing every <see cref="InboxMessageIdStrategy"/>.</summary>
    public sealed class MessageIdResolver : IMessageIdResolver
    {
        // Unit Separator: vanishingly unlikely to appear in a header/topic/partition name, so two
        // different (scope, value) splits can never join into the same string - a plain '/' or ':'
        // could (scope=["a"], value="b/c") == (scope=["a/b"], value="c").
        private const char ScopeSeparator = '';

        private readonly MessageIdResolverOptions _options;

        public MessageIdResolver(MessageIdResolverOptions options)
        {
            options.Validate();
            _options = options;
        }

        /// <inheritdoc/>
        public MessageIdResolutionResult Resolve(MessageIdResolutionRequest request)
        {
            var (value, isFallback) = ResolveCore(request);

            var messageId = _options.Strategy == InboxMessageIdStrategy.NewGuid
                ? value
                : ApplyScope(value, request.ScopeSegments);

            if (messageId.Length > InboxMessageLimits.MaxMessageIdLength)
                throw new MessageIdResolutionException(
                    _options.Strategy,
                    $"Resolved message ID of length {messageId.Length} exceeds the maximum of {InboxMessageLimits.MaxMessageIdLength}.");

            return new MessageIdResolutionResult
            {
                MessageId = messageId,
                Strategy = _options.Strategy,
                IsFallback = isFallback,
            };
        }

        private (string Value, bool IsFallback) ResolveCore(MessageIdResolutionRequest request) =>
            _options.Strategy switch
            {
                InboxMessageIdStrategy.BrokerHeader => ResolveBrokerHeader(request),
                InboxMessageIdStrategy.PayloadHash => ResolvePayloadHash(request),
                InboxMessageIdStrategy.FeederField => ResolveFeederField(request),
                InboxMessageIdStrategy.NewGuid => (NewGuidValue(), false),
                _ => throw new ArgumentOutOfRangeException(nameof(request), _options.Strategy, "Unsupported message ID strategy."),
            };

        private (string, bool) ResolveBrokerHeader(MessageIdResolutionRequest request)
        {
            if (request.Headers is not null
                && request.Headers.TryGetValue(_options.HeaderName!, out var value)
                && !string.IsNullOrEmpty(value))
                return (value, false);

            return Missing($"Header '{_options.HeaderName}' was not present on the message.");
        }

        private (string, bool) ResolvePayloadHash(MessageIdResolutionRequest request)
        {
            if (request.Payload is not { Length: > 0 } payload)
                return Missing("No payload was supplied to hash.");

            var canonical = _options.PayloadCanonicalizer?.Invoke(payload) ?? payload;
            var hash = SHA256.HashData(canonical);
            return (Convert.ToHexString(hash).ToLowerInvariant(), false);
        }

        private (string, bool) ResolveFeederField(MessageIdResolutionRequest request)
        {
            if (!string.IsNullOrEmpty(request.FeederFieldValue))
                return (request.FeederFieldValue, false);

            return Missing("No feeder field value was supplied.");
        }

        private (string, bool) Missing(string reason) =>
            _options.MissingValueBehavior switch
            {
                MessageIdMissingValueBehavior.FallBackToNewGuid => (NewGuidValue(), true),
                _ => throw new MessageIdResolutionException(_options.Strategy, reason),
            };

        private static string NewGuidValue() => Guid.NewGuid().ToString("N");

        private static string ApplyScope(string value, IReadOnlyList<string>? scopeSegments) =>
            scopeSegments is not { Count: > 0 }
                ? value
                : string.Join(ScopeSeparator, scopeSegments.Append(value));
    }
}
