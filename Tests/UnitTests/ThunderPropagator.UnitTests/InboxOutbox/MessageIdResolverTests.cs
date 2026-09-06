using System.Text;
using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class MessageIdResolverTests
    {
        [Fact]
        public void Resolve_BrokerHeader_ShouldBeConsistentForTheSameHeaderValue()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions
            {
                Strategy = InboxMessageIdStrategy.BrokerHeader,
                HeaderName = "message-id",
            });
            var request = new MessageIdResolutionRequest { Headers = new Dictionary<string, string> { ["message-id"] = "abc-123" } };

            var first = resolver.Resolve(request);
            var second = resolver.Resolve(request);

            Assert.Equal("abc-123", first.MessageId);
            Assert.Equal(first.MessageId, second.MessageId);
            Assert.False(first.IsFallback);
        }

        [Fact]
        public void Resolve_BrokerHeader_MissingHeader_ShouldThrowByDefault()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions
            {
                Strategy = InboxMessageIdStrategy.BrokerHeader,
                HeaderName = "message-id",
            });
            var request = new MessageIdResolutionRequest { Headers = new Dictionary<string, string>() };

            var exception = Assert.Throws<MessageIdResolutionException>(() => resolver.Resolve(request));
            Assert.Equal(InboxMessageIdStrategy.BrokerHeader, exception.Strategy);
        }

        [Fact]
        public void Resolve_BrokerHeader_MissingHeader_ShouldFallBackWhenConfigured()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions
            {
                Strategy = InboxMessageIdStrategy.BrokerHeader,
                HeaderName = "message-id",
                MissingValueBehavior = MessageIdMissingValueBehavior.FallBackToNewGuid,
            });
            var request = new MessageIdResolutionRequest();

            var first = resolver.Resolve(request);
            var second = resolver.Resolve(request);

            Assert.True(first.IsFallback);
            Assert.NotEqual(first.MessageId, second.MessageId);
        }

        [Fact]
        public void Constructing_BrokerHeaderWithoutHeaderName_ShouldThrow() =>
            Assert.Throws<ArgumentException>(() => new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.BrokerHeader }));

        [Fact]
        public void Constructing_FeederFieldWithoutFieldName_ShouldThrow() =>
            Assert.Throws<ArgumentException>(() => new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.FeederField }));

        [Fact]
        public void Resolve_FeederField_ShouldUseTheSuppliedValue()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.FeederField, FieldName = "OrderId" });

            var result = resolver.Resolve(new MessageIdResolutionRequest { FeederFieldValue = "order-42" });

            Assert.Equal("order-42", result.MessageId);
        }

        [Fact]
        public void Resolve_FeederField_MissingValue_ShouldThrowByDefault()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.FeederField, FieldName = "OrderId" });

            Assert.Throws<MessageIdResolutionException>(() => resolver.Resolve(new MessageIdResolutionRequest()));
        }

        [Fact]
        public void Resolve_PayloadHash_ShouldBeDeterministicForTheSamePayload()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.PayloadHash });
            var payload = Encoding.UTF8.GetBytes("""{"orderId":42}""");

            var first = resolver.Resolve(new MessageIdResolutionRequest { Payload = payload });
            var second = resolver.Resolve(new MessageIdResolutionRequest { Payload = (byte[])payload.Clone() });

            Assert.Equal(first.MessageId, second.MessageId);
        }

        [Fact]
        public void Resolve_PayloadHash_DifferentPayloads_ShouldProduceDifferentIds()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.PayloadHash });

            var first = resolver.Resolve(new MessageIdResolutionRequest { Payload = "a"u8.ToArray() });
            var second = resolver.Resolve(new MessageIdResolutionRequest { Payload = "b"u8.ToArray() });

            Assert.NotEqual(first.MessageId, second.MessageId);
        }

        [Fact]
        public void Resolve_PayloadHash_MissingPayload_ShouldThrowByDefault()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.PayloadHash });

            Assert.Throws<MessageIdResolutionException>(() => resolver.Resolve(new MessageIdResolutionRequest()));
        }

        [Fact]
        public void Resolve_PayloadHash_WithCanonicalizer_ShouldHashEquivalentPayloadsIdentically()
        {
            // Same logical JSON, different whitespace - a serializer-variation the canonicalizer normalizes away.
            var resolver = new MessageIdResolver(new MessageIdResolverOptions
            {
                Strategy = InboxMessageIdStrategy.PayloadHash,
                PayloadCanonicalizer = bytes => Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(bytes).Replace(" ", string.Empty)),
            });

            var compact = resolver.Resolve(new MessageIdResolutionRequest { Payload = Encoding.UTF8.GetBytes("""{"orderId":42}""") });
            var spaced = resolver.Resolve(new MessageIdResolutionRequest { Payload = Encoding.UTF8.GetBytes("""{ "orderId": 42 }""") });

            Assert.Equal(compact.MessageId, spaced.MessageId);
        }

        [Fact]
        public void Resolve_NewGuid_ShouldProduceAUniqueIdOnEveryCall()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.NewGuid });

            var first = resolver.Resolve(new MessageIdResolutionRequest());
            var second = resolver.Resolve(new MessageIdResolutionRequest());

            Assert.NotEqual(first.MessageId, second.MessageId);
            Assert.False(first.IsFallback);
        }

        [Fact]
        public void Resolve_NewGuid_ShouldIgnoreScopeSegments()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.NewGuid });

            var result = resolver.Resolve(new MessageIdResolutionRequest { ScopeSegments = ["topic", "0"] });

            Assert.DoesNotContain("topic", result.MessageId);
        }

        [Fact]
        public void Resolve_ScopeSegments_DifferentPartitionsOfTheSameBaseId_ShouldNotCollide()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions
            {
                Strategy = InboxMessageIdStrategy.BrokerHeader,
                HeaderName = "offset",
            });
            var headers = new Dictionary<string, string> { ["offset"] = "100" };

            var partitionZero = resolver.Resolve(new MessageIdResolutionRequest { Headers = headers, ScopeSegments = ["orders", "0"] });
            var partitionOne = resolver.Resolve(new MessageIdResolutionRequest { Headers = headers, ScopeSegments = ["orders", "1"] });

            Assert.NotEqual(partitionZero.MessageId, partitionOne.MessageId);
        }

        [Fact]
        public void Resolve_ScopeSegments_AmbiguousConcatenation_ShouldNotCollide()
        {
            // Without a separator unlikely to appear in a segment, (scope=["a"], value="b/c") and
            // (scope=["a/b"], value="c") could join into the same string under naive concatenation.
            var resolver = new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.FeederField, FieldName = "OrderId" });

            var first = resolver.Resolve(new MessageIdResolutionRequest { FeederFieldValue = "b/c", ScopeSegments = ["a"] });
            var second = resolver.Resolve(new MessageIdResolutionRequest { FeederFieldValue = "c", ScopeSegments = ["a/b"] });

            Assert.NotEqual(first.MessageId, second.MessageId);
        }

        [Fact]
        public void Resolve_ResolvedIdExceedingTheMaxLength_ShouldThrow()
        {
            var resolver = new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.FeederField, FieldName = "OrderId" });
            var request = new MessageIdResolutionRequest { FeederFieldValue = new string('x', InboxMessageLimits.MaxMessageIdLength + 1) };

            Assert.Throws<MessageIdResolutionException>(() => resolver.Resolve(request));
        }
    }
}
