using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// Realistic per-provider <see cref="IMessageIdResolver"/> configurations (issue #110's acceptance
    /// criterion that provider-specific resolver tests cover Kafka and RabbitMQ examples). These use
    /// plain header/scope inputs shaped like what each feeder would supply - the Inbox library stays
    /// transport-agnostic, so it depends on neither client library.
    /// </summary>
    public class ProviderMessageIdResolverExampleTests
    {
        [Fact]
        public void Kafka_NoBrokerMessageIdHeader_ShouldFallBackToTopicPartitionOffsetScope()
        {
            // Kafka has no built-in per-record message ID: the stable identity is (topic, partition, offset).
            var resolver = new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.FeederField, FieldName = "Offset" });
            var feederExtractedOffset = "784512";

            var first = resolver.Resolve(new MessageIdResolutionRequest
            {
                FeederFieldValue = feederExtractedOffset,
                ScopeSegments = ["orders-topic", "partition-3"],
            });
            var replay = resolver.Resolve(new MessageIdResolutionRequest
            {
                FeederFieldValue = feederExtractedOffset,
                ScopeSegments = ["orders-topic", "partition-3"],
            });
            var sameOffsetDifferentPartition = resolver.Resolve(new MessageIdResolutionRequest
            {
                FeederFieldValue = feederExtractedOffset,
                ScopeSegments = ["orders-topic", "partition-7"],
            });

            Assert.Equal(first.MessageId, replay.MessageId);
            Assert.NotEqual(first.MessageId, sameOffsetDifferentPartition.MessageId);
        }

        [Fact]
        public void Kafka_ProducerSuppliedHeader_ShouldResolveConsistentlyAcrossConsumerGroups()
        {
            // A producer that sets a custom dedup header (e.g. "x-idempotency-key") makes the ID globally
            // unique on its own - no partition scope needed.
            var resolver = new MessageIdResolver(new MessageIdResolverOptions
            {
                Strategy = InboxMessageIdStrategy.BrokerHeader,
                HeaderName = "x-idempotency-key",
            });
            var headers = new Dictionary<string, string> { ["x-idempotency-key"] = "order-created-42" };

            var consumerGroupA = resolver.Resolve(new MessageIdResolutionRequest { Headers = headers });
            var consumerGroupB = resolver.Resolve(new MessageIdResolutionRequest { Headers = headers });

            Assert.Equal(consumerGroupA.MessageId, consumerGroupB.MessageId);
        }

        [Fact]
        public void RabbitMQ_BasicPropertiesMessageIdHeader_ShouldResolveConsistently()
        {
            // RabbitMQ's BasicProperties.MessageId is optional and producer-set; feeders surface it as a header.
            var resolver = new MessageIdResolver(new MessageIdResolverOptions
            {
                Strategy = InboxMessageIdStrategy.BrokerHeader,
                HeaderName = "basic-properties-message-id",
            });
            var headers = new Dictionary<string, string> { ["basic-properties-message-id"] = "9f2c8b6e-4b7a-4b1a-9f7e-1a2b3c4d5e6f" };

            var first = resolver.Resolve(new MessageIdResolutionRequest { Headers = headers });
            var redelivered = resolver.Resolve(new MessageIdResolutionRequest { Headers = headers });

            Assert.Equal(first.MessageId, redelivered.MessageId);
        }

        [Fact]
        public void RabbitMQ_MissingMessageId_ShouldFallBackToPayloadHashScopedByExchangeAndRoutingKey()
        {
            // When a producer omits BasicProperties.MessageId, hash the payload - scoped by exchange/routing
            // key so the same payload published to a different queue is not treated as the same message.
            var resolver = new MessageIdResolver(new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.PayloadHash });
            var payload = "{\"orderId\":42}"u8.ToArray();

            var ordersQueue = resolver.Resolve(new MessageIdResolutionRequest { Payload = payload, ScopeSegments = ["orders-exchange", "orders.created"] });
            var auditQueue = resolver.Resolve(new MessageIdResolutionRequest { Payload = payload, ScopeSegments = ["audit-exchange", "orders.created"] });

            Assert.NotEqual(ordersQueue.MessageId, auditQueue.MessageId);
        }
    }
}
