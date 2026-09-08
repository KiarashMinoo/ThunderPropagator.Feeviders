using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class ProviderOutboxDeadLetterBrokerPublisherTests
    {
        // A hand-written fake, not an NSubstitute mock: IProvider.PublishDirectAsync(bytes, headers, ct)
        // is a default interface method (it throws NotSupportedException unless overridden), and Castle
        // DynamicProxy (which NSubstitute uses) cannot intercept default interface method bodies -
        // mirrors OutboxRelayWorkerTests.DelegateProvider, which hits the same constraint.
        private sealed class DelegateProvider(Action<byte[], IReadOnlyDictionary<string, string>?> onPublish) : IProvider
        {
            public Task PublishDirectAsync(byte[] bytes, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken = default)
            {
                onPublish(bytes, headers);
                return Task.CompletedTask;
            }

            public void Dispose()
            {
            }
        }

        [Fact]
        public async Task PublishAsync_ShouldDelegateToTheProvidersDirectPublishBypassPath()
        {
            byte[]? receivedPayload = null;
            IReadOnlyDictionary<string, string>? receivedHeaders = null;
            var provider = new DelegateProvider((bytes, headers) =>
            {
                receivedPayload = bytes;
                receivedHeaders = headers;
            });
            var publisher = new ProviderOutboxDeadLetterBrokerPublisher(provider);
            var context = new OutboxDeadLetterContext
            {
                Id = Guid.NewGuid(),
                MessageId = "message-1",
                ProviderKey = "dlq-provider",
                Attempts = 1,
                CreatedAtUtc = DateTimeOffset.UnixEpoch,
                DeadLetteredAtUtc = DateTimeOffset.UnixEpoch,
                FailureCategory = OutboxDeadLetterFailureCategory.Poison,
                FailureReason = "boom",
            };
            byte[] payload = [1, 2, 3];
            var headers = new Dictionary<string, string> { ["k"] = "v" };

            await publisher.PublishAsync(context, payload, headers, default);

            Assert.Equal(payload, receivedPayload);
            Assert.Equal(headers, receivedHeaders);
        }
    }
}
