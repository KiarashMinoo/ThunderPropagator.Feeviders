using NSubstitute;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class NonTransactionalOutboxUnitOfWorkTests
    {
        private static OutboxEnqueueRequest CreateRequest(string messageId) => new()
        {
            MessageId = messageId,
            ProviderKey = "provider-a",
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1, 2, 3],
        };

        private static OutboxMessage Persisted(OutboxEnqueueRequest request) =>
            OutboxMessage.CreatePending(Guid.NewGuid(), request.MessageId, request.ProviderKey, 0,
                request.SchemaVersion, request.PayloadContentType, request.Payload,
                request.Headers, request.PartitionKey, TimeProvider.System);

        [Fact]
        public async Task CommitAsync_ShouldEnqueueEveryStagedMessageInStagingOrder()
        {
            var store = Substitute.For<IOutboxStore>();
            var seenOrder = new List<string>();
            store.EnqueueAsync(Arg.Any<OutboxEnqueueRequest>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    var request = call.Arg<OutboxEnqueueRequest>();
                    seenOrder.Add(request.MessageId);
                    return Task.FromResult(Persisted(request));
                });
            await using var unitOfWork = new NonTransactionalOutboxUnitOfWork(store);
            unitOfWork.Enqueue(CreateRequest("m1"));
            unitOfWork.Enqueue(CreateRequest("m2"));
            unitOfWork.Enqueue(CreateRequest("m3"));

            var committed = await unitOfWork.CommitAsync();

            Assert.Equal(["m1", "m2", "m3"], seenOrder);
            Assert.Equal(3, committed.Count);
        }

        [Fact]
        public async Task DisposeAsync_WithoutCommit_ShouldNeverEnqueueAnything()
        {
            var store = Substitute.For<IOutboxStore>();
            var unitOfWork = new NonTransactionalOutboxUnitOfWork(store);
            unitOfWork.Enqueue(CreateRequest("abandoned"));

            await unitOfWork.DisposeAsync();

            await store.DidNotReceiveWithAnyArgs().EnqueueAsync(default!, default);
        }

        [Fact]
        public async Task CommitAsync_SecondMessageFails_ShouldLeaveTheFirstAlreadyEnqueued()
        {
            // Documents the lack of cross-message atomicity this mode explicitly does not provide -
            // unlike EfCoreOutboxUnitOfWork, a partial failure here is a real, expected possibility.
            var store = Substitute.For<IOutboxStore>();
            store.EnqueueAsync(Arg.Is<OutboxEnqueueRequest>(r => r.MessageId == "m1"), Arg.Any<CancellationToken>())
                .Returns(call => Task.FromResult(Persisted(call.Arg<OutboxEnqueueRequest>())));
            store.EnqueueAsync(Arg.Is<OutboxEnqueueRequest>(r => r.MessageId == "m2"), Arg.Any<CancellationToken>())
                .Returns<Task<OutboxMessage>>(_ => throw new InvalidOperationException("store unavailable"));
            await using var unitOfWork = new NonTransactionalOutboxUnitOfWork(store);
            unitOfWork.Enqueue(CreateRequest("m1"));
            unitOfWork.Enqueue(CreateRequest("m2"));

            await Assert.ThrowsAsync<InvalidOperationException>(() => unitOfWork.CommitAsync());

            await store.Received(1).EnqueueAsync(Arg.Is<OutboxEnqueueRequest>(r => r.MessageId == "m1"), Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task CommitAsync_WithNoStagedMessages_ShouldReturnEmptyWithoutCallingTheStore()
        {
            var store = Substitute.For<IOutboxStore>();
            await using var unitOfWork = new NonTransactionalOutboxUnitOfWork(store);

            var committed = await unitOfWork.CommitAsync();

            Assert.Empty(committed);
            await store.DidNotReceiveWithAnyArgs().EnqueueAsync(default!, default);
        }
    }
}
