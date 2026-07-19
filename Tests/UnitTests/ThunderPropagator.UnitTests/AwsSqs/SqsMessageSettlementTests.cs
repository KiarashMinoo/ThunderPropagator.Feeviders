using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using NSubstitute;
using ThunderPropagator.Feeders.AwsSqs;

namespace ThunderPropagator.UnitTests.AwsSqs
{
    public class SqsMessageSettlementTests
    {
        private const string QueueUrl = "https://sqs.us-east-1.amazonaws.com/000000000000/test-queue";

        private static Message CreateMessage() => new()
        {
            MessageId = "message-1",
            ReceiptHandle = "receipt-1",
            Body = "payload"
        };

        [Fact]
        public async Task YieldAndSettleAsync_ShouldDeleteOnlyAfterProcessingAdvances()
        {
            var client = Substitute.For<IAmazonSQS>();
            var message = CreateMessage();
            var logger = Substitute.For<ILogger>();

            await using var enumerator = SqsMessageSettlement
                .YieldAndSettleAsync(client, QueueUrl, message, "payload", logger, CancellationToken.None)
                .GetAsyncEnumerator();

            Assert.True(await enumerator.MoveNextAsync());
            Assert.Equal("payload", enumerator.Current);
            _ = client.DidNotReceiveWithAnyArgs().DeleteMessageAsync(default(string), default, default);
            _ = client.DidNotReceiveWithAnyArgs().ChangeMessageVisibilityAsync(default(string), default, default(int), default);

            Assert.False(await enumerator.MoveNextAsync());
            await client.Received(1).DeleteMessageAsync(QueueUrl, message.ReceiptHandle, CancellationToken.None);
            _ = client.DidNotReceiveWithAnyArgs().ChangeMessageVisibilityAsync(default(string), default, default(int), default);
        }

        [Fact]
        public async Task YieldAndSettleAsync_ShouldResetVisibilityWhenProcessingStopsEnumeration()
        {
            var client = Substitute.For<IAmazonSQS>();
            var message = CreateMessage();
            var logger = Substitute.For<ILogger>();
            using var cts = new CancellationTokenSource();
            var cancellationToken = cts.Token;

            var enumerator = SqsMessageSettlement
                .YieldAndSettleAsync(client, QueueUrl, message, "payload", logger, cancellationToken)
                .GetAsyncEnumerator();

            Assert.True(await enumerator.MoveNextAsync());

            await enumerator.DisposeAsync();

            _ = client.DidNotReceiveWithAnyArgs().DeleteMessageAsync(default(string), default, default);
            await client.Received(1).ChangeMessageVisibilityAsync(QueueUrl, message.ReceiptHandle, 0, cancellationToken);
        }

        [Fact]
        public async Task YieldAndSettleAsync_ShouldResetVisibilityWhenDeleteFails()
        {
            var client = Substitute.For<IAmazonSQS>();
            var message = CreateMessage();
            var logger = Substitute.For<ILogger>();
            using var cts = new CancellationTokenSource();
            var cancellationToken = cts.Token;
            client.DeleteMessageAsync(QueueUrl, message.ReceiptHandle, cancellationToken)
                .Returns(Task.FromException<DeleteMessageResponse>(new InvalidOperationException("delete failed")));

            await using var enumerator = SqsMessageSettlement
                .YieldAndSettleAsync(client, QueueUrl, message, "payload", logger, cancellationToken)
                .GetAsyncEnumerator();

            Assert.True(await enumerator.MoveNextAsync());

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await enumerator.MoveNextAsync().AsTask());
            await client.Received(1).ChangeMessageVisibilityAsync(QueueUrl, message.ReceiptHandle, 0, cancellationToken);
        }
    }
}
