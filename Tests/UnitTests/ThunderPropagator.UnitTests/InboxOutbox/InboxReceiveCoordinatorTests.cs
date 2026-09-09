using NSubstitute;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.SharedKernel;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class InboxReceiveCoordinatorTests
    {
        private static readonly byte[] Payload = "payload"u8.ToArray();
        private static readonly Guid ChannelKey = Guid.NewGuid();
        private static readonly Guid FeederId = Guid.NewGuid();

        [Fact]
        public async Task ReceiveAsync_Disabled_ShouldInvokeHandlerAndNeverTouchTheStoreOrAcknowledge()
        {
            var store = Substitute.For<IInboxStore>();
            var coordinator = BuildCoordinator(new InboxOptions(), store);
            var handlerInvoked = false;
            var acknowledged = false;

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => { acknowledged = true; return ValueTask.CompletedTask; });

            Assert.Equal(InboxReceiveOutcome.Disabled, outcome);
            Assert.True(handlerInvoked);
            Assert.False(acknowledged);
            await store.DidNotReceiveWithAnyArgs().TryClaimAsync(default!);
        }

        [Fact]
        public async Task ReceiveAsync_ClaimedAndHandlerSucceeds_ShouldAcknowledgeBeforeTheHandlerRunsThenComplete()
        {
            var claimed = BuildClaimedMessage(attemptCount: 1);
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.Claimed(claimed));
            var coordinator = BuildCoordinator(EnabledOptions(), store);
            var sequence = new List<string>();

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { sequence.Add("handler"); return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => { sequence.Add("ack"); return ValueTask.CompletedTask; });

            Assert.Equal(InboxReceiveOutcome.Processed, outcome);
            Assert.Equal(["ack", "handler"], sequence);
            await store.Received(1).CompleteAsync(claimed.Id, Arg.Any<string>(), Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task ReceiveAsync_ProcessCrashesDuringHandler_ShouldLetASubsequentReceiveReclaimAndComplete()
        {
            // A real, stateful store (not a substitute) is required here - the whole point is that the
            // second call reclaims the exact same durable entry the first call abandoned.
            var timeProvider = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
            var store = new ReferenceInboxStore(timeProvider);
            var options = EnabledOptions() with
            {
                ClaimLeaseDuration = TimeSpan.FromMinutes(1),
                MessageIdResolution = new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.PayloadHash },
            };

            // First "process": claims and acknowledges through the real coordinator, then the handler
            // hangs forever - the call is deliberately never awaited to completion, so it never reaches
            // CompleteAsync/FailAsync, exactly like the process dying mid-handler.
            var firstAcknowledged = false;
            var crashedCall = BuildCoordinator(options, store).ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => new ValueTask(new TaskCompletionSource().Task),
                acknowledgeAsync: _ => { firstAcknowledged = true; return ValueTask.CompletedTask; });

            Assert.True(firstAcknowledged);
            Assert.False(crashedCall.IsCompleted);

            timeProvider.Advance(TimeSpan.FromMinutes(2));

            // A brand-new coordinator instance, mirroring a restarted process, shares only the durable
            // store - it must reclaim the lease the first call abandoned and complete the message.
            var handlerInvoked = false;
            var outcome = await BuildCoordinator(options, store).ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => ValueTask.CompletedTask);

            Assert.Equal(InboxReceiveOutcome.Processed, outcome);
            Assert.True(handlerInvoked);

            var messageId = new MessageIdResolver(options.MessageIdResolution)
                .Resolve(new MessageIdResolutionRequest { Headers = null, Payload = Payload }).MessageId;
            var message = await store.GetAsync(messageId, ChannelKey, null);
            Assert.Equal(InboxMessageStatus.Processed, message!.Status);
        }

        [Fact]
        public async Task ReceiveAsync_ClaimedAndHandlerThrows_BelowMaxRetryAttempts_ShouldFailWithSanitizedReason()
        {
            var claimed = BuildClaimedMessage(attemptCount: 1);
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.Claimed(claimed));
            var options = EnabledOptions() with { MaxRetryAttempts = 5 };
            var coordinator = BuildCoordinator(options, store);

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => throw new InvalidOperationException("payload had secret order-42 in it"),
                acknowledgeAsync: _ => ValueTask.CompletedTask);

            Assert.Equal(InboxReceiveOutcome.Failed, outcome);
            await store.Received(1).FailAsync(
                claimed.Id, Arg.Any<string>(),
                Arg.Is<string>(reason => !reason.Contains("secret") && !reason.Contains("order-42") && reason.Contains(nameof(InvalidOperationException))),
                Arg.Any<DateTimeOffset?>(), Arg.Any<CancellationToken>());
            await store.DidNotReceiveWithAnyArgs().DeadLetterAsync(default, default!, default!);
        }

        [Fact]
        public async Task ReceiveAsync_ClaimedAndHandlerThrows_AtMaxRetryAttempts_ShouldDeadLetter()
        {
            var claimed = BuildClaimedMessage(attemptCount: 3);
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.Claimed(claimed));
            var options = EnabledOptions() with { MaxRetryAttempts = 3 };
            var coordinator = BuildCoordinator(options, store);

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => throw new InvalidOperationException("boom"),
                acknowledgeAsync: _ => ValueTask.CompletedTask);

            Assert.Equal(InboxReceiveOutcome.DeadLettered, outcome);
            await store.Received(1).DeadLetterAsync(claimed.Id, Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
            await store.DidNotReceiveWithAnyArgs().FailAsync(default, default!, default!, default, default);
        }

        [Fact]
        public async Task ReceiveAsync_AlreadyProcessed_ShouldAcknowledgeWithoutInvokingTheHandler()
        {
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.AlreadyProcessed());
            var coordinator = BuildCoordinator(EnabledOptions(), store);
            var handlerInvoked = false;
            var acknowledged = false;

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => { acknowledged = true; return ValueTask.CompletedTask; });

            Assert.Equal(InboxReceiveOutcome.Duplicate, outcome);
            Assert.False(handlerInvoked);
            Assert.True(acknowledged);
        }

        [Fact]
        public async Task ReceiveAsync_AlreadyDeadLettered_ShouldAcknowledgeWithoutInvokingTheHandler()
        {
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.DeadLettered());
            var coordinator = BuildCoordinator(EnabledOptions(), store);
            var handlerInvoked = false;
            var acknowledged = false;

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => { acknowledged = true; return ValueTask.CompletedTask; });

            Assert.Equal(InboxReceiveOutcome.AlreadyDeadLettered, outcome);
            Assert.False(handlerInvoked);
            Assert.True(acknowledged);
        }

        [Fact]
        public async Task ReceiveAsync_ClaimedByAnotherOwner_ShouldNotAcknowledgeOrInvokeTheHandler()
        {
            var store = Substitute.For<IInboxStore>();
            store.TryClaimAsync(Arg.Any<InboxClaimRequest>(), Arg.Any<CancellationToken>())
                .Returns(InboxClaimResult.ClaimedByAnotherOwner());
            var coordinator = BuildCoordinator(EnabledOptions(), store);
            var handlerInvoked = false;
            var acknowledged = false;

            var outcome = await coordinator.ReceiveAsync(
                Payload, "application/octet-stream", headers: null, partitionKey: null,
                invokeHandlerAsync: _ => { handlerInvoked = true; return ValueTask.CompletedTask; },
                acknowledgeAsync: _ => { acknowledged = true; return ValueTask.CompletedTask; });

            Assert.Equal(InboxReceiveOutcome.InProgress, outcome);
            Assert.False(handlerInvoked);
            Assert.False(acknowledged);
        }

        [Fact]
        public void Constructor_EnabledWithoutStoreConnectionName_ShouldThrow()
        {
            var options = EnabledOptions() with { StoreConnectionName = null, StoreType = InboxStoreType.InMemory };

            Assert.Throws<InvalidOperationException>(() => new InboxReceiveCoordinator(options, ChannelKey, FeederId, Substitute.For<IInboxStoreFactory>()));
        }

        [Fact]
        public void Constructor_EnabledWithoutAStoreFactory_ShouldThrow()
        {
            Assert.Throws<InvalidOperationException>(() => new InboxReceiveCoordinator(EnabledOptions(), ChannelKey, FeederId, storeFactory: null));
        }

        [Fact]
        public void Constructor_InvalidOptions_ShouldThrow()
        {
            var options = EnabledOptions() with { RetryBaseDelay = TimeSpan.FromSeconds(10), RetryMaxDelay = TimeSpan.FromSeconds(1) };

            Assert.Throws<ArgumentException>(() => new InboxReceiveCoordinator(options, ChannelKey, FeederId, Substitute.For<IInboxStoreFactory>()));
        }

        [Fact]
        public void Constructor_Disabled_ShouldNotTouchTheStoreFactory()
        {
            var storeFactory = Substitute.For<IInboxStoreFactory>();

            _ = new InboxReceiveCoordinator(new InboxOptions(), ChannelKey, FeederId, storeFactory);

            storeFactory.DidNotReceiveWithAnyArgs().GetStore(default!, default);
        }

        private static InboxReceiveCoordinator BuildCoordinator(InboxOptions options, IInboxStore store)
        {
            var storeFactory = Substitute.For<IInboxStoreFactory>();
            if (options.InboxEnabled)
                storeFactory.GetStore(options.StoreConnectionName!, options.StoreType).Returns(store);

            return new InboxReceiveCoordinator(options, ChannelKey, FeederId, storeFactory);
        }

        private static InboxOptions EnabledOptions() => new()
        {
            InboxEnabled = true,
            StoreType = InboxStoreType.InMemory,
            StoreConnectionName = "test-store",
        };

        private static InboxMessage BuildClaimedMessage(int attemptCount) =>
            InboxMessage.CreateReceived(
                Guid.NewGuid(), "message-id", ChannelKey, FeederId,
                schemaVersion: 1, payloadContentType: "application/octet-stream", payload: Payload,
                headers: null, partitionKey: null, timeProvider: TimeProvider.System)
            .TryTransitionTo(InboxMessageStatus.Processing, TimeProvider.System, leaseOwner: "owner", leaseExpiresAtUtc: DateTimeOffset.UtcNow.AddSeconds(30), incrementAttempt: true)
            with
            { AttemptCount = attemptCount };
    }
}
