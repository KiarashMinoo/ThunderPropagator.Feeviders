using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.UnitTests
{
    public class AbstractProviderOutboxTests
    {
        private static readonly byte[] SerializedBytes = [1, 2, 3];

        [Fact]
        public async Task ExecuteAsync_OutboxDisabled_ShouldPublishDirectlyAndNeverTouchTheStore()
        {
            var storeFactory = Substitute.For<IOutboxStoreFactory>();
            var provider = BuildProvider(new TestProviderConfiguration(), storeFactory);

            await provider.ExecuteAsync(new TestFeederMessage());

            Assert.Single(provider.PublishedPayloads, SerializedBytes);
            storeFactory.DidNotReceiveWithAnyArgs().GetStore(default!, default);
        }

        [Fact]
        public async Task ExecuteWithResultAsync_OutboxDisabled_ShouldReturnPublished()
        {
            var provider = BuildProvider(new TestProviderConfiguration(), Substitute.For<IOutboxStoreFactory>());

            var result = await provider.ExecuteWithResultAsync(new TestFeederMessage());

            Assert.Equal(ProviderExecuteOutcome.Published, result.Outcome);
            Assert.Null(result.EnqueuedMessage);
        }

        [Fact]
        public async Task ExecuteWithResultAsync_OutboxEnabled_ShouldPersistBeforeReturningEnqueuedResultAndNeverPublishDirectly()
        {
            var store = BuildStoreSpy();
            var storeFactory = BuildStoreFactory("orders-store", OutboxStoreType.InMemory, store);
            var provider = BuildProvider(EnabledConfiguration(), storeFactory);

            var result = await provider.ExecuteWithResultAsync(new TestFeederMessage());

            Assert.Equal(ProviderExecuteOutcome.Enqueued, result.Outcome);
            Assert.NotNull(result.EnqueuedMessage);
            await store.Received(1).EnqueueAsync(Arg.Any<OutboxEnqueueRequest>(), Arg.Any<CancellationToken>());
            Assert.Empty(provider.PublishedPayloads);
        }

        [Fact]
        public async Task ExecuteWithResultAsync_OutboxEnabled_ShouldSerializeExactlyOnce()
        {
            var storeFactory = BuildStoreFactory("orders-store", OutboxStoreType.InMemory, BuildStoreSpy());
            var serializer = new FakeFeederMessageSerializer();
            var provider = BuildProvider(EnabledConfiguration(), storeFactory, serializer);

            await provider.ExecuteWithResultAsync(new TestFeederMessage());

            Assert.Equal(1, serializer.SerializeToBytesCallCount);
        }

        [Fact]
        public async Task ExecuteWithResultAsync_OutboxEnabled_EnqueuedPayloadShouldMatchTheSerializedBytes()
        {
            var store = BuildStoreSpy();
            var storeFactory = BuildStoreFactory("orders-store", OutboxStoreType.InMemory, store);
            var provider = BuildProvider(EnabledConfiguration(), storeFactory);

            await provider.ExecuteWithResultAsync(new TestFeederMessage());

            await store.Received(1).EnqueueAsync(
                Arg.Is<OutboxEnqueueRequest>(r => r.Payload.SequenceEqual(SerializedBytes)),
                Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task ExecuteWithResultAsync_OutboxEnabled_ShouldPropagateTraceContextIntoHeaders()
        {
            var store = BuildStoreSpy();
            var storeFactory = BuildStoreFactory("orders-store", OutboxStoreType.InMemory, store);
            var provider = BuildProvider(EnabledConfiguration(), storeFactory);

            // DefaultIdFormat must be forced to W3C: with no ActivityListener registered, the runtime
            // isn't guaranteed to default a plain Activity to W3C, and without it Activity.Context
            // carries no real trace/span IDs for the propagator to inject.
            // Propagators.DefaultTextMapPropagator defaults to a no-op until a host app's OpenTelemetry
            // SDK calls the internal Sdk.SetDefaultTextMapPropagator - this test project depends only on
            // OpenTelemetry.Api (as production code does), so it installs a real propagator itself via
            // reflection against the property's internal setter, and restores the previous one afterward.
            var propagatorProperty = typeof(Propagators).GetProperty(nameof(Propagators.DefaultTextMapPropagator))!;
            var previousPropagator = Propagators.DefaultTextMapPropagator;
            var previousDefaultIdFormat = Activity.DefaultIdFormat;
            propagatorProperty.SetValue(null, new CompositeTextMapPropagator([new TraceContextPropagator(), new BaggagePropagator()]));
            Activity.DefaultIdFormat = ActivityIdFormat.W3C;
            using var activity = new Activity("test-span").Start();
            try
            {
                await provider.ExecuteWithResultAsync(new TestFeederMessage());
            }
            finally
            {
                activity.Stop();
                Activity.DefaultIdFormat = previousDefaultIdFormat;
                propagatorProperty.SetValue(null, previousPropagator);
            }

            await store.Received(1).EnqueueAsync(
                Arg.Is<OutboxEnqueueRequest>(r => r.Headers != null && r.Headers.ContainsKey("traceparent")),
                Arg.Any<CancellationToken>());
        }

        [Fact]
        public async Task PublishDirectAsync_ShouldBypassTheOutboxDecisionEntirely()
        {
            // Outbox is enabled here specifically to prove PublishDirectAsync bypasses the decision
            // regardless - a relay worker republishing an already-durable message must never re-enqueue.
            var storeFactory = BuildStoreFactory("orders-store", OutboxStoreType.InMemory, BuildStoreSpy());
            var provider = BuildProvider(EnabledConfiguration(), storeFactory);

            await ((IProvider)provider).PublishDirectAsync(SerializedBytes);

            Assert.Single(provider.PublishedPayloads, SerializedBytes);
            storeFactory.DidNotReceiveWithAnyArgs().GetStore(default!, default);
        }

        [Fact]
        public void Constructor_OutboxEnabledWithoutStoreConnectionName_ShouldThrow()
        {
            var config = new TestProviderConfiguration { Outbox = new OutboxOptions { OutboxEnabled = true, StoreType = OutboxStoreType.InMemory } };

            Assert.Throws<InvalidOperationException>(() => BuildProvider(config, Substitute.For<IOutboxStoreFactory>()));
        }

        [Fact]
        public void Constructor_OutboxEnabledWithoutAStoreFactory_ShouldThrow()
        {
            Assert.Throws<InvalidOperationException>(() => BuildProvider(EnabledConfiguration(), storeFactory: null));
        }

        [Fact]
        public void Constructor_OutboxEnabledWithInvalidOptions_ShouldThrow()
        {
            var config = new TestProviderConfiguration
            {
                Outbox = new OutboxOptions { OutboxEnabled = true, StoreType = OutboxStoreType.InMemory, RetryMaxDelay = TimeSpan.Zero, RetryBaseDelay = TimeSpan.FromSeconds(10) },
            };

            Assert.Throws<ArgumentException>(() => BuildProvider(config, Substitute.For<IOutboxStoreFactory>()));
        }

        [Fact]
        public async Task ExecuteWithResultAsync_SerializationFailure_ShouldThrowAndNeverTouchTheStore()
        {
            var storeFactory = BuildStoreFactory("orders-store", OutboxStoreType.InMemory, BuildStoreSpy());
            var serializer = new FakeFeederMessageSerializer { ThrowOnSerialize = new InvalidOperationException("bad payload") };
            var provider = BuildProvider(EnabledConfiguration(), storeFactory, serializer);

            await Assert.ThrowsAsync<InvalidOperationException>(() => provider.ExecuteWithResultAsync(new TestFeederMessage()));

            storeFactory.DidNotReceiveWithAnyArgs().GetStore(default!, default);
        }

        [Fact]
        public async Task ExecuteWithResultAsync_EnlistedModeWithoutAnOverride_ShouldThrow()
        {
            var config = new TestProviderConfiguration
            {
                Outbox = new OutboxOptions { OutboxEnabled = true, StoreType = OutboxStoreType.EFCore, StoreConnectionName = "sql", TransactionMode = OutboxTransactionMode.Enlisted },
            };
            var storeFactory = BuildStoreFactory("sql", OutboxStoreType.EFCore, BuildStoreSpy());
            var provider = BuildProvider(config, storeFactory);

            await Assert.ThrowsAsync<InvalidOperationException>(() => provider.ExecuteWithResultAsync(new TestFeederMessage()));
        }

        [Fact]
        public async Task ExecuteWithResultAsync_CommitFails_ShouldPropagateAndNeverPublishDirectly()
        {
            var store = Substitute.For<IOutboxStore>();
            store.EnqueueAsync(Arg.Any<OutboxEnqueueRequest>(), Arg.Any<CancellationToken>())
                .Returns<Task<OutboxMessage>>(_ => throw new InvalidOperationException("store unavailable"));
            var storeFactory = BuildStoreFactory("orders-store", OutboxStoreType.InMemory, store);
            var provider = BuildProvider(EnabledConfiguration(), storeFactory);

            await Assert.ThrowsAsync<InvalidOperationException>(() => provider.ExecuteWithResultAsync(new TestFeederMessage()));

            Assert.Empty(provider.PublishedPayloads);
        }

        [Fact]
        public async Task ExecuteWithResultAsync_CancelledToken_ShouldPropagateCancellationAndNeverEnqueue()
        {
            var storeFactory = BuildStoreFactory("orders-store", OutboxStoreType.InMemory, BuildStoreSpy());
            var serializer = new FakeFeederMessageSerializer { ThrowCancellation = true };
            var provider = BuildProvider(EnabledConfiguration(), storeFactory, serializer);
            using var cts = new CancellationTokenSource();
            await cts.CancelAsync();

            await Assert.ThrowsAsync<OperationCanceledException>(() => provider.ExecuteWithResultAsync(new TestFeederMessage(), cts.Token));

            storeFactory.DidNotReceiveWithAnyArgs().GetStore(default!, default);
        }

        private static TestProviderConfiguration EnabledConfiguration(Func<OutboxOptions, OutboxOptions>? with = null, IOutboxStoreFactory? storeFactory = null)
        {
            var options = new OutboxOptions { OutboxEnabled = true, StoreType = OutboxStoreType.InMemory, StoreConnectionName = "orders-store" };
            return new TestProviderConfiguration { Outbox = with?.Invoke(options) ?? options };
        }

        private static IOutboxStore BuildStoreSpy()
        {
            var store = Substitute.For<IOutboxStore>();
            store.EnqueueAsync(Arg.Any<OutboxEnqueueRequest>(), Arg.Any<CancellationToken>())
                .Returns(call => Task.FromResult(ToPersisted(call.Arg<OutboxEnqueueRequest>())));
            return store;
        }

        private static IOutboxStoreFactory BuildStoreFactory(string name, OutboxStoreType type, IOutboxStore store)
        {
            var factory = Substitute.For<IOutboxStoreFactory>();
            factory.GetStore(name, type).Returns(store);
            return factory;
        }

        private static OutboxMessage ToPersisted(OutboxEnqueueRequest request) =>
            OutboxMessage.CreatePending(Guid.NewGuid(), request.MessageId, request.ProviderKey, 0,
                request.SchemaVersion, request.PayloadContentType, request.Payload,
                request.Headers, request.PartitionKey, TimeProvider.System);

        private static TestProvider BuildProvider(TestProviderConfiguration configuration, IOutboxStoreFactory? storeFactory, FakeFeederMessageSerializer? serializer = null)
        {
            var services = new ServiceCollection();
            services.AddSingleton<ILoggerFactory>(Substitute.For<ILoggerFactory>());
            services.AddSingleton<IFeederMessageSerializer<TestFeederMessage, TestProviderConfiguration>>(serializer ?? new FakeFeederMessageSerializer());
            if (storeFactory is not null)
                services.AddSingleton(storeFactory);

            var serviceProvider = services.BuildServiceProvider();
            return new TestProvider(configuration, serviceProvider);
        }

        internal sealed class TestFeederMessage : FeederMessage;

        private sealed class TestProviderConfiguration : AbstractProviderConfiguration;

        private sealed class FakeFeederMessageSerializer : IFeederMessageSerializer<TestFeederMessage, TestProviderConfiguration>
        {
            public Exception? ThrowOnSerialize { get; set; }
            public bool ThrowCancellation { get; set; }
            public int SerializeToBytesCallCount { get; private set; }

            public string Serialize(TestFeederMessage feederMessage, CancellationToken cancellationToken = default) => throw new NotSupportedException();

            public byte[] SerializeToBytes(TestFeederMessage feederMessage, CancellationToken cancellationToken = default)
            {
                SerializeToBytesCallCount++;
                if (ThrowCancellation)
                    cancellationToken.ThrowIfCancellationRequested();
                if (ThrowOnSerialize is not null)
                    throw ThrowOnSerialize;
                return SerializedBytes;
            }
        }

        private sealed class TestProvider(TestProviderConfiguration providerConfiguration, IServiceProvider serviceProvider)
            : AbstractProvider<TestFeederMessage, TestProviderConfiguration>(providerConfiguration, serviceProvider)
        {
            public List<byte[]> PublishedPayloads { get; } = [];

            protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default)
            {
                PublishedPayloads.Add(bytes);
                return Task.CompletedTask;
            }
        }
    }
}
