using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using ThunderPropagator.BuildingBlocks.Application;
using ThunderPropagator.BuildingBlocks.Application.Serializations;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.SharedKernel;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// End-to-end proof for issue #136: a business change and its Outbox entry commit atomically in one
/// relational transaction even while RabbitMQ is down, survive a provider/relay "process restart," and
/// relay safely - in partition order, under a stable MessageId - once the broker recovers. Also verifies
/// (and documents) that this Outbox gives at-least-once, never exactly-once, delivery: an entry whose
/// publish acknowledgement is ambiguous is retried under the same MessageId, which a downstream consumer
/// must be prepared to see as a duplicate.
/// </summary>
[Trait("Category", "Integration")]
public sealed class RabbitMqOutboxRecoveryTests(RabbitMqContainerFixture rabbitMqFixture, PostgresContainerFixture postgresFixture)
    : IClassFixture<RabbitMqContainerFixture>, IClassFixture<PostgresContainerFixture>
{
    [Fact]
    public async Task AtomicCommit_WhileBrokerIsDown_ShouldPersistBusinessStateAndOutboxRowTogether()
    {
        var (schema, options) = NewSchema();
        var providerKey = Guid.NewGuid().ToString("N");

        await rabbitMqFixture.StopBrokerAsync();
        try
        {
            await using var dbContext = new TestOrderDbContext(options, schema);
            dbContext.Orders.Add(new TestOrder { Id = 1, CustomerName = "Ada" });
            await using var unitOfWork = new EfCoreOutboxUnitOfWork(dbContext);
            unitOfWork.Enqueue(new OutboxEnqueueRequest
            {
                MessageId = "order-1-created",
                ProviderKey = providerKey,
                SchemaVersion = 1,
                PayloadContentType = "application/octet-stream",
                Payload = "payload"u8.ToArray(),
            });

            var committed = await unitOfWork.CommitAsync();
            Assert.Single(committed);
            Assert.Equal(OutboxMessageStatus.Pending, committed[0].Status);
        }
        finally
        {
            await rabbitMqFixture.StartBrokerAsync();
        }

        await using var verify = new TestOrderDbContext(options, schema);
        Assert.Equal("Ada", (await verify.Orders.FindAsync(1))!.CustomerName);
        var store = new EfCoreOutboxStore(() => new TestOrderDbContext(options, schema));
        Assert.Equal(1, await store.GetDepthAsync(null));
    }

    [Fact]
    public async Task RelayAfterBrokerRecovery_ShouldPublishInPartitionOrderUnderStableMessageIds()
    {
        var (schema, options) = NewSchema();
        var providerKey = Guid.NewGuid().ToString("N");
        var queue = $"queue-{Guid.NewGuid():N}";
        const string partitionKey = "partition-a";

        await rabbitMqFixture.StopBrokerAsync();

        // Three separate business transactions committed while the broker is down - one reused
        // EfCoreOutboxUnitOfWork/DbContext across them, so OrderingSequence (assigned client-side,
        // starting at zero per EfCoreOutboxUnitOfWork instance - see its own remarks) stays correctly
        // relative across all three. Cross-instance Enlisted ordering is a separately documented,
        // pre-existing gap (a database-level sequence is "a real backend's concern, not this
        // enlistment boundary's") - not what this test exercises.
        var expectedMessageIds = new[] { "order-1-created", "order-2-created", "order-3-created" };
        await using (var dbContext = new TestOrderDbContext(options, schema))
        {
            await using var unitOfWork = new EfCoreOutboxUnitOfWork(dbContext);
            for (var i = 0; i < expectedMessageIds.Length; i++)
            {
                dbContext.Orders.Add(new TestOrder { Id = i + 1, CustomerName = $"Customer{i + 1}" });
                unitOfWork.Enqueue(new OutboxEnqueueRequest
                {
                    MessageId = expectedMessageIds[i],
                    ProviderKey = providerKey,
                    PartitionKey = partitionKey,
                    SchemaVersion = 1,
                    PayloadContentType = "application/octet-stream",
                    Payload = "payload"u8.ToArray(),
                });
                await unitOfWork.CommitAsync();
            }
        }

        var relayOptions = new OutboxOptions
        {
            OutboxEnabled = true,
            StoreType = OutboxStoreType.EFCore,
            StoreConnectionName = "test-outbox",
            TransactionMode = OutboxTransactionMode.Enlisted,
            MaxRetryAttempts = 1000,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
            RetryMaxDelay = TimeSpan.FromMilliseconds(50),
        };

        var provider = CreateProvider(queue);
        var store = new EfCoreOutboxStore(() => new TestOrderDbContext(options, schema));
        var worker = CreateRelayWorker(providerKey, relayOptions, provider, store);

        // Attempt to relay while the broker is still down - every attempt fails (connection refused),
        // each entry backs off for retry. "Restart provider/relay at controlled points": dispose this
        // worker/provider and build brand-new instances below, simulating a process restart - nothing
        // durable was lost, since the entries only ever lived in Postgres, never in-memory here.
        await worker.RunOnceAsync();
        await provider.DisposeAsync();

        await rabbitMqFixture.StartBrokerAsync();

        var restartedProvider = CreateProvider(queue);
        var restartedWorker = CreateRelayWorker(providerKey, relayOptions, restartedProvider, store);
        try
        {
            await WaitUntilAsync(async () =>
            {
                await restartedWorker.RunOnceAsync();
                return await store.GetDepthAsync(partitionKey) == 0;
            }, TimeSpan.FromSeconds(60));
        }
        finally
        {
            await restartedProvider.DisposeAsync();
        }

        var received = await ConsumeAllAsync(queue, expectedMessageIds.Length);
        Assert.Equal(expectedMessageIds, received.Select(m => m.MessageId));
    }

    [Fact]
    public async Task AmbiguousAcknowledgement_ShouldRetryUnderTheSameMessageId_DocumentingAtLeastOnceDelivery()
    {
        var (schema, options) = NewSchema();
        var providerKey = Guid.NewGuid().ToString("N");
        var queue = $"queue-{Guid.NewGuid():N}";
        var store = new EfCoreOutboxStore(() => new TestOrderDbContext(options, schema));

        await store.EnqueueAsync(new OutboxEnqueueRequest
        {
            MessageId = "ambiguous-publish",
            ProviderKey = providerKey,
            SchemaVersion = 1,
            PayloadContentType = "application/octet-stream",
            Payload = "payload"u8.ToArray(),
        });

        // Simulates a relay crash between the broker's acknowledgement and this worker's own
        // MarkPublishedAsync call: the entry is claimed (Publishing) but never marked Published. This
        // Outbox documents (OutboxOptions' own remarks) that it cannot tell the two apart from an
        // ambiguous publish outcome either - both leave an entry that must be retried, and a downstream
        // consumer that cannot tolerate a duplicate must deduplicate on OutboxHeaderNames.MessageId
        // itself, since this library gives no stronger guarantee than at-least-once.
        var claimed = await store.ClaimBatchAsync(null, maxCount: 10, "crashed-worker", TimeSpan.FromMilliseconds(1));
        Assert.Single(claimed);
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        var relayOptions = new OutboxOptions
        {
            OutboxEnabled = true,
            StoreType = OutboxStoreType.EFCore,
            StoreConnectionName = "test-outbox",
            MaxRetryAttempts = 1000,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
            RetryMaxDelay = TimeSpan.FromMilliseconds(50),
        };
        var provider = CreateProvider(queue);
        var worker = CreateRelayWorker(providerKey, relayOptions, provider, store);
        try
        {
            await WaitUntilAsync(async () =>
            {
                await worker.RunOnceAsync();
                return await store.GetDepthAsync(null) == 0;
            }, TimeSpan.FromSeconds(60));
        }
        finally
        {
            await provider.DisposeAsync();
        }

        var received = Assert.Single(await ConsumeAllAsync(queue, 1));
        Assert.Equal("ambiguous-publish", received.MessageId);
    }

    private (string Schema, DbContextOptions<TestOrderDbContext> Options) NewSchema()
    {
        var schema = $"s{Guid.NewGuid():N}";
        var options = TestOrderDbContext.BuildOptions(postgresFixture.GetConnectionString());
        using (var db = new TestOrderDbContext(options, schema))
            db.CreateTables();

        return (schema, options);
    }

    private TestOutboxProvider CreateProvider(string queue)
    {
        var configuration = new TestOutboxProviderConfiguration();
        var serializer = Substitute.For<IFeederMessageSerializer<TestOutboxMessage, TestOutboxProviderConfiguration>>();
        serializer.SerializeToBytes(Arg.Any<TestOutboxMessage>(), Arg.Any<CancellationToken>()).Returns("payload"u8.ToArray());

        var services = new TestServiceProvider(serializer);
        // Resolves the connection string lazily (at publish time), never captured here - the broker
        // may still be down when the provider is constructed, and a restart is not guaranteed to keep
        // the same mapped port, so only the CURRENT value at the moment of connecting is ever valid.
        return new TestOutboxProvider(configuration, services, () => rabbitMqFixture.ConnectionString, queue);
    }

    private static OutboxRelayWorker CreateRelayWorker(string providerKey, OutboxOptions options, TestOutboxProvider provider, EfCoreOutboxStore store)
    {
        var storeFactory = Substitute.For<IOutboxStoreFactory>();
        storeFactory.GetStore("test-outbox", OutboxStoreType.EFCore).Returns(store);

        var subscription = new OutboxRelaySubscription
        {
            ProviderKey = providerKey,
            Options = options,
            ResolveProvider = _ => provider,
        };

        return new OutboxRelayWorker([subscription], storeFactory, new TestServiceProvider(null));
    }

    private async Task<IReadOnlyList<(string MessageId, byte[] Body)>> ConsumeAllAsync(string queue, int expectedCount)
    {
        // A just-restarted broker's readiness check (container-level) can pass slightly before its AMQP
        // listener actually accepts connections - retry rather than fail on the first attempt.
        var factory = new ConnectionFactory { Uri = new Uri(rabbitMqFixture.ConnectionString) };
        await using var connection = await ConnectWithRetryAsync(factory);
        await using var channel = await connection.CreateChannelAsync();

        var results = new List<(string, byte[])>();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        while (results.Count < expectedCount && DateTime.UtcNow < deadline)
        {
            var result = await channel.BasicGetAsync(queue, autoAck: true);
            if (result is null)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(200));
                continue;
            }

            var messageId = result.BasicProperties.Headers is { } headers && headers.TryGetValue(OutboxHeaderNames.MessageId, out var raw)
                ? raw switch { byte[] bytes => Encoding.UTF8.GetString(bytes), string s => s, _ => raw?.ToString() ?? "" }
                : "";
            results.Add((messageId, result.Body.ToArray()));
        }

        return results;
    }

    private static async Task<IConnection> ConnectWithRetryAsync(ConnectionFactory factory, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(60));
        while (true)
        {
            try
            {
                return await factory.CreateConnectionAsync();
            }
            catch (BrokerUnreachableException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(300));
            }
        }
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        Assert.Fail($"Condition was not met within {timeout}.");
    }

    private sealed class TestServiceProvider(object? serializer) : IServiceProvider
    {
        public object? GetService(Type serviceType) => serviceType switch
        {
            _ when serviceType == typeof(IFeederMessageSerializer<TestOutboxMessage, TestOutboxProviderConfiguration>) => serializer,
            _ when serviceType == typeof(ILoggerFactory) => NullLoggerFactory.Instance,
            _ => null,
        };
    }

    internal sealed class TestOutboxProviderConfiguration : AbstractProviderConfiguration;

    internal sealed class TestOutboxMessage : FeederMessage;

    private sealed class TestOutboxProvider(TestOutboxProviderConfiguration configuration, IServiceProvider serviceProvider, Func<string> resolveConnectionString, string queue)
        : AbstractProvider<TestOutboxMessage, TestOutboxProviderConfiguration>(configuration, serviceProvider), IProvider
    {
        // This test enqueues directly via EfCoreOutboxUnitOfWork (see the test bodies), never through
        // AbstractProvider.ExecuteAsync, so the base CreateOutboxUnitOfWork/enqueue path is never
        // exercised here - only PublishDirectAsync (the relay worker's bypass) is.
        Task IProvider.PublishDirectAsync(byte[] bytes, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken) =>
            PublishAsync(bytes, headers, cancellationToken);

        private async Task PublishAsync(byte[] bytes, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken)
        {
            // Resolved now, not at construction - see CreateProvider's remarks.
            var factory = new ConnectionFactory { Uri = new Uri(resolveConnectionString()) };
            await using var connection = await factory.CreateConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);
            await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken);

            var properties = new BasicProperties();
            if (headers is not null)
            {
                properties.Headers = new Dictionary<string, object?>();
                foreach (var (key, value) in headers)
                    properties.Headers[key] = value;
            }

            await channel.BasicPublishAsync(exchange: "", routingKey: queue, mandatory: false, basicProperties: properties, body: bytes, cancellationToken: cancellationToken);
        }

        protected override Task InternalExecuteAsync(byte[] bytes, CancellationToken cancellationToken = default) =>
            PublishAsync(bytes, headers: null, cancellationToken);
    }

    private sealed class TestOrderDbContext(DbContextOptions<TestOrderDbContext> options, string schema) : DbContext(options)
    {
        public string Schema { get; } = schema;

        public DbSet<TestOrder> Orders => Set<TestOrder>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ApplyConfiguration(new OutboxMessageEntityTypeConfiguration(schema));
            modelBuilder.ApplyConfiguration(new OutboxSequenceCounterEntityTypeConfiguration(schema));
            modelBuilder.ApplyConfiguration(new OutboxSchemaVersionEntityTypeConfiguration(schema));
            modelBuilder.Entity<TestOrder>(builder =>
            {
                builder.ToTable("TestOrders", schema);
                builder.HasKey(order => order.Id);
                builder.Property(order => order.CustomerName).IsRequired();
            });
        }

        // EF Core caches the built model per DbContext CLR type by default, assuming OnModelCreating
        // always produces the same model for a given type - not true here, since schema varies per
        // test (mirrors EfCoreTestDbContext's own SchemaModelCacheKeyFactory).
        public static DbContextOptions<TestOrderDbContext> BuildOptions(string connectionString) =>
            new DbContextOptionsBuilder<TestOrderDbContext>()
                .UseNpgsql(connectionString)
                .ReplaceService<IModelCacheKeyFactory, SchemaModelCacheKeyFactory>()
                .Options;

        public void CreateTables() => Database.GetService<IRelationalDatabaseCreator>().CreateTables();

        private sealed class SchemaModelCacheKeyFactory : IModelCacheKeyFactory
        {
            public object Create(DbContext context, bool designTime) =>
                context is TestOrderDbContext testContext ? (context.GetType(), testContext.Schema, designTime) : context.GetType();
        }
    }

    private sealed class TestOrder
    {
        public int Id { get; set; }
        public required string CustomerName { get; set; }
    }
}
