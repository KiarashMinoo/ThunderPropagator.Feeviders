using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    /// <summary>
    /// Proves <see cref="EfCoreOutboxUnitOfWork"/> genuinely shares a relational transaction with
    /// business state - not just that its API looks right - using a real SQLite in-memory database (the
    /// EF Core InMemory provider does not support real transactions/rollback, so it cannot prove this).
    /// </summary>
    public sealed class EfCoreOutboxUnitOfWorkTests : IDisposable
    {
        private readonly SqliteConnection _connection = new("DataSource=:memory:");
        private readonly DbContextOptions<TestOutboxDbContext> _options;

        public EfCoreOutboxUnitOfWorkTests()
        {
            _connection.Open();
            _options = new DbContextOptionsBuilder<TestOutboxDbContext>().UseSqlite(_connection).Options;

            using var context = new TestOutboxDbContext(_options);
            context.Database.EnsureCreated();
        }

        public void Dispose() => _connection.Dispose();

        private static OutboxEnqueueRequest CreateRequest(string messageId) => new()
        {
            MessageId = messageId,
            ProviderKey = "provider-a",
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1, 2, 3],
        };

        [Fact]
        public async Task CommitAsync_ShouldPersistBusinessStateAndOutboxMessageInTheSameTransaction()
        {
            await using var context = new TestOutboxDbContext(_options);
            context.Orders.Add(new TestOrder { Id = 1, CustomerName = "Ada" });
            await using var unitOfWork = new EfCoreOutboxUnitOfWork(context);
            unitOfWork.Enqueue(CreateRequest("order-1-created"));

            var committed = await unitOfWork.CommitAsync();

            Assert.Single(committed);
            await using var verify = new TestOutboxDbContext(_options);
            Assert.Equal("Ada", (await verify.Orders.FindAsync(1))!.CustomerName);
            Assert.Equal("order-1-created", (await verify.Set<OutboxMessage>().SingleAsync()).MessageId);
        }

        [Fact]
        public async Task DisposeWithoutCommit_ShouldPersistNeitherBusinessStateNorOutboxMessage()
        {
            await using (var context = new TestOutboxDbContext(_options))
            {
                context.Orders.Add(new TestOrder { Id = 2, CustomerName = "Grace" });
                await using var unitOfWork = new EfCoreOutboxUnitOfWork(context);
                unitOfWork.Enqueue(CreateRequest("order-2-created"));

                // Simulates a crash before commit - the caller never calls CommitAsync.
            }

            await using var verify = new TestOutboxDbContext(_options);
            Assert.Null(await verify.Orders.FindAsync(2));
            Assert.Empty(await verify.Set<OutboxMessage>().ToListAsync());
        }

        [Fact]
        public async Task CommitAsync_WhenSaveChangesFails_ShouldRollBackBothBusinessStateAndOutboxMessage()
        {
            await using (var seed = new TestOutboxDbContext(_options))
            {
                seed.Orders.Add(new TestOrder { Id = 3, CustomerName = "Seed" });
                await seed.SaveChangesAsync();
            }

            await using var context = new TestOutboxDbContext(_options);
            // A primary-key conflict with the seeded row - SaveChangesAsync will throw, rolling back
            // this call's entire transaction, the outbox insert included. This simulates a crash
            // *during* commit, after both writes were staged but before either persisted.
            context.Orders.Add(new TestOrder { Id = 3, CustomerName = "Conflict" });
            await using var unitOfWork = new EfCoreOutboxUnitOfWork(context);
            unitOfWork.Enqueue(CreateRequest("order-3-created"));

            await Assert.ThrowsAsync<DbUpdateException>(() => unitOfWork.CommitAsync());

            await using var verify = new TestOutboxDbContext(_options);
            Assert.Equal("Seed", (await verify.Orders.FindAsync(3))!.CustomerName);
            Assert.Empty(await verify.Set<OutboxMessage>().ToListAsync());
        }

        [Fact]
        public async Task Enqueue_MultipleMessages_ShouldCommitAllOfThemTogether()
        {
            await using var context = new TestOutboxDbContext(_options);
            await using var unitOfWork = new EfCoreOutboxUnitOfWork(context);
            unitOfWork.Enqueue(CreateRequest("m1"));
            unitOfWork.Enqueue(CreateRequest("m2"));
            unitOfWork.Enqueue(CreateRequest("m3"));

            var committed = await unitOfWork.CommitAsync();

            Assert.Equal(3, committed.Count);
            Assert.Equal([0, 1, 2], committed.Select(m => m.OrderingSequence));
            await using var verify = new TestOutboxDbContext(_options);
            Assert.Equal(3, await verify.Set<OutboxMessage>().CountAsync());
        }

        [Fact]
        public async Task DisposeAsync_WithoutCommit_ShouldDetachStagedEntitiesSoALaterUnrelatedSaveDoesNotWriteThem()
        {
            await using var context = new TestOutboxDbContext(_options);
            var unitOfWork = new EfCoreOutboxUnitOfWork(context);
            unitOfWork.Enqueue(CreateRequest("abandoned"));

            await unitOfWork.DisposeAsync();

            // An unrelated later save on the SAME context must not silently include the abandoned stage.
            context.Orders.Add(new TestOrder { Id = 4, CustomerName = "Later" });
            await context.SaveChangesAsync();

            await using var verify = new TestOutboxDbContext(_options);
            Assert.Empty(await verify.Set<OutboxMessage>().ToListAsync());
            Assert.NotNull(await verify.Orders.FindAsync(4));
        }
    }

    internal sealed class TestOutboxDbContext(DbContextOptions<TestOutboxDbContext> options) : DbContext(options)
    {
        public DbSet<TestOrder> Orders => Set<TestOrder>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ApplyConfiguration(new OutboxMessageEntityTypeConfiguration());
            modelBuilder.Entity<TestOrder>(builder =>
            {
                builder.HasKey(order => order.Id);
                builder.Property(order => order.CustomerName).IsRequired();
            });
        }
    }

    internal sealed class TestOrder
    {
        public int Id { get; set; }
        public required string CustomerName { get; set; }
    }
}
