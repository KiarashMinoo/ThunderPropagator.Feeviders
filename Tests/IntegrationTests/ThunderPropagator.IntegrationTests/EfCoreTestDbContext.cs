using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Applies every EF Core entity configuration issue #125's packages ship, all under one caller-chosen
/// schema - what a real consumer's own <c>DbContext</c> would do.
/// </summary>
/// <remarks>
/// EF Core caches the built model per <see cref="DbContext"/> CLR type by default, assuming
/// <see cref="OnModelCreating"/> always produces the same model for a given type - not true here, since
/// <see cref="Schema"/> varies per test. <see cref="SchemaModelCacheKeyFactory"/> below (registered via
/// <see cref="BuildOptions"/>'s <c>ReplaceService</c>) fixes this by including the schema in the cache
/// key, so each distinct schema gets its own model instead of every test after the first silently
/// reusing the first test's table.
/// </remarks>
internal sealed class EfCoreTestDbContext(DbContextOptions<EfCoreTestDbContext> options, string schema) : DbContext(options)
{
    public string Schema { get; } = schema;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfiguration(new InboxMessageEntityTypeConfiguration(Schema));
        modelBuilder.ApplyConfiguration(new InboxSchemaVersionEntityTypeConfiguration(Schema));
        modelBuilder.ApplyConfiguration(new OutboxMessageEntityTypeConfiguration(Schema));
        modelBuilder.ApplyConfiguration(new OutboxSequenceCounterEntityTypeConfiguration(Schema));
        modelBuilder.ApplyConfiguration(new OutboxSchemaVersionEntityTypeConfiguration(Schema));
    }

    public static DbContextOptions<EfCoreTestDbContext> BuildOptions(string connectionString) =>
        new DbContextOptionsBuilder<EfCoreTestDbContext>()
            .UseNpgsql(connectionString)
            .ReplaceService<IModelCacheKeyFactory, SchemaModelCacheKeyFactory>()
            .Options;

    /// <summary>
    /// Creates this schema's tables. Deliberately not <c>Database.EnsureCreated()</c>: every test shares
    /// one Postgres database (just a different schema each), and <c>EnsureCreated()</c> only creates
    /// anything the first time it is ever called against a given DATABASE - once that database exists
    /// (which it does, from the first test onward), later calls silently do nothing, leaving every
    /// subsequent test's own schema without its tables. <see cref="IRelationalDatabaseCreator.CreateTables"/>
    /// instead unconditionally issues this model's DDL, schema-qualified CREATE SCHEMA included.
    /// </summary>
    public void CreateTables() => Database.GetService<IRelationalDatabaseCreator>().CreateTables();

    private sealed class SchemaModelCacheKeyFactory : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime) =>
            context is EfCoreTestDbContext testContext ? (context.GetType(), testContext.Schema, designTime) : context.GetType();
    }
}
