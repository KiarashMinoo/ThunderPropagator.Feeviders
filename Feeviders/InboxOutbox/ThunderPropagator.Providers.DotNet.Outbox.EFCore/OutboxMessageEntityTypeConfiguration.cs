using System.Linq;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Maps <see cref="OutboxMessage"/> directly as an EF Core entity - there is no separate mutable
    /// "record" type. Both <see cref="EfCoreOutboxUnitOfWork"/> (inserts, enlisted in the caller's own
    /// transaction) and <see cref="EfCoreOutboxStore"/> (the full claim/publish/retry lifecycle) read and
    /// write rows through this one mapping. A caller's own <c>DbContext</c> opts into carrying Outbox
    /// rows by applying this configuration - <c>modelBuilder.ApplyConfiguration(new OutboxMessageEntityTypeConfiguration())</c> -
    /// typically inside the same <c>OnModelCreating</c> that configures the caller's business entities,
    /// so both share one model and therefore one <c>SaveChangesAsync</c> transaction.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Table/schema.</b> Defaults to <c>__TP_Outbox</c> in the provider's default schema; both are
    /// constructor parameters so a caller can rename either to fit their own naming convention or
    /// multi-tenant schema-per-tenant layout.
    /// </para>
    /// <para>
    /// <b>Concurrency.</b> <c>Version</c> is a shadow property (not part of <see cref="OutboxMessage"/>'s
    /// own public API - <see cref="EfCoreOutboxStore"/> reads/writes it via <c>EntityEntry.Property</c>)
    /// configured as an application-managed (not database-generated) concurrency token: portable across
    /// every relational provider, unlike a native <c>rowversion</c>/<c>xmin</c> column, which would need
    /// per-provider handling to behave identically.
    /// </para>
    /// <para>
    /// <b>Migrations.</b> This type declares no migrations of its own - there is no owning
    /// <c>DbContext</c> here, only a configuration a caller applies to theirs. Once applied, this table
    /// participates in that caller's own <c>dotnet ef migrations add</c>/<c>database update</c> lifecycle
    /// exactly like any other entity in their model; rollback is that same standard EF Core migration
    /// rollback (<c>dotnet ef database update &lt;previous-migration&gt;</c>), nothing bespoke.
    /// </para>
    /// </remarks>
    public sealed class OutboxMessageEntityTypeConfiguration(string? schema = null, string tableName = "__TP_Outbox") : IEntityTypeConfiguration<OutboxMessage>
    {
        /// <summary>Shadow property name backing the concurrency token - see the class remarks.</summary>
        public const string VersionPropertyName = "Version";

        public void Configure(EntityTypeBuilder<OutboxMessage> builder)
        {
            builder.ToTable(tableName, schema);
            builder.HasKey(m => m.Id);

            builder.Property(m => m.MessageId).IsRequired().HasMaxLength(OutboxMessageLimits.MaxMessageIdLength);
            builder.Property(m => m.ProviderKey).IsRequired().HasMaxLength(OutboxMessageLimits.MaxProviderKeyLength);
            builder.Property(m => m.PartitionKey).HasMaxLength(OutboxMessageLimits.MaxPartitionKeyLength);
            builder.Property(m => m.PayloadContentType).IsRequired();
            builder.Property(m => m.Payload).IsRequired();
            builder.Property(m => m.LeaseOwner).HasMaxLength(OutboxMessageLimits.MaxLeaseOwnerLength);
            builder.Property(m => m.FailureReason).HasMaxLength(OutboxMessageLimits.MaxFailureReasonLength);
            builder.Property(m => m.Status).HasConversion<string>().HasMaxLength(32);

            builder.Property<long>(VersionPropertyName).IsConcurrencyToken().HasDefaultValue(1L);

            // No native relational mapping for a dictionary - round-trip as JSON. A ValueComparer is
            // still supplied so EF Core does not warn about a reference-typed converted property lacking
            // one, now that EfCoreOutboxStore also updates (not just inserts) rows through this mapping.
            builder.Property(m => m.Headers)
                .HasConversion(
                    headers => JsonSerializer.Serialize(headers, (JsonSerializerOptions?)null),
                    json => JsonSerializer.Deserialize<Dictionary<string, string>>(json, (JsonSerializerOptions?)null) ?? new Dictionary<string, string>())
                .Metadata.SetValueComparer(new ValueComparer<IReadOnlyDictionary<string, string>>(
                    (left, right) => ReferenceEquals(left, right) || (left != null && right != null && left.Count == right.Count && !left.Except(right).Any()),
                    headers => headers.Aggregate(0, (hash, pair) => HashCode.Combine(hash, pair.Key, pair.Value)),
                    headers => headers.ToDictionary(pair => pair.Key, pair => pair.Value)));

            // Supports EfCoreOutboxStore.ClaimBatchAsync's per-partition SKIP LOCKED scan, in
            // OutboxMessage.OrderingSequence order.
            builder.HasIndex(m => new { m.PartitionKey, m.Status, m.OrderingSequence });

            // Supports GetClaimablePartitionKeysAsync/aggregate GetDepthAsync/GetOldestPendingAgeAsync
            // scanning across every partition by status alone.
            builder.HasIndex(m => m.Status);
        }
    }
}
