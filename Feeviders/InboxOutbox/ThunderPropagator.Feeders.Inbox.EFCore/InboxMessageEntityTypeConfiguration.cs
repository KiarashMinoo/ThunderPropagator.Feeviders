using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Maps <see cref="InboxMessage"/> directly as an EF Core entity - there is no separate mutable
    /// "record" type; <see cref="EfCoreInboxStore"/> reads/writes rows through this mapping alone. A
    /// caller's own <c>DbContext</c> opts into carrying Inbox rows by applying this configuration -
    /// <c>modelBuilder.ApplyConfiguration(new InboxMessageEntityTypeConfiguration())</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Table/schema.</b> Defaults to <c>__TP_Inbox</c> in the provider's default schema; both are
    /// constructor parameters so a caller can rename either to fit their own naming convention or
    /// multi-tenant schema-per-tenant layout.
    /// </para>
    /// <para>
    /// <b>Concurrency.</b> <c>Version</c> is a shadow property (not part of <see cref="InboxMessage"/>'s
    /// own public API - <see cref="EfCoreInboxStore"/> reads/writes it via <c>EntityEntry.Property</c>)
    /// configured as an application-managed (not database-generated) concurrency token: portable across
    /// every relational provider, unlike a native <c>rowversion</c>/<c>xmin</c> column, which would need
    /// per-provider handling to behave identically. <see cref="EfCoreInboxStore"/> increments it by hand
    /// on every write; EF Core's own concurrency check (an added <c>WHERE Version = &#64;original</c>
    /// clause) then guarantees two concurrent writers to the same row can never both succeed.
    /// </para>
    /// <para>
    /// <b>Migrations.</b> This type declares no migrations of its own - there is no owning
    /// <c>DbContext</c> here, only a configuration a caller applies to theirs. Once applied, this table
    /// participates in that caller's own <c>dotnet ef migrations add</c>/<c>database update</c> lifecycle
    /// exactly like any other entity in their model; rollback is that same standard EF Core migration
    /// rollback (<c>dotnet ef database update &lt;previous-migration&gt;</c>), nothing bespoke.
    /// </para>
    /// </remarks>
    public sealed class InboxMessageEntityTypeConfiguration(string? schema = null, string tableName = "__TP_Inbox") : IEntityTypeConfiguration<InboxMessage>
    {
        /// <summary>Shadow property name backing the concurrency token - see the class remarks.</summary>
        public const string VersionPropertyName = "Version";

        /// <summary>
        /// Shadow property name backing the dedup-scoped unique index (see <see cref="Configure"/>) -
        /// a non-null mirror of <see cref="InboxMessage.PartitionKey"/>, set by
        /// <see cref="EfCoreInboxStore"/> on every insert. A unique index directly on the nullable
        /// <c>PartitionKey</c> column would NOT actually enforce uniqueness for a null partition: SQL's
        /// three-valued logic treats every <c>NULL</c> as distinct from every other <c>NULL</c>, so most
        /// engines (PostgreSQL, MySQL) happily accept unlimited "duplicate" rows differing only by having
        /// <c>PartitionKey IS NULL</c> in common - exactly the message-loses-its-uniqueness bug this
        /// sentinel column exists to avoid.
        /// </summary>
        public const string DedupPartitionKeyPropertyName = "DedupPartitionKey";

        public void Configure(EntityTypeBuilder<InboxMessage> builder)
        {
            builder.ToTable(tableName, schema);
            builder.HasKey(m => m.Id);

            builder.Property(m => m.MessageId).IsRequired().HasMaxLength(InboxMessageLimits.MaxMessageIdLength);
            builder.Property(m => m.PartitionKey).HasMaxLength(InboxMessageLimits.MaxPartitionKeyLength);
            builder.Property(m => m.PayloadContentType).IsRequired();
            builder.Property(m => m.Payload).IsRequired();
            builder.Property(m => m.LeaseOwner).HasMaxLength(InboxMessageLimits.MaxLeaseOwnerLength);
            builder.Property(m => m.FailureReason).HasMaxLength(InboxMessageLimits.MaxFailureReasonLength);
            builder.Property(m => m.Status).HasConversion<string>().HasMaxLength(32);

            builder.Property<long>(VersionPropertyName).IsConcurrencyToken().HasDefaultValue(1L);
            builder.Property<string>(DedupPartitionKeyPropertyName).IsRequired().HasMaxLength(InboxMessageLimits.MaxPartitionKeyLength);

            // No native relational mapping for a dictionary - round-trip as JSON.
            builder.Property(m => m.Headers)
                .HasConversion(
                    headers => JsonSerializer.Serialize(headers, (JsonSerializerOptions?)null),
                    json => JsonSerializer.Deserialize<Dictionary<string, string>>(json, (JsonSerializerOptions?)null) ?? new Dictionary<string, string>())
                .Metadata.SetValueComparer(new ValueComparer<IReadOnlyDictionary<string, string>>(
                    (left, right) => ReferenceEquals(left, right) || (left != null && right != null && left.Count == right.Count && !left.Except(right).Any()),
                    headers => headers.Aggregate(0, (hash, pair) => HashCode.Combine(hash, pair.Key, pair.Value)),
                    headers => headers.ToDictionary(pair => pair.Key, pair => pair.Value)));

            // The dedup key IInboxStore.TryClaimAsync scopes uniqueness by - a database-level backstop
            // against ever persisting two rows for the same logical message, on top of the application's
            // own find-before-insert check. Uses the non-null DedupPartitionKeyPropertyName shadow
            // property, not PartitionKey directly - see its own remarks for why.
            builder.HasIndex(nameof(InboxMessage.ChannelKey), DedupPartitionKeyPropertyName, nameof(InboxMessage.MessageId)).IsUnique();

            // Supports IInboxStore.QueryRetryableAsync's per-channel scan for Processing (lease-expired)
            // and Failed (retry-eligible) candidates.
            builder.HasIndex(m => new { m.ChannelKey, m.Status });
        }
    }
}
