using System.Linq;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Maps <see cref="OutboxMessage"/> directly as an EF Core entity - there is no separate mutable
    /// "record" type, since <see cref="EfCoreOutboxUnitOfWork"/> only ever inserts new rows through this
    /// mapping (claiming/publishing/updating a row remains an <see cref="IOutboxStore"/> backend's own
    /// concern, out of scope here). A caller's own <c>DbContext</c> opts into carrying Outbox rows by
    /// applying this configuration - <c>modelBuilder.ApplyConfiguration(new OutboxMessageEntityTypeConfiguration())</c> -
    /// typically inside the same <c>OnModelCreating</c> that configures the caller's business entities,
    /// so both share one model and therefore one <c>SaveChangesAsync</c> transaction.
    /// </summary>
    public sealed class OutboxMessageEntityTypeConfiguration : IEntityTypeConfiguration<OutboxMessage>
    {
        public void Configure(EntityTypeBuilder<OutboxMessage> builder)
        {
            builder.ToTable("OutboxMessages");
            builder.HasKey(m => m.Id);

            builder.Property(m => m.MessageId).IsRequired().HasMaxLength(OutboxMessageLimits.MaxMessageIdLength);
            builder.Property(m => m.ProviderKey).IsRequired().HasMaxLength(OutboxMessageLimits.MaxProviderKeyLength);
            builder.Property(m => m.PartitionKey).HasMaxLength(OutboxMessageLimits.MaxPartitionKeyLength);
            builder.Property(m => m.PayloadContentType).IsRequired();
            builder.Property(m => m.Payload).IsRequired();
            builder.Property(m => m.LeaseOwner).HasMaxLength(OutboxMessageLimits.MaxLeaseOwnerLength);
            builder.Property(m => m.FailureReason).HasMaxLength(OutboxMessageLimits.MaxFailureReasonLength);

            // No native relational mapping for a dictionary - round-trip as JSON. Insert-only usage
            // (see the class remarks) means change-tracking never needs to detect an in-place mutation
            // of this property, but a ValueComparer is still supplied so EF Core does not warn about a
            // reference-typed converted property lacking one.
            builder.Property(m => m.Headers)
                .HasConversion(
                    headers => JsonSerializer.Serialize(headers, (JsonSerializerOptions?)null),
                    json => JsonSerializer.Deserialize<Dictionary<string, string>>(json, (JsonSerializerOptions?)null) ?? new Dictionary<string, string>())
                .Metadata.SetValueComparer(new ValueComparer<IReadOnlyDictionary<string, string>>(
                    (left, right) => ReferenceEquals(left, right) || (left != null && right != null && left.Count == right.Count && !left.Except(right).Any()),
                    headers => headers.Aggregate(0, (hash, pair) => HashCode.Combine(hash, pair.Key, pair.Value)),
                    headers => headers.ToDictionary(pair => pair.Key, pair => pair.Value)));

            builder.HasIndex(m => new { m.PartitionKey, m.Status, m.OrderingSequence });
        }
    }
}
