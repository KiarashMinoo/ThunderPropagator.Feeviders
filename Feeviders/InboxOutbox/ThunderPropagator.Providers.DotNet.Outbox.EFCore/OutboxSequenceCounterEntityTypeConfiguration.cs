using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Maps <see cref="OutboxSequenceCounter"/>. Apply alongside <see cref="OutboxMessageEntityTypeConfiguration"/> -
    /// <c>modelBuilder.ApplyConfiguration(new OutboxSequenceCounterEntityTypeConfiguration())</c> - see
    /// <see cref="OutboxSequenceCounter"/>'s remarks.
    /// </summary>
    public sealed class OutboxSequenceCounterEntityTypeConfiguration(string? schema = null, string tableName = "__TP_Outbox_Sequence") : IEntityTypeConfiguration<OutboxSequenceCounter>
    {
        /// <summary>Shadow property name backing the concurrency token, mirroring <see cref="OutboxMessageEntityTypeConfiguration.VersionPropertyName"/>.</summary>
        public const string VersionPropertyName = "Version";

        public void Configure(EntityTypeBuilder<OutboxSequenceCounter> builder)
        {
            builder.ToTable(tableName, schema);
            builder.HasKey(c => c.PartitionKey);

            builder.Property(c => c.PartitionKey).HasMaxLength(OutboxMessageLimits.MaxPartitionKeyLength);
            builder.Property<long>(VersionPropertyName).IsConcurrencyToken().HasDefaultValue(1L);
        }
    }
}
