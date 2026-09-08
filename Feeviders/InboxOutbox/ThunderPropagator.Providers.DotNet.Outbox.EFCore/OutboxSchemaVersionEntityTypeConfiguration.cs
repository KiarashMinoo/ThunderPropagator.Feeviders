using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Maps <see cref="OutboxSchemaVersion"/>. Apply alongside <see cref="OutboxMessageEntityTypeConfiguration"/> -
    /// <c>modelBuilder.ApplyConfiguration(new OutboxSchemaVersionEntityTypeConfiguration())</c> - see
    /// <see cref="OutboxSchemaVersion"/>'s remarks.
    /// </summary>
    public sealed class OutboxSchemaVersionEntityTypeConfiguration(string? schema = null, string tableName = "__TP_Outbox_SchemaVersion") : IEntityTypeConfiguration<OutboxSchemaVersion>
    {
        public void Configure(EntityTypeBuilder<OutboxSchemaVersion> builder)
        {
            builder.ToTable(tableName, schema);
            builder.HasKey(x => x.Id);
            builder.Property(x => x.Id).HasMaxLength(64);
        }
    }
}
