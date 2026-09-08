using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Maps <see cref="InboxSchemaVersion"/>. Apply alongside <see cref="InboxMessageEntityTypeConfiguration"/> -
    /// <c>modelBuilder.ApplyConfiguration(new InboxSchemaVersionEntityTypeConfiguration())</c> - see
    /// <see cref="InboxSchemaVersion"/>'s remarks.
    /// </summary>
    public sealed class InboxSchemaVersionEntityTypeConfiguration(string? schema = null, string tableName = "__TP_Inbox_SchemaVersion") : IEntityTypeConfiguration<InboxSchemaVersion>
    {
        public void Configure(EntityTypeBuilder<InboxSchemaVersion> builder)
        {
            builder.ToTable(tableName, schema);
            builder.HasKey(x => x.Id);
            builder.Property(x => x.Id).HasMaxLength(64);
        }
    }
}
