namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>
    /// Backs <see cref="EfCoreOutboxStore.InitializeAsync"/>'s persisted schema-version tracking - a
    /// single row (<see cref="Id"/> is always <c>"schema"</c>) holding the version this database's
    /// Outbox schema is currently at. An implementation detail, never surfaced through
    /// <see cref="IOutboxStore"/> - but since there is no owning <c>DbContext</c> here, a caller must
    /// apply <see cref="OutboxSchemaVersionEntityTypeConfiguration"/> to their own model alongside the
    /// main one for <see cref="EfCoreOutboxStore.InitializeAsync"/> to work at all.
    /// </summary>
    public sealed class OutboxSchemaVersion
    {
        public required string Id { get; init; }

        public int Version { get; set; }
    }
}
