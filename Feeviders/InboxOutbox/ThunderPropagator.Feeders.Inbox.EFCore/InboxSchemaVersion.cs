namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Backs <see cref="EfCoreInboxStore.InitializeAsync"/>'s persisted schema-version tracking - a
    /// single row (<see cref="Id"/> is always <c>"schema"</c>) holding the version this database's Inbox
    /// schema is currently at. An implementation detail, never surfaced through <see cref="IInboxStore"/> -
    /// but since there is no owning <c>DbContext</c> here (see <see cref="InboxMessageEntityTypeConfiguration"/>'s
    /// remarks), a caller must apply <see cref="InboxSchemaVersionEntityTypeConfiguration"/> to their own
    /// model alongside the main one for <see cref="EfCoreInboxStore.InitializeAsync"/> to work at all.
    /// </summary>
    public sealed class InboxSchemaVersion
    {
        public required string Id { get; init; }

        public int Version { get; set; }
    }
}
