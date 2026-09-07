using System.Text.Json;
using StackExchange.Redis;

namespace ThunderPropagator.Providers.DotNet.Outbox.Redis
{
    /// <summary>
    /// Converts between <see cref="OutboxMessage"/> and the flat Redis hash <see cref="RedisOutboxStore"/>
    /// stores it as. Field names below are the actual Redis hash field names - keep any change here in
    /// lockstep with every Lua script in <see cref="RedisOutboxStore"/> that references one by name.
    /// </summary>
    /// <remarks>
    /// Field/value pairs travel into Lua as a single JSON-encoded array (decoded there via <c>cjson.decode</c>) -
    /// <see cref="StackExchange.Redis.LuaScript"/>'s named-parameter binding does not support array-typed
    /// parameters, only scalars. <see cref="FieldPayload"/> is the one exception: it is raw, arbitrary
    /// bytes (never valid to embed as a JSON string), so it always travels as its own separate,
    /// natively binary-safe <see cref="RedisValue"/> parameter instead of going through the JSON blob.
    /// </remarks>
    internal static class RedisOutboxMessageMapper
    {
        public const string FieldId = "Id";
        public const string FieldMessageId = "MessageId";
        public const string FieldProviderKey = "ProviderKey";
        public const string FieldPartitionKey = "PartitionKey";
        public const string FieldOrderingSequence = "OrderingSequence";
        public const string FieldSchemaVersion = "SchemaVersion";
        public const string FieldPayloadContentType = "PayloadContentType";
        public const string FieldPayload = "Payload";
        public const string FieldHeaders = "Headers";
        public const string FieldStatus = "Status";
        public const string FieldAttempts = "Attempts";
        public const string FieldCreatedAtUtc = "CreatedAtUtc";
        public const string FieldPublishingStartedAtUtc = "PublishingStartedAtUtc";
        public const string FieldPublishedAtUtc = "PublishedAtUtc";
        public const string FieldDeadLetteredAtUtc = "DeadLetteredAtUtc";
        public const string FieldNextRetryAtUtc = "NextRetryAtUtc";
        public const string FieldLeaseOwner = "LeaseOwner";
        public const string FieldLeaseExpiresAtUtc = "LeaseExpiresAtUtc";
        public const string FieldFailureReason = "FailureReason";
        public const string FieldVersion = "Version";

        /// <summary>
        /// Every field <paramref name="message"/> currently has except <see cref="FieldPayload"/> (see
        /// the class remarks), JSON-encoded as a flat name/value array - suitable for a full <c>HSET</c>
        /// of a brand-new entry.
        /// </summary>
        public static string ToFullFieldListJson(OutboxMessage message, long version)
        {
            var fields = new List<string>(22)
            {
                FieldId, message.Id.ToString("N"),
                FieldMessageId, message.MessageId,
                FieldProviderKey, message.ProviderKey,
                FieldOrderingSequence, message.OrderingSequence.ToString(),
                FieldSchemaVersion, message.SchemaVersion.ToString(),
                FieldPayloadContentType, message.PayloadContentType,
                FieldHeaders, JsonSerializer.Serialize(message.Headers),
                FieldStatus, message.Status.ToString(),
                FieldAttempts, message.Attempts.ToString(),
                FieldCreatedAtUtc, message.CreatedAtUtc.ToUnixTimeMilliseconds().ToString(),
                FieldVersion, version.ToString(),
            };

            AddOptional(fields, FieldPartitionKey, message.PartitionKey);
            AddOptional(fields, FieldPublishingStartedAtUtc, message.PublishingStartedAtUtc);
            AddOptional(fields, FieldPublishedAtUtc, message.PublishedAtUtc);
            AddOptional(fields, FieldDeadLetteredAtUtc, message.DeadLetteredAtUtc);
            AddOptional(fields, FieldNextRetryAtUtc, message.NextRetryAtUtc);
            AddOptional(fields, FieldLeaseOwner, message.LeaseOwner);
            AddOptional(fields, FieldLeaseExpiresAtUtc, message.LeaseExpiresAtUtc);
            AddOptional(fields, FieldFailureReason, message.FailureReason);

            return JsonSerializer.Serialize(fields);
        }

        /// <summary>
        /// Only the fields a transition changes (JSON-encoded, see the class remarks), plus fields that
        /// must be deleted (<c>HDEL</c>) because they became null.
        /// </summary>
        public static (string SetFieldsJson, string DeleteFieldsJson) ToChangedFieldListJson(OutboxMessage message, long version)
        {
            var fields = new List<string>(14)
            {
                FieldStatus, message.Status.ToString(),
                FieldAttempts, message.Attempts.ToString(),
                FieldVersion, version.ToString(),
            };
            var toDelete = new List<string>(6);

            AddOptionalOrDelete(fields, toDelete, FieldPublishingStartedAtUtc, message.PublishingStartedAtUtc);
            AddOptionalOrDelete(fields, toDelete, FieldPublishedAtUtc, message.PublishedAtUtc);
            AddOptionalOrDelete(fields, toDelete, FieldDeadLetteredAtUtc, message.DeadLetteredAtUtc);
            AddOptionalOrDelete(fields, toDelete, FieldNextRetryAtUtc, message.NextRetryAtUtc);
            AddOptionalOrDelete(fields, toDelete, FieldLeaseOwner, message.LeaseOwner);
            AddOptionalOrDelete(fields, toDelete, FieldLeaseExpiresAtUtc, message.LeaseExpiresAtUtc);
            AddOptionalOrDelete(fields, toDelete, FieldFailureReason, message.FailureReason);

            return (JsonSerializer.Serialize(fields), JsonSerializer.Serialize(toDelete));
        }

        public static OutboxMessage FromHashEntries(HashEntry[] entries)
        {
            var map = entries.ToDictionary(e => e.Name.ToString(), e => e.Value);

            return new OutboxMessage
            {
                Id = Guid.ParseExact((string)map[FieldId]!, "N"),
                MessageId = (string)map[FieldMessageId]!,
                ProviderKey = (string)map[FieldProviderKey]!,
                PartitionKey = map.TryGetValue(FieldPartitionKey, out var partitionKey) ? (string?)partitionKey : null,
                OrderingSequence = (long)map[FieldOrderingSequence],
                SchemaVersion = (int)map[FieldSchemaVersion],
                PayloadContentType = (string)map[FieldPayloadContentType]!,
                Payload = (byte[])map[FieldPayload]!,
                Headers = JsonSerializer.Deserialize<Dictionary<string, string>>((string)map[FieldHeaders]!)!,
                Status = Enum.Parse<OutboxMessageStatus>((string)map[FieldStatus]!),
                Attempts = (int)map[FieldAttempts],
                CreatedAtUtc = DateTimeOffset.FromUnixTimeMilliseconds((long)map[FieldCreatedAtUtc]),
                PublishingStartedAtUtc = ReadOptionalDateTimeOffset(map, FieldPublishingStartedAtUtc),
                PublishedAtUtc = ReadOptionalDateTimeOffset(map, FieldPublishedAtUtc),
                DeadLetteredAtUtc = ReadOptionalDateTimeOffset(map, FieldDeadLetteredAtUtc),
                NextRetryAtUtc = ReadOptionalDateTimeOffset(map, FieldNextRetryAtUtc),
                LeaseOwner = map.TryGetValue(FieldLeaseOwner, out var leaseOwner) ? (string?)leaseOwner : null,
                LeaseExpiresAtUtc = ReadOptionalDateTimeOffset(map, FieldLeaseExpiresAtUtc),
                FailureReason = map.TryGetValue(FieldFailureReason, out var failureReason) ? (string?)failureReason : null,
            };
        }

        public static long ReadVersion(HashEntry[] entries) =>
            (long)entries.First(e => e.Name == FieldVersion).Value;

        private static DateTimeOffset? ReadOptionalDateTimeOffset(Dictionary<string, RedisValue> map, string field) =>
            map.TryGetValue(field, out var value) ? DateTimeOffset.FromUnixTimeMilliseconds((long)value) : null;

        private static void AddOptional(List<string> fields, string name, string? value)
        {
            if (value is not null)
            {
                fields.Add(name);
                fields.Add(value);
            }
        }

        private static void AddOptional(List<string> fields, string name, DateTimeOffset? value)
        {
            if (value is not null)
            {
                fields.Add(name);
                fields.Add(value.Value.ToUnixTimeMilliseconds().ToString());
            }
        }

        private static void AddOptionalOrDelete(List<string> fields, List<string> toDelete, string name, string? value)
        {
            if (value is not null)
            {
                fields.Add(name);
                fields.Add(value);
            }
            else
            {
                toDelete.Add(name);
            }
        }

        private static void AddOptionalOrDelete(List<string> fields, List<string> toDelete, string name, DateTimeOffset? value)
        {
            if (value is not null)
            {
                fields.Add(name);
                fields.Add(value.Value.ToUnixTimeMilliseconds().ToString());
            }
            else
            {
                toDelete.Add(name);
            }
        }
    }
}
