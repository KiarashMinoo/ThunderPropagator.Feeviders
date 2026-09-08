using System.Text.Json;
using StackExchange.Redis;

namespace ThunderPropagator.Feeders.Inbox.Redis
{
    /// <summary>
    /// Converts between <see cref="InboxMessage"/> and the flat Redis hash <see cref="RedisInboxStore"/>
    /// stores it as. Field names below are the actual Redis hash field names - keep any change here in
    /// lockstep with every Lua script in <see cref="RedisInboxStore"/> that references one by name.
    /// </summary>
    /// <remarks>
    /// Field/value pairs travel into Lua as a single JSON-encoded array (decoded there via <c>cjson.decode</c>) -
    /// <see cref="StackExchange.Redis.LuaScript"/>'s named-parameter binding does not support array-typed
    /// parameters, only scalars. <see cref="FieldPayload"/> is the one exception: it is raw, arbitrary
    /// bytes (never valid to embed as a JSON string), so it always travels as its own separate,
    /// natively binary-safe <see cref="RedisValue"/> parameter instead of going through the JSON blob.
    /// </remarks>
    internal static class RedisInboxMessageMapper
    {
        public const string FieldId = "Id";
        public const string FieldMessageId = "MessageId";
        public const string FieldChannelKey = "ChannelKey";
        public const string FieldFeederId = "FeederId";
        public const string FieldPartitionKey = "PartitionKey";
        public const string FieldSchemaVersion = "SchemaVersion";
        public const string FieldPayloadContentType = "PayloadContentType";
        public const string FieldPayload = "Payload";
        public const string FieldHeaders = "Headers";
        public const string FieldStatus = "Status";
        public const string FieldAttemptCount = "AttemptCount";
        public const string FieldReceivedAtUtc = "ReceivedAtUtc";
        public const string FieldProcessingStartedAtUtc = "ProcessingStartedAtUtc";
        public const string FieldProcessedAtUtc = "ProcessedAtUtc";
        public const string FieldDeadLetteredAtUtc = "DeadLetteredAtUtc";
        public const string FieldNextRetryAtUtc = "NextRetryAtUtc";
        public const string FieldLeaseOwner = "LeaseOwner";
        public const string FieldLeaseExpiresAtUtc = "LeaseExpiresAtUtc";
        public const string FieldFailureReason = "FailureReason";
        public const string FieldVersion = "Version";
        public const string FieldDedupKey = "DedupKey";

        /// <summary>
        /// Every field <paramref name="message"/> currently has except <see cref="FieldPayload"/> (see
        /// the class remarks), JSON-encoded as a flat name/value array - suitable for a full <c>HSET</c>
        /// of a brand-new entry. <paramref name="dedupKey"/> is stored alongside so
        /// <see cref="RedisInboxStore.PurgeAsync"/> can delete it without reconstructing it.
        /// </summary>
        public static string ToFullFieldListJson(InboxMessage message, long version, string dedupKey)
        {
            var fields = new List<string>(40)
            {
                FieldId, message.Id.ToString("N"),
                FieldMessageId, message.MessageId,
                FieldChannelKey, message.ChannelKey.ToString("N"),
                FieldFeederId, message.FeederId.ToString("N"),
                FieldSchemaVersion, message.SchemaVersion.ToString(),
                FieldPayloadContentType, message.PayloadContentType,
                FieldHeaders, JsonSerializer.Serialize(message.Headers),
                FieldStatus, message.Status.ToString(),
                FieldAttemptCount, message.AttemptCount.ToString(),
                FieldReceivedAtUtc, message.ReceivedAtUtc.ToUnixTimeMilliseconds().ToString(),
                FieldVersion, version.ToString(),
                FieldDedupKey, dedupKey,
            };

            AddOptional(fields, FieldPartitionKey, message.PartitionKey);
            AddOptional(fields, FieldProcessingStartedAtUtc, message.ProcessingStartedAtUtc);
            AddOptional(fields, FieldProcessedAtUtc, message.ProcessedAtUtc);
            AddOptional(fields, FieldDeadLetteredAtUtc, message.DeadLetteredAtUtc);
            AddOptional(fields, FieldNextRetryAtUtc, message.NextRetryAtUtc);
            AddOptional(fields, FieldLeaseOwner, message.LeaseOwner);
            AddOptional(fields, FieldLeaseExpiresAtUtc, message.LeaseExpiresAtUtc);
            AddOptional(fields, FieldFailureReason, message.FailureReason);

            return JsonSerializer.Serialize(fields);
        }

        /// <summary>
        /// Only the fields a transition changes (JSON-encoded, see the class remarks) - the mutable
        /// subset of <see cref="ToFullFieldListJson"/>, used by every write after the initial claim.
        /// Fields that become null on this transition are deleted (<c>HDEL</c>), never left stale, via
        /// the returned delete-list JSON.
        /// </summary>
        public static (string SetFieldsJson, string DeleteFieldsJson) ToChangedFieldListJson(InboxMessage message, long version)
        {
            var fields = new List<string>(14)
            {
                FieldStatus, message.Status.ToString(),
                FieldAttemptCount, message.AttemptCount.ToString(),
                FieldVersion, version.ToString(),
                // Never changes except via InboxMessage.Replay (every other transition leaves it as-is) -
                // always included here regardless, since it is cheap and correct either way.
                FieldReceivedAtUtc, message.ReceivedAtUtc.ToUnixTimeMilliseconds().ToString(),
            };
            var toDelete = new List<string>(6);

            AddOptionalOrDelete(fields, toDelete, FieldProcessingStartedAtUtc, message.ProcessingStartedAtUtc);
            AddOptionalOrDelete(fields, toDelete, FieldProcessedAtUtc, message.ProcessedAtUtc);
            AddOptionalOrDelete(fields, toDelete, FieldDeadLetteredAtUtc, message.DeadLetteredAtUtc);
            AddOptionalOrDelete(fields, toDelete, FieldNextRetryAtUtc, message.NextRetryAtUtc);
            AddOptionalOrDelete(fields, toDelete, FieldLeaseOwner, message.LeaseOwner);
            AddOptionalOrDelete(fields, toDelete, FieldLeaseExpiresAtUtc, message.LeaseExpiresAtUtc);
            AddOptionalOrDelete(fields, toDelete, FieldFailureReason, message.FailureReason);

            return (JsonSerializer.Serialize(fields), JsonSerializer.Serialize(toDelete));
        }

        public static InboxMessage FromHashEntries(HashEntry[] entries)
        {
            var map = entries.ToDictionary(e => e.Name.ToString(), e => e.Value);

            return new InboxMessage
            {
                Id = Guid.ParseExact((string)map[FieldId]!, "N"),
                MessageId = (string)map[FieldMessageId]!,
                ChannelKey = Guid.ParseExact((string)map[FieldChannelKey]!, "N"),
                FeederId = Guid.ParseExact((string)map[FieldFeederId]!, "N"),
                PartitionKey = map.TryGetValue(FieldPartitionKey, out var partitionKey) ? (string?)partitionKey : null,
                SchemaVersion = (int)map[FieldSchemaVersion],
                PayloadContentType = (string)map[FieldPayloadContentType]!,
                Payload = (byte[])map[FieldPayload]!,
                Headers = JsonSerializer.Deserialize<Dictionary<string, string>>((string)map[FieldHeaders]!)!,
                Status = Enum.Parse<InboxMessageStatus>((string)map[FieldStatus]!),
                AttemptCount = (int)map[FieldAttemptCount],
                ReceivedAtUtc = DateTimeOffset.FromUnixTimeMilliseconds((long)map[FieldReceivedAtUtc]),
                ProcessingStartedAtUtc = ReadOptionalDateTimeOffset(map, FieldProcessingStartedAtUtc),
                ProcessedAtUtc = ReadOptionalDateTimeOffset(map, FieldProcessedAtUtc),
                DeadLetteredAtUtc = ReadOptionalDateTimeOffset(map, FieldDeadLetteredAtUtc),
                NextRetryAtUtc = ReadOptionalDateTimeOffset(map, FieldNextRetryAtUtc),
                LeaseOwner = map.TryGetValue(FieldLeaseOwner, out var leaseOwner) ? (string?)leaseOwner : null,
                LeaseExpiresAtUtc = ReadOptionalDateTimeOffset(map, FieldLeaseExpiresAtUtc),
                FailureReason = map.TryGetValue(FieldFailureReason, out var failureReason) ? (string?)failureReason : null,
            };
        }

        public static long ReadVersion(HashEntry[] entries) =>
            (long)entries.First(e => e.Name == FieldVersion).Value;

        /// <summary>
        /// The retry-tracking score for <paramref name="message"/>'s current state: a Processing
        /// entry's lease expiry, a Failed entry's next-retry time (or "already eligible" when unset),
        /// or <see langword="null"/> for any terminal/Received state, which is never tracked here.
        /// </summary>
        public static long? RetryableScore(InboxMessage message) => message.Status switch
        {
            InboxMessageStatus.Processing => message.LeaseExpiresAtUtc?.ToUnixTimeMilliseconds() ?? 0,
            InboxMessageStatus.Failed => message.NextRetryAtUtc?.ToUnixTimeMilliseconds() ?? 0,
            _ => null,
        };

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
