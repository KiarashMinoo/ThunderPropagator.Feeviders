namespace ThunderPropagator.Providers.DotNet.Outbox
{
    /// <summary>Well-known header keys a relay worker attaches to every broker publish.</summary>
    public static class OutboxHeaderNames
    {
        /// <summary>
        /// Carries <see cref="OutboxMessage.MessageId"/>, stamped by a relay worker on every publish
        /// attempt and always overriding whatever the enqueuing call site may already have set under
        /// this key - the value must stay authoritative and stable across every attempt of the same
        /// entry, including a retried attempt after an ambiguous broker outcome (a publish call that
        /// throws without the caller knowing whether the broker actually received it first). A downstream
        /// consumer must key its own deduplication off this header, never off broker-assigned delivery
        /// metadata, since at-least-once delivery means the same <see cref="OutboxMessage.MessageId"/> can
        /// legitimately arrive more than once.
        /// </summary>
        public const string MessageId = "x-outbox-message-id";

        /// <summary>
        /// The <see href="https://www.w3.org/TR/trace-context/">W3C Trace Context</see> <c>traceparent</c>
        /// value captured when this entry was enqueued - this library's own persisted record of it,
        /// restored later by the relay worker to link back to the enqueuing trace rather than pretending
        /// an asynchronous relay is a synchronous child span. A relay attempt stamps a fresh traceparent
        /// of its own on the outgoing broker message (see <c>OutboxRelayWorker</c>) - that is a separate
        /// concern from this persisted-at-enqueue-time value.
        /// </summary>
        public const string TraceParent = "traceparent";

        /// <summary>The corresponding W3C <c>tracestate</c> value, if one was present. Optional - many traces never set one.</summary>
        public const string TraceState = "tracestate";
    }
}
