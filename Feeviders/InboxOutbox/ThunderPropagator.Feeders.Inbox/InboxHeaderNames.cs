namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>Well-known header keys the Inbox itself reads/writes on <see cref="InboxMessage.Headers"/>.</summary>
    public static class InboxHeaderNames
    {
        /// <summary>
        /// The <see href="https://www.w3.org/TR/trace-context/">W3C Trace Context</see> <c>traceparent</c>
        /// value captured when this entry was first durably claimed - not the broker/transport header of
        /// the same name (which this Inbox never overwrites on the inbound side), but this library's own
        /// persisted record of it, restored later by a retry worker to link back to the original trace
        /// rather than pretending an asynchronous retry is a synchronous child span.
        /// </summary>
        public const string TraceParent = "traceparent";

        /// <summary>The corresponding W3C <c>tracestate</c> value, if one was present. Optional - many traces never set one.</summary>
        public const string TraceState = "tracestate";
    }
}
