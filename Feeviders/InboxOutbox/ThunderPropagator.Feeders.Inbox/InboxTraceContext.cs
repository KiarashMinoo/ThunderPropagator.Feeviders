using System.Diagnostics;

namespace ThunderPropagator.Feeders.Inbox
{
    /// <summary>
    /// Persists/restores W3C trace context through <see cref="InboxMessage.Headers"/>, so an
    /// asynchronous retry attempt (possibly after a process restart, possibly minutes or hours later) can
    /// still be correlated back to the trace that first received the message - as an
    /// <see cref="ActivityLink"/>, never as a synchronous parent span, since a retry is not a child of an
    /// operation that already finished.
    /// </summary>
    internal static class InboxTraceContext
    {
        /// <summary>
        /// Returns <paramref name="headers"/> with <see cref="Activity.Current"/>'s trace context added
        /// under <see cref="InboxHeaderNames.TraceParent"/>/<see cref="InboxHeaderNames.TraceState"/> -
        /// unchanged if there is no current activity (e.g. no listener is configured, or sampling
        /// discarded it), so this never invents a trace context that does not exist.
        /// </summary>
        internal static IReadOnlyDictionary<string, string>? WithCurrentTraceContext(IReadOnlyDictionary<string, string>? headers)
        {
            var current = Activity.Current;
            if (current is null)
                return headers;

            var merged = headers is null ? new Dictionary<string, string>() : new Dictionary<string, string>(headers);
            merged[InboxHeaderNames.TraceParent] = current.Id!;

            if (!string.IsNullOrEmpty(current.TraceStateString))
                merged[InboxHeaderNames.TraceState] = current.TraceStateString;

            return merged;
        }

        /// <summary>
        /// Parses a previously-persisted <see cref="InboxHeaderNames.TraceParent"/>/<see cref="InboxHeaderNames.TraceState"/>
        /// pair (if present and valid) into the <see cref="ActivityLink"/>s a new activity should be
        /// started with. Empty when nothing was persisted, or what was persisted no longer parses as a
        /// valid W3C trace context.
        /// </summary>
        internal static IEnumerable<ActivityLink> ExtractLinks(IReadOnlyDictionary<string, string> headers)
        {
            if (headers.TryGetValue(InboxHeaderNames.TraceParent, out var traceParent)
                && ActivityContext.TryParse(traceParent, headers.GetValueOrDefault(InboxHeaderNames.TraceState), out var context))
            {
                yield return new ActivityLink(context);
            }
        }
    }
}
