using System.Diagnostics;
using Google.Cloud.PubSub.V1;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace ThunderPropagator.Feeviders.GcpPubSub.SharedKernel;

internal static class PubSubMessagePropagation
{
    private static readonly TextMapPropagator Propagator = new CompositeTextMapPropagator(
        [new TraceContextPropagator(), new BaggagePropagator()]);

    public static void Inject(PubsubMessage message, ActivityContext? activityContext, Baggage baggage)
    {
        Propagator.Inject(
            new PropagationContext(activityContext ?? default, baggage),
            message,
            static (carrier, key, value) => carrier.Attributes[key] = value);
    }

    public static (ActivityContext? ActivityContext, Baggage? Baggage) Extract(
        IEnumerable<KeyValuePair<string, string>> attributes)
    {
        var propagationContext = Propagator.Extract(
            default,
            attributes,
            static (carrier, key) => carrier
                .Where(attribute => string.Equals(attribute.Key, key, StringComparison.OrdinalIgnoreCase))
                .Select(attribute => attribute.Value));

        ActivityContext? activityContext = propagationContext.ActivityContext == default
            ? null
            : propagationContext.ActivityContext;
        return (activityContext, propagationContext.Baggage);
    }
}
