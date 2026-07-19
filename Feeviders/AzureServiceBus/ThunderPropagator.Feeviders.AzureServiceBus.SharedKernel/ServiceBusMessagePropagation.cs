using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel;

internal static class ServiceBusMessagePropagation
{
    private static readonly TextMapPropagator Propagator = new CompositeTextMapPropagator(
        [new TraceContextPropagator(), new BaggagePropagator()]);

    public static void Inject(ServiceBusMessage message, ActivityContext? activityContext, Baggage baggage)
    {
        Propagator.Inject(
            new PropagationContext(activityContext ?? default, baggage),
            message,
            static (carrier, key, value) => carrier.ApplicationProperties[key] = value);
    }

    public static (ActivityContext? ActivityContext, Baggage? Baggage) Extract(
        IEnumerable<KeyValuePair<string, object>> applicationProperties)
    {
        var propagationContext = Propagator.Extract(
            default,
            applicationProperties,
            static (carrier, key) => carrier
                .Where(property => string.Equals(property.Key, key, StringComparison.OrdinalIgnoreCase))
                .Select(property => property.Value?.ToString() ?? string.Empty));

        ActivityContext? activityContext = propagationContext.ActivityContext == default
            ? null
            : propagationContext.ActivityContext;
        return (activityContext, propagationContext.Baggage);
    }
}
