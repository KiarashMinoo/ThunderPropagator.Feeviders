using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using OpenTelemetry;
using ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel;

namespace ThunderPropagator.UnitTests.AzureServiceBus;

public class ServiceBusMessagePropagationTests
{
    [Fact]
    public void InjectAndExtract_ShouldRoundTripPropagationValues()
    {
        var context = new ActivityContext(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
        var baggage = Baggage.Current.SetBaggage("tenant", "northwind");
        var message = new ServiceBusMessage("payload");

        ServiceBusMessagePropagation.Inject(message, context, baggage);
        var result = ServiceBusMessagePropagation.Extract(message.ApplicationProperties);

        Assert.Equal(context.TraceId, result.ActivityContext?.TraceId);
        Assert.Equal(context.SpanId, result.ActivityContext?.SpanId);
        Assert.Equal(context.TraceFlags, result.ActivityContext?.TraceFlags);
        Assert.Equal("northwind", result.Baggage?.GetBaggage("tenant"));
    }
}
