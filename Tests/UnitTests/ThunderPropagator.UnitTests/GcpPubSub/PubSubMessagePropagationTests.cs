using System.Diagnostics;
using Google.Cloud.PubSub.V1;
using OpenTelemetry;
using ThunderPropagator.Feeviders.GcpPubSub.SharedKernel;

namespace ThunderPropagator.UnitTests.GcpPubSub;

public class PubSubMessagePropagationTests
{
    [Fact]
    public void InjectAndExtract_ShouldRoundTripPropagationValues()
    {
        var context = new ActivityContext(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
        var baggage = Baggage.Current.SetBaggage("tenant", "northwind");
        var message = new PubsubMessage();

        PubSubMessagePropagation.Inject(message, context, baggage);
        var result = PubSubMessagePropagation.Extract(message.Attributes);

        Assert.Equal(context.TraceId, result.ActivityContext?.TraceId);
        Assert.Equal(context.SpanId, result.ActivityContext?.SpanId);
        Assert.Equal(context.TraceFlags, result.ActivityContext?.TraceFlags);
        Assert.Equal("northwind", result.Baggage?.GetBaggage("tenant"));
    }
}
