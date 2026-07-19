using System.Diagnostics;
using OpenTelemetry;
using ThunderPropagator.Providers.DotNet.AwsSqs;

namespace ThunderPropagator.UnitTests.AwsSqs
{
    public class SqsMessageAttributeBuilderTests
    {
        [Fact]
        public void Build_ShouldOmitActivityContextWhenNull()
        {
            var attributes = SqsMessageAttributeBuilder.Build(null, default);

            Assert.False(attributes.ContainsKey(nameof(ActivityContext)));
            Assert.True(attributes.ContainsKey(nameof(Baggage)));
            Assert.Equal("String", attributes[nameof(Baggage)].DataType);
            Assert.False(string.IsNullOrEmpty(attributes[nameof(Baggage)].StringValue));
        }

        [Fact]
        public void Build_ShouldIncludeActivityContextWhenProvided()
        {
            var activityContext = new ActivityContext(
                ActivityTraceId.CreateRandom(),
                ActivitySpanId.CreateRandom(),
                ActivityTraceFlags.Recorded);

            var attributes = SqsMessageAttributeBuilder.Build(activityContext, default);

            Assert.True(attributes.ContainsKey(nameof(ActivityContext)));
            Assert.Equal("String", attributes[nameof(ActivityContext)].DataType);
            Assert.False(string.IsNullOrEmpty(attributes[nameof(ActivityContext)].StringValue));
        }
    }
}
