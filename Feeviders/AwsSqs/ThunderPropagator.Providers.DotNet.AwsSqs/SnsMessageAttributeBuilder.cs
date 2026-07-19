using System.Diagnostics;
using Amazon.SimpleNotificationService.Model;
using OpenTelemetry;
using ThunderPropagator.BuildingBlocks.Application.Helpers;

namespace ThunderPropagator.Providers.DotNet.AwsSqs
{
    internal static class SnsMessageAttributeBuilder
    {
        public static Dictionary<string, MessageAttributeValue> Build(ActivityContext? activityContext, Baggage baggage)
        {
            var attributes = new Dictionary<string, MessageAttributeValue>();

            if (activityContext is not null)
            {
                attributes[nameof(ActivityContext)] = new MessageAttributeValue
                {
                    DataType = "String",
                    StringValue = activityContext.Value.ToNJsonBase64()
                };
            }

            attributes[nameof(Baggage)] = new MessageAttributeValue
            {
                DataType = "String",
                StringValue = baggage.ToNJsonBase64()
            };

            return attributes;
        }
    }
}
