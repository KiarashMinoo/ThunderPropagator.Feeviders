using Google.Cloud.PubSub.V1;

namespace ThunderPropagator.Feeders.GcpPubSub;

internal sealed record PubSubMessageContext(PubsubMessage Message, TaskCompletionSource<SubscriberClient.Reply> ProcessingCompleted);
