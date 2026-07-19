using Azure.Messaging.ServiceBus;

namespace ThunderPropagator.Feeders.AzureServiceBus;

internal sealed record ServiceBusMessageContext(
    ProcessMessageEventArgs EventArgs,
    TaskCompletionSource ProcessingCompleted);
