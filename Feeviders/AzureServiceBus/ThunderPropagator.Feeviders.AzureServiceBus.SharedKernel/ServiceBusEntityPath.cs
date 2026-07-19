namespace ThunderPropagator.Feeviders.AzureServiceBus.SharedKernel;

internal sealed record ServiceBusEntityPath(string EntityName, string? SubscriptionName)
{
    public static ServiceBusEntityPath Parse(string entityPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(entityPath);

        var parts = entityPath.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return parts.Length switch
        {
            1 => new ServiceBusEntityPath(parts[0], null),
            2 => new ServiceBusEntityPath(parts[0], parts[1]),
            _ => throw new ArgumentException("EntityPath must be a queue name or a topic/subscription path.", nameof(entityPath))
        };
    }
}
