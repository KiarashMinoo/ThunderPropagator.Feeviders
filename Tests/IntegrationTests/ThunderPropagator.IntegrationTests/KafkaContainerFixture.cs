using System.Text;
using Confluent.Kafka;
using Testcontainers.Kafka;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// One real Kafka broker (single-node, KRaft mode) shared across every test in a class via
/// <see cref="IClassFixture{TFixture}"/> - starting a fresh broker per test would make the suite
/// prohibitively slow. Isolation between tests instead comes from each test using its own unique
/// topic/consumer-group, not from a fresh broker.
/// </summary>
public sealed class KafkaContainerFixture : IAsyncLifetime
{
    private readonly KafkaContainer _container = new KafkaBuilder("confluentinc/cp-kafka:7.5.12").WithKRaft().Build();

    public string BootstrapAddress => _container.GetBootstrapAddress();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    /// <summary>Produces one raw record for test setup, using a real Kafka producer.</summary>
    public async Task ProduceAsync(
        string topic,
        string key,
        byte[] value,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        using var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = BootstrapAddress }).Build();

        var message = new Message<string, byte[]> { Key = key, Value = value };
        if (headers is not null)
        {
            message.Headers = [];
            foreach (var (headerKey, headerValue) in headers)
                message.Headers.Add(headerKey, Encoding.UTF8.GetBytes(headerValue));
        }

        await producer.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
        producer.Flush(cancellationToken);
    }
}
