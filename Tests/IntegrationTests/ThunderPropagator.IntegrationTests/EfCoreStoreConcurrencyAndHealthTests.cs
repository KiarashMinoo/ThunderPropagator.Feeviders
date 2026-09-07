using Microsoft.Extensions.Diagnostics.HealthChecks;
using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Providers.DotNet.Outbox;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Covers issue #125's "concurrent pollers do not double-claim" and "provide DI and provider capability
/// abstraction" (health integration half) acceptance criteria, beyond what the shared contract suites
/// already exercise, against a real PostgreSQL container.
/// </summary>
[Trait("Category", "Integration")]
public sealed class EfCoreStoreConcurrencyAndHealthTests(PostgresContainerFixture fixture) : IClassFixture<PostgresContainerFixture>
{
    private (string Schema, Microsoft.EntityFrameworkCore.DbContextOptions<EfCoreTestDbContext> Options) NewSchema()
    {
        var schema = $"s{Guid.NewGuid():N}";
        var options = EfCoreTestDbContext.BuildOptions(fixture.GetConnectionString());
        using (var db = new EfCoreTestDbContext(options, schema))
            db.CreateTables();

        return (schema, options);
    }

    [Fact]
    public async Task EfCoreInboxStore_ManyPollersRacingOneMessage_ShouldClaimExactlyOnce()
    {
        var (schema, options) = NewSchema();
        var store = new EfCoreInboxStore(() => new EfCoreTestDbContext(options, schema));
        var channelKey = Guid.NewGuid();
        var feederId = Guid.NewGuid();

        var attempts = Enumerable.Range(0, 20).Select(i => store.TryClaimAsync(new InboxClaimRequest
        {
            MessageId = "shared-message",
            ChannelKey = channelKey,
            FeederId = feederId,
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1, 2, 3],
            LeaseOwner = $"worker-{i}",
            LeaseDuration = TimeSpan.FromMinutes(5),
        }));

        var results = await Task.WhenAll(attempts);

        Assert.Single(results, r => r.Outcome == InboxClaimOutcome.Claimed);
        Assert.Equal(19, results.Count(r => r.Outcome == InboxClaimOutcome.ClaimedByAnotherOwner));
    }

    [Fact]
    public async Task EfCoreOutboxStore_ManyPollersRacingOnePartition_ShouldNeverClaimTheSameEntryTwice()
    {
        var (schema, options) = NewSchema();
        var store = new EfCoreOutboxStore(() => new EfCoreTestDbContext(options, schema));
        for (var i = 0; i < 20; i++)
        {
            await store.EnqueueAsync(new OutboxEnqueueRequest
            {
                MessageId = $"m{i}",
                ProviderKey = "provider-a",
                SchemaVersion = 1,
                PayloadContentType = "application/json",
                Payload = [1],
            });
        }

        var claims = Enumerable.Range(0, 20).Select(i => store.ClaimBatchAsync(null, maxCount: 3, $"worker-{i}", TimeSpan.FromMinutes(5)));
        var batches = await Task.WhenAll(claims);

        var claimedIds = batches.SelectMany(b => b.Select(m => m.Id)).ToArray();
        Assert.Equal(20, claimedIds.Length);
        Assert.Equal(20, claimedIds.Distinct().Count());
    }

    [Fact]
    public async Task EfCoreInboxStore_CheckHealthAsync_ShouldReportHealthyWhileConnected()
    {
        var (schema, options) = NewSchema();
        var store = new EfCoreInboxStore(() => new EfCoreTestDbContext(options, schema));

        var result = await store.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task EfCoreOutboxStore_CheckHealthAsync_ShouldReportHealthyWhileConnected()
    {
        var (schema, options) = NewSchema();
        var store = new EfCoreOutboxStore(() => new EfCoreTestDbContext(options, schema));

        var result = await store.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }
}
