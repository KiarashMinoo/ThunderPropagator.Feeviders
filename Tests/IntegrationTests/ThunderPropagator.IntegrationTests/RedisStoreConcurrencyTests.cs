using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.Redis;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.Outbox.Redis;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// High-concurrency claim-uniqueness coverage for issue #124's "concurrent workers cannot
/// double-claim" acceptance criterion, beyond what the shared contract suites already exercise (8-way)
/// - many more callers racing the same entries against a real Redis server.
/// </summary>
[Trait("Category", "Integration")]
public sealed class RedisStoreConcurrencyTests(RedisContainerFixture fixture) : IClassFixture<RedisContainerFixture>
{
    [Fact]
    public async Task RedisInboxStore_ManyWorkersRacingOneMessage_ShouldClaimExactlyOnce()
    {
        var store = new RedisInboxStore(fixture.Multiplexer, keyPrefix: Guid.NewGuid().ToString("N"));
        var channelKey = Guid.NewGuid();
        var feederId = Guid.NewGuid();

        var attempts = Enumerable.Range(0, 50).Select(i => store.TryClaimAsync(new InboxClaimRequest
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
        Assert.Equal(49, results.Count(r => r.Outcome == InboxClaimOutcome.ClaimedByAnotherOwner));
    }

    [Fact]
    public async Task RedisOutboxStore_ManyWorkersRacingOnePartition_ShouldNeverClaimTheSameEntryTwice()
    {
        var store = new RedisOutboxStore(fixture.Multiplexer, keyPrefix: Guid.NewGuid().ToString("N"));
        for (var i = 0; i < 30; i++)
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

        var claims = Enumerable.Range(0, 50).Select(i => store.ClaimBatchAsync(null, maxCount: 3, $"worker-{i}", TimeSpan.FromMinutes(5)));
        var batches = await Task.WhenAll(claims);

        var claimedIds = batches.SelectMany(b => b.Select(m => m.Id)).ToArray();
        Assert.Equal(30, claimedIds.Length);
        Assert.Equal(30, claimedIds.Distinct().Count());
    }
}
