using ThunderPropagator.Feeders.Inbox;
using ThunderPropagator.Feeders.Inbox.Redis;
using ThunderPropagator.Providers.DotNet.Outbox;
using ThunderPropagator.Providers.DotNet.Outbox.Redis;
using Xunit;

namespace ThunderPropagator.IntegrationTests;

/// <summary>
/// Covers issue #124's "implement TTL/retention without expiring active entries prematurely"
/// acceptance criterion: a non-terminal entry never carries a TTL (so it can never expire mid-flight,
/// no matter how long <see cref="RedisStoreRetentionTests"/> or an unretried failure takes), while a
/// terminal entry does pick up the configured passive-safety-net TTL and is eventually gone even if
/// nobody ever calls <c>PurgeAsync</c>.
/// </summary>
[Trait("Category", "Integration")]
public sealed class RedisStoreRetentionTests(RedisContainerFixture fixture) : IClassFixture<RedisContainerFixture>
{
    [Fact]
    public async Task RedisInboxStore_ActiveEntry_ShouldNeverCarryATtl()
    {
        var keyPrefix = Guid.NewGuid().ToString("N");
        var store = new RedisInboxStore(fixture.Multiplexer, keyPrefix, terminalEntryTtl: TimeSpan.FromSeconds(1));
        var claim = await store.TryClaimAsync(new InboxClaimRequest
        {
            MessageId = "m1",
            ChannelKey = Guid.NewGuid(),
            FeederId = Guid.NewGuid(),
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1],
            LeaseOwner = "owner-a",
            LeaseDuration = TimeSpan.FromMinutes(5),
        });

        var db = fixture.Multiplexer.GetDatabase();
        var messageKey = $"{{{keyPrefix}}}:inbox:msg:{claim.Message!.Id:N}";
        Assert.Null(await db.KeyTimeToLiveAsync(messageKey)); // no TTL at all, immediately after claiming

        await Task.Delay(TimeSpan.FromSeconds(2)); // past the configured terminal TTL, but this entry is still Processing

        var stillThere = await store.GetAsync("m1", claim.Message!.ChannelKey, null);
        Assert.NotNull(stillThere);
        Assert.Equal(InboxMessageStatus.Processing, stillThere!.Status);
        Assert.Null(await db.KeyTimeToLiveAsync(messageKey));
    }

    [Fact]
    public async Task RedisInboxStore_TerminalEntry_ShouldExpireOnItsOwnViaThePassiveTtl()
    {
        var store = new RedisInboxStore(fixture.Multiplexer, keyPrefix: Guid.NewGuid().ToString("N"), terminalEntryTtl: TimeSpan.FromSeconds(1));
        var claim = await store.TryClaimAsync(new InboxClaimRequest
        {
            MessageId = "m1",
            ChannelKey = Guid.NewGuid(),
            FeederId = Guid.NewGuid(),
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1],
            LeaseOwner = "owner-a",
            LeaseDuration = TimeSpan.FromMinutes(5),
        });
        await store.CompleteAsync(claim.Message!.Id, "owner-a");

        // No PurgeAsync call at all - the passive TTL alone must eventually clear this entry.
        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.Null(await store.GetAsync("m1", claim.Message!.ChannelKey, null));
    }

    [Fact]
    public async Task RedisOutboxStore_ActiveEntry_ShouldNeverCarryATtl()
    {
        var keyPrefix = Guid.NewGuid().ToString("N");
        var store = new RedisOutboxStore(fixture.Multiplexer, keyPrefix, terminalEntryTtl: TimeSpan.FromSeconds(1));
        var enqueued = await store.EnqueueAsync(new OutboxEnqueueRequest
        {
            MessageId = "m1",
            ProviderKey = "provider-a",
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1],
        });

        var db = fixture.Multiplexer.GetDatabase();
        var messageKey = $"{{{keyPrefix}}}:outbox:msg:{enqueued.Id:N}";
        Assert.Null(await db.KeyTimeToLiveAsync(messageKey)); // no TTL at all, immediately after enqueuing

        await Task.Delay(TimeSpan.FromSeconds(2)); // past the configured terminal TTL, but this entry is still Pending

        Assert.Equal(1, await store.GetDepthAsync(null));
        Assert.Null(await db.KeyTimeToLiveAsync(messageKey));
    }

    [Fact]
    public async Task RedisOutboxStore_TerminalEntry_ShouldExpireOnItsOwnViaThePassiveTtl()
    {
        // GetDepthAsync/GetClaimablePartitionKeysAsync would report this entry gone immediately after
        // MarkPublishedAsync regardless of any TTL (it is actively removed from the partition's active
        // set the moment it becomes terminal) - so this checks the underlying Redis key itself, which
        // only the passive TTL (not that bookkeeping) can remove.
        var keyPrefix = Guid.NewGuid().ToString("N");
        var store = new RedisOutboxStore(fixture.Multiplexer, keyPrefix, terminalEntryTtl: TimeSpan.FromSeconds(1));
        var enqueued = await store.EnqueueAsync(new OutboxEnqueueRequest
        {
            MessageId = "m1",
            ProviderKey = "provider-a",
            SchemaVersion = 1,
            PayloadContentType = "application/json",
            Payload = [1],
        });
        var claimed = await store.ClaimBatchAsync(null, 10, "owner-a", TimeSpan.FromMinutes(5));
        await store.MarkPublishedAsync(claimed[0].Id, "owner-a");

        var db = fixture.Multiplexer.GetDatabase();
        var messageKey = $"{{{keyPrefix}}}:outbox:msg:{enqueued.Id:N}";
        Assert.True(await db.KeyExistsAsync(messageKey)); // not gone immediately - the passive TTL is what removes it

        // No PurgeAsync call at all - the passive TTL alone must eventually clear this entry.
        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.False(await db.KeyExistsAsync(messageKey));
    }
}
