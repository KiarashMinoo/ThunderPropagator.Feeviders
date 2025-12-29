using ThunderPropagator.Providers.DotNet.SharedKernel;
using StackExchange.Redis;

namespace ThunderPropagator.Providers.DotNet.RedisPubSub
{
    public abstract class RedisPubSubProviderConfiguration : AbstractProviderConfiguration
    {
        public required string ConnectionString
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public required string Channel
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public RedisChannel.PatternMode PatternMode
        {
            get => Get(RedisChannel.PatternMode.Auto);
            set => Set(value);
        }
    }
}