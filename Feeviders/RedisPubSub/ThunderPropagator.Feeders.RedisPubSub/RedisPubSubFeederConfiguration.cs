using ThunderPropagator.Application.Feeders;
using StackExchange.Redis;

namespace ThunderPropagator.Feeders.RedisPubSub
{
    public abstract class RedisPubSubFeederConfiguration : AbstractFeederConfiguration
    {
        public string ConnectionString
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public string Channel
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