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

        public RedisChannel.PatternMode PatternMode
        {
            get => Get(RedisChannel.PatternMode.Auto);
            set
            {
                if (value == RedisChannel.PatternMode.Auto && ContainsWildcard(Get<string>(nameof(Channel))))
                    throw CreateWildcardException(nameof(PatternMode));

                Set(value);
            }
        }

        public string Channel
        {
            get
            {
                var channel = Get<string>()!;
                if (PatternMode == RedisChannel.PatternMode.Auto && ContainsWildcard(channel))
                    throw CreateWildcardException(nameof(Channel));

                return channel;
            }
            set => Set(value);
        }

        private static bool ContainsWildcard(string? channel)
            => channel?.IndexOfAny(['*', '?', '[']) >= 0;

        private static ArgumentException CreateWildcardException(string parameterName)
            => new(
                "Wildcard channel names require an explicit PatternMode. " +
                "Use PatternMode.Pattern for pattern subscriptions or PatternMode.Literal for an exact channel name.",
                parameterName);
    }
}
