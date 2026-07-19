using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using ThunderPropagator.Feeders.RedisPubSub;

namespace ThunderPropagator.UnitTests
{
    public class RedisPubSubFeederConfigurationTests
    {
        [Theory]
        [InlineData("*")]
        [InlineData("tenant-?")]
        [InlineData("tenant-[ab]")]
        public void Channel_ShouldRejectWildcards_WhenPatternModeIsAuto(string channel)
        {
            var configuration = new TestRedisPubSubFeederConfiguration();

            var exception = Assert.Throws<ArgumentException>(() => configuration.Channel = channel);

            Assert.Contains("explicit PatternMode", exception.Message);
        }

        [Fact]
        public void Channel_ShouldAllowWildcards_WhenPatternModeIsExplicitlyPattern()
        {
            var configuration = new TestRedisPubSubFeederConfiguration
            {
                PatternMode = RedisChannel.PatternMode.Pattern,
                Channel = "tenant-*"
            };

            Assert.Equal("tenant-*", configuration.Channel);
            Assert.Equal(RedisChannel.PatternMode.Pattern, configuration.PatternMode);
        }

        [Fact]
        public void Channel_ShouldAllowWildcards_WhenPatternModeIsExplicitlyLiteral()
        {
            var configuration = new TestRedisPubSubFeederConfiguration
            {
                PatternMode = RedisChannel.PatternMode.Literal,
                Channel = "tenant-*"
            };

            Assert.Equal("tenant-*", configuration.Channel);
            Assert.Equal(RedisChannel.PatternMode.Literal, configuration.PatternMode);
        }

        [Fact]
        public void PatternMode_ShouldRejectAuto_WhenChannelContainsWildcard()
        {
            var configuration = new TestRedisPubSubFeederConfiguration
            {
                PatternMode = RedisChannel.PatternMode.Pattern,
                Channel = "tenant-*"
            };

            var exception = Assert.Throws<ArgumentException>(
                () => configuration.PatternMode = RedisChannel.PatternMode.Auto);

            Assert.Contains("explicit PatternMode", exception.Message);
            Assert.Equal(RedisChannel.PatternMode.Pattern, configuration.PatternMode);
        }

        [Fact]
        public void Channel_ShouldAllowLiteralName_WhenPatternModeIsAuto()
        {
            var configuration = new TestRedisPubSubFeederConfiguration
            {
                Channel = "tenant-orders"
            };

            Assert.Equal("tenant-orders", configuration.Channel);
            Assert.Equal(RedisChannel.PatternMode.Auto, configuration.PatternMode);
        }

        [Fact]
        public void ConfigurationBinding_ShouldAllowExplicitPatternSubscription()
        {
            var configurationRoot = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [nameof(RedisPubSubFeederConfiguration.Channel)] = "tenant-*",
                    [nameof(RedisPubSubFeederConfiguration.PatternMode)] = nameof(RedisChannel.PatternMode.Pattern)
                })
                .Build();
            var configuration = new TestRedisPubSubFeederConfiguration();

            configurationRoot.Bind(configuration);

            Assert.Equal("tenant-*", configuration.Channel);
            Assert.Equal(RedisChannel.PatternMode.Pattern, configuration.PatternMode);
        }

        private sealed class TestRedisPubSubFeederConfiguration : RedisPubSubFeederConfiguration;
    }
}
