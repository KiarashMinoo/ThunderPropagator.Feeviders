using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class OutboxRetryBackoffTests
    {
        private static OutboxOptions Options(TimeSpan baseDelay, TimeSpan maxDelay) => new()
        {
            RetryBaseDelay = baseDelay,
            RetryMaxDelay = maxDelay,
        };

        [Fact]
        public void Compute_FullJitter_ShouldScaleByTheFixedFraction()
        {
            var options = Options(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5));
            var random = new FixedRandom(0.5);

            var delay = OutboxRetryBackoff.Compute(options, attemptCount: 1, random);

            Assert.Equal(TimeSpan.FromSeconds(0.5), delay);
        }

        [Fact]
        public void Compute_SubsequentAttempts_ShouldDoubleTheExponentialCeilingBeforeJitter()
        {
            var options = Options(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5));
            var random = new FixedRandom(1.0);

            var first = OutboxRetryBackoff.Compute(options, attemptCount: 1, random);
            var second = OutboxRetryBackoff.Compute(options, attemptCount: 2, random);
            var third = OutboxRetryBackoff.Compute(options, attemptCount: 3, random);

            Assert.Equal(TimeSpan.FromSeconds(1), first);
            Assert.Equal(TimeSpan.FromSeconds(2), second);
            Assert.Equal(TimeSpan.FromSeconds(4), third);
        }

        [Fact]
        public void Compute_ShouldNeverExceedRetryMaxDelay()
        {
            var options = Options(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10));
            var random = new FixedRandom(1.0);

            var delay = OutboxRetryBackoff.Compute(options, attemptCount: 20, random);

            Assert.Equal(TimeSpan.FromSeconds(10), delay);
        }

        [Fact]
        public void Compute_ZeroJitterFraction_ShouldReturnZero()
        {
            var options = Options(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5));
            var random = new FixedRandom(0.0);

            var delay = OutboxRetryBackoff.Compute(options, attemptCount: 5, random);

            Assert.Equal(TimeSpan.Zero, delay);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(-3)]
        public void Compute_AttemptCountAtOrBelowOne_ShouldTreatAsFirstAttempt(int attemptCount)
        {
            var options = Options(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5));
            var random = new FixedRandom(1.0);

            var delay = OutboxRetryBackoff.Compute(options, attemptCount, random);

            Assert.Equal(TimeSpan.FromSeconds(1), delay);
        }

        private sealed class FixedRandom(double value) : Random
        {
            public override double NextDouble() => value;
        }
    }
}
