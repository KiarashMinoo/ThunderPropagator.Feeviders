using Polly;
using ThunderPropagator.Providers.DotNet.SharedKernel;

namespace ThunderPropagator.Providers.DotNet.WebApi
{
    public abstract class WebApiProviderConfiguration : AbstractProviderConfiguration
    {
        public required string BaseAddress
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public required string Path
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public DelayBackoffType BackoffType
        {
            get => Get(DelayBackoffType.Exponential);
            set => Set(value);
        }

        public int MaxRetryAttempts
        {
            get => Get(3);
            set => Set(value);
        }

        public int MaxDelay
        {
            get => Get(3);
            set => Set(value);
        }

        public bool UseJitter
        {
            get => Get(true);
            set => Set(value);
        }

        public int SamplingDuration
        {
            get => Get(10);
            set => Set(value);
        }

        public double FailureRatio
        {
            get => Get(0.2);
            set => Set(value);
        }

        public int MinimumThroughput
        {
            get => Get(3);
            set => Set(value);
        }

        public int CircuitBreakerRetryCount
        {
            get => Get(3);
            set => Set(value);
        }

        public int CircuitBreakerDurationOfBreak
        {
            get => Get(3);
            set => Set(value);
        }

        public int RequestTimeout
        {
            get => Get(20);
            set => Set(value);
        }
    }
}