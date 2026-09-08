using ThunderPropagator.Feeders.Inbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class InboxOptionsTests
    {
        [Fact]
        public void Defaults_ShouldBindWithInboxDisabledAndPassValidation()
        {
            var options = new InboxOptions();

            Assert.False(options.InboxEnabled);
            var exception = Record.Exception(options.Validate);
            Assert.Null(exception);
        }

        [Fact]
        public void Validate_RequireDurableStoreWithInMemory_ShouldThrow()
        {
            var options = new InboxOptions { RequireDurableStore = true, StoreType = InboxStoreType.InMemory };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RequireDurableStoreWithDurableBackend_ShouldPass()
        {
            var options = new InboxOptions
            {
                RequireDurableStore = true,
                StoreType = InboxStoreType.Redis,
                StoreConnectionName = "inbox-redis",
            };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Theory]
        [InlineData(InboxStoreType.Redis)]
        [InlineData(InboxStoreType.EFCore)]
        [InlineData(InboxStoreType.MongoDB)]
        public void Validate_DurableBackendWithoutStoreConnectionName_ShouldThrow(InboxStoreType storeType)
        {
            var options = new InboxOptions { StoreType = storeType };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_InMemoryWithoutStoreConnectionName_ShouldPass()
        {
            var options = new InboxOptions { StoreType = InboxStoreType.InMemory };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_DeduplicationWindowNotPositive_ShouldThrow()
        {
            var options = new InboxOptions { DeduplicationWindow = TimeSpan.Zero };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RetentionPeriodShorterThanDeduplicationWindow_ShouldThrow()
        {
            var options = new InboxOptions
            {
                DeduplicationWindow = TimeSpan.FromDays(7),
                RetentionPeriod = TimeSpan.FromDays(1),
            };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RetentionPeriodEqualToDeduplicationWindow_ShouldPass()
        {
            var options = new InboxOptions
            {
                DeduplicationWindow = TimeSpan.FromDays(1),
                RetentionPeriod = TimeSpan.FromDays(1),
            };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_DeadLetterRetentionPeriodShorterThanDeduplicationWindow_ShouldThrow()
        {
            var options = new InboxOptions
            {
                DeduplicationWindow = TimeSpan.FromDays(7),
                DeadLetterRetentionPeriod = TimeSpan.FromDays(1),
            };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_DeadLetterRetentionPeriodNull_ShouldPass()
        {
            var options = new InboxOptions { DeadLetterRetentionPeriod = null };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_PurgePollingIntervalNotPositive_ShouldThrow()
        {
            var options = new InboxOptions { PurgePollingInterval = TimeSpan.Zero };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(InboxMessageLimits.MaxPurgeBatchSize + 1)]
        public void Validate_PurgeBatchSizeOutOfBounds_ShouldThrow(int purgeBatchSize)
        {
            var options = new InboxOptions { PurgeBatchSize = purgeBatchSize };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_PurgeBatchSizeAtMax_ShouldPass()
        {
            var options = new InboxOptions { PurgeBatchSize = InboxMessageLimits.MaxPurgeBatchSize };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_MaxPurgeBatchesPerRunNotPositive_ShouldThrow()
        {
            var options = new InboxOptions { MaxPurgeBatchesPerRun = 0 };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_NegativeMaxRetryAttempts_ShouldThrow()
        {
            var options = new InboxOptions { MaxRetryAttempts = -1 };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RetryBaseDelayNotPositive_ShouldThrow()
        {
            var options = new InboxOptions { RetryBaseDelay = TimeSpan.Zero };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RetryMaxDelayLessThanBaseDelay_ShouldThrow()
        {
            var options = new InboxOptions
            {
                RetryBaseDelay = TimeSpan.FromSeconds(10),
                RetryMaxDelay = TimeSpan.FromSeconds(1),
            };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RetryPollingIntervalNotPositive_ShouldThrow()
        {
            var options = new InboxOptions { RetryPollingInterval = TimeSpan.Zero };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(InboxMessageLimits.MaxRetryBatchSize + 1)]
        public void Validate_RetryBatchSizeOutOfBounds_ShouldThrow(int retryBatchSize)
        {
            var options = new InboxOptions { RetryBatchSize = retryBatchSize };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RetryBatchSizeAtMax_ShouldPass()
        {
            var options = new InboxOptions { RetryBatchSize = InboxMessageLimits.MaxRetryBatchSize };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_ClaimLeaseDurationNotPositive_ShouldThrow()
        {
            var options = new InboxOptions { ClaimLeaseDuration = TimeSpan.Zero };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(InboxMessageLimits.MaxPayloadSizeBytes + 1)]
        public void Validate_MaxPayloadSizeBytesOutOfBounds_ShouldThrow(int maxPayloadSizeBytes)
        {
            var options = new InboxOptions { MaxPayloadSizeBytes = maxPayloadSizeBytes };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_MessageIdResolutionInvalid_ShouldThrow()
        {
            var options = new InboxOptions
            {
                MessageIdResolution = new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.BrokerHeader },
            };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_MessageIdResolutionFeederFieldWithoutFieldName_ShouldThrow()
        {
            var options = new InboxOptions
            {
                MessageIdResolution = new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.FeederField },
            };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_MessageIdResolutionFeederFieldWithFieldName_ShouldPass()
        {
            var options = new InboxOptions
            {
                MessageIdResolution = new MessageIdResolverOptions { Strategy = InboxMessageIdStrategy.FeederField, FieldName = "OrderId" },
            };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Fact]
        public void ToString_ShouldRedactAConfiguredStoreConnectionName()
        {
            var options = new InboxOptions { StoreType = InboxStoreType.Redis, StoreConnectionName = "super-secret-connection" };

            var text = options.ToString();

            Assert.DoesNotContain("super-secret-connection", text);
            Assert.Contains("***", text);
        }

        [Fact]
        public void ToString_ShouldNotRedactAnUnsetStoreConnectionName()
        {
            var options = new InboxOptions();

            var text = options.ToString();

            Assert.Contains("<none>", text);
        }
    }
}
