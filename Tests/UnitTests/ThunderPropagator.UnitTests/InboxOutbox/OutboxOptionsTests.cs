using ThunderPropagator.Providers.DotNet.Outbox;

namespace ThunderPropagator.UnitTests.InboxOutbox
{
    public class OutboxOptionsTests
    {
        [Fact]
        public void Defaults_ShouldBindWithOutboxDisabledAndPassValidation()
        {
            var options = new OutboxOptions();

            Assert.False(options.OutboxEnabled);
            Assert.Equal(OutboxOrderingPolicy.StrictPerPartition, options.OrderingPolicy);
            var exception = Record.Exception(options.Validate);
            Assert.Null(exception);
        }

        [Fact]
        public void ToString_ShouldIncludeTheConfiguredOrderingPolicy()
        {
            var options = new OutboxOptions { OrderingPolicy = OutboxOrderingPolicy.ContinueOnFailure };

            Assert.Contains(nameof(OutboxOrderingPolicy.ContinueOnFailure), options.ToString());
        }

        [Fact]
        public void Validate_RequireDurableStoreWithInMemory_ShouldThrow()
        {
            var options = new OutboxOptions { RequireDurableStore = true, StoreType = OutboxStoreType.InMemory };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RequireDurableStoreWithDurableBackend_ShouldPass()
        {
            var options = new OutboxOptions
            {
                RequireDurableStore = true,
                StoreType = OutboxStoreType.Redis,
                StoreConnectionName = "outbox-redis",
            };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Theory]
        [InlineData(OutboxStoreType.Redis)]
        [InlineData(OutboxStoreType.EFCore)]
        [InlineData(OutboxStoreType.MongoDB)]
        public void Validate_DurableBackendWithoutStoreConnectionName_ShouldThrow(OutboxStoreType storeType)
        {
            var options = new OutboxOptions { StoreType = storeType };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_InMemoryWithoutStoreConnectionName_ShouldPass()
        {
            var options = new OutboxOptions { StoreType = OutboxStoreType.InMemory };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Theory]
        [InlineData(OutboxStoreType.InMemory)]
        [InlineData(OutboxStoreType.Redis)]
        [InlineData(OutboxStoreType.MongoDB)]
        public void Validate_EnlistedWithoutEfCore_ShouldThrow(OutboxStoreType storeType)
        {
            var options = new OutboxOptions
            {
                StoreType = storeType,
                StoreConnectionName = storeType == OutboxStoreType.InMemory ? null : "outbox-store",
                TransactionMode = OutboxTransactionMode.Enlisted,
            };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_EnlistedWithEfCore_ShouldPass()
        {
            var options = new OutboxOptions
            {
                StoreType = OutboxStoreType.EFCore,
                StoreConnectionName = "outbox-sql",
                TransactionMode = OutboxTransactionMode.Enlisted,
            };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_NonTransactionalWithAnyBackend_ShouldPass()
        {
            var options = new OutboxOptions { StoreType = OutboxStoreType.Redis, StoreConnectionName = "outbox-redis", TransactionMode = OutboxTransactionMode.NonTransactional };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_FixedPartitionStrategyWithoutFixedPartitionKey_ShouldThrow()
        {
            var options = new OutboxOptions { PartitionStrategy = OutboxPartitionStrategy.Fixed };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_FixedPartitionStrategyWithFixedPartitionKey_ShouldPass()
        {
            var options = new OutboxOptions { PartitionStrategy = OutboxPartitionStrategy.Fixed, FixedPartitionKey = "orders" };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Theory]
        [InlineData(OutboxPartitionStrategy.None)]
        [InlineData(OutboxPartitionStrategy.CallerSupplied)]
        public void Validate_NonFixedPartitionStrategies_ShouldNotRequireFixedPartitionKey(OutboxPartitionStrategy strategy)
        {
            var options = new OutboxOptions { PartitionStrategy = strategy };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_RelayPollingIntervalNotPositive_ShouldThrow()
        {
            var options = new OutboxOptions { RelayPollingInterval = TimeSpan.Zero };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(OutboxMessageLimits.MaxRelayBatchSize + 1)]
        public void Validate_RelayBatchSizeOutOfBounds_ShouldThrow(int relayBatchSize)
        {
            var options = new OutboxOptions { RelayBatchSize = relayBatchSize };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RelayBatchSizeAtMax_ShouldPass()
        {
            var options = new OutboxOptions { RelayBatchSize = OutboxMessageLimits.MaxRelayBatchSize };

            var exception = Record.Exception(options.Validate);

            Assert.Null(exception);
        }

        [Fact]
        public void Validate_NegativeMaxRetryAttempts_ShouldThrow()
        {
            var options = new OutboxOptions { MaxRetryAttempts = -1 };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RetryBaseDelayNotPositive_ShouldThrow()
        {
            var options = new OutboxOptions { RetryBaseDelay = TimeSpan.Zero };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RetryMaxDelayLessThanBaseDelay_ShouldThrow()
        {
            var options = new OutboxOptions
            {
                RetryBaseDelay = TimeSpan.FromSeconds(10),
                RetryMaxDelay = TimeSpan.FromSeconds(1),
            };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_RetentionPeriodNotPositive_ShouldThrow()
        {
            var options = new OutboxOptions { RetentionPeriod = TimeSpan.Zero };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void Validate_ClaimLeaseDurationNotPositive_ShouldThrow()
        {
            var options = new OutboxOptions { ClaimLeaseDuration = TimeSpan.Zero };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(OutboxMessageLimits.MaxPayloadSizeBytes + 1)]
        public void Validate_MaxPayloadSizeBytesOutOfBounds_ShouldThrow(int maxPayloadSizeBytes)
        {
            var options = new OutboxOptions { MaxPayloadSizeBytes = maxPayloadSizeBytes };

            Assert.Throws<ArgumentException>(options.Validate);
        }

        [Fact]
        public void ToString_ShouldRedactAConfiguredStoreConnectionName()
        {
            var options = new OutboxOptions { StoreType = OutboxStoreType.Redis, StoreConnectionName = "super-secret-connection" };

            var text = options.ToString();

            Assert.DoesNotContain("super-secret-connection", text);
            Assert.Contains("***", text);
        }

        [Fact]
        public void ToString_ShouldNotRedactAnUnsetStoreConnectionName()
        {
            var options = new OutboxOptions();

            var text = options.ToString();

            Assert.Contains("<none>", text);
        }
    }
}
