using Apache.NMS;
using RapidStreamer.Feeviders.ActiveMQ.SharedKernel;
using RapidStreamer.Providers.DotNet.SharedKernel;

namespace RapidStreamer.Providers.DotNet.ActiveMQ
{
    public abstract class ActiveMQProviderConfiguration : AbstractProviderConfiguration,
        IActiveMQFeeviderConfiguration
    {
        public Uri BrokerUri
        {
            get => Get<Uri>()!;
            set => Set(value);
        }

        public string? ClientId
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? ClientIdPrefix
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? UserName
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? Password
        {
            get => Get<string>();
            set => Set(value);
        }

        public int? AuditMaximumProducerNumber
        {
            get => Get<int>();
            set => Set(value);
        }

        public string Queue
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public bool? UseCompression
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? CopyMessageOnSend
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? AlwaysSyncSend
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? AsyncClose
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? SendAcksAsync
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? AsyncSend
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? DispatchAsync
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? WatchTopicAdvisories
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? MessagePrioritySupported
        {
            get => Get<bool>();
            set => Set(value);
        }

        public int? RequestTimeout
        {
            get => Get<int>();
            set => Set(value);
        }

        public AcknowledgementMode? AcknowledgementMode
        {
            get => Get<AcknowledgementMode>();
            set => Set(value);
        }

        public int? ProducerWindowSize
        {
            get => Get<int>();
            set => Set(value);
        }

        public bool? OptimizeAcknowledge
        {
            get => Get<bool>();
            set => Set(value);
        }

        public long? OptimizeAcknowledgeTimeOut
        {
            get => Get<long>();
            set => Set(value);
        }

        public long? OptimizedAckScheduledAckInterval
        {
            get => Get<long>();
            set => Set(value);
        }

        public bool? UseRetroactiveConsumer
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? ExclusiveConsumer
        {
            get => Get<bool>();
            set => Set(value);
        }

        public long? ConsumerFailoverRedeliveryWaitPeriod
        {
            get => Get<long>();
            set => Set(value);
        }

        public bool? CheckForDuplicates
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? TransactedIndividualAck
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? NonBlockingRedelivery
        {
            get => Get<bool>();
            set => Set(value);
        }

        public int? AuditDepth
        {
            get => Get<int>();
            set => Set(value);
        }

        //Producer Configurations
        public MsgDeliveryMode? DeliveryMode
        {
            get => Get<MsgDeliveryMode>();
            set => Set(value);
        }

        public TimeSpan? TimeToLive
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public TimeSpan? ProducerRequestTimeout
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public MsgPriority? Priority
        {
            get => Get<MsgPriority>();
            set => Set(value);
        }

        public bool? DisableMessageID
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? DisableMessageTimestamp
        {
            get => Get<bool>();
            set => Set(value);
        }

        public TimeSpan? DeliveryDelay
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }
    }
}