using Apache.NMS;
using RapidStreamer.Application.Feeders;
using RapidStreamer.Feeviders.ActiveMQ.SharedKernel;

namespace RapidStreamer.Feeders.ActiveMQ
{
    public abstract class ActiveMQFeederConfiguration : AbstractFeederConfiguration,
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
    }
}