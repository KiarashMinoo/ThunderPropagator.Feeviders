using Apache.NMS;

namespace RapidStreamer.Feeviders.ActiveMQ.SharedKernel;

public interface IActiveMQFeeviderConfiguration
{
    Uri BrokerUri { get; set; }
    string? UserName { get; set; }
    string? Password { get; set; }
    string? ClientId { get; set; }
    string? ClientIdPrefix { get; set; }
    bool? UseCompression { get; set; }
    bool? CopyMessageOnSend { get; set; }
    bool? AlwaysSyncSend { get; set; }
    bool? AsyncClose { get; set; }
    bool? SendAcksAsync { get; set; }
    bool? AsyncSend { get; set; }
    bool? DispatchAsync { get; set; }
    bool? WatchTopicAdvisories { get; set; }
    bool? MessagePrioritySupported { get; set; }
    int? RequestTimeout { get; set; }
    AcknowledgementMode? AcknowledgementMode { get; set; }
    int? ProducerWindowSize { get; set; }
    bool? OptimizeAcknowledge { get; set; }
    long? OptimizeAcknowledgeTimeOut { get; set; }
    long? OptimizedAckScheduledAckInterval { get; set; }
    bool? UseRetroactiveConsumer { get; set; }
    bool? ExclusiveConsumer { get; set; }
    long? ConsumerFailoverRedeliveryWaitPeriod { get; set; }
    bool? CheckForDuplicates { get; set; }
    bool? TransactedIndividualAck { get; set; }
    bool? NonBlockingRedelivery { get; set; }
    int? AuditDepth { get; set; }
    int? AuditMaximumProducerNumber { get; set; }
    string Queue { get; set; }
}