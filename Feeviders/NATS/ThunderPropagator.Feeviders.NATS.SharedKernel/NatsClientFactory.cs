using System.Text;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Net;

namespace ThunderPropagator.Feeviders.NATS.SharedKernel
{
    internal
#if !DEBUG
        sealed
#endif
        class NatsClientFactory
    {
        public static INatsClient CreateClient(AbstractNatsFeevidersConfiguration configuration, ILoggerFactory loggerFactory)
        {
            var natsOpts = new NatsOpts
            {
                Url = string.IsNullOrWhiteSpace(configuration.Url) ? throw new ArgumentNullException(nameof(configuration.Url)) : configuration.Url,
                Name = string.IsNullOrWhiteSpace(configuration.Name) ? throw new ArgumentNullException(nameof(configuration.Name)) : configuration.Name,
                Echo = configuration.Echo,
                Verbose = configuration.Verbose,
                Headers = configuration.Headers,
                AuthOpts = configuration.AuthOpts,
                TlsOpts = configuration.TlsOpts,
                WebSocketOpts = configuration.WebSocketOpts,
                WriterBufferSize = configuration.WriterBufferSize,
                ReaderBufferSize = configuration.ReaderBufferSize,
                UseThreadPoolCallback = configuration.UseThreadPoolCallback,
                InboxPrefix = configuration.InboxPrefix,
                NoRandomize = configuration.NoRandomize,
                PingInterval = configuration.PingInterval,
                MaxPingOut = configuration.MaxPingOut,
                ReconnectWaitMin = configuration.ReconnectWaitMin,
                ReconnectJitter = configuration.ReconnectJitter,
                ConnectTimeout = configuration.ConnectTimeout,
                ObjectPoolSize = configuration.ObjectPoolSize,
                RequestTimeout = configuration.RequestTimeout,
                CommandTimeout = configuration.CommandTimeout,
                SubscriptionCleanUpInterval = configuration.SubscriptionCleanUpInterval,
                HeaderEncoding = Encoding.GetEncoding(configuration.HeaderEncoding),
                SubjectEncoding = Encoding.GetEncoding(configuration.SubjectEncoding),
                WaitUntilSent = configuration.WaitUntilSent,
                MaxReconnectRetry = configuration.MaxReconnectRetry,
                ReconnectWaitMax = configuration.ReconnectWaitMax,
                IgnoreAuthErrorAbort = configuration.IgnoreAuthErrorAbort,
                SubPendingChannelCapacity = configuration.SubPendingChannelCapacity,
                SubPendingChannelFullMode = configuration.SubPendingChannelFullMode,
                LoggerFactory = loggerFactory,
                SerializerRegistry = new JsonNatsSerializerRegistry(configuration.SerializerType)
            };

            return new NatsClient(natsOpts, configuration.BoundedChannelFullMode);
        }
    }
}