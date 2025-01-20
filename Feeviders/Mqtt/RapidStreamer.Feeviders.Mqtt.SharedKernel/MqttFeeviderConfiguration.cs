using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.AspNetCore.Connections;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using RapidStreamer.BuildingBlocks.Application;
using RapidStreamer.BuildingBlocks.Application.Helpers;

namespace RapidStreamer.Feeviders.Mqtt.SharedKernel
{
    public abstract class MqttFeeviderConfiguration : ServiceConfiguration
    {
        public AddressFamily? AddressFamily
        {
            get => Get<AddressFamily>();
            set => Set(value);
        }

        public bool? CleanSession
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? CleanStart
        {
            get => Get<bool>();
            set => Set(value);
        }

        public string ClientId
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public string? ConnectionUri
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? Username
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? Password
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? EndPoint
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? Host
        {
            get => Get<string>();
            set => Set(value);
        }

        public int? Port
        {
            get => Get<int>();
            set => Set(value);
        }

        public AddressFamily TcpServerAddressFamily
        {
            get => Get(System.Net.Sockets.AddressFamily.Unspecified);
            set => Set(value);
        }

        public TimeSpan? KeepAlivePeriod
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public uint? MaximumPacketSize
        {
            get => Get<uint>();
            set => Set(value);
        }

        public ProtocolType? ProtocolType
        {
            get => Get<ProtocolType>();
            set => Set(value);
        }

        public MqttProtocolVersion? ProtocolVersion
        {
            get => Get<MqttProtocolVersion>();
            set => Set(value);
        }

        public ushort? ReceiveMaximum
        {
            get => Get<ushort>();
            set => Set(value);
        }

        public bool? RequestProblemInformation
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool? RequestResponseInformation
        {
            get => Get<bool>();
            set => Set(value);
        }

        public uint? SessionExpiryInterval
        {
            get => Get<uint>();
            set => Set(value);
        }

        public TimeSpan? Timeout
        {
            get => Get<TimeSpan>();
            set => Set(value);
        }

        public ushort? TopicAliasMaximum
        {
            get => Get<ushort>();
            set => Set(value);
        }

        public bool? TryPrivate
        {
            get => Get<bool>();
            set => Set(value);
        }

        public string? ContentType
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? CorrelationData
        {
            get => Get<string>();
            set => Set(value);
        }

        public uint? DelayInterval
        {
            get => Get<uint>();
            set => Set(value);
        }

        public uint? MessageExpiryInterval
        {
            get => Get<uint>();
            set => Set(value);
        }

        public string? Payload
        {
            get => Get<string>();
            set => Set(value);
        }

        public MqttPayloadFormatIndicator? PayloadFormatIndicator
        {
            get => Get<MqttPayloadFormatIndicator>();
            set => Set(value);
        }

        public MqttQualityOfServiceLevel? QualityOfServiceLevel
        {
            get => Get<MqttQualityOfServiceLevel>();
            set => Set(value);
        }

        public string? ResponseTopic
        {
            get => Get<string>();
            set => Set(value);
        }

        public bool? Retain
        {
            get => Get<bool>();
            set => Set(value);
        }

        public string Topic
        {
            get => Get<string>()!;
            set => Set(value);
        }

        public bool WithoutPacketFragmentation
        {
            get => Get<bool>();
            set => Set(value);
        }

        public bool NoKeepAlive
        {
            get => Get<bool>();
            set => Set(value);
        }

        public IDictionary<string, string>? UserProperties
        {
            get => Get<string>()?.FromNJson<Dictionary<string, string>>();
            set => Set(value?.ToNJson());
        }

        public IDictionary<string, string>? WillUserProperties
        {
            get => Get<string>()?.FromNJson<Dictionary<string, string>>();
            set => Set(value?.ToNJson());
        }

        public MqttClientTlsOptions? TlsOptions
        {
            get => Get<MqttClientTlsOptions>();
            set => Set(value);
        }

        public string? EnhancedAuthenticationMethod
        {
            get => Get<string>();
            set => Set(value);
        }

        public string? EnhancedAuthenticationData
        {
            get => Get<string>();
            set => Set(value);
        }

        public uint? SubscriptionIdentifier
        {
            get => Get<uint>();
            set => Set(value);
        }

        public MqttClientOptions ToMqttClientOptions()
        {
            var mqttClientOptionsBuilder = new MqttClientOptionsBuilder();

            if (!string.IsNullOrWhiteSpace(Host))
                mqttClientOptionsBuilder.WithTcpServer(Host, Port, TcpServerAddressFamily);
            else if (!string.IsNullOrWhiteSpace(EndPoint))
            {
                if (IPEndPoint.TryParse(EndPoint, out var ipEndPoint))
                    mqttClientOptionsBuilder.WithEndPoint(ipEndPoint);
                else if (Uri.TryCreate(EndPoint, UriKind.Absolute, out var uri))
                {
                    UriEndPoint uriEndPoint = new(uri);
                    mqttClientOptionsBuilder.WithEndPoint(uriEndPoint);
                }
                else
                    throw new InvalidOperationException("Invalid mqtt feed configuration");
            }
            else
                throw new InvalidOperationException("Invalid mqtt feed configuration");

            ArgumentException.ThrowIfNullOrWhiteSpace(ClientId);
            mqttClientOptionsBuilder.WithClientId(ClientId);

            ArgumentException.ThrowIfNullOrWhiteSpace(Topic);
            mqttClientOptionsBuilder.WithWillTopic(Topic);

            if (AddressFamily is not null)
                mqttClientOptionsBuilder.WithAddressFamily(AddressFamily.Value);

            if (CleanSession is not null)
                mqttClientOptionsBuilder.WithCleanSession(CleanSession.Value);

            if (CleanStart is not null)
                mqttClientOptionsBuilder.WithCleanStart(CleanStart.Value);

            if (!string.IsNullOrWhiteSpace(ConnectionUri))
                mqttClientOptionsBuilder.WithConnectionUri(ConnectionUri);

            if (!string.IsNullOrWhiteSpace(Username))
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(Password);
                mqttClientOptionsBuilder.WithCredentials(Username, Password);
            }

            if (!string.IsNullOrWhiteSpace(EnhancedAuthenticationMethod))
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(EnhancedAuthenticationData);
                var data = Encoding.UTF8.GetBytes(EnhancedAuthenticationData);
                mqttClientOptionsBuilder.WithEnhancedAuthentication(EnhancedAuthenticationMethod, data);
            }

            if (KeepAlivePeriod is not null)
                mqttClientOptionsBuilder.WithKeepAlivePeriod(KeepAlivePeriod.Value);

            if (MaximumPacketSize is not null)
                mqttClientOptionsBuilder.WithMaximumPacketSize(MaximumPacketSize.Value);

            if (NoKeepAlive)
                mqttClientOptionsBuilder.WithNoKeepAlive();

            if (MaximumPacketSize is not null)
                mqttClientOptionsBuilder.WithMaximumPacketSize(MaximumPacketSize.Value);

            if (WithoutPacketFragmentation)
                mqttClientOptionsBuilder.WithoutPacketFragmentation();

            if (ProtocolType is not null)
                mqttClientOptionsBuilder.WithProtocolType(ProtocolType.Value);

            if (ProtocolVersion is not null)
                mqttClientOptionsBuilder.WithProtocolVersion(ProtocolVersion.Value);

            if (ReceiveMaximum is not null)
                mqttClientOptionsBuilder.WithReceiveMaximum(ReceiveMaximum.Value);

            if (RequestProblemInformation is not null)
                mqttClientOptionsBuilder.WithRequestProblemInformation(RequestProblemInformation.Value);

            if (RequestResponseInformation is not null)
                mqttClientOptionsBuilder.WithRequestResponseInformation(RequestResponseInformation.Value);

            if (SessionExpiryInterval is not null)
                mqttClientOptionsBuilder.WithSessionExpiryInterval(SessionExpiryInterval.Value);

            if (Timeout is not null)
                mqttClientOptionsBuilder.WithTimeout(Timeout.Value);

            if (TlsOptions is not null)
                mqttClientOptionsBuilder.WithTlsOptions(TlsOptions);

            if (TopicAliasMaximum is not null)
                mqttClientOptionsBuilder.WithTopicAliasMaximum(TopicAliasMaximum.Value);

            if (TryPrivate is not null)
                mqttClientOptionsBuilder.WithTryPrivate(TryPrivate.Value);

            if (!string.IsNullOrWhiteSpace(ContentType))
                mqttClientOptionsBuilder.WithWillContentType(ContentType);

            if (!string.IsNullOrWhiteSpace(CorrelationData))
            {
                var data = Encoding.UTF8.GetBytes(CorrelationData);
                mqttClientOptionsBuilder.WithWillCorrelationData(data);
            }

            if (DelayInterval is not null)
                mqttClientOptionsBuilder.WithWillDelayInterval(DelayInterval.Value);

            if (MessageExpiryInterval is not null)
                mqttClientOptionsBuilder.WithWillMessageExpiryInterval(MessageExpiryInterval.Value);

            if (!string.IsNullOrWhiteSpace(Payload))
            {
                var data = Encoding.UTF8.GetBytes(Payload);
                mqttClientOptionsBuilder.WithWillPayload(data);
            }

            if (PayloadFormatIndicator is not null)
                mqttClientOptionsBuilder.WithWillPayloadFormatIndicator(PayloadFormatIndicator.Value);

            if (QualityOfServiceLevel is not null)
                mqttClientOptionsBuilder.WithWillQualityOfServiceLevel(QualityOfServiceLevel.Value);

            if (ResponseTopic is not null)
                mqttClientOptionsBuilder.WithWillResponseTopic(ResponseTopic);

            if (Retain is not null)
                mqttClientOptionsBuilder.WithWillRetain(Retain.Value);

            if (UserProperties?.Count > 0)
            {
                foreach (var userProperty in UserProperties)
                {
                    mqttClientOptionsBuilder.WithUserProperty(userProperty.Key, userProperty.Value);
                }
            }

            if (WillUserProperties?.Count > 0)
            {
                foreach (var userProperty in WillUserProperties)
                {
                    mqttClientOptionsBuilder.WithWillUserProperty(userProperty.Key, userProperty.Value);
                }
            }

            return mqttClientOptionsBuilder.Build();
        }
    }
}