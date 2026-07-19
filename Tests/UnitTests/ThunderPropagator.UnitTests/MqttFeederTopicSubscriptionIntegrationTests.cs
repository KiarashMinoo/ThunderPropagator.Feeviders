using System.Net;
using System.Net.Sockets;
using MQTTnet;
using MQTTnet.Server;
using ThunderPropagator.Feeders.Mqtt;

namespace ThunderPropagator.UnitTests
{
    public class MqttFeederTopicSubscriptionIntegrationTests
    {
        private const string ConfiguredTopic = "channels/updates";
        private const string OtherTopic = "channels/other";

        [Fact]
        public async Task Subscription_ShouldReceiveMessagesPublishedToConfiguredTopic()
        {
            var port = GetFreeTcpPort();
            var serverFactory = new MqttServerFactory();
            using var server = serverFactory.CreateMqttServer(serverFactory.CreateServerOptionsBuilder()
                .WithDefaultEndpoint()
                .WithDefaultEndpointPort(port)
                .Build());
            await server.StartAsync();

            try
            {
                var mqttFactory = new MqttClientFactory();
                using var subscriber = mqttFactory.CreateMqttClient();
                await subscriber.ConnectAsync(CreateClientOptions(port), CancellationToken.None);

                var received = new List<string>();
                var messageReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                subscriber.ApplicationMessageReceivedAsync += args =>
                {
                    received.Add(args.ApplicationMessage.Topic);
                    messageReceived.TrySetResult();
                    return Task.CompletedTask;
                };

                var configuration = new TestMqttFeederConfiguration { Topic = ConfiguredTopic, SubscriptionIdentifier = 1 };
                var subscribeOptions = MqttSubscriptionOptionsFactory.Create(mqttFactory, configuration);
                await subscriber.SubscribeAsync(subscribeOptions, CancellationToken.None);

                using var publisher = mqttFactory.CreateMqttClient();
                await publisher.ConnectAsync(CreateClientOptions(port), CancellationToken.None);
                await publisher.PublishAsync(new MqttApplicationMessageBuilder()
                    .WithTopic(ConfiguredTopic)
                    .WithPayload("hello")
                    .Build(), CancellationToken.None);

                await messageReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

                Assert.Equal([ConfiguredTopic], received);
            }
            finally
            {
                await server.StopAsync(new MqttServerStopOptions());
            }
        }

        [Fact]
        public async Task Subscription_ShouldNotReceiveMessagesPublishedToADifferentTopic()
        {
            var port = GetFreeTcpPort();
            var serverFactory = new MqttServerFactory();
            using var server = serverFactory.CreateMqttServer(serverFactory.CreateServerOptionsBuilder()
                .WithDefaultEndpoint()
                .WithDefaultEndpointPort(port)
                .Build());
            await server.StartAsync();

            try
            {
                var mqttFactory = new MqttClientFactory();
                using var subscriber = mqttFactory.CreateMqttClient();
                await subscriber.ConnectAsync(CreateClientOptions(port), CancellationToken.None);

                var received = new List<string>();
                var messageReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                subscriber.ApplicationMessageReceivedAsync += args =>
                {
                    received.Add(args.ApplicationMessage.Topic);
                    messageReceived.TrySetResult();
                    return Task.CompletedTask;
                };

                var configuration = new TestMqttFeederConfiguration { Topic = ConfiguredTopic, SubscriptionIdentifier = 1 };
                var subscribeOptions = MqttSubscriptionOptionsFactory.Create(mqttFactory, configuration);
                await subscriber.SubscribeAsync(subscribeOptions, CancellationToken.None);

                using var publisher = mqttFactory.CreateMqttClient();
                await publisher.ConnectAsync(CreateClientOptions(port), CancellationToken.None);

                // Published first: if the subscription filter were missing/wildcard, this would be the message
                // that unblocks messageReceived below, and the final assertion would catch the regression.
                await publisher.PublishAsync(new MqttApplicationMessageBuilder()
                    .WithTopic(OtherTopic)
                    .WithPayload("should not arrive")
                    .Build(), CancellationToken.None);

                // Published second: proves the subscriber connection is actually live and receiving messages.
                await publisher.PublishAsync(new MqttApplicationMessageBuilder()
                    .WithTopic(ConfiguredTopic)
                    .WithPayload("should arrive")
                    .Build(), CancellationToken.None);

                await messageReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

                Assert.Equal([ConfiguredTopic], received);
            }
            finally
            {
                await server.StopAsync(new MqttServerStopOptions());
            }
        }

        private static MqttClientOptions CreateClientOptions(int port)
            => new MqttClientOptionsBuilder().WithTcpServer("127.0.0.1", port).Build();

        private static int GetFreeTcpPort()
        {
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }

        private sealed class TestMqttFeederConfiguration : MqttFeederConfiguration;
    }
}
