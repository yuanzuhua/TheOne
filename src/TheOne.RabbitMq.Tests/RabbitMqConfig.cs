using System;
using RabbitMQ.Client;

namespace TheOne.RabbitMq.Tests {

    internal static class RabbitMqConfig {

        public static readonly string RabbitMqHostName = "localhost";

        public static readonly ConnectionFactory MqFactory = new ConnectionFactory { HostName = RabbitMqHostName };

        public static void UsingChannel(Action<IModel> action) {
            using (var connection = MqFactory.CreateConnection()) {
                using (var channel = connection.CreateModel()) {
                    action(channel);
                }
            }
        }

        public static void UsingChannel(ConnectionFactory mqFactory, Action<IModel> action) {
            using (var connection = mqFactory.CreateConnection()) {
                using (var channel = connection.CreateModel()) {
                    action(channel);
                }
            }
        }

    }

}
