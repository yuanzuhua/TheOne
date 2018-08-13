using System;
using System.Collections.Generic;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using TheOne.RabbitMq.Extensions;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq {

    public static class RabbitMqExtensions {

        public const string XDeadLetterExchange = "x-dead-letter-exchange";
        public const string XDeadLetterRoutingKey = "x-dead-letter-routing-key";
        public const string XMaxPriority = "x-max-priority";

        /// <summary>
        ///     queueName and args.
        ///     WARNING: if a queue already exists, you cannot change it, this action shoud be deterministic.
        /// </summary>
        public static Action<string, Dictionary<string, object>> CreateQueueFilter { get; set; }

        public static IModel OpenChannel(this IConnection connection) {
            IModel channel = connection.CreateModel();
            channel.RegisterDirectExchange();
            channel.RegisterDlqExchange();
            channel.RegisterTopicExchange();
            return channel;
        }

        public static void RegisterDirectExchange(this IModel channel, string exchangeName = null) {
            channel.ExchangeDeclare(exchangeName ?? MqQueueNames.Exchange, "direct", true, false, null);
        }

        public static void RegisterDlqExchange(this IModel channel, string exchangeName = null) {
            channel.ExchangeDeclare(exchangeName ?? MqQueueNames.ExchangeDlq, "direct", true, false, null);
        }

        public static void RegisterTopicExchange(this IModel channel, string exchangeName = null) {
            channel.ExchangeDeclare(exchangeName ?? MqQueueNames.ExchangeTopic, "topic", false, false, null);
        }

        public static void RegisterFanoutExchange(this IModel channel, string exchangeName) {
            channel.ExchangeDeclare(exchangeName, "fanout", false, false, null);
        }

        public static void RegisterQueues<T>(this IModel channel) {
            channel.RegisterQueue(MqQueueNames<T>.Direct);
            // channel.RegisterTopic(MqQueueNames<T>.Topic);
            channel.RegisterDlq(MqQueueNames<T>.Dlq);
        }

        public static void RegisterQueues(this IModel channel, MqQueueNames queueNames) {
            channel.RegisterQueue(queueNames.Direct);
            // channel.RegisterTopic(queueNames.Topic);
            channel.RegisterDlq(queueNames.Dlq);
        }

        public static void RegisterQueue(this IModel channel, string queueName) {
            var args = new Dictionary<string, object> {
                { XDeadLetterExchange, MqQueueNames.ExchangeDlq },
                { XDeadLetterRoutingKey, queueName.Replace(MqQueueNames.PostfixDirect, MqQueueNames.PostfixDlq) },
                { XMaxPriority, 10 }
            };

            CreateQueueFilter?.Invoke(queueName, args);

            // Already declared in TheOne.RabbitMq.RabbitMqQueueClient.GetTempQueueName
            if (!MqQueueNames.IsTempQueue(queueName)) {
                channel.QueueDeclare(queueName, true, false, false, args);
            }

            channel.QueueBind(queueName, MqQueueNames.Exchange, queueName);
        }

        public static void RegisterDlq(this IModel channel, string queueName) {
            var args = new Dictionary<string, object>();

            CreateQueueFilter?.Invoke(queueName, args);

            channel.QueueDeclare(queueName, true, false, false, args);
            channel.QueueBind(queueName, MqQueueNames.ExchangeDlq, queueName);
        }

        public static void RegisterTopic(this IModel channel, string queueName) {
            var args = new Dictionary<string, object>();

            CreateQueueFilter?.Invoke(queueName, args);

            channel.QueueDeclare(queueName, false, false, false, args);
            channel.QueueBind(queueName, MqQueueNames.ExchangeTopic, queueName);
        }

        public static void DeleteQueue<T>(this IModel model) {
            model.DeleteQueues(MqQueueNames<T>.AllQueueNames);
        }

        public static void DeleteQueues(this IModel channel, params string[] queues) {
            foreach (var queue in queues) {
                try {
                    channel.QueueDelete(queue, false, false);
                } catch (OperationInterruptedException ex) {
                    if (!ex.Is404()) {
                        throw;
                    }
                }
            }
        }

        public static void PurgeQueue<T>(this IModel model) {
            model.PurgeQueues(MqQueueNames<T>.AllQueueNames);
        }

        public static void PurgeQueues(this IModel model, params string[] queues) {
            foreach (var queue in queues) {
                try {
                    model.QueuePurge(queue);
                } catch (OperationInterruptedException ex) {
                    if (!ex.Is404()) {
                        throw;
                    }
                }
            }
        }

        /// <summary>
        ///     dlq or topic or direct
        /// </summary>
        public static void RegisterExchangeByName(this IModel channel, string exchange) {
            if (exchange.EndsWith(MqQueueNames.PostfixDlq)) {
                channel.RegisterDlqExchange(exchange);
            } else if (exchange.EndsWith(MqQueueNames.PostfixTopic)) {
                channel.RegisterTopicExchange(exchange);
            } else {
                channel.RegisterDirectExchange(exchange);
            }
        }

        /// <summary>
        ///     dlq or topic or direct
        /// </summary>
        public static void RegisterQueueByName(this IModel channel, string queueName) {
            if (queueName.EndsWith(MqQueueNames.PostfixDlq)) {
                channel.RegisterDlq(queueName);
            } else if (queueName.EndsWith(MqQueueNames.PostfixTopic)) {
                channel.RegisterTopic(queueName);
            } else {
                channel.RegisterQueue(queueName);
            }
        }

        internal static bool Is404(this OperationInterruptedException ex) {
            return ex.Message.Contains("code=404");
        }

        /// <summary>
        ///     starts with amq. or see <see cref="MqQueueNames.TempMqPrefix" />
        /// </summary>
        public static bool IsServerNamedQueue(string queueName) {
            if (string.IsNullOrEmpty(queueName)) {
                throw new ArgumentNullException(nameof(queueName));
            }

            var lowerCaseQueue = queueName.ToLower();
            return lowerCaseQueue.StartsWith("amq.")
                   || lowerCaseQueue.StartsWith(MqQueueNames.TempMqPrefix);
        }

        public static void PopulateFromMessage(this IBasicProperties props, IMqMessage message) {
            props.MessageId = message.Id.ToString();
            props.Timestamp = new AmqpTimestamp(message.CreatedDate.ToUnixTime());
            props.Priority = (byte)message.Priority;
            props.ContentType = Const.Json;

            if (message.Body != null) {
                props.Type = message.Body.GetType().Name;
            }

            if (message.ReplyTo != null) {
                props.ReplyTo = message.ReplyTo;
            }

            if (message.ReplyId != null) {
                props.CorrelationId = message.ReplyId.Value.ToString();
            }

            if (message.Error != null) {
                if (props.Headers == null) {
                    props.Headers = new Dictionary<string, object>();
                }

                props.Headers[nameof(IMqMessage.Error)] = message.Error.ToJson();
            }

            if (message.Meta != null) {
                if (props.Headers == null) {
                    props.Headers = new Dictionary<string, object>();
                }

                foreach (KeyValuePair<string, string> entry in message.Meta) {
                    props.Headers[entry.Key] = entry.Value;
                }
            }
        }

        public static IMqMessage<T> ToMessage<T>(this BasicGetResult msgResult) {
            if (msgResult == null) {
                return null;
            }

            IBasicProperties props = msgResult.BasicProperties;
            T body;

            // json only
            if (string.IsNullOrEmpty(props.ContentType) || props.ContentType.ToLower().Contains(Const.Json)) {
                body = MqMessageExtensions.FromJsonBytes<T>(msgResult.Body);
            } else {
                throw new NotSupportedException("Unknown Content-Type: " + props.ContentType);
            }

            var message = new MqMessage<T>(body) {
                Id = props.MessageId != null ? Guid.Parse(props.MessageId) : new Guid(),
                CreatedDate = ((int)props.Timestamp.UnixTime).FromUnixTime(),
                Priority = props.Priority,
                ReplyTo = props.ReplyTo,
                Tag = msgResult.DeliveryTag.ToString(),
                RetryAttempts = msgResult.Redelivered ? 1 : 0
            };

            if (props.CorrelationId != null) {
                message.ReplyId = Guid.Parse(props.CorrelationId);
            }

            if (props.Headers != null) {
                foreach (KeyValuePair<string, object> entry in props.Headers) {
                    if (entry.Key == nameof(IMqMessage.Error)) {
                        object errors = entry.Value;
                        if (errors != null) {
                            var errorsJson = errors is byte[] errorBytes
                                ? errorBytes.FromUtf8Bytes()
                                : errors.ToString();
                            message.Error = errorsJson.FromJson<MqErrorStatus>();
                        }
                    } else {
                        if (message.Meta == null) {
                            message.Meta = new Dictionary<string, string>();
                        }

                        var value = entry.Value is byte[] bytes
                            ? bytes.FromUtf8Bytes()
                            : entry.Value?.ToString();

                        message.Meta[entry.Key] = value;
                    }
                }
            }

            return message;
        }

    }

}
