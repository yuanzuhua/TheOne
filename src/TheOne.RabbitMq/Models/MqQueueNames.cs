using System;

namespace TheOne.RabbitMq.Models {

    /// <summary>
    ///     Util static generic class to create unique queue names for types
    /// </summary>
    public static class MqQueueNames<T> {

        /// <summary>
        ///     1:1 mapping
        /// </summary>
        public static string Direct => MqQueueNames.ResolveQueueNameFn(typeof(T).Name, MqQueueNames.PostfixDirect);

        /// <summary>
        ///     <see cref="MqQueueNames.Topic" />
        /// </summary>
        public static string Topic => MqQueueNames.ResolveQueueNameFn(typeof(T).Name, MqQueueNames.PostfixTopic);

        public static string Dlq => MqQueueNames.ResolveQueueNameFn(typeof(T).Name, MqQueueNames.PostfixDlq);

        public static string[] AllQueueNames => new[] { Direct, Topic, Dlq };

    }

    /// <summary>
    ///     Util class to create unique queue names for runtime types
    /// </summary>
    public class MqQueueNames {

        public const string PostfixDirect = ".direct";
        public const string PostfixDlq = ".dlq";
        public const string PostfixTopic = ".topic";
        public const string PostfixTmp = "tmp.";

        public static string Exchange = "theone:mx";
        public static string ExchangeDlq = Exchange + PostfixDlq;
        public static string ExchangeTopic = Exchange + PostfixTopic;

        public static string MqPrefix = "theone:mq.";
        public static string QueuePrefix = "";

        public static string TempMqPrefix = MqPrefix + PostfixTmp;

        public static Func<string, string, string> ResolveQueueNameFn = ResolveQueueName;

        private readonly Type _messageType;

        public MqQueueNames(Type messageType) {
            this._messageType = messageType;
        }

        /// <summary>
        ///     1:1 mapping
        /// </summary>
        public string Direct => ResolveQueueNameFn(this._messageType.Name, PostfixDirect);

        /// <summary>
        ///     non-durable topic, designed to be transient,
        ///     and only used for notification purposes,
        ///     it's not meant to be relied on as a durable queue for persisting all Request DTO's processed.
        /// </summary>
        public string Topic => ResolveQueueNameFn(this._messageType.Name, PostfixTopic);

        public string Dlq => ResolveQueueNameFn(this._messageType.Name, PostfixDlq);

        public static string ResolveQueueName(string typeName, string queueSuffix) {
            return QueuePrefix + MqPrefix + typeName + queueSuffix;
        }

        public static bool IsTempQueue(string queueName) {
            return queueName != null
                   && queueName.StartsWith(TempMqPrefix, StringComparison.OrdinalIgnoreCase);
        }

        public static void SetQueuePrefix(string prefix) {
            QueuePrefix = prefix;
            TempMqPrefix = prefix + MqPrefix + PostfixTmp;
        }

        public static string GetTempQueueName() {
            return TempMqPrefix + Guid.NewGuid().ToString("n");
        }

    }

}
