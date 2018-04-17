using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq.Messaging {

    public static class MqMessageExtensions {

        private static Dictionary<Type, ToMessageDelegate> _toMessageFnCache = new Dictionary<Type, ToMessageDelegate>();

        public static T FromJsonBytes<T>(byte[] bytes) {
            if (bytes == null) {
                return default(T);
            }

            var s = Encoding.UTF8.GetString(bytes);

            if (string.IsNullOrEmpty(s)) {
                return default(T);
            }

            return JsonConvert.DeserializeObject<T>(s);
        }

        public static byte[] ToJsonBytes(object o) {
            if (o == null) {
                return null;
            }

            var s = JsonConvert.SerializeObject(o);

            if (string.IsNullOrEmpty(s)) {
                return null;
            }

            return Encoding.UTF8.GetBytes(s);
        }

        internal static ToMessageDelegate GetToMessageFn(Type type) {
            _toMessageFnCache.TryGetValue(type, out ToMessageDelegate toMessageFn);

            if (toMessageFn != null) {
                return toMessageFn;
            }

            Type genericType = typeof(MqMessageExtensions<>).MakeGenericType(type);
            MethodInfo mi = genericType.GetMethod(
                                nameof(MqMessageExtensions<object>.ConvertToMessage),
                                BindingFlags.Static | BindingFlags.Public
                            ) ?? throw new ApplicationException();
            toMessageFn = (ToMessageDelegate)Delegate.CreateDelegate(typeof(ToMessageDelegate), mi, true);

            Dictionary<Type, ToMessageDelegate> snapshot, newCache;
            do {
                snapshot = _toMessageFnCache;
                newCache = new Dictionary<Type, ToMessageDelegate>(_toMessageFnCache) {
                    [type] = toMessageFn
                };
            } while (!ReferenceEquals(
                Interlocked.CompareExchange(ref _toMessageFnCache, newCache, snapshot),
                snapshot));

            return toMessageFn;
        }

        internal static IMqMessage ToMessage(this byte[] bytes, Type ofType) {
            if (bytes == null) {
                return null;
            }

            ToMessageDelegate msgFn = GetToMessageFn(ofType);
            IMqMessage msg = msgFn(bytes);
            return msg;
        }

        public static IMqMessage<T> ToMessage<T>(byte[] bytes) {
            return FromJsonBytes<MqMessage<T>>(bytes);
        }

        public static byte[] ToJsonBytes(this IMqMessage message) {
            return ToJsonBytes((object)message);
        }

        public static byte[] ToJsonBytes<T>(this IMqMessage<T> message) {
            return ToJsonBytes((object)message);
        }

        public static string ToInQueueName(this IMqMessage message) {
            return new MqQueueNames(message.Body.GetType()).Direct;
        }

        public static string ToDlqQueueName(this IMqMessage message) {
            return new MqQueueNames(message.Body.GetType()).Dlq;
        }

        public static IMqMessageQueueClient CreateMessageQueueClient(this IMqMessageService mqServer) {
            return mqServer.MessageFactory.CreateMessageQueueClient();
        }

        public static IMqMessageProducer CreateMessageProducer(this IMqMessageService mqServer) {
            return mqServer.MessageFactory.CreateMessageProducer();
        }
    }

    internal delegate IMqMessage ToMessageDelegate(object param);

    internal static class MqMessageExtensions<T> {
        public static IMqMessage ConvertToMessage(object oBytes) {
            var bytes = (byte[])oBytes;
            return MqMessageExtensions.ToMessage<T>(bytes);
        }
    }
}
