using System;
using System.Collections.Generic;
using System.Reflection;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq.Messaging {

    internal delegate IMqMessage MessageFactoryDelegate(object body);

    public static class MqMessageFactory {

        private static readonly Dictionary<Type, MessageFactoryDelegate> _cacheFn
            = new Dictionary<Type, MessageFactoryDelegate>();

        public static IMqMessage Create(object response) {
            if (response is IMqMessage responseMessage) {
                return responseMessage;
            }

            if (response == null) {
                return null;
            }

            var type = response.GetType();

            MessageFactoryDelegate factoryFn;
            lock (_cacheFn) {
                _cacheFn.TryGetValue(type, out factoryFn);
            }

            if (factoryFn != null) {
                return factoryFn(response);
            }

            var genericMessageType = typeof(MqMessage<>).MakeGenericType(type);
            var mi = genericMessageType.GetMethod(
                         nameof(MqMessage<object>.Create),
                         BindingFlags.Public | BindingFlags.Static
                     ) ?? throw new ApplicationException();
            factoryFn = (MessageFactoryDelegate)Delegate.CreateDelegate(typeof(MessageFactoryDelegate), mi);

            lock (_cacheFn) {
                _cacheFn[type] = factoryFn;
            }

            return factoryFn(response);
        }

    }

}
