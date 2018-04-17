using System;

namespace TheOne.RabbitMq.Models {

    public class MqMessagingException : Exception {

        public MqMessagingException() { }

        public MqMessagingException(string message) : base(message) { }

        public MqMessagingException(string message, Exception innerException) : base(message, innerException) { }

        public object ResponseDto { get; set; }
    }
}
