using System;

namespace TheOne.RabbitMq.Models {

    /// <summary>
    ///     For messaging exceptions that should bypass the messaging service's configured
    ///     retry attempts and store the message straight into the DLQ
    /// </summary>
    /// <inheritdoc />
    public class UnRetryableMqMessagingException : MqMessagingException {

        /// <inheritdoc />
        public UnRetryableMqMessagingException() { }

        /// <inheritdoc />
        public UnRetryableMqMessagingException(string message) : base(message) { }

        /// <inheritdoc />
        public UnRetryableMqMessagingException(string message, Exception innerException) : base(message, innerException) { }
    }
}
