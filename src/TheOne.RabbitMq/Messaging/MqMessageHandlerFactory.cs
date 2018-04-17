using System;
using TheOne.RabbitMq.Interfaces;

namespace TheOne.RabbitMq.Messaging {

    /// <inheritdoc />
    public class MqMessageHandlerFactory<T> : IMqMessageHandlerFactory {

        /// <summary>
        ///     3 attempts
        /// </summary>
        public const int DefaultRetryCount = 2;

        private readonly IMqMessageService _messageService;
        private readonly Action<IMqMessageHandler, IMqMessage<T>, Exception> _processExceptionFn;
        private readonly Func<IMqMessage<T>, object> _processMessageFn;

        /// <inheritdoc />
        public MqMessageHandlerFactory(IMqMessageService messageService, Func<IMqMessage<T>, object> processMessageFn)
            : this(messageService, processMessageFn, null) { }

        /// <inheritdoc />
        public MqMessageHandlerFactory(IMqMessageService messageService,
            Func<IMqMessage<T>, object> processMessageFn,
            Action<IMqMessageHandler, IMqMessage<T>, Exception> processExceptionEx) {
            this._messageService = messageService ?? throw new ArgumentNullException(nameof(messageService));
            this._processMessageFn = processMessageFn ?? throw new ArgumentNullException(nameof(processMessageFn));
            this._processExceptionFn = processExceptionEx;
            this.RetryCount = DefaultRetryCount;
        }

        public Func<IMqMessage, IMqMessage> RequestFilter { get; set; }
        public Func<object, object> ResponseFilter { get; set; }
        public int RetryCount { get; set; }

        /// <inheritdoc />
        public IMqMessageHandler CreateMessageHandler() {
            if (this.RequestFilter == null && this.ResponseFilter == null) {
                return new MqMessageHandler<T>(this._messageService, this._processMessageFn, this._processExceptionFn, this.RetryCount);
            }

            object ProcessMessageFn(IMqMessage<T> msg) {
                if (this.RequestFilter != null) {
                    msg = (IMqMessage<T>)this.RequestFilter(msg);
                }

                object result = this._processMessageFn(msg);

                if (this.ResponseFilter != null) {
                    result = this.ResponseFilter(result);
                }

                return result;
            }

            return new MqMessageHandler<T>(this._messageService, ProcessMessageFn, this._processExceptionFn, this.RetryCount);
        }
    }
}
