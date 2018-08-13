using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TheOne.RabbitMq.Extensions;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;

namespace TheOne.RabbitMq.InMemoryMq {

    /// <inheritdoc cref="IMqMessageService" />
    internal abstract class MqTransientMessageServiceBase : IMqMessageService, IMqMessageHandlerDisposer {

        /// <summary>
        ///     Will be a total of 3 attempts
        /// </summary>
        public const int DefaultRetryCount = 2;

        private readonly Dictionary<Type, IMqMessageHandlerFactory> _handlerMap = new Dictionary<Type, IMqMessageHandlerFactory>();
        private bool _isRunning;
        private IMqMessageHandler[] _messageHandlers;

        protected MqTransientMessageServiceBase() : this(DefaultRetryCount, null) { }

        protected MqTransientMessageServiceBase(int retryAttempts, TimeSpan? requestTimeout) {
            this.RetryCount = retryAttempts;
            this.RequestTimeout = requestTimeout;
        }

        public int RetryCount { get; set; }
        public TimeSpan? RequestTimeout { get; protected set; }

        /// <inheritdoc />
        public virtual void DisposeMessageHandler(IMqMessageHandler messageHandler) {
            lock (this._handlerMap) {
                if (!this._isRunning) {
                    return;
                }

                var allHandlersAreDisposed = true;
                for (var i = 0; i < this._messageHandlers.Length; i++) {
                    if (this._messageHandlers[i] == messageHandler) {
                        this._messageHandlers[i] = null;
                    }

                    allHandlersAreDisposed = allHandlersAreDisposed && this._messageHandlers[i] == null;
                }

                if (allHandlersAreDisposed) {
                    this.Stop();
                }
            }
        }

        /// <inheritdoc />
        public List<Type> RegisteredTypes => this._handlerMap.Keys.ToList();

        /// <inheritdoc />
        public abstract IMqMessageFactory MessageFactory { get; }

        /// <inheritdoc />
        public virtual void Start() {
            this._isRunning = true;

            lock (this._handlerMap) {
                if (this._messageHandlers == null) {
                    this._messageHandlers = this._handlerMap.Values
                                                .ToList()
                                                .ConvertAll(x => x.CreateMessageHandler())
                                                .ToArray();
                }

                using (IMqMessageQueueClient mqClient = this.MessageFactory.CreateMessageQueueClient()) {
                    foreach (IMqMessageHandler handler in this._messageHandlers) {
                        handler.Process(mqClient);
                    }
                }
            }

            this.Stop();
        }

        /// <inheritdoc />
        public IMqMessageHandlerStats GetStats() {
            var total = new MqMessageHandlerStats("All Handlers");
            lock (this._messageHandlers) {
                foreach (IMqMessageHandler handler in this._messageHandlers) {
                    total.Add(handler.GetStats());
                }
            }

            return total;
        }

        /// <inheritdoc />
        public string GetStatus() {
            return this._isRunning ? "Started" : "Stopped";
        }

        /// <inheritdoc />
        public string GetStatsDescription() {
            var sb = new StringBuilder();
            sb.Append("#MQ HOST STATS:\n");
            sb.AppendLine("===============");
            lock (this._messageHandlers) {
                foreach (IMqMessageHandler messageHandler in this._messageHandlers) {
                    sb.AppendLine(messageHandler.GetStats().ToString());
                    sb.AppendLine("---------------");
                }
            }

            return sb.ToString();
        }

        /// <inheritdoc />
        public void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn) {
            this.RegisterHandler(processMessageFn, null, 1);
        }

        /// <inheritdoc />
        public void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn, int noOfThreads) {
            this.RegisterHandler(processMessageFn, null, 1);
        }

        /// <inheritdoc />
        public void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn,
            Action<IMqMessageHandler, IMqMessage<T>, Exception> processExceptionEx) {
            this.RegisterHandler(processMessageFn, processExceptionEx, 1);
        }

        public void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn,
            Action<IMqMessageHandler, IMqMessage<T>, Exception> processExceptionEx, int noOfThreads) {
            if (this._handlerMap.ContainsKey(typeof(T))) {
                throw new ArgumentException("Message handler has already been registered for type: " + typeof(T).ExpandTypeName());
            }

            this._handlerMap[typeof(T)] = this.CreateMessageHandlerFactory(processMessageFn, processExceptionEx);
        }

        /// <inheritdoc />
        public virtual void Stop() {
            this._isRunning = false;
            lock (this._handlerMap) {
                this._messageHandlers = null;
            }
        }

        /// <inheritdoc />
        public virtual void Dispose() {
            this.Stop();
        }

        protected IMqMessageHandlerFactory CreateMessageHandlerFactory<T>(
            Func<IMqMessage<T>, object> processMessageFn,
            Action<IMqMessageHandler, IMqMessage<T>, Exception> processExceptionEx) {
            return new MqMessageHandlerFactory<T>(this, processMessageFn, processExceptionEx) {
                RetryCount = this.RetryCount
            };
        }

    }

}
