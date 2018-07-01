using System;
using NLog;
using TheOne.RabbitMq.Extensions;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq.Messaging {

    /// <summary>
    ///     Processes all messages in a Queue.
    ///     Expects to be called in 1 thread. i.e. Non Thread-Safe.
    /// </summary>
    public class MqMessageHandler<T> : IMqMessageHandler, IDisposable {

        /// <summary>
        ///     Will be a total of 3 attempts
        /// </summary>
        public const int DefaultRetryCount = 2;

        private readonly ILogger _logger = LogManager.GetCurrentClassLogger();
        private readonly IMqMessageService _messageService;
        private readonly Action<IMqMessageHandler, IMqMessage<T>, Exception> _processInExceptionFn;
        private readonly Func<IMqMessage<T>, object> _processMessageFn;
        private readonly int _retryCount;

        public MqMessageHandler(IMqMessageService messageService,
            Func<IMqMessage<T>, object> processMessageFn)
            : this(messageService, processMessageFn, null, DefaultRetryCount) { }

        public MqMessageHandler(IMqMessageService messageService,
            Func<IMqMessage<T>, object> processMessageFn,
            Action<IMqMessageHandler, IMqMessage<T>, Exception> processInExceptionFn,
            int retryCount) {
            this._messageService = messageService ?? throw new ArgumentNullException(nameof(messageService));
            this._processMessageFn = processMessageFn ?? throw new ArgumentNullException(nameof(processMessageFn));
            this._processInExceptionFn = processInExceptionFn ?? this.DefaultInExceptionHandler;
            this._retryCount = retryCount;
            this.ProcessQueueNames = new[] { MqQueueNames<T>.Direct };
        }

        public int TotalMessagesProcessed { get; private set; }
        public int TotalMessagesFailed { get; private set; }
        public int TotalRetries { get; private set; }
        public int TotalNormalMessagesReceived { get; private set; }
        public DateTime? LastMessageProcessed { get; private set; }
        public string[] ProcessQueueNames { get; set; }

        /// <inheritdoc />
        public void Dispose() {
            var shouldDispose = this._messageService as IMqMessageHandlerDisposer;
            shouldDispose?.DisposeMessageHandler(this);
        }

        /// <inheritdoc />
        public IMqMessageQueueClient MqClient { get; private set; }

        /// <inheritdoc />
        public Type MessageType => typeof(T);

        /// <inheritdoc />
        public void Process(IMqMessageQueueClient mqClient) {
            foreach (var processQueueName in this.ProcessQueueNames) {
                this.ProcessQueue(mqClient, processQueueName);
            }
        }

        /// <inheritdoc />
        public int ProcessQueue(IMqMessageQueueClient mqClient, string queueName, Func<bool> doNext = null) {
            var msgsProcessed = 0;
            try {
                IMqMessage<T> message;
                while ((message = mqClient.GetAsync<T>(queueName)) != null) {
                    this.ProcessMessage(mqClient, message);

                    msgsProcessed++;

                    if (doNext != null && !doNext()) {
                        return msgsProcessed;
                    }
                }
            } catch (Exception ex) {
                this._logger.Error(ex, "Error serializing message from mq server");
            }

            return msgsProcessed;
        }

        /// <inheritdoc />
        public void ProcessMessage(IMqMessageQueueClient mqClient, object mqResponse) {
            IMqMessage<T> message = mqClient.CreateMessage<T>(mqResponse);
            this.ProcessMessage(mqClient, message);
        }

        /// <inheritdoc />
        public IMqMessageHandlerStats GetStats() {
            return new MqMessageHandlerStats(
                typeof(T).ExpandTypeName(),
                this.TotalMessagesProcessed,
                this.TotalMessagesFailed,
                this.TotalRetries,
                this.TotalNormalMessagesReceived,
                this.LastMessageProcessed
            );
        }

        public void ProcessMessage(IMqMessageQueueClient mqClient, IMqMessage<T> message) {
            this.MqClient = mqClient;
            var msgHandled = false;

            try {
                object response = this._processMessageFn(message);

                if (response is Exception responseEx) {
                    this.TotalMessagesFailed++;

                    if (message.ReplyTo != null) {
                        object responseDto = response;
                        mqClient.Publish(message.ReplyTo, MqMessageFactory.Create(responseDto));
                        return;
                    }

                    msgHandled = true;
                    this._processInExceptionFn(this, message, responseEx);
                    return;
                }

                this.TotalMessagesProcessed++;

                // If there's no response, do nothing

                if (response != null) {
                    var responseMessage = response as IMqMessage;
                    Type responseType = responseMessage != null
                        ? responseMessage.Body?.GetType() ?? typeof(object)
                        : response.GetType();

                    // If there's no explicit ReplyTo, send it to the typed Response InQ by default
                    var mqReplyTo = message.ReplyTo ?? new MqQueueNames(responseType).Direct;

                    // Otherwise send to our trusty response Queue
                    if (responseMessage == null) {
                        responseMessage = MqMessageFactory.Create(response);
                    }

                    responseMessage.ReplyId = message.Id;
                    mqClient.Publish(mqReplyTo, responseMessage);
                }
            } catch (Exception ex) {
                try {
                    this.TotalMessagesFailed++;
                    msgHandled = true;
                    this._processInExceptionFn(this, message, ex);
                } catch (Exception exHandlerEx) {
                    this._logger.Error(exHandlerEx, "Message exception handler threw an error");
                }
            } finally {
                if (!msgHandled) {
                    mqClient.Ack(message);
                }

                this.TotalNormalMessagesReceived++;
                this.LastMessageProcessed = DateTime.Now;
            }
        }

        private void DefaultInExceptionHandler(IMqMessageHandler mqHandler, IMqMessage<T> message, Exception ex) {
            this._logger.Error(ex, "Message exception handler threw an error");

            var requeue = !(ex is UnRetryableMqMessagingException)
                          && message.RetryAttempts < this._retryCount;

            if (requeue) {
                message.RetryAttempts++;
                this.TotalRetries++;
            }

            message.Error = new MqErrorStatus(ex);
            mqHandler.MqClient.Nak(message, requeue, ex);
        }
    }
}
