using System;

namespace TheOne.RabbitMq.Interfaces {

    /// <summary>
    ///     Single threaded message handler that can process all messages
    ///     of a particular message type.
    /// </summary>
    public interface IMqMessageHandler {

        /// <summary>
        ///     The type of the message this handler processes
        /// </summary>
        Type MessageType { get; }

        /// <summary>
        ///     The MqClient processing the message
        /// </summary>
        IMqMessageQueueClient MqClient { get; }

        /// <summary>
        ///     Process all messages pending
        /// </summary>
        void Process(IMqMessageQueueClient mqClient);

        /// <summary>
        ///     Process messages from a single queue.
        /// </summary>
        /// <param name="mqClient" >mqClient</param>
        /// <param name="queueName" >The queue to process</param>
        /// <param name="doNext" >A predicate on whether to continue processing the next message if any</param>
        int ProcessQueue(IMqMessageQueueClient mqClient, string queueName, Func<bool> doNext = null);

        /// <summary>
        ///     Process a single message
        /// </summary>
        void ProcessMessage(IMqMessageQueueClient mqClient, object mqResponse);

        /// <summary>
        ///     Get Current Stats for this Message Handler
        /// </summary>
        IMqMessageHandlerStats GetStats();
    }

    /// <summary>
    ///     Encapsulates creating a new message handler
    /// </summary>
    public interface IMqMessageHandlerFactory {
        IMqMessageHandler CreateMessageHandler();
    }

    public interface IMqMessageHandlerDisposer {
        void DisposeMessageHandler(IMqMessageHandler messageHandler);
    }

    public interface IMqMessageHandlerStats {
        string Name { get; }
        int TotalMessagesProcessed { get; }
        int TotalMessagesFailed { get; }
        int TotalRetries { get; }
        int TotalNormalMessagesReceived { get; }
        DateTime? LastMessageProcessed { get; }
        void Add(IMqMessageHandlerStats stats);
    }
}
