using System;
using System.Collections.Generic;

namespace TheOne.RabbitMq.Interfaces {

    /// <summary>
    ///     Simple definition of an MQ Host
    /// </summary>
    public interface IMqMessageService : IDisposable {

        /// <summary>
        ///     Factory to create consumers and producers that work with this service
        /// </summary>
        IMqMessageFactory MessageFactory { get; }

        /// <summary>
        ///     Get a list of all message types registered on this MQ Host
        /// </summary>
        List<Type> RegisteredTypes { get; }

        /// <summary>
        ///     Register DTOs and hanlders the MQ Server will process
        /// </summary>
        void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn);

        /// <summary>
        ///     Register DTOs and hanlders the MQ Server will process using specified number of threads
        /// </summary>
        void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn, int noOfThreads);

        /// <summary>
        ///     Register DTOs and hanlders the MQ Server will process
        /// </summary>
        void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn,
            Action<IMqMessageHandler, IMqMessage<T>, Exception> processExceptionEx);

        /// <summary>
        ///     Register DTOs and hanlders the MQ Server will process using specified number of threads
        /// </summary>
        void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn,
            Action<IMqMessageHandler, IMqMessage<T>, Exception> processExceptionEx, int noOfThreads);

        /// <summary>
        ///     Get Total Current Stats for all Message Handlers
        /// </summary>
        IMqMessageHandlerStats GetStats();

        /// <summary>
        ///     Get the status of the service. Potential Statuses: Disposed, Stopped, Stopping, Starting, Started
        /// </summary>
        string GetStatus();

        /// <summary>
        ///     Get a Stats dump
        /// </summary>
        string GetStatsDescription();

        /// <summary>
        ///     Start the MQ Host if not already started.
        /// </summary>
        void Start();

        /// <summary>
        ///     Stop the MQ Host if not already stopped.
        /// </summary>
        void Stop();
    }
}
