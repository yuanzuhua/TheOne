using System;
using TheOne.Redis.ClientManager;

namespace TheOne.Redis.PubSub {

    /// <summary>
    ///     Processes messages in a managed background thread that automatically reconnects when the redis-server connection fails,
    ///     and works like an independent background Service that can be stopped and started on command.
    /// </summary>
    public interface IRedisPubSubServer : IDisposable {

        /// <summary>
        ///     Run once on initial StartUp
        /// </summary>
        Action OnInit { get; set; }

        /// <summary>
        ///     Called each time a new Connection is Started
        /// </summary>
        Action OnStart { get; set; }

        /// <summary>
        ///     Invoked when Connection is broken or Stopped
        /// </summary>
        Action OnStop { get; set; }

        /// <summary>
        ///     Invoked after Dispose()
        /// </summary>
        Action OnDispose { get; set; }

        /// <summary>
        ///     Fired when each message is received, handle with (channel, msg) => ...
        /// </summary>
        Action<string, string> OnMessage { get; set; }

        /// <summary>
        ///     Fired after successfully subscribing to the specified channels
        /// </summary>
        Action<string> OnUnSubscribe { get; set; }

        /// <summary>
        ///     Called when an exception occurs
        /// </summary>
        Action<Exception> OnError { get; set; }

        /// <summary>
        ///     Called before attempting to Failover to a new redis master
        /// </summary>
        Action<IRedisPubSubServer> OnFailover { get; set; }

        IRedisClientManager ClientManager { get; }
        string[] Channels { get; }

        TimeSpan? WaitBeforeNextRestart { get; set; }

        /// <summary>
        ///     The Current Time for RedisServer
        /// </summary>
        DateTime CurrentServerTime { get; }

        /// <summary>
        ///     Current Status: Starting, Started, Stopping, Stopped, Disposed
        /// </summary>
        string GetStatus();

        /// <summary>
        ///     Different life-cycle stats
        /// </summary>
        string GetStatsDescription();

        /// <summary>
        ///     Subscribe to specified Channels and listening for new messages
        /// </summary>
        IRedisPubSubServer Start();

        /// <summary>
        ///     Close active Connection and stop running background thread
        /// </summary>
        void Stop();

        /// <summary>
        ///     Stop than Start
        /// </summary>
        void Restart();

    }

}
