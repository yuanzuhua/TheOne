using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using TheOne.Logging;
using TheOne.RabbitMq.Extensions;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Messaging;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq {

    /// <inheritdoc />
    public class RabbitMqServer : IMqMessageService {

        /// <summary>
        ///     Will be a total of 2 attempts
        /// </summary>
        public const int DefaultRetryCount = 1;

        private static readonly ILog _logger = LogProvider.GetCurrentClassLogger();
        private readonly Dictionary<Type, IMqMessageHandlerFactory> _handlerMap = new Dictionary<Type, IMqMessageHandlerFactory>();
        private readonly Dictionary<Type, int> _handlerThreadCountMap = new Dictionary<Type, int>();
        private readonly object _msgLock = new object();

        /// <summary>
        ///     Subscription controller thread
        /// </summary>
        private Thread _bgThread;

        private long _bgThreadCount;
        private IConnection _connection;
        private long _doOperation = MqWorkerOperation.NoOp;
        private string _lastExMsg;

        /// <summary>
        ///     The Message Factory used by this MQ Server
        /// </summary>
        private RabbitMqMessageFactory _messageFactory;

        private int _noOfContinuousErrors;
        private long _noOfErrors;
        private Dictionary<string, int[]> _queueWorkerIndexMap;
        private int _status;

        /// <summary>
        ///     Stats
        /// </summary>
        private long _timesStarted;

        private RabbitMqWorker[] _workers;

        public RabbitMqServer(string hostName = "localhost") : this(new RabbitMqConnectionModel { HostName = hostName }) {
            //
        }

        public RabbitMqServer(RabbitMqConnectionModel model) : this(new RabbitMqMessageFactory(model)) {
            //
        }

        public RabbitMqServer(RabbitMqMessageFactory messageFactory) {
            this.Init(messageFactory);
        }

        /// <summary>
        ///     The RabbitMQ.Client Connection factory to introspect connection properties and create a low-level connection
        /// </summary>
        public ConnectionFactory ConnectionFactory => this._messageFactory.ConnectionFactory;

        /// <summary>
        ///     Whether Rabbit MQ should auto-retry connecting when a connection to Rabbit MQ Server instance is dropped
        /// </summary>
        public bool AutoReconnect { get; set; }

        /// <summary>
        ///     How many times a message should be retried before sending to the DLQ (Max of 1).
        /// </summary>
        public int RetryCount {
            get => this._messageFactory.RetryCount;
            set => this._messageFactory.RetryCount = value;
        }

        /// <summary>
        ///     Whether to use polling for consuming messages instead of a long-term subscription
        /// </summary>
        public bool UsePolling {
            get => this._messageFactory.UsePolling;
            set => this._messageFactory.UsePolling = value;
        }

        /// <summary>
        ///     Wait before Starting the MQ Server after a restart
        /// </summary>
        public int? KeepAliveRetryAfterMs { get; set; }

        public Action<string, IBasicProperties, IMqMessage> PublishMessageFilter {
            get => this._messageFactory.PublishMessageFilter;
            set => this._messageFactory.PublishMessageFilter = value;
        }

        public Action<string, BasicGetResult> GetMessageFilter {
            get => this._messageFactory.GetMessageFilter;
            set => this._messageFactory.GetMessageFilter = value;
        }

        /// <summary>
        ///     Execute global transformation or custom logic before a request is processed.
        ///     Must be thread-safe.
        /// </summary>
        public Func<IMqMessage, IMqMessage> RequestFilter { get; set; }

        /// <summary>
        ///     Execute global transformation or custom logic on the response.
        ///     Must be thread-safe.
        /// </summary>
        public Func<object, object> ResponseFilter { get; set; }

        /// <summary>
        ///     Execute global error handler logic. Must be thread-safe.
        /// </summary>
        public Action<Exception> ErrorHandler { get; set; }

        private IConnection Connection => this._connection ?? (this._connection = this.ConnectionFactory.CreateConnection());
        public long BgThreadCount => Interlocked.CompareExchange(ref this._bgThreadCount, 0, 0);

        /// <inheritdoc />
        public IMqMessageFactory MessageFactory => this._messageFactory;

        /// <inheritdoc />
        public List<Type> RegisteredTypes => this._handlerMap.Keys.ToList();

        /// <inheritdoc />
        public virtual void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn) {
            this.RegisterHandler(processMessageFn, null, 1);
        }

        /// <inheritdoc />
        public virtual void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn, int noOfThreads) {
            this.RegisterHandler(processMessageFn, null, noOfThreads);
        }

        /// <inheritdoc />
        public virtual void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn,
            Action<IMqMessageHandler, IMqMessage<T>, Exception> processExceptionEx) {
            this.RegisterHandler(processMessageFn, processExceptionEx, 1);
        }

        /// <inheritdoc />
        public virtual void RegisterHandler<T>(Func<IMqMessage<T>, object> processMessageFn,
            Action<IMqMessageHandler, IMqMessage<T>, Exception> processExceptionEx, int noOfThreads) {
            if (this._handlerMap.ContainsKey(typeof(T))) {
                throw new ArgumentException("Message handler has already been registered for type: " + typeof(T).Name);
            }

            this._handlerMap[typeof(T)] = this.CreateMessageHandlerFactory(processMessageFn, processExceptionEx);
            this._handlerThreadCountMap[typeof(T)] = noOfThreads;
        }

        /// <inheritdoc />
        public virtual IMqMessageHandlerStats GetStats() {
            lock (this._workers) {
                var total = new MqMessageHandlerStats("All Handlers");
                this._workers.ToList().ForEach(x => total.Add(x.GetStats()));
                return total;
            }
        }

        /// <inheritdoc />
        public virtual string GetStatus() {
            return MqWorkerStatus.ToString(Interlocked.CompareExchange(ref this._status, 0, 0));
        }

        /// <inheritdoc />
        public virtual string GetStatsDescription() {
            lock (this._workers) {
                var sb = new StringBuilder();
                sb.Append("#MQ SERVER STATS:\n");
                sb.AppendLine("===============");
                sb.AppendLine("Current Status: " + this.GetStatus());
                sb.AppendLine("Listening On: " + string.Join(", ", this._workers.ToList().ConvertAll(x => x.QueueName).ToArray()));
                sb.AppendLine("Times Started: " + Interlocked.CompareExchange(ref this._timesStarted, 0, 0));
                sb.AppendLine("Num of Errors: " + Interlocked.CompareExchange(ref this._noOfErrors, 0, 0));
                sb.AppendLine("Num of Continuous Errors: " + Interlocked.CompareExchange(ref this._noOfContinuousErrors, 0, 0));
                sb.AppendLine("Last ErrorMsg: " + this._lastExMsg);
                sb.AppendLine("===============");
                foreach (var worker in this._workers) {
                    sb.AppendLine(worker.GetStats().ToString());
                    sb.AppendLine("---------------\n");
                }

                return sb.ToString();
            }
        }

        /// <inheritdoc />
        public virtual void Start() {
            if (Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Started) {
                // Start any stopped worker threads
                this.StartWorkerThreads();
                return;
            }

            if (Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Disposed) {
                throw new ObjectDisposedException("MQ Host has been disposed");
            }

            // Only 1 thread allowed past
            if (Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Starting, MqWorkerStatus.Stopped) == MqWorkerStatus.Stopped) {
                // Should only be 1 thread past this point
                try {
                    this.Init();

                    if (this._workers == null || this._workers.Length == 0) {
                        _logger.Warn("Cannot start a MQ Server with no Message Handlers registered, ignoring.");
                        Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopped, MqWorkerStatus.Starting);
                        return;
                    }

                    this.StartWorkerThreads();

                    // Don't kill us if we're the thread that's retrying to Start() after a failure.
                    if (this._bgThread != Thread.CurrentThread) {
                        this.KillBgThreadIfExists();

                        this._bgThread = new Thread(this.RunLoop) {
                            IsBackground = true,
                            Name = "Rabbit MQ Server " + Interlocked.Increment(ref this._bgThreadCount)
                        };
                        this._bgThread.Start();
                        _logger.Debug($"Started background thread: {this._bgThread.Name}.");
                    } else {
                        _logger.Debug($"Retrying RunLoop() on thread: {this._bgThread.Name}.");
                        this.RunLoop();
                    }
                } catch (Exception ex) {
                    if (this.ErrorHandler != null) {
                        this.ErrorHandler(ex);
                    } else {
                        throw;
                    }
                }
            }
        }

        /// <inheritdoc />
        public virtual void Stop() {
            if (Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Disposed) {
                throw new ObjectDisposedException("MQ Host has been disposed");
            }

            if (Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopping, MqWorkerStatus.Started) == MqWorkerStatus.Started) {
                lock (this._msgLock) {
                    Interlocked.CompareExchange(ref this._doOperation, MqWorkerOperation.Stop, this._doOperation);
                    Monitor.Pulse(this._msgLock);
                }
            }
        }

        /// <inheritdoc />
        public virtual void Dispose() {
            if (Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Disposed) {
                return;
            }

            this.Stop();

            if (Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Disposed, MqWorkerStatus.Stopped) != MqWorkerStatus.Stopped) {
                Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Disposed, MqWorkerStatus.Stopping);
            }

            try {
                this.DisposeWorkerThreads();
            } catch (Exception ex) {
                _logger.Error(ex, "Error DisposeWorkerThreads().");
            }

            try {
                // give it a small chance to die gracefully
                Thread.Sleep(100);
                this.KillBgThreadIfExists();
            } catch (Exception ex) {
                this.ErrorHandler?.Invoke(ex);
            }

            try {
                if (this._connection != null) {
                    this._connection.Dispose();
                    this._connection = null;
                }
            } catch (Exception ex) {
                this.ErrorHandler?.Invoke(ex);
            }

        }

        private void Init(RabbitMqMessageFactory messageFactory) {
            this._messageFactory = messageFactory;
            this.ErrorHandler = ex => _logger.Error(ex, "Exception in Rabbit MQ Server.");
            this.RetryCount = DefaultRetryCount;
            this.AutoReconnect = true;
        }

        protected IMqMessageHandlerFactory CreateMessageHandlerFactory<T>(Func<IMqMessage<T>, object> processMessageFn,
            Action<IMqMessageHandler, IMqMessage<T>, Exception> processExceptionEx) {
            return new MqMessageHandlerFactory<T>(this, processMessageFn, processExceptionEx) {
                RequestFilter = this.RequestFilter,
                ResponseFilter = this.ResponseFilter,
                RetryCount = this.RetryCount
            };
        }

        public virtual void Init() {
            if (this._workers != null) {
                return;
            }

            var workerBuilder = new List<RabbitMqWorker>();

            using (var channel = this.Connection.OpenChannel()) {
                foreach (var entry in this._handlerMap) {
                    var msgType = entry.Key;
                    var handlerFactory = entry.Value;

                    var queueNames = new MqQueueNames(msgType);
                    var noOfThreads = this._handlerThreadCountMap[msgType];

                    for (var i2 = 0; i2 < noOfThreads; i2++) {
                        workerBuilder.Add(new RabbitMqWorker(this._messageFactory,
                            handlerFactory.CreateMessageHandler(),
                            queueNames.Direct,
                            this.WorkerErrorHandler,
                            this.AutoReconnect));
                    }

                    channel.RegisterQueues(queueNames);
                }

                this._workers = workerBuilder.ToArray();

                this._queueWorkerIndexMap = new Dictionary<string, int[]>();
                for (var i = 0; i < this._workers.Length; i++) {
                    var worker = this._workers[i];

                    if (!this._queueWorkerIndexMap.TryGetValue(worker.QueueName, out var workerIds)) {
                        this._queueWorkerIndexMap[worker.QueueName] = new[] { i };
                    } else {
                        workerIds = new List<int>(workerIds) { i }.ToArray();
                        this._queueWorkerIndexMap[worker.QueueName] = workerIds;
                    }
                }
            }
        }

        private void RunLoop() {
            if (Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Started, MqWorkerStatus.Starting) != MqWorkerStatus.Starting) {
                return;
            }

            Interlocked.Increment(ref this._timesStarted);

            try {
                lock (this._msgLock) {
                    // RESET
                    while (Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Started) {
                        Monitor.Wait(this._msgLock);
                        _logger.Debug("msgLock received...");

                        var op = Interlocked.CompareExchange(ref this._doOperation, MqWorkerOperation.NoOp, this._doOperation);
                        switch (op) {
                            case MqWorkerOperation.Stop:
                                _logger.Debug("Stop command issued...");

                                Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopping, MqWorkerStatus.Started);
                                try {
                                    this.StopWorkerThreads();
                                } finally {
                                    Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopped, MqWorkerStatus.Stopping);
                                }

                                return; // exits

                            case MqWorkerOperation.Restart:
                                _logger.Debug("Restart command issued...");

                                Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopping, MqWorkerStatus.Started);
                                try {
                                    this.StopWorkerThreads();
                                } finally {
                                    Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopped, MqWorkerStatus.Stopping);
                                }

                                this.StartWorkerThreads();
                                Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Started, MqWorkerStatus.Stopped);
                                break; // continues
                        }
                    }
                }
            } catch (Exception ex) {
                this._lastExMsg = ex.Message;
                Interlocked.Increment(ref this._noOfErrors);
                Interlocked.Increment(ref this._noOfContinuousErrors);

                if (Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopped, MqWorkerStatus.Started) !=
                    MqWorkerStatus.Started) {
                    Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopped, MqWorkerStatus.Stopping);
                }

                this.StopWorkerThreads();

                this.ErrorHandler?.Invoke(ex);

                if (this.KeepAliveRetryAfterMs != null) {
                    Thread.Sleep(this.KeepAliveRetryAfterMs.Value);
                    this.Start();
                }
            }

            _logger.Debug("Exiting RunLoop()...");
        }

        public virtual void WaitForWorkersToStop(TimeSpan? timeout = null) {
            ExecUtils.RetryUntilTrue(
                () => Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Stopped,
                timeout);
        }

        public virtual void Restart() {
            if (Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Disposed) {
                throw new ObjectDisposedException("MQ Host has been disposed.");
            }

            if (Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopping, MqWorkerStatus.Started) == MqWorkerStatus.Started) {
                lock (this._msgLock) {
                    Interlocked.CompareExchange(ref this._doOperation, MqWorkerOperation.Restart, this._doOperation);
                    Monitor.Pulse(this._msgLock);
                }
            }
        }

        public virtual void StartWorkerThreads() {
            _logger.Info("Starting all Rabbit MQ Server worker threads...");

            foreach (var worker in this._workers) {
                try {
                    worker.Start();
                } catch (Exception ex) {
                    this.ErrorHandler?.Invoke(ex);
                    _logger.Error(ex, "Could not START Rabbit MQ worker thread...");
                }
            }
        }

        public virtual void StopWorkerThreads() {
            _logger.Info("Stopping all Rabbit MQ Server worker threads...");

            foreach (var worker in this._workers) {
                try {
                    worker.Stop();
                } catch (Exception ex) {
                    this.ErrorHandler?.Invoke(ex);
                    _logger.Warn(ex, "Could not STOP Rabbit MQ worker thread...");
                }
            }
        }

        private void DisposeWorkerThreads() {
            _logger.Info("Disposing all Rabbit MQ Server worker threads...");
            foreach (var value in this._workers) {
                value.Dispose();
            }
        }

        private void WorkerErrorHandler(RabbitMqWorker source, Exception ex) {
            _logger.Error(ex, $"Received exception in Worker: {source.QueueName}.");
            for (var i = 0; i < this._workers.Length; i++) {
                var worker = this._workers[i];
                if (worker == source) {
                    _logger.Info("Starting new {0} Worker at index {1}...", source.QueueName, i);
                    this._workers[i] = source.Clone();
                    this._workers[i].Start();
                    worker.Dispose();
                    return;
                }
            }
        }

        private void KillBgThreadIfExists() {
            if (this._bgThread != null && this._bgThread.IsAlive) {
                // give it a small chance to die gracefully
                if (!this._bgThread.Join(500)) {
                    // ideally we shouldn't get here, but lets try our hardest to clean it up
                    _logger.Warn($"Interrupting previous background thread: {this._bgThread.Name}...");
                    this._bgThread.Interrupt();
                    if (!this._bgThread.Join(TimeSpan.FromSeconds(3))) {
                        _logger.Warn($"{this._bgThread.Name} just won\'t die, so we\'re now aborting it...");
                        this._bgThread.Abort();
                    }
                }

                this._bgThread = null;
            }
        }

    }

}
