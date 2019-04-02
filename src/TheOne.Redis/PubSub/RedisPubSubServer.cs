using System;
using System.Diagnostics;
using System.Threading;
using TheOne.Logging;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;
using TheOne.Redis.External;

namespace TheOne.Redis.PubSub {

    /// <inheritdoc />
    public partial class RedisPubSubServer : IRedisPubSubServer {

        public const string AllChannelsWildCard = "*";

        private const int _no = 0;
        private const int _yes = 1;
        private static readonly ILog _logger = LogProvider.GetLogger(typeof(RedisPubSubServer));
        private readonly Random _random = new Random(Environment.TickCount);
        private int _autoRestart = _yes;

        /// <summary>
        ///     Subscription controller thread
        /// </summary>
        private Thread _bgThread;

        private long _bgThreadCount;
        private int _doOperation = Operation.NoOp;
        private Timer _heartbeatTimer;
        private string _lastExMsg;
        private long _lastHeartbeatTicks;
        private IRedisClient _masterClient;
        private int _noOfContinuousErrors;
        private long _noOfErrors;
        private DateTime _serverTimeAtStart;
        private Stopwatch _startedAt;
        private int _status;
        private long _timesStarted;
        public TimeSpan? HeartbeatInterval = TimeSpan.FromSeconds(10);
        public TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(30);

        public RedisPubSubServer(IRedisClientManager clientsManager, params string[] channels) {
            this.ClientsManager = clientsManager;
            this.Channels = channels;

            var failoverHost = clientsManager as IRedisFailover;
            failoverHost?.OnFailover.Add(this.HandleFailover);
        }

        public Action OnHeartbeatSent { get; set; }
        public Action OnHeartbeatReceived { get; set; }
        public Action<string, byte[]> OnMessageBytes { get; set; }
        public Action<string> OnControlCommand { get; set; }
        public bool IsSentinelSubscription { get; set; }
        public string[] ChannelsMatching { get; set; }

        public bool AutoRestart {
            get => Interlocked.CompareExchange(ref this._autoRestart, 0, 0) == _yes;
            set => Interlocked.CompareExchange(ref this._autoRestart, value ? _yes : _no, this._autoRestart);
        }

        public long BgThreadCount => Interlocked.CompareExchange(ref this._bgThreadCount, 0, 0);

        /// <inheritdoc />
        public Action OnInit { get; set; }

        /// <inheritdoc />
        public Action OnStart { get; set; }

        /// <inheritdoc />
        public Action OnStop { get; set; }

        /// <inheritdoc />
        public Action OnDispose { get; set; }

        /// <inheritdoc />
        public Action<string, string> OnMessage { get; set; }

        /// <inheritdoc />
        public Action<string> OnUnSubscribe { get; set; }

        /// <inheritdoc />
        public Action<Exception> OnError { get; set; }

        /// <inheritdoc />
        public Action<IRedisPubSubServer> OnFailover { get; set; }

        /// <inheritdoc />
        public DateTime CurrentServerTime => new DateTime(this._serverTimeAtStart.Ticks + this._startedAt.ElapsedTicks, DateTimeKind.Utc);

        /// <inheritdoc />
        public IRedisClientManager ClientsManager { get; set; }

        /// <inheritdoc />
        public string[] Channels { get; set; }

        /// <inheritdoc />
        public TimeSpan? WaitBeforeNextRestart { get; set; }

        /// <inheritdoc />
        public IRedisPubSubServer Start() {
            this.AutoRestart = true;

            if (Interlocked.CompareExchange(ref this._status, 0, 0) == Status.Started) {
                // Start any stopped worker threads
                this.OnStart?.Invoke();

                return this;
            }

            if (Interlocked.CompareExchange(ref this._status, 0, 0) == Status.Disposed) {
                throw new ObjectDisposedException("RedisPubSubServer has been disposed");
            }

            // Only 1 thread allowed past
            if (Interlocked.CompareExchange(ref this._status, Status.Starting, Status.Stopped) == Status.Stopped) {
                // Should only be 1 thread past this point
                try {
                    this.Init();

                    this.SleepBackOffMultiplier(Interlocked.CompareExchange(ref this._noOfContinuousErrors, 0, 0));

                    this.OnStart?.Invoke();

                    // Don't kill us if we're the thread that's retrying to Start() after a failure.
                    if (this._bgThread != Thread.CurrentThread) {
                        this.KillBgThreadIfExists();

                        this._bgThread = new Thread(this.RunLoop) {
                            IsBackground = true,
                            Name = "RedisPubSubServer " + Interlocked.Increment(ref this._bgThreadCount)
                        };
                        this._bgThread.Start();

                        _logger.Debug("Started Background Thread: " + this._bgThread.Name);
                    } else {
                        _logger.Debug("Retrying RunLoop() on Thread: " + this._bgThread.Name);

                        this.RunLoop();
                    }
                } catch (Exception ex) {
                    this.OnError?.Invoke(ex);
                }
            }

            return this;
        }

        /// <inheritdoc />
        public void Stop() {
            this.Stop(false);
        }

        /// <inheritdoc />
        public void Restart() {
            this.Stop(true);
        }

        /// <inheritdoc />
        public string GetStatus() {
            switch (Interlocked.CompareExchange(ref this._status, 0, 0)) {
                case Status.Disposed:
                    return "Disposed";
                case Status.Stopped:
                    return "Stopped";
                case Status.Stopping:
                    return "Stopping";
                case Status.Starting:
                    return "Starting";
                case Status.Started:
                    return "Started";
            }

            return null;
        }

        /// <inheritdoc />
        public string GetStatsDescription() {
            var sb = StringBuilderCache.Acquire();
            sb.AppendLine("===============");
            sb.AppendLine("Current Status: " + this.GetStatus());
            sb.AppendLine("Times Started: " + Interlocked.CompareExchange(ref this._timesStarted, 0, 0));
            sb.AppendLine("Num of Errors: " + Interlocked.CompareExchange(ref this._noOfErrors, 0, 0));
            sb.AppendLine("Num of Continuous Errors: " + Interlocked.CompareExchange(ref this._noOfContinuousErrors, 0, 0));
            sb.AppendLine("Last ErrorMsg: " + this._lastExMsg);
            sb.AppendLine("===============");
            return StringBuilderCache.GetStringAndRelease(sb);
        }

        /// <inheritdoc />
        public virtual void Dispose() {
            if (Interlocked.CompareExchange(ref this._status, 0, 0) == Status.Disposed) {
                return;
            }

            this.Stop();

            if (Interlocked.CompareExchange(ref this._status, Status.Disposed, Status.Stopped) != Status.Stopped) {
                Interlocked.CompareExchange(ref this._status, Status.Disposed, Status.Stopping);
            }

            try {
                this.OnDispose?.Invoke();
            } catch (Exception ex) {
                _logger.Error(ex, "Error in OnDispose()");
            }

            try {
                Thread.Sleep(100); // give it a small chance to die gracefully
                this.KillBgThreadIfExists();
            } catch (Exception ex) {
                this.OnError?.Invoke(ex);
            }

            this.DisposeHeartbeatTimer();
        }

        private void Init() {
            try {
                using (var redis = this.ClientsManager.GetReadOnlyClient()) {
                    this._startedAt = Stopwatch.StartNew();
                    this._serverTimeAtStart = this.IsSentinelSubscription
                        ? DateTime.UtcNow
                        : redis.GetServerTime();
                }
            } catch (Exception ex) {
                this.OnError?.Invoke(ex);
            }

            this.DisposeHeartbeatTimer();

            void SendHeartbeat(object state) {
                var currentStatus = Interlocked.CompareExchange(ref this._status, 0, 0);
                if (currentStatus != Status.Started) {
                    return;
                }

                // ReSharper disable once PossibleInvalidOperationException
                if (DateTime.UtcNow - new DateTime(this._lastHeartbeatTicks) < this.HeartbeatInterval.Value) {
                    return;
                }

                this.OnHeartbeatSent?.Invoke();

                this.NotifyAllSubscribers(ControlCommand.Pulse);

                if (DateTime.UtcNow - new DateTime(this._lastHeartbeatTicks) > this.HeartbeatTimeout) {
                    currentStatus = Interlocked.CompareExchange(ref this._status, 0, 0);
                    if (currentStatus == Status.Started) {
                        this.Restart();
                    }
                }
            }

            if (this.HeartbeatInterval != null) {
                this._heartbeatTimer = new Timer(SendHeartbeat, null, TimeSpan.FromMilliseconds(0), this.HeartbeatInterval.Value);
            }

            Interlocked.CompareExchange(ref this._lastHeartbeatTicks, DateTime.UtcNow.Ticks, this._lastHeartbeatTicks);

            this.OnInit?.Invoke();
        }

        private void Pulse() {
            Interlocked.CompareExchange(ref this._lastHeartbeatTicks, DateTime.UtcNow.Ticks, this._lastHeartbeatTicks);

            this.OnHeartbeatReceived?.Invoke();
        }

        private void DisposeHeartbeatTimer() {
            if (this._heartbeatTimer == null) {
                return;
            }

            try {
                this._heartbeatTimer.Dispose();
            } catch (Exception ex) {
                this.OnError?.Invoke(ex);
            }

            this._heartbeatTimer = null;
        }

        private void RunLoop() {
            if (Interlocked.CompareExchange(ref this._status, Status.Started, Status.Starting) != Status.Starting) {
                return;
            }

            Interlocked.Increment(ref this._timesStarted);

            try {
                // RESET
                while (Interlocked.CompareExchange(ref this._status, 0, 0) == Status.Started) {
                    using (var redis = this.ClientsManager.GetReadOnlyClient()) {
                        this._masterClient = redis;

                        // Record that we had a good run...
                        Interlocked.CompareExchange(ref this._noOfContinuousErrors, 0, this._noOfContinuousErrors);

                        using (var subscription = redis.CreateSubscription()) {
                            subscription.OnUnSubscribe = this.HandleUnSubscribe;

                            if (this.OnMessageBytes != null) {
                                bool IsCtrlMessage(byte[] msg) {
                                    if (msg.Length < 4) {
                                        return false;
                                    }

                                    return msg[0] == 'C' && msg[1] == 'T' && msg[0] == 'R' && msg[0] == 'L';
                                }

                                ((RedisSubscription)subscription).OnMessageBytes = (channel, msg) => {
                                    if (IsCtrlMessage(msg)) {
                                        return;
                                    }

                                    this.OnMessageBytes(channel, msg);
                                };
                            }


                            subscription.OnMessage = (channel, msg) => {
                                if (string.IsNullOrEmpty(msg)) {
                                    return;
                                }

                                var ctrlMsg = msg.LeftPart(':');
                                if (ctrlMsg == ControlCommand.Control) {
                                    var op = Interlocked.CompareExchange(ref this._doOperation, Operation.NoOp, this._doOperation);
                                    var msgType = msg.IndexOf(':') >= 0 ? msg.RightPart(':') : null;

                                    this.OnControlCommand?.Invoke(msgType ?? Operation.GetName(op));

                                    switch (op) {
                                        case Operation.Stop:
                                            _logger.Debug("Stop Command Issued");

                                            Interlocked.CompareExchange(ref this._status, Status.Stopping, Status.Started);

                                            try {
                                                _logger.Debug("UnSubscribe From All Channels...");

                                                subscription.UnSubscribeFromAllChannels(); // Unblock thread.
                                            } finally {
                                                Interlocked.CompareExchange(ref this._status, Status.Stopped, Status.Stopping);
                                            }

                                            return;

                                        case Operation.Reset:
                                            subscription.UnSubscribeFromAllChannels(); // Unblock thread.
                                            return;
                                    }

                                    switch (msgType) {
                                        case ControlCommand.Pulse:
                                            this.Pulse();
                                            break;
                                    }
                                } else {
                                    this.OnMessage(channel, msg);
                                }
                            };

                            // blocks thread
                            if (this.ChannelsMatching != null && this.ChannelsMatching.Length > 0) {
                                subscription.SubscribeToChannelsMatching(this.ChannelsMatching);
                            } else {
                                subscription.SubscribeToChannels(this.Channels);
                            }

                            this._masterClient = null;
                        }
                    }
                }

                this.OnStop?.Invoke();
            } catch (Exception ex) {
                this._lastExMsg = ex.Message;
                Interlocked.Increment(ref this._noOfErrors);
                Interlocked.Increment(ref this._noOfContinuousErrors);

                if (Interlocked.CompareExchange(ref this._status, Status.Stopped, Status.Started) != Status.Started) {
                    Interlocked.CompareExchange(ref this._status, Status.Stopped, Status.Stopping);
                }

                this.OnStop?.Invoke();

                this.OnError?.Invoke(ex);
            }

            if (this.AutoRestart && Interlocked.CompareExchange(ref this._status, 0, 0) != Status.Disposed) {
                if (this.WaitBeforeNextRestart != null) {
                    Thread.Sleep(this.WaitBeforeNextRestart.Value);
                }

                this.Start();
            }
        }

        private void Stop(bool shouldRestart) {
            this.AutoRestart = shouldRestart;

            if (Interlocked.CompareExchange(ref this._status, 0, 0) == Status.Disposed) {
                throw new ObjectDisposedException("RedisPubSubServer has been disposed");
            }

            if (Interlocked.CompareExchange(ref this._status, Status.Stopping, Status.Started) == Status.Started) {
                _logger.Debug("Stopping RedisPubSubServer...");

                // Unblock current bgthread by issuing StopCommand
                this.SendControlCommand(Operation.Stop);
            }
        }

        private void SendControlCommand(int operation) {
            Interlocked.CompareExchange(ref this._doOperation, operation, this._doOperation);
            this.NotifyAllSubscribers();
        }

        private void NotifyAllSubscribers(string commandType = null) {
            var msg = ControlCommand.Control;
            if (commandType != null) {
                msg += ":" + commandType;
            }

            try {
                using (var redis = this.ClientsManager.GetClient()) {
                    foreach (var channel in this.Channels) {
                        redis.PublishMessage(channel, msg);
                    }
                }
            } catch (Exception ex) {
                this.OnError?.Invoke(ex);
                _logger.Warn(ex, "Could not send '{0}' message to bg thread: {1}", msg, ex.Message);
            }
        }

        private void HandleFailover(IRedisClientManager clientsManager) {
            try {
                this.OnFailover?.Invoke(this);

                if (this._masterClient != null) {
                    // New thread-safe client with same connection info as connected master
                    using (var currentlySubscribedClient = ((RedisClient)this._masterClient).CloneClient()) {
                        Interlocked.CompareExchange(ref this._doOperation, Operation.Reset, this._doOperation);
                        foreach (var channel in this.Channels) {
                            currentlySubscribedClient.PublishMessage(channel, ControlCommand.Control);
                        }
                    }
                } else {
                    this.Restart();
                }
            } catch (Exception ex) {
                this.OnError?.Invoke(ex);
                _logger.Warn(ex, "Error trying to UnSubscribeFromChannels in OnFailover. Restarting...");
                this.Restart();
            }
        }

        private void HandleUnSubscribe(string channel) {
            _logger.Debug("OnUnSubscribe: " + channel);

            this.OnUnSubscribe?.Invoke(channel);
        }

        private void KillBgThreadIfExists() {
            if (this._bgThread?.IsAlive == true) {
                // give it a small chance to die gracefully
                if (!this._bgThread.Join(500)) {
                    // Ideally we shouldn't get here, but lets try our hardest to clean it up
                    _logger.Warn("Interrupting previous Background Thread: " + this._bgThread.Name);
                    this._bgThread.Interrupt();
                    if (!this._bgThread.Join(TimeSpan.FromSeconds(3))) {
                        _logger.Warn(this._bgThread.Name + " just wont die, so we're now aborting it...");
                        this._bgThread.Abort();
                    }
                }

                this._bgThread = null;
            }
        }

        private void SleepBackOffMultiplier(int continuousErrorsCount) {
            if (continuousErrorsCount == 0) {
                return;
            }

            const int maxSleepMs = 60 * 1000;

            // exponential/random retry back-off.
            var nextTry = Math.Min(
                this._random.Next((int)Math.Pow(continuousErrorsCount, 3), (int)Math.Pow(continuousErrorsCount + 1, 3) + 1),
                maxSleepMs);

            _logger.Debug("Sleeping for {0}ms after {1} continuous errors", nextTry, continuousErrorsCount);

            Thread.Sleep(nextTry);
        }

    }

}
