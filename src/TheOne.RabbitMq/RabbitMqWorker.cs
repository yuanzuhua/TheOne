using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using TheOne.Logging;
using TheOne.RabbitMq.Interfaces;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq {

    public class RabbitMqWorker : IDisposable {

        private static readonly ILog _logger = LogProvider.GetCurrentClassLogger();
        private readonly IMqMessageHandler _messageHandler;
        private readonly RabbitMqMessageFactory _mqFactory;
        private readonly object _msgLock = new object();
        private Thread _bgThread;
        private IMqMessageQueueClient _mqClient;
        private bool _receivedNewMsgs;
        private int _status;
        private int _timesStarted;

        public int SleepTimeoutMs = 1000;

        public RabbitMqWorker(RabbitMqMessageFactory mqFactory,
            IMqMessageHandler messageHandler, string queueName,
            Action<RabbitMqWorker, Exception> errorHandler,
            bool autoConnect = true) {
            this._mqFactory = mqFactory;
            this._messageHandler = messageHandler;
            this.QueueName = queueName;
            this.ErrorHandler = errorHandler;
            this.AutoReconnect = autoConnect;
        }

        public string QueueName { get; set; }
        public bool AutoReconnect { get; set; }
        public int Status => this._status;
        public Action<RabbitMqWorker, Exception> ErrorHandler { get; set; }
        public DateTime LastMsgProcessed { get; private set; }
        public int TotalMessagesProcessed { get; private set; }
        public IMqMessageQueueClient MqClient => this._mqClient ?? (this._mqClient = this._mqFactory.CreateMessageQueueClient());

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
                this.KillBgThreadIfExists();
            } catch (Exception ex) {
                _logger.Error(ex, $"Error disposing MessageHandlerWorker for: {this.QueueName}...");
            }
        }

        public virtual RabbitMqWorker Clone() {
            return new RabbitMqWorker(this._mqFactory, this._messageHandler, this.QueueName, this.ErrorHandler, this.AutoReconnect);
        }

        private IModel GetChannel() {
            var rabbitClient = (RabbitMqQueueClient)this.MqClient;
            return rabbitClient.Channel;
        }

        public virtual void Start() {
            if (Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Started) {
                return;
            }

            if (Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Disposed) {
                throw new ObjectDisposedException("MQ Host has been disposed");
            }

            if (Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Stopping) {
                this.KillBgThreadIfExists();
            }

            if (Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Starting, MqWorkerStatus.Stopped) == MqWorkerStatus.Stopped) {
                _logger.Info("Starting MQ Handler Worker: {0}...", this.QueueName);

                // Should only be 1 thread past this point
                this._bgThread = new Thread(this.Run) {
                    Name = $"{this.GetType().Name}: {this.QueueName}",
                    IsBackground = true
                };
                this._bgThread.Start();
            }
        }

        public virtual void ForceRestart() {
            this.KillBgThreadIfExists();
            this.Start();
        }

        [SuppressMessage("ReSharper", "CyclomaticComplexity", Justification = "intentionally")]
        private void Run() {
            if (Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Started, MqWorkerStatus.Starting) != MqWorkerStatus.Starting) {
                return;
            }

            this._timesStarted++;

            try {
                if (this._mqFactory.UsePolling) {
                    lock (this._msgLock) {
                        this.StartPolling();
                    }
                } else {
                    this.StartSubscription();
                }
            } catch (Exception ex) {
                // Ignore handling rare, but expected exceptions from KillBgThreadIfExists()
                if (ex is ThreadInterruptedException || ex is ThreadAbortException) {
                    _logger.Warn("Received {0} in Worker: {1}", ex.GetType().Name, this.QueueName);
                    return;
                }

                this.Stop();
                this.ErrorHandler?.Invoke(this, ex);
            } finally {
                try {
                    this.DisposeMqClient();
                } catch {
                    //
                }

                // If it's in an invalid state, Dispose() this worker.
                if (Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopped, MqWorkerStatus.Stopping) !=
                    MqWorkerStatus.Stopping) {
                    this.Dispose();
                }
                // status is either 'Stopped' or 'Disposed' at this point

                this._bgThread = null;
            }
        }

        private void StartPolling() {
            while (Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Started) {
                try {
                    this._receivedNewMsgs = false;

                    var msgsProcessedThisTime = this._messageHandler.ProcessQueue(this.MqClient,
                        this.QueueName,
                        () => Interlocked.CompareExchange(ref this._status, 0, 0) ==
                              MqWorkerStatus.Started);

                    this.TotalMessagesProcessed += msgsProcessedThisTime;

                    if (msgsProcessedThisTime > 0) {
                        this.LastMsgProcessed = DateTime.Now;
                    }

                    if (!this._receivedNewMsgs) {
                        Monitor.Wait(this._msgLock, this.SleepTimeoutMs);
                    }
                } catch (Exception ex) {
                    if (!(ex is OperationInterruptedException
                          || ex is EndOfStreamException)) {
                        throw;
                    }

                    // The consumer was cancelled, the model or the connection went away.
                    if (Interlocked.CompareExchange(ref this._status, 0, 0) != MqWorkerStatus.Started
                        || !this.AutoReconnect) {
                        return;
                    }

                    // If it was an unexpected exception, try reconnecting
                    this.WaitForReconnect();
                }
            }
        }

        [SuppressMessage("ReSharper", "FunctionNeverReturns", Justification = "intentionally")]
        private void WaitForReconnect() {
            var retries = 1;
            while (true) {
                this.DisposeMqClient();
                try {
                    this.GetChannel();
                } catch (Exception ex) {
                    var waitMs = Math.Min(retries++ * 100, 10000);
                    _logger.Debug(ex, "Retrying to Reconnect after {0}ms...", waitMs);
                    Thread.Sleep(waitMs);
                }
            }
        }

        private void StartSubscription() {
            var consumer = this.ConnectSubscription();

            // At this point, messages will be being asynchronously delivered,
            // and will be queueing up in consumer.Queue.
            while (true) {
                try {
                    var e = consumer.Queue.Dequeue();

                    this._mqFactory.GetMessageFilter?.Invoke(this.QueueName, e);

                    this._messageHandler.ProcessMessage(this._mqClient, e);
                } catch (Exception ex) {
                    if (!(ex is OperationInterruptedException
                          || ex is EndOfStreamException)) {
                        throw;
                    }

                    // The consumer was cancelled, the model closed, or the connection went away.
                    if (Interlocked.CompareExchange(ref this._status, 0, 0) != MqWorkerStatus.Started
                        || !this.AutoReconnect) {
                        return;
                    }

                    // If it was an unexpected exception, try reconnecting
                    consumer = this.WaitForReconnectSubscription();
                }
            }
        }

        private RabbitMqBasicConsumer WaitForReconnectSubscription() {
            var retries = 1;
            while (true) {
                this.DisposeMqClient();
                try {
                    return this.ConnectSubscription();
                } catch (Exception ex) {
                    var waitMs = Math.Min(retries++ * 100, 10000);
                    _logger.Warn(ex, "Retrying to Reconnect Subscription after {0}ms...", waitMs);
                    Thread.Sleep(waitMs);
                }
            }
        }

        private RabbitMqBasicConsumer ConnectSubscription() {
            var channel = this.GetChannel();
            var consumer = new RabbitMqBasicConsumer(channel);
            channel.BasicConsume(this.QueueName, false, consumer);
            return consumer;
        }

        public virtual void Stop() {
            if (Interlocked.CompareExchange(ref this._status, 0, 0) == MqWorkerStatus.Disposed) {
                return;
            }

            if (Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopping, MqWorkerStatus.Started) == MqWorkerStatus.Started) {
                _logger.Info("Stopping Rabbit MQ Handler Worker: {0}...", this.QueueName);
                if (this._mqFactory.UsePolling) {
                    Thread.Sleep(100);
                    lock (this._msgLock) {
                        Monitor.Pulse(this._msgLock);
                    }
                }

                this.DisposeMqClient();
            }
        }

        private void DisposeMqClient() {
            // Disposing mqClient causes an EndOfStreamException to be thrown in StartSubscription
            if (this._mqClient == null) {
                return;
            }

            this._mqClient.Dispose();
            this._mqClient = null;
        }

        private void KillBgThreadIfExists() {
            try {
                if (this._bgThread != null && this._bgThread.IsAlive) {
                    // give it a small chance to die gracefully
                    if (!this._bgThread.Join(500)) {
                        // ideally we shouldn't get here, but lets try our hardest to clean it up
                        _logger.Warn($"Interrupting previous background worker: {this._bgThread.Name}.");
                        this._bgThread.Interrupt();
                        if (!this._bgThread.Join(TimeSpan.FromSeconds(3))) {
                            _logger.Warn($"{this._bgThread.Name} just won\'t die, so we\'re now aborting it...");
                            this._bgThread.Abort();
                        }
                    }
                }
            } finally {
                this._bgThread = null;
                Interlocked.CompareExchange(ref this._status, MqWorkerStatus.Stopped, this._status);
            }
        }

        public virtual IMqMessageHandlerStats GetStats() {
            return this._messageHandler.GetStats();
        }

        public virtual string GetStatus() {
            return string.Format("[Worker: {0}, Status: {1}, ThreadStatus: {2}, LastMsgAt: {3}]",
                this.QueueName,
                MqWorkerStatus.ToString(this._status),
                this._bgThread.ThreadState,
                this.LastMsgProcessed
            );
        }

    }

}
