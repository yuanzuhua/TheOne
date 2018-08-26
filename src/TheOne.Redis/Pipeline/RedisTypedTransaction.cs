using System;
using TheOne.Redis.Client;

namespace TheOne.Redis.Pipeline {

    /// <summary>
    ///     Redis Transactions (i.e. MULTI/EXEC/DISCARD operations).
    /// </summary>
    public class RedisTypedTransaction<T> : RedisTypedPipeline<T>, IRedisTypedTransaction<T>, IRedisTransactionBase {

        private int _numCommands;

        internal RedisTypedTransaction(RedisTypedClient<T> redisClient) : base(redisClient) { }

        public override bool Replay() {
            var rc = true;
            try {
                this.Execute();

                /////////////////////////////
                // receive expected results
                foreach (QueuedRedisOperation queuedCommand in this.QueuedCommands) {
                    queuedCommand.ProcessResult();
                }
            } catch (RedisTransactionFailedException) {
                rc = false;
            } finally {

                this.RedisClient.Transaction = null;
                this.ClosePipeline();
                this.RedisClient.AddTypeIdsRegisteredDuringPipeline();
            }

            return rc;
        }

        public bool Commit() {
            var rc = true;
            try {
                this._numCommands = this.QueuedCommands.Count / 2;

                // insert multi command at beginning
                this.QueuedCommands.Insert(0,
                    new QueuedRedisCommand {
                        VoidReturnCommand = r => this.Init(),
                        VoidReadCommand = this.RedisClient.ExpectOk
                    });


                // the first half of the responses will be "QUEUED",
                // so insert reading of multiline after these responses
                this.QueuedCommands.Insert(this._numCommands + 1,
                    new QueuedRedisOperation {
                        IntReadCommand = this.RedisClient.ReadMultiDataResultCount,
                        OnSuccessIntCallback = this.HandleMultiDataResultCount
                    });

                // add Exec command at end (not queued)
                this.QueuedCommands.Add(new RedisCommand {
                    VoidReturnCommand = r => this.Exec()
                });

                // execute transaction
                this.Exec();

                /////////////////////////////
                // receive expected results
                foreach (QueuedRedisOperation queuedCommand in this.QueuedCommands) {
                    queuedCommand.ProcessResult();
                }
            } catch (RedisTransactionFailedException) {
                rc = false;
            } finally {
                this.RedisClient.Transaction = null;
                this.ClosePipeline();
                this.RedisClient.AddTypeIdsRegisteredDuringPipeline();
            }

            return rc;
        }

        public void Rollback() {
            if (this.RedisClient.Transaction == null) {
                throw new InvalidOperationException("There is no current transaction to Rollback");
            }

            this.RedisClient.Transaction = null;
            this.RedisClient.ClearTypeIdsRegisteredDuringPipeline();
        }

        public override void Dispose() {
            base.Dispose();
            if (this.RedisClient.Transaction == null) {
                return;
            }

            this.Rollback();
        }

        protected override void Init() {
            // start pipelining
            base.Init();
            // queue multi command
            this.RedisClient.Multi();
            // set transaction
            this.RedisClient.Transaction = this;
        }

        /// <summary>
        ///     Put "QUEUED" messages at back of queue
        /// </summary>
        private void QueueExpectQueued() {
            this.QueuedCommands.Insert(0,
                new QueuedRedisOperation {
                    VoidReadCommand = this.RedisClient.ExpectQueued
                });
        }


        /// <summary>
        ///     Issue exec command (not queued)
        /// </summary>
        private void Exec() {
            this.RedisClient.Exec();
            this.RedisClient.FlushSendBuffer();

        }

        /// <summary>
        ///     callback for after result count is read in
        /// </summary>
        private void HandleMultiDataResultCount(int count) {
            if (count != this._numCommands) {
                throw new InvalidOperationException(
                    $"Invalid results received from 'EXEC', expected '{this._numCommands}' received '{count}'" +
                    "\nWarning: Transaction was committed");
            }
        }

        protected override void AddCurrentQueuedOperation() {
            base.AddCurrentQueuedOperation();
            this.QueueExpectQueued();
        }

    }

}
