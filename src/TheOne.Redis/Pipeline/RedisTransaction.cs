using System;
using TheOne.Redis.Client;

namespace TheOne.Redis.Pipeline {

    /// <summary>
    ///     Redis Transactions (i.e. MULTI/EXEC/DISCARD operations).
    /// </summary>
    public class RedisTransaction : RedisAllPurposePipeline, IRedisTransaction {

        private int _numCommands;

        public RedisTransaction(RedisClient redisClient) : base(redisClient) { }

        /// <inheritdoc />
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

                // receive expected results
                foreach (var queuedCommand in this.QueuedCommands) {
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

        /// <inheritdoc />
        public void Rollback() {
            if (this.RedisClient.Transaction == null) {
                throw new InvalidOperationException("There is no current transaction to Rollback");
            }

            this.RedisClient.Transaction = null;
            this.RedisClient.ClearTypeIdsRegisteredDuringPipeline();
        }

        /// <inheritdoc />
        public override bool Replay() {
            var rc = true;
            try {
                this.Execute();

                // receive expected results
                foreach (var queuedCommand in this.QueuedCommands) {
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

        /// <inheritdoc />
        public override void Dispose() {
            base.Dispose();
            if (this.RedisClient.Transaction == null) {
                return;
            }

            this.Rollback();
        }

        /// <inheritdoc />
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
            this.RedisClient.FlushAndResetSendBuffer();
        }

        /// <summary>
        ///     callback for after result count is read in
        /// </summary>
        private void HandleMultiDataResultCount(int count) {
            // transaction failed due to WATCH condition
            if (count == -1) {
                throw new RedisTransactionFailedException();
            }

            if (count != this._numCommands) {
                throw new InvalidOperationException(string.Format(
                    "Invalid results received from 'EXEC', expected '{0}' received '{1}'"
                    + "\nWarning: Transaction was committed",
                    this._numCommands,
                    count));
            }
        }

        /// <inheritdoc />
        protected override void AddCurrentQueuedOperation() {
            base.AddCurrentQueuedOperation();
            this.QueueExpectQueued();
        }

    }

}
