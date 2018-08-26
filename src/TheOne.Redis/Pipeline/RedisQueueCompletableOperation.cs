using System;
using System.Collections.Generic;
using TheOne.Redis.Client;

namespace TheOne.Redis.Pipeline {

    /// <summary>
    ///     Redis operation (transaction/pipeline) that allows queued commands to be completed
    /// </summary>
    public class RedisQueueCompletableOperation {

        internal readonly List<QueuedRedisOperation> QueuedCommands = new List<QueuedRedisOperation>();
        internal QueuedRedisOperation CurrentQueuedOperation;

        internal void BeginQueuedCommand(QueuedRedisOperation queuedRedisOperation) {
            if (this.CurrentQueuedOperation != null) {
                throw new InvalidOperationException("The previous queued operation has not been commited");
            }

            this.CurrentQueuedOperation = queuedRedisOperation;
        }

        internal void AssertCurrentOperation() {
            if (this.CurrentQueuedOperation == null) {
                throw new InvalidOperationException("No queued operation is currently set");
            }
        }

        protected virtual void AddCurrentQueuedOperation() {
            this.QueuedCommands.Add(this.CurrentQueuedOperation);
            this.CurrentQueuedOperation = null;
        }

        public virtual void CompleteVoidQueuedCommand(Action voidReadCommand) {
            // AssertCurrentOperation();
            // this can happen when replaying pipeline/transaction
            if (this.CurrentQueuedOperation == null) {
                return;
            }

            this.CurrentQueuedOperation.VoidReadCommand = voidReadCommand;
            this.AddCurrentQueuedOperation();
        }

        public virtual void CompleteIntQueuedCommand(Func<int> intReadCommand) {
            // AssertCurrentOperation();
            // this can happen when replaying pipeline/transaction
            if (this.CurrentQueuedOperation == null) {
                return;
            }

            this.CurrentQueuedOperation.IntReadCommand = intReadCommand;
            this.AddCurrentQueuedOperation();
        }

        public virtual void CompleteLongQueuedCommand(Func<long> longReadCommand) {
            // AssertCurrentOperation();
            // this can happen when replaying pipeline/transaction
            if (this.CurrentQueuedOperation == null) {
                return;
            }

            this.CurrentQueuedOperation.LongReadCommand = longReadCommand;
            this.AddCurrentQueuedOperation();
        }

        public virtual void CompleteBytesQueuedCommand(Func<byte[]> bytesReadCommand) {
            // AssertCurrentOperation();
            // this can happen when replaying pipeline/transaction
            if (this.CurrentQueuedOperation == null) {
                return;
            }

            this.CurrentQueuedOperation.BytesReadCommand = bytesReadCommand;
            this.AddCurrentQueuedOperation();
        }

        public virtual void CompleteMultiBytesQueuedCommand(Func<byte[][]> multiBytesReadCommand) {
            // AssertCurrentOperation();
            // this can happen when replaying pipeline/transaction
            if (this.CurrentQueuedOperation == null) {
                return;
            }

            this.CurrentQueuedOperation.MultiBytesReadCommand = multiBytesReadCommand;
            this.AddCurrentQueuedOperation();
        }

        public virtual void CompleteStringQueuedCommand(Func<string> stringReadCommand) {
            // AssertCurrentOperation();
            // this can happen when replaying pipeline/transaction
            if (this.CurrentQueuedOperation == null) {
                return;
            }

            this.CurrentQueuedOperation.StringReadCommand = stringReadCommand;
            this.AddCurrentQueuedOperation();
        }

        public virtual void CompleteMultiStringQueuedCommand(Func<List<string>> multiStringReadCommand) {
            // AssertCurrentOperation();
            // this can happen when replaying pipeline/transaction
            if (this.CurrentQueuedOperation == null) {
                return;
            }

            this.CurrentQueuedOperation.MultiStringReadCommand = multiStringReadCommand;
            this.AddCurrentQueuedOperation();
        }

        public virtual void CompleteDoubleQueuedCommand(Func<double> doubleReadCommand) {
            // AssertCurrentOperation();
            // this can happen when replaying pipeline/transaction
            if (this.CurrentQueuedOperation == null) {
                return;
            }

            this.CurrentQueuedOperation.DoubleReadCommand = doubleReadCommand;
            this.AddCurrentQueuedOperation();
        }

        public virtual void CompleteRedisDataQueuedCommand(Func<RedisData> redisDataReadCommand) {
            // AssertCurrentOperation();
            // this can happen when replaying pipeline/transaction
            if (this.CurrentQueuedOperation == null) {
                return;
            }

            this.CurrentQueuedOperation.RedisDataReadCommand = redisDataReadCommand;
            this.AddCurrentQueuedOperation();
        }

    }

}
