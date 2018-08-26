using System;
using TheOne.Redis.Client;

namespace TheOne.Redis.Pipeline {

    public class RedisAllPurposePipeline : RedisCommandQueue, IRedisPipeline {

        /// <summary>
        ///     General purpose pipeline
        /// </summary>
        public RedisAllPurposePipeline(RedisClient redisClient) : base(redisClient) {
            this.Init();

        }

        /// <summary>
        ///     Flush send buffer, and read responses
        /// </summary>
        public void Flush() {
            // flush send buffers
            this.RedisClient.FlushAndResetSendBuffer();

            try {
                // receive expected results
                foreach (QueuedRedisOperation queuedCommand in this.QueuedCommands) {
                    queuedCommand.ProcessResult();
                }
            } catch {
                // The connection cannot be reused anymore.
                // All queued commands have been sent to redis. Even if a new command is executed, the next response read from the
                // network stream can be the response of one of the queued commands, depending on when the exception occurred.
                // This response would be invalid for the new command.
                this.RedisClient.DisposeConnection();
                throw;
            }

            this.ClosePipeline();
        }

        public virtual bool Replay() {
            this.Init();
            this.Execute();
            this.Flush();
            return true;
        }

        public virtual void Dispose() {
            this.ClosePipeline();
        }

        protected virtual void Init() {
            if (this.RedisClient.Transaction != null) {
                throw new InvalidOperationException("A transaction is already in use");
            }

            if (this.RedisClient.Pipeline != null) {
                throw new InvalidOperationException("A pipeline is already in use");
            }

            this.RedisClient.Pipeline = this;
        }

        protected void Execute() {
            var count = this.QueuedCommands.Count;
            for (var i = 0; i < count; ++i) {
                QueuedRedisOperation op = this.QueuedCommands[0];
                this.QueuedCommands.RemoveAt(0);
                op.Execute(this.RedisClient);
                this.QueuedCommands.Add(op);
            }
        }

        protected void ClosePipeline() {
            this.RedisClient.EndPipeline();
        }

    }

}
