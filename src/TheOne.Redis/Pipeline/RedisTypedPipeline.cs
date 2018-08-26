using System;
using TheOne.Redis.Client;

namespace TheOne.Redis.Pipeline {

    /// <summary>
    ///     Pipeline for redis typed client
    /// </summary>
    public class RedisTypedPipeline<T> : RedisTypedCommandQueue<T>, IRedisTypedPipeline<T> {

        internal RedisTypedPipeline(RedisTypedClient<T> redisClient)
            : base(redisClient) {
            this.Init();
        }

        public void Flush() {
            try {


                // flush send buffers
                this.RedisClient.FlushSendBuffer();

                // receive expected results
                foreach (QueuedRedisOperation queuedCommand in this.QueuedCommands) {
                    queuedCommand.ProcessResult();
                }

            } finally {
                this.ClosePipeline();
                this.RedisClient.AddTypeIdsRegisteredDuringPipeline();
            }
        }

        public virtual bool Replay() {
            this.RedisClient.Pipeline = this;
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
            foreach (QueuedRedisOperation queuedCommand in this.QueuedCommands) {
                if (queuedCommand is QueuedRedisTypedCommand<T> cmd) {
                    cmd.Execute(this.RedisClient);
                }
            }
        }

        protected void ClosePipeline() {
            this.RedisClient.EndPipeline();
        }

    }

}
