using System;
using TheOne.Logging;
using TheOne.Redis.Client;

namespace TheOne.Redis.Pipeline {

    /// <summary>
    ///     A complete redis command, with method to send command, receive response, and run callback on success or failure
    /// </summary>
    internal class QueuedRedisCommand : RedisCommand {

        public override void Execute(IRedisClient client) {
            try {
                if (this.VoidReturnCommand != null) {
                    this.VoidReturnCommand(client);

                } else if (this.IntReturnCommand != null) {
                    this.IntReturnCommand(client);

                } else if (this.LongReturnCommand != null) {
                    this.LongReturnCommand(client);

                } else if (this.DoubleReturnCommand != null) {
                    this.DoubleReturnCommand(client);

                } else if (this.BytesReturnCommand != null) {
                    this.BytesReturnCommand(client);

                } else if (this.StringReturnCommand != null) {
                    this.StringReturnCommand(client);

                } else if (this.MultiBytesReturnCommand != null) {
                    this.MultiBytesReturnCommand(client);

                } else if (this.MultiStringReturnCommand != null) {
                    this.MultiStringReturnCommand(client);
                }
            } catch (Exception ex) {
                Logger.Error(ex, "Error in QueuedRedisCommand.Execute");
                throw;
            }

        }

    }

}
