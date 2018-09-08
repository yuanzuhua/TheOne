using System;
using System.Collections.Generic;
using TheOne.Logging;
using TheOne.Redis.Client;

namespace TheOne.Redis.Pipeline {

    /// <summary>
    ///     Redis command that does not get queued
    /// </summary>
    internal class RedisCommand : QueuedRedisOperation {

        public Action<IRedisClient> VoidReturnCommand { get; set; }
        public Func<IRedisClient, int> IntReturnCommand { get; set; }
        public Func<IRedisClient, long> LongReturnCommand { get; set; }
        public Func<IRedisClient, bool> BoolReturnCommand { get; set; }
        public Func<IRedisClient, byte[]> BytesReturnCommand { get; set; }
        public Func<IRedisClient, byte[][]> MultiBytesReturnCommand { get; set; }
        public Func<IRedisClient, string> StringReturnCommand { get; set; }
        public Func<IRedisClient, List<string>> MultiStringReturnCommand { get; set; }
        public Func<IRedisClient, Dictionary<string, string>> DictionaryStringReturnCommand { get; set; }
        public Func<IRedisClient, RedisData> RedisDataReturnCommand { get; set; }
        public Func<IRedisClient, RedisText> RedisTextReturnCommand { get; set; }
        public Func<IRedisClient, double> DoubleReturnCommand { get; set; }

        /// <inheritdoc />
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
                } else if (this.DictionaryStringReturnCommand != null) {
                    this.DictionaryStringReturnCommand(client);
                } else if (this.RedisDataReturnCommand != null) {
                    this.RedisDataReturnCommand(client);
                } else if (this.RedisTextReturnCommand != null) {
                    this.RedisTextReturnCommand(client);
                }
            } catch (Exception ex) {
                Logger.Error(ex, "Error in RedisCommand.Execute");
            }
        }

    }

}
