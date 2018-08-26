using System;
using System.Collections.Generic;
using TheOne.Redis.Client;

namespace TheOne.Redis.Pipeline {

    /// <summary>
    ///     Interface to operations that allow queued commands to be completed
    /// </summary>
    public interface IRedisQueueCompletableOperation {

        void CompleteVoidQueuedCommand(Action voidReadCommand);
        void CompleteIntQueuedCommand(Func<int> intReadCommand);
        void CompleteLongQueuedCommand(Func<long> longReadCommand);
        void CompleteBytesQueuedCommand(Func<byte[]> bytesReadCommand);
        void CompleteMultiBytesQueuedCommand(Func<byte[][]> multiBytesReadCommand);
        void CompleteStringQueuedCommand(Func<string> stringReadCommand);
        void CompleteMultiStringQueuedCommand(Func<List<string>> multiStringReadCommand);
        void CompleteDoubleQueuedCommand(Func<double> doubleReadCommand);
        void CompleteRedisDataQueuedCommand(Func<RedisData> redisDataReadCommand);

    }

}
