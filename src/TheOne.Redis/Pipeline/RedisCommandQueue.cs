using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client;

namespace TheOne.Redis.Pipeline {

    public class RedisCommandQueue : RedisQueueCompletableOperation {

        protected readonly RedisClient RedisClient;

        public RedisCommandQueue(RedisClient redisClient) {
            this.RedisClient = redisClient;

        }

        public void QueueCommand(Action<IRedisClient> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Action<IRedisClient> command, Action onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Action<IRedisClient> command, Action onSuccessCallback, Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                VoidReturnCommand = command,
                OnSuccessVoidCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }


        public void QueueCommand(Func<IRedisClient, int> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, int> command, Action<int> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, int> command, Action<int> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                IntReturnCommand = command,
                OnSuccessIntCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisClient, long> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, long> command, Action<long> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, long> command, Action<long> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                LongReturnCommand = command,
                OnSuccessLongCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisClient, bool> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, bool> command, Action<bool> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, bool> command, Action<bool> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                BoolReturnCommand = command,
                OnSuccessBoolCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }


        public void QueueCommand(Func<IRedisClient, double> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, double> command, Action<double> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, double> command, Action<double> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                DoubleReturnCommand = command,
                OnSuccessDoubleCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisClient, byte[]> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, byte[]> command, Action<byte[]> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, byte[]> command, Action<byte[]> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                BytesReturnCommand = command,
                OnSuccessBytesCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisClient, string> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, string> command, Action<string> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, string> command, Action<string> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                StringReturnCommand = command,
                OnSuccessStringCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisClient, byte[][]> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, byte[][]> command, Action<byte[][]> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, byte[][]> command, Action<byte[][]> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                MultiBytesReturnCommand = command,
                OnSuccessMultiBytesCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisClient, List<string>> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, List<string>> command, Action<List<string>> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, List<string>> command, Action<List<string>> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                MultiStringReturnCommand = command,
                OnSuccessMultiStringCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisClient, HashSet<string>> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, HashSet<string>> command, Action<HashSet<string>> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, HashSet<string>> command, Action<HashSet<string>> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                MultiStringReturnCommand = r => command(r).ToList(),
                OnSuccessMultiStringCallback = list => onSuccessCallback(new HashSet<string>(list)),
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisClient, Dictionary<string, string>> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, Dictionary<string, string>> command,
            Action<Dictionary<string, string>> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisClient, Dictionary<string, string>> command,
            Action<Dictionary<string, string>> onSuccessCallback, Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                DictionaryStringReturnCommand = command,
                OnSuccessDictionaryStringCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisClient, RedisData> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, RedisData> command, Action<RedisData> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisClient, RedisData> command, Action<RedisData> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                RedisDataReturnCommand = command,
                OnSuccessRedisDataCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisClient, RedisText> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, RedisText> command, Action<RedisText> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisClient, RedisText> command, Action<RedisText> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisCommand {
                RedisTextReturnCommand = command,
                OnSuccessRedisTextCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

    }

}
