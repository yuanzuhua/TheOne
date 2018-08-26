using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.Pipeline {

    /// <summary>
    ///     Queue of commands for redis typed client
    /// </summary>
    public class RedisTypedCommandQueue<T> : RedisQueueCompletableOperation {

        internal readonly RedisTypedClient<T> RedisClient;

        internal RedisTypedCommandQueue(RedisTypedClient<T> redisClient) {
            this.RedisClient = redisClient;

        }

        public void QueueCommand(Action<IRedisTypedClient<T>> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Action<IRedisTypedClient<T>> command, Action onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Action<IRedisTypedClient<T>> command, Action onSuccessCallback, Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                VoidReturnCommand = command,
                OnSuccessVoidCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }


        public void QueueCommand(Func<IRedisTypedClient<T>, int> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, int> command, Action<int> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, int> command, Action<int> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                IntReturnCommand = command,
                OnSuccessIntCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, long> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, long> command, Action<long> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, long> command, Action<long> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                LongReturnCommand = command,
                OnSuccessLongCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, bool> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, bool> command, Action<bool> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, bool> command, Action<bool> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                BoolReturnCommand = command,
                OnSuccessBoolCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, double> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, double> command, Action<double> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, double> command, Action<double> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                DoubleReturnCommand = command,
                OnSuccessDoubleCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, byte[]> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, byte[]> command, Action<byte[]> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, byte[]> command, Action<byte[]> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                BytesReturnCommand = command,
                OnSuccessBytesCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, string> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, string> command, Action<string> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, string> command, Action<string> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                StringReturnCommand = command,
                OnSuccessStringCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, T> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, T> command, Action<T> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, T> command, Action<T> onSuccessCallback, Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                ObjectReturnCommand = command,
                OnSuccessTypeCallback = x => onSuccessCallback(x.FromJson<T>()),
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, byte[][]> command, Action<byte[][]> onSuccessCallback = null,
            Action<Exception> onErrorCallback = null) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                MultiBytesReturnCommand = command,
                OnSuccessMultiBytesCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }


        public void QueueCommand(Func<IRedisTypedClient<T>, List<string>> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, List<string>> command, Action<List<string>> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, List<string>> command, Action<List<string>> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                MultiStringReturnCommand = command,
                OnSuccessMultiStringCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, List<T>> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, List<T>> command, Action<List<T>> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, List<T>> command, Action<List<T>> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                MultiObjectReturnCommand = command,
                OnSuccessMultiTypeCallback = x => onSuccessCallback(x.ConvertAll(y => y.FromJson<T>())),
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, HashSet<string>> command) {
            this.QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, HashSet<string>> command, Action<HashSet<string>> onSuccessCallback) {
            this.QueueCommand(command, onSuccessCallback, null);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, HashSet<string>> command, Action<HashSet<string>> onSuccessCallback,
            Action<Exception> onErrorCallback) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                MultiStringReturnCommand = r => command(r).ToList(),
                OnSuccessMultiStringCallback = list => onSuccessCallback(new HashSet<string>(list)),
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

        public void QueueCommand(Func<IRedisTypedClient<T>, HashSet<T>> command, Action<HashSet<T>> onSuccessCallback = null,
            Action<Exception> onErrorCallback = null) {
            this.BeginQueuedCommand(new QueuedRedisTypedCommand<T> {
                MultiObjectReturnCommand = r => command(r).ToList(),
                OnSuccessMultiTypeCallback = x => onSuccessCallback?.Invoke(new HashSet<T>(x.ConvertAll(y => y.FromJson<T>()))),
                OnErrorCallback = onErrorCallback
            });
            command(this.RedisClient);
        }

    }

}
