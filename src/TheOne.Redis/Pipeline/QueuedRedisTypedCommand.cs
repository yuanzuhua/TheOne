using System;
using System.Collections.Generic;
using TheOne.Logging;
using TheOne.Redis.Client;

namespace TheOne.Redis.Pipeline {

    /// <summary>
    ///     A complete redis command, with method to send command, receive response, and run callback on success or failure
    /// </summary>
    internal class QueuedRedisTypedCommand<T> : QueuedRedisOperation {

        public Action<IRedisTypedClient<T>> VoidReturnCommand { get; set; }
        public Func<IRedisTypedClient<T>, int> IntReturnCommand { get; set; }
        public Func<IRedisTypedClient<T>, long> LongReturnCommand { get; set; }
        public Func<IRedisTypedClient<T>, bool> BoolReturnCommand { get; set; }
        public Func<IRedisTypedClient<T>, byte[]> BytesReturnCommand { get; set; }
        public Func<IRedisTypedClient<T>, byte[][]> MultiBytesReturnCommand { get; set; }
        public Func<IRedisTypedClient<T>, string> StringReturnCommand { get; set; }
        public Func<IRedisTypedClient<T>, List<string>> MultiStringReturnCommand { get; set; }
        public Func<IRedisTypedClient<T>, List<T>> MultiObjectReturnCommand { get; set; }
        public Func<IRedisTypedClient<T>, double> DoubleReturnCommand { get; set; }
        public Func<IRedisTypedClient<T>, T> ObjectReturnCommand { get; set; }

        public void Execute(IRedisTypedClient<T> client) {
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

                } else {
                    this.MultiStringReturnCommand?.Invoke(client);
                }
            } catch (Exception ex) {
                Logger.Error(ex, "Erro in QueuedRedisTypedCommand.Execute");
            }
        }

    }

}
