using System;
using TheOne.Redis.Client;

namespace TheOne.Redis.ClientManager {

    public partial class PooledRedisClientManager {

        public DisposablePooledClient<T> GetDisposableClient<T>() where T : RedisNativeClient {
            return new DisposablePooledClient<T>(this);
        }

        /// <summary>
        ///     Manage a client acquired from the PooledRedisClientManager
        ///     Dispose method will release the client back to the pool.
        /// </summary>
        public class DisposablePooledClient<T> : IDisposable where T : RedisNativeClient {

            private readonly PooledRedisClientManager _clientManager;

            /// <summary>
            ///     wrap the acquired client
            /// </summary>
            /// <param name="clientManager" ></param>
            public DisposablePooledClient(PooledRedisClientManager clientManager) {
                this._clientManager = clientManager;
                if (clientManager != null) {
                    this.Client = (T)clientManager.GetClient();
                }
            }

            /// <summary>
            ///     access the wrapped client
            /// </summary>
            public T Client { get; private set; }

            /// <summary>
            ///     release the wrapped client back to the pool
            /// </summary>
            public void Dispose() {
                if (this.Client != null) {
                    this._clientManager.DisposeClient(this.Client);
                }

                this.Client = null;
            }

        }

    }

}
