using System;
using System.Collections.Generic;
using TheOne.Redis.Client;
using TheOne.Redis.Pipeline;
using TheOne.Redis.PubSub;

namespace TheOne.Redis.ClientManager {

    /// <summary>
    ///     Useful wrapper IRedisClientManager to cut down the boiler plate of most IRedisClient access
    /// </summary>
    public static class RedisClientManagerExtensions {

        /// <summary>
        ///     Creates a PubSubServer that uses a background thread to listen and process for
        ///     Redis Pub/Sub messages published to the specified channel.
        ///     Use optional callbacks to listen for message, error and life-cycle events.
        ///     Callbacks can be assigned later, then call Start() for PubSubServer to start listening for messages
        /// </summary>
        public static IRedisPubSubServer CreatePubSubServer(this IRedisClientManager redisManager,
            string channel,
            Action<string, string> onMessage = null,
            Action<Exception> onError = null,
            Action onInit = null,
            Action onStart = null,
            Action onStop = null) {
            return new RedisPubSubServer(redisManager, channel) {
                OnMessage = onMessage,
                OnError = onError,
                OnInit = onInit,
                OnStart = onStart,
                OnStop = onStop
            };
        }

        public static void Exec(this IRedisClientManager redisManager, Action<IRedisClient> lambda) {
            using (var redis = redisManager.GetClient()) {
                lambda(redis);
            }
        }

        public static string Exec(this IRedisClientManager redisManager, Func<IRedisClient, string> lambda) {
            using (var redis = redisManager.GetClient()) {
                return lambda(redis);
            }
        }

        public static long Exec(this IRedisClientManager redisManager, Func<IRedisClient, long> lambda) {
            using (var redis = redisManager.GetClient()) {
                return lambda(redis);
            }
        }

        public static int Exec(this IRedisClientManager redisManager, Func<IRedisClient, int> lambda) {
            using (var redis = redisManager.GetClient()) {
                return lambda(redis);
            }
        }

        public static double Exec(this IRedisClientManager redisManager, Func<IRedisClient, double> lambda) {
            using (var redis = redisManager.GetClient()) {
                return lambda(redis);
            }
        }

        public static bool Exec(this IRedisClientManager redisManager, Func<IRedisClient, bool> lambda) {
            using (var redis = redisManager.GetClient()) {
                return lambda(redis);
            }
        }

        public static void ExecTrans(this IRedisClientManager redisManager, Action<IRedisTransaction> lambda) {
            using (var redis = redisManager.GetClient()) {
                using (var trans = redis.CreateTransaction()) {
                    lambda(trans);

                    trans.Commit();
                }
            }
        }

        public static void ExecAs<T>(this IRedisClientManager redisManager, Action<IRedisTypedClient<T>> lambda) {
            using (var redis = redisManager.GetClient()) {
                lambda(redis.As<T>());
            }
        }

        public static T ExecAs<T>(this IRedisClientManager redisManager, Func<IRedisTypedClient<T>, T> lambda) {
            using (var redis = redisManager.GetClient()) {
                return lambda(redis.As<T>());
            }
        }

        public static IList<T> ExecAs<T>(this IRedisClientManager redisManager, Func<IRedisTypedClient<T>, IList<T>> lambda) {
            using (var redis = redisManager.GetClient()) {
                return lambda(redis.As<T>());
            }
        }

        public static List<T> ExecAs<T>(this IRedisClientManager redisManager, Func<IRedisTypedClient<T>, List<T>> lambda) {
            using (var redis = redisManager.GetClient()) {
                return lambda(redis.As<T>());
            }
        }

        public static ManagedList<T> GetManagedList<T>(this IRedisClientManager manager, string key) {
            return new ManagedList<T>(manager, key);
        }

    }

}
