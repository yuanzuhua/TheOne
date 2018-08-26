using TheOne.Redis.Client;

namespace TheOne.Redis.ClientManager {

    public static class RedisResolverExtensions {

        public static RedisClient CreateRedisClient(this IRedisResolver resolver, RedisEndpoint config, bool master) {
            return ((IRedisResolverExtended)resolver).CreateRedisClient(config, master);
        }

        public static RedisEndpoint GetReadWriteHost(this IRedisResolver resolver, int desiredIndex) {
            return ((IRedisResolverExtended)resolver).GetReadWriteHost(desiredIndex);
        }

        public static RedisEndpoint GetReadOnlyHost(this IRedisResolver resolver, int desiredIndex) {
            return ((IRedisResolverExtended)resolver).GetReadOnlyHost(desiredIndex);
        }

    }

}
