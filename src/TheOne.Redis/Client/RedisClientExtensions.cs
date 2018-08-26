namespace TheOne.Redis.Client {

    public static class RedisClientExtensions {

        public static string GetHostString(this IRedisClient redis) {
            return string.Format("{0}:{1}", redis.Host, redis.Port);
        }

    }

}
