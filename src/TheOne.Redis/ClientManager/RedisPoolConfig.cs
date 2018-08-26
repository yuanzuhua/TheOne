using TheOne.Redis.Client;

namespace TheOne.Redis.ClientManager {

    /// <summary>
    ///     Configuration class for the RedisManagerPool
    /// </summary>
    public class RedisPoolConfig {

        /// <summary>
        ///     Default pool size used by every new instance of <see cref="RedisPoolConfig" />. (default: 40)
        /// </summary>
        public static int DefaultMaxPoolSize = 40;

        public RedisPoolConfig() {
            // maybe a bit overkill? could be deprecated if you add max int on RedisManagerPool
            this.MaxPoolSize = RedisConfig.DefaultMaxPoolSize ?? DefaultMaxPoolSize;
        }

        /// <summary>
        ///     Maximum ammount of <see cref="ICacheClient" />s created by the <see cref="RedisManagerPool" />.
        /// </summary>
        public int MaxPoolSize { get; set; }

    }

}
