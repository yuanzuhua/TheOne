namespace TheOne.Redis.ClientManager {

    public class RedisClientManagerConfig {

        public RedisClientManagerConfig() {
            // Simplifies the most common use-case - registering in an IOC
            this.AutoStart = true;
        }

        public long? DefaultDb { get; set; }
        public int MaxReadPoolSize { get; set; }
        public int MaxWritePoolSize { get; set; }
        public bool AutoStart { get; set; }

    }

}
