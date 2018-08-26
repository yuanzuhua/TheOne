namespace TheOne.Redis.Tests {

    public static class Config {

        public const string MasterHost = "123456@127.0.0.1:6379";
        public const string SlaveHost = "123456@127.0.0.1:6378";
        public const string Localhost = "127.0.0.1";
        public const int LocalhostPort = 6379;
        public const string LocalhostPassword = "123456";
        public const string LocalhostWithPassword = "123456@127.0.0.1";

        public const string SentinelMasterName = "mymaster";
        public const string Sentinel6380 = "127.0.0.1:6380";
        public const string Sentinel6381 = "127.0.0.1:6381";
        public const string Sentinel6382 = "127.0.0.1:6382";
        public const string Sentinel26380 = "127.0.0.1:26380";
        public const string Sentinel26381 = "127.0.0.1:26381";
        public const string Sentinel26382 = "127.0.0.1:26382";

        public static readonly string[] MasterHosts = { Sentinel6380 };
        public static readonly string[] SlaveHosts = { Sentinel6381, Sentinel6382 };
        public static readonly string[] SentinelHosts = { Sentinel26380, Sentinel26381, Sentinel26382 };

    }

}
