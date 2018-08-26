using System;
using System.Collections.Generic;
using System.Linq;

namespace TheOne.Redis.Sentinel {

    public class SentinelInfo {

        public SentinelInfo(string masterName, IEnumerable<string> redisMasters, IEnumerable<string> redisSlaves) {
            this.MasterName = masterName;
            this.RedisMasters = redisMasters?.ToArray() ?? Array.Empty<string>();
            this.RedisSlaves = redisSlaves?.ToArray() ?? Array.Empty<string>();
        }

        public string MasterName { get; set; }
        public string[] RedisMasters { get; set; }
        public string[] RedisSlaves { get; set; }

        public override string ToString() {
            return $"{this.MasterName} masters: {string.Join(", ", this.RedisMasters)}, slaves: {string.Join(", ", this.RedisSlaves)}";
        }

    }

}
