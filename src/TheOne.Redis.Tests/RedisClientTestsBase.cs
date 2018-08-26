using NUnit.Framework;
using TheOne.Redis.Client;

namespace TheOne.Redis.Tests {

    internal abstract class RedisClientTestsBase : RedisTestBase {

        protected RedisClient Redis;

        [SetUp]
        public virtual void SetUp() {
            this.Redis = new RedisClient(Config.MasterHost);
        }

        [TearDown]
        public virtual void TearDown() {
            this.Redis.FlushAll();
            this.Redis?.Dispose();
            this.Redis = null;
        }

        protected string PrefixedKey(string key) {
            return string.Concat(this.Redis.NamespacePrefix, key);
        }

    }

}
