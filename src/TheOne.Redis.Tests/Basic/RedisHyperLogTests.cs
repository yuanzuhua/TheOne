using System;
using NUnit.Framework;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class RedisHyperLogTests : RedisClientTestsBase {

        [Test]
        public void Can_Add_to_HyperLog() {
            this.Redis.AddToHyperLog("hyperlog", "a", "b", "c");
            this.Redis.AddToHyperLog("hyperlog", "c", "d");

            var count = this.Redis.CountHyperLog("hyperlog");

            Assert.That(count, Is.EqualTo(4));

            this.Redis.AddToHyperLog("hyperlog2", "c", "d", "e", "f");

            this.Redis.MergeHyperLogs("hypermerge", "hyperlog", "hyperlog2");

            var mergeCount = this.Redis.CountHyperLog("hypermerge");

            Assert.That(mergeCount, Is.EqualTo(6));
        }

        [Test]
        public void Test_on_old_RedisServer() {

            // Redis.ExpireEntryIn("key", TimeSpan.FromDays(14));

            this.Redis.Set("key", "value", TimeSpan.FromDays(14));

            byte[] value = this.Redis.Get("key");

            Console.WriteLine(value.FromUtf8Bytes());
        }

    }

}
