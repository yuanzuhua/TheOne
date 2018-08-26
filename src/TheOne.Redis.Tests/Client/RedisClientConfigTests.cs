using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisClientConfigTests : RedisClientTestsBase {

        [Test]
        public void Can_get_Role_Info() {
            RedisText result = this.Redis.Role();
            Console.WriteLine(result.ToJson());
            Assert.That(result.Children[0].Text, Is.EqualTo("master"));
            Assert.That(this.Redis.GetServerRole(), Is.EqualTo(RedisServerRole.Master));

            using (var slave = new RedisClient(Config.SlaveHost)) {
                result = slave.Role();
                Assert.That(result.Children[0].Text, Is.EqualTo("slave"));
                Assert.That(slave.GetServerRole(), Is.EqualTo(RedisServerRole.Slave));
            }

        }

        [Test]
        public void Can_GetClientsInfo() {
            List<Dictionary<string, string>> clientList = this.Redis.GetClientsInfo();
            Console.WriteLine(clientList.ToJson());
        }

        [Test]
        public void Can_Kill_Client() {
            List<Dictionary<string, string>> clientList = this.Redis.GetClientsInfo();
            var firstAddr = clientList.First()["addr"];
            this.Redis.KillClient(firstAddr);
        }

        [Test]
        public void Can_Kill_Clients() {
            this.Redis.KillClients("192.168.0.1:6379");
            this.Redis.KillClients(withId: "1");
            this.Redis.KillClients(ofType: RedisClientType.Normal);
            this.Redis.KillClients(ofType: RedisClientType.PubSub);
            this.Redis.KillClients(ofType: RedisClientType.Slave);
            this.Redis.KillClients(skipMe: true);
            this.Redis.KillClients("192.168.0.1:6379", "1", RedisClientType.Normal);
            this.Redis.KillClients(skipMe: false);
        }

        [Test]
        public void Can_PauseAllClients() {
            using (var slave = new RedisClient(Config.SlaveHost)) {
                slave.PauseAllClients(TimeSpan.FromSeconds(2));
            }
        }

        [Test]
        public void Can_Rewrite_Info_Stats() {
            this.Redis.ResetInfoStats();
        }

        [Test]
        public void Can_Rewrite_Redis_Config() {
            try {
                this.Redis.SaveConfig();
            } catch (RedisResponseException ex) {
                // if (ex.Message.Contains("The server is running without a config file")) {
                //     return;
                // }

                if (ex.Message.Contains("Rewriting config file: Permission denied")) {
                    return;
                }

                throw;
            }
        }

        [Test]
        public void Can_set_and_Get_Client_Name() {
            var clientName = "CLIENT-" + Environment.TickCount;
            this.Redis.SetClient(clientName);
            var client = this.Redis.GetClient();

            Assert.That(client, Is.EqualTo(clientName));
        }

        [Test]
        [Ignore("Hurts MSOpenTech Redis Server")]
        public void Can_Set_and_Get_Config() {
            var orig = this.Redis.GetConfig("maxmemory");
            var newMaxMemory = (long.Parse(orig) + 1).ToString();
            this.Redis.SetConfig("maxmemory", newMaxMemory);
            var current = this.Redis.GetConfig("maxmemory");
            Assert.That(current, Is.EqualTo(newMaxMemory));
        }

    }

}
