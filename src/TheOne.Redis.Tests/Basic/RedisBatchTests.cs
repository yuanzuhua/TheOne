using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class RedisBatchTests : RedisClientTestsBase {

        #region Models

        public class Message {

            public long Id { get; set; }
            public string Key { get; set; }
            public string Value { get; set; }
            public string Description { get; set; }

        }

        #endregion

        [Test]
        public void Store_batch_items_in_List() {
            IRedisTypedClient<Message> redisMessages = this.Redis.As<Message>();
            const int batchSize = 500;
            var nextIds = redisMessages.GetNextSequence(batchSize);

            List<Message> msgBatch = Enumerable.Range(0, batchSize).Select(i =>
                new Message {
                    Id = nextIds - (batchSize - i) + 1,
                    Key = i.ToString(),
                    Value = Guid.NewGuid().ToString(),
                    Description = "Description"
                }).ToList();

            redisMessages.Lists["listName"].AddRange(msgBatch);

            List<Message> msgs = redisMessages.Lists["listName"].GetAll();
            Assert.That(msgs.Count, Is.EqualTo(batchSize));

            Assert.That(msgs.First().Id, Is.EqualTo(1));
            Assert.That(msgs.Last().Id, Is.EqualTo(500));
        }

    }

}
