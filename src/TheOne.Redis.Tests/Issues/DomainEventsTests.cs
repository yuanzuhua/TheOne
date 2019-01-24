using System;
using System.Collections.Generic;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Issues {

    [TestFixture]
    [Ignore("Could not create an instance of type DomainEvent. Type is an interface or abstract class and cannot be instantiated")]
    internal sealed class DomainEventsTests {

        #region Models

        public class AggregateEvents {

            public Guid Id { get; set; }
            public List<DomainEvent> Events { get; set; }

        }

        public abstract class DomainEvent { }

        public class UserPromotedEvent : DomainEvent {

            public Guid UserId { get; set; }
            public string NewRole { get; set; }

        }

        public class UserRegisteredEvent : DomainEvent {

            public Guid UserId { get; set; }
            public string Name { get; set; }

        }

        #endregion

        [Test]
        public void Can_deserialize_DomainEvent_into_Concrete_Type() {
            var userId = Guid.NewGuid();
            var dto = (DomainEvent)new UserPromotedEvent { UserId = userId };
            var json = dto.ToJson();
            Console.WriteLine(json);
            var userPromoEvent = json.FromJson<DomainEvent>();
            Console.WriteLine(userPromoEvent.ToJson());
        }

        [Test]
        public void Can_from_Retrieve_DomainEvents_list() {
            var client = new RedisClient(Config.MasterHost);
            var users = client.As<AggregateEvents>();

            var userId = Guid.NewGuid();

            var eventsForUser = new AggregateEvents {
                Id = userId,
                Events = new List<DomainEvent>()
            };

            eventsForUser.Events.Add(new UserPromotedEvent { UserId = userId });

            users.Store(eventsForUser);

            var all = users.GetAll();
        }

        [Test]
        public void Can_Retrieve_DomainEvents() {
            var userId = Guid.NewGuid();
            var client = new RedisClient(Config.MasterHost);
            client.FlushAll();

            client.As<DomainEvent>().Lists["urn:domainevents-" + userId].Add(new UserPromotedEvent { UserId = userId });


            var users = client.As<DomainEvent>().Lists["urn:domainevents-" + userId];

            Assert.That(users.Count, Is.EqualTo(1));

            var userPromoEvent = (UserPromotedEvent)users[0];
            Assert.That(userPromoEvent.UserId, Is.EqualTo(userId));
        }

    }

}
