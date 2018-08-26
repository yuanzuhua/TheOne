using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Shared;

namespace TheOne.Redis.Tests.Examples {

    [TestFixture]
    internal sealed class ShippersExample {

        #region Models

        public class Shipper {

            public long Id { get; set; }
            public string CompanyName { get; set; }
            public DateTime DateCreated { get; set; }
            public ShipperType ShipperType { get; set; }
            public Guid UniqueRef { get; set; }

        }

        #endregion

        private static void Dump<T>(string message, T entity) {
            var text = entity.ToJson();

            // make it a little easier on the eyes
            List<string> prettyLines = text.Split(new[] { "[", "},{", "]" },
                                               StringSplitOptions.RemoveEmptyEntries)
                                           .ToList().ConvertAll(x => x.Replace("{", "").Replace("}", ""));

            Console.WriteLine("\n" + message);
            foreach (var l in prettyLines) {
                Console.WriteLine(l);
            }
        }

        [Test]
        public void Shippers_UseCase() {
            using (var redisClient = new RedisClient(Config.MasterHost)) {
                // Create a 'strongly-typed' API that makes all Redis Value operations to apply against Shippers
                IRedisTypedClient<Shipper> redis = redisClient.As<Shipper>();

                // Redis lists implement IList<T> while Redis sets implement ICollection<T>
                IRedisList<Shipper> currentShippers = redis.Lists["urn:shippers:current"];
                IRedisList<Shipper> prospectiveShippers = redis.Lists["urn:shippers:prospective"];

                currentShippers.Add(
                    new Shipper {
                        Id = redis.GetNextSequence(),
                        CompanyName = "Trains R Us",
                        DateCreated = DateTime.UtcNow,
                        ShipperType = ShipperType.Trains,
                        UniqueRef = Guid.NewGuid()
                    });

                currentShippers.Add(
                    new Shipper {
                        Id = redis.GetNextSequence(),
                        CompanyName = "Planes R Us",
                        DateCreated = DateTime.UtcNow,
                        ShipperType = ShipperType.Planes,
                        UniqueRef = Guid.NewGuid()
                    });

                var lameShipper = new Shipper {
                    Id = redis.GetNextSequence(),
                    CompanyName = "We do everything!",
                    DateCreated = DateTime.UtcNow,
                    ShipperType = ShipperType.All,
                    UniqueRef = Guid.NewGuid()
                };

                currentShippers.Add(lameShipper);

                Dump("ADDED 3 SHIPPERS:", currentShippers);

                currentShippers.Remove(lameShipper);

                Dump("REMOVED 1:", currentShippers);

                prospectiveShippers.Add(
                    new Shipper {
                        Id = redis.GetNextSequence(),
                        CompanyName = "Trucks R Us",
                        DateCreated = DateTime.UtcNow,
                        ShipperType = ShipperType.Automobiles,
                        UniqueRef = Guid.NewGuid()
                    });

                Dump("ADDED A PROSPECTIVE SHIPPER:", prospectiveShippers);

                redis.PopAndPushItemBetweenLists(prospectiveShippers, currentShippers);

                Dump("CURRENT SHIPPERS AFTER POP n' PUSH:", currentShippers);
                Dump("PROSPECTIVE SHIPPERS AFTER POP n' PUSH:", prospectiveShippers);

                Shipper poppedShipper = redis.PopItemFromList(currentShippers);
                Dump("POPPED a SHIPPER:", poppedShipper);
                Dump("CURRENT SHIPPERS AFTER POP:", currentShippers);

                // reset sequence and delete all lists
                redis.SetSequence(0);
                redis.RemoveEntry(currentShippers, prospectiveShippers);
                Dump("DELETING CURRENT AND PROSPECTIVE SHIPPERS:", currentShippers);
            }

        }

    }

}
