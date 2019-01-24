using NUnit.Framework;
using TheOne.Redis.Client;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class RedisGeoTests : RedisClientTestsBase {

        [Test]
        public void CalculateDistanceBetweenGeoMembers_on_NonExistingMember_returns_NaN() {
            this.Redis.AddGeoMembers("Sicily",
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));

            var distance = this.Redis.CalculateDistanceBetweenGeoMembers("Sicily", "Palermo", "NonExistingMember");
            Assert.That(distance, Is.EqualTo(double.NaN));
        }

        [Test]
        public void Can_AddGeoMember_and_GetGeoCoordinates() {
            var count = this.Redis.AddGeoMember("Sicily", 13.361389, 38.115556, "Palermo");
            Assert.That(count, Is.EqualTo(1));
            var results = this.Redis.GetGeoCoordinates("Sicily", "Palermo");

            Assert.That(results.Count, Is.EqualTo(1));
            Assert.That(results[0].Longitude, Is.EqualTo(13.361389).Within(.1));
            Assert.That(results[0].Latitude, Is.EqualTo(38.115556).Within(.1));
            Assert.That(results[0].Member, Is.EqualTo("Palermo"));
        }

        [Test]
        public void Can_AddGeoMembers_and_GetGeoCoordinates_multiple() {
            var count = this.Redis.AddGeoMembers("Sicily",
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));
            Assert.That(count, Is.EqualTo(2));

            var results = this.Redis.GetGeoCoordinates("Sicily", "Palermo", "Catania");

            Assert.That(results.Count, Is.EqualTo(2));
            Assert.That(results[0].Longitude, Is.EqualTo(13.361389).Within(.1));
            Assert.That(results[0].Latitude, Is.EqualTo(38.115556).Within(.1));
            Assert.That(results[0].Member, Is.EqualTo("Palermo"));

            Assert.That(results[1].Longitude, Is.EqualTo(15.087269).Within(.1));
            Assert.That(results[1].Latitude, Is.EqualTo(37.502669).Within(.1));
            Assert.That(results[1].Member, Is.EqualTo("Catania"));
        }

        [Test]
        public void Can_CalculateDistanceBetweenGeoMembers() {
            this.Redis.AddGeoMembers("Sicily",
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));

            var distance = this.Redis.CalculateDistanceBetweenGeoMembers("Sicily", "Palermo", "Catania");
            Assert.That(distance, Is.EqualTo(166274.15156960039).Within(.1));
        }

        [Test]
        public void Can_FindGeoMembersInRadius() {
            this.Redis.GeoAdd("Sicily",
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));

            var results = this.Redis.FindGeoMembersInRadius("Sicily", 15, 37, 200, RedisGeoUnit.Kilometers);

            Assert.That(results.Length, Is.EqualTo(2));
            Assert.That(results[0], Is.EqualTo("Palermo"));
            Assert.That(results[1], Is.EqualTo("Catania"));
        }

        [Test]
        public void Can_FindGeoResultsInRadius() {
            this.Redis.GeoAdd("Sicily",
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));

            var results = this.Redis.FindGeoResultsInRadius("Sicily", 15, 37, 200, RedisGeoUnit.Kilometers);

            Assert.That(results.Count, Is.EqualTo(2));
            Assert.That(results[0].Member, Is.EqualTo("Palermo"));
            Assert.That(results[0].Unit, Is.EqualTo(RedisGeoUnit.Kilometers));
            Assert.That(results[0].Longitude, Is.EqualTo(13.361389).Within(.1));
            Assert.That(results[0].Latitude, Is.EqualTo(38.115556).Within(.1));
            Assert.That(results[0].Distance, Is.EqualTo(190.4424).Within(.1));
            Assert.That(results[0].Hash, Is.EqualTo(3479099956230698));

            Assert.That(results[1].Member, Is.EqualTo("Catania"));
            Assert.That(results[1].Unit, Is.EqualTo(RedisGeoUnit.Kilometers));
            Assert.That(results[1].Longitude, Is.EqualTo(15.087269).Within(.1));
            Assert.That(results[1].Latitude, Is.EqualTo(37.502669).Within(.1));
            Assert.That(results[1].Distance, Is.EqualTo(56.4413).Within(.1));
            Assert.That(results[1].Hash, Is.EqualTo(3479447370796909));
        }

        [Test]
        public void Can_FindGeoResultsInRadius_by_Member() {
            this.Redis.GeoAdd("Sicily",
                new RedisGeo(13.583333, 37.316667, "Agrigento"),
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));

            var results = this.Redis.FindGeoResultsInRadius("Sicily", "Agrigento", 100, RedisGeoUnit.Kilometers);

            Assert.That(results.Count, Is.EqualTo(2));
            Assert.That(results[0].Member, Is.EqualTo("Agrigento"));
            Assert.That(results[0].Unit, Is.EqualTo(RedisGeoUnit.Kilometers));
            Assert.That(results[0].Longitude, Is.EqualTo(13.583333).Within(.1));
            Assert.That(results[0].Latitude, Is.EqualTo(37.316667).Within(.1));
            Assert.That(results[0].Distance, Is.EqualTo(0));
            Assert.That(results[0].Hash, Is.EqualTo(3479030013248308));

            Assert.That(results[1].Member, Is.EqualTo("Palermo"));
            Assert.That(results[1].Unit, Is.EqualTo(RedisGeoUnit.Kilometers));
            Assert.That(results[1].Longitude, Is.EqualTo(13.361389).Within(.1));
            Assert.That(results[1].Latitude, Is.EqualTo(38.115556).Within(.1));
            Assert.That(results[1].Distance, Is.EqualTo(90.9778).Within(.1));
            Assert.That(results[1].Hash, Is.EqualTo(3479099956230698));
        }

        [Test]
        public void Can_GeoRadius_WithCoord_WithDist_WithHash_Count_and_Asc() {
            this.Redis.GeoAdd("Sicily",
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));

            var results = this.Redis.FindGeoResultsInRadius("Sicily",
                15,
                37,
                200,
                RedisGeoUnit.Kilometers,
                1,
                false);

            Assert.That(results.Count, Is.EqualTo(1));
            Assert.That(results[0].Member, Is.EqualTo("Palermo"));
            Assert.That(results[0].Unit, Is.EqualTo(RedisGeoUnit.Kilometers));
            Assert.That(results[0].Longitude, Is.EqualTo(13.361389).Within(.1));
            Assert.That(results[0].Latitude, Is.EqualTo(38.115556).Within(.1));
            Assert.That(results[0].Distance, Is.EqualTo(190.4424).Within(.1));
            Assert.That(results[0].Hash, Is.EqualTo(3479099956230698));

            results = this.Redis.FindGeoResultsInRadius("Sicily",
                15,
                37,
                200,
                RedisGeoUnit.Kilometers,
                1,
                true);

            Assert.That(results.Count, Is.EqualTo(1));
            Assert.That(results[0].Member, Is.EqualTo("Catania"));
            Assert.That(results[0].Unit, Is.EqualTo(RedisGeoUnit.Kilometers));
            Assert.That(results[0].Longitude, Is.EqualTo(15.087269).Within(.1));
            Assert.That(results[0].Latitude, Is.EqualTo(37.502669).Within(.1));
            Assert.That(results[0].Distance, Is.EqualTo(56.4413).Within(.1));
            Assert.That(results[0].Hash, Is.EqualTo(3479447370796909));
        }

        [Test]
        public void Can_GeoRadiusByMember() {
            this.Redis.GeoAdd("Sicily",
                new RedisGeo(13.583333, 37.316667, "Agrigento"),
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));

            var results = this.Redis.GeoRadiusByMember("Sicily", "Agrigento", 100, RedisGeoUnit.Kilometers);

            Assert.That(results.Count, Is.EqualTo(2));
            Assert.That(results[0].Member, Is.EqualTo("Agrigento"));
            Assert.That(results[0].Unit, Is.Null);
            Assert.That(results[1].Member, Is.EqualTo("Palermo"));
            Assert.That(results[1].Unit, Is.Null);
        }

        [Test]
        public void Can_GetGeohashes() {
            this.Redis.AddGeoMembers("Sicily",
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));

            var hashes = this.Redis.GetGeohashes("Sicily", "Palermo", "Catania");
            Assert.That(hashes[0], Is.EqualTo("sqc8b49rny0"));
            Assert.That(hashes[1], Is.EqualTo("sqdtr74hyu0"));

            hashes = this.Redis.GetGeohashes("Sicily", "Palermo", "NonExistingMember", "Catania");
            Assert.That(hashes[0], Is.EqualTo("sqc8b49rny0"));
            Assert.That(hashes[1], Is.Null);
            Assert.That(hashes[2], Is.EqualTo("sqdtr74hyu0"));
        }

        [Test]
        public void GetGeoCoordinates_on_NonExistingMember_returns_no_results() {
            this.Redis.AddGeoMember("Sicily", 13.361389, 38.115556, "Palermo");
            var results = this.Redis.GetGeoCoordinates("Sicily", "NonExistingMember");
            Assert.That(results.Count, Is.EqualTo(0));

            results = this.Redis.GetGeoCoordinates("Sicily", "Palermo", "NonExistingMember");
            Assert.That(results.Count, Is.EqualTo(1));
        }

    }

}
