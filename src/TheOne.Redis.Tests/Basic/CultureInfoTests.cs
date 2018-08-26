using System.Globalization;
using NUnit.Framework;
#if NET46
using System.Threading;
#endif

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class CultureInfoTests : RedisClientTestsBase {

        private CultureInfo _previousCulture = CultureInfo.InvariantCulture;

        [OneTimeSetUp]
        public void OneTimeSetUp() {
#if NETCOREAPP2_1
            this._previousCulture = CultureInfo.CurrentCulture;
            CultureInfo.CurrentCulture = new CultureInfo("fr-FR");
#elif NET46
            this._previousCulture = Thread.CurrentThread.CurrentCulture;
            Thread.CurrentThread.CurrentCulture = new CultureInfo("fr-FR");
            Thread.CurrentThread.CurrentUICulture = new CultureInfo("fr-FR");
#endif
        }

        [OneTimeTearDown]
        public void OneTimeTearDown() {
#if NETCOREAPP2_1
            CultureInfo.CurrentCulture = this._previousCulture;
#elif NET46
            Thread.CurrentThread.CurrentCulture = this._previousCulture;
#endif
        }

        [Test]
        public void Can_AddItemToSortedSet_in_different_Culture() {
            this.Redis.AddItemToSortedSet("somekey1", "somevalue", 66121.202);
            var score = this.Redis.GetItemScoreInSortedSet("somekey1", "somevalue");
            Assert.That(score, Is.EqualTo(66121.202));
        }

    }

}
