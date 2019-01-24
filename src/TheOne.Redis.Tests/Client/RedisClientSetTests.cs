using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisClientSetTests : RedisClientTestsBase {

        private const string _setIdSuffix = "testset";
        private List<string> _storeMembers;

        private string SetId => this.PrefixedKey(_setIdSuffix);

        [SetUp]
        public override void SetUp() {
            base.SetUp();
            this.Redis.NamespacePrefix = "RedisClientSetTests";
            this._storeMembers = new List<string> { "one", "two", "three", "four" };
        }

        [Test]
        public void Can_Add_to_ICollection_Set() {
            var list = this.Redis.Sets[this.SetId];
            this._storeMembers.ForEach(list.Add);

            var members = list.ToList();
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_AddRangeToSet_and_GetAllFromSet() {
            this.Redis.AddRangeToSet(this.SetId, this._storeMembers);

            var members = this.Redis.GetAllItemsFromSet(this.SetId);
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_AddToSet_and_GetAllFromSet() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSet(this.SetId, x));

            var members = this.Redis.GetAllItemsFromSet(this.SetId);
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_Clear_ICollection_Set() {
            var list = this.Redis.Sets[this.SetId];
            this._storeMembers.ForEach(list.Add);

            Assert.That(list.Count, Is.EqualTo(this._storeMembers.Count));

            list.Clear();

            Assert.That(list.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_DiffBetweenSets() {
            var set1Name = this.PrefixedKey("testdiffset1");
            var set2Name = this.PrefixedKey("testdiffset2");
            var set3Name = this.PrefixedKey("testdiffset3");
            var set1Members = new List<string> { "one", "two", "three", "four", "five" };
            var set2Members = new List<string> { "four", "five", "six", "seven" };
            var set3Members = new List<string> { "one", "five", "seven", "eleven" };

            set1Members.ForEach(x => this.Redis.AddItemToSet(set1Name, x));
            set2Members.ForEach(x => this.Redis.AddItemToSet(set2Name, x));
            set3Members.ForEach(x => this.Redis.AddItemToSet(set3Name, x));

            var diffMembers = this.Redis.GetDifferencesFromSet(set1Name, set2Name, set3Name);

            Assert.That(diffMembers, Is.EquivalentTo(new List<string> { "two", "three" }));
        }

        [Test]
        public void Can_enumerate_large_ICollection_Set() {
            const int setSize = 2500;

            this._storeMembers = new List<string>();
            for (var i = 0; i < setSize; i++) {
                this.Redis.AddItemToSet(this.SetId, i.ToString());
                this._storeMembers.Add(i.ToString());
            }

            var members = new List<string>();

            foreach (var item in this.Redis.Sets[this.SetId].ToList()) {
                members.Add(item);
            }

            members.Sort((x, y) => int.Parse(x).CompareTo(int.Parse(y)));

            Assert.That(members.Count, Is.EqualTo(this._storeMembers.Count));
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_enumerate_small_ICollection_Set() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSet(this.SetId, x));

            var members = new List<string>();
            foreach (var item in this.Redis.Sets[this.SetId]) {
                members.Add(item);
            }

            members.Sort();
            Assert.That(members.Count, Is.EqualTo(this._storeMembers.Count));
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_GetRandomEntryFromSet() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSet(this.SetId, x));

            var randomEntry = this.Redis.GetRandomItemFromSet(this.SetId);

            Assert.That(this._storeMembers.Contains(randomEntry), Is.True);
        }

        [Test]
        public void Can_GetSetCount() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSet(this.SetId, x));

            var setCount = this.Redis.GetSetCount(this.SetId);

            Assert.That(setCount, Is.EqualTo(this._storeMembers.Count));
        }

        [Test]
        public void Can_IntersectBetweenSets() {
            var set1Name = this.PrefixedKey("testintersectset1");
            var set2Name = this.PrefixedKey("testintersectset2");
            var set1Members = new List<string> { "one", "two", "three", "four", "five" };
            var set2Members = new List<string> { "four", "five", "six", "seven" };

            set1Members.ForEach(x => this.Redis.AddItemToSet(set1Name, x));
            set2Members.ForEach(x => this.Redis.AddItemToSet(set2Name, x));

            var intersectingMembers = this.Redis.GetIntersectFromSets(set1Name, set2Name);

            Assert.That(intersectingMembers, Is.EquivalentTo(new List<string> { "four", "five" }));
        }

        [Test]
        public void Can_MoveBetweenSets() {
            var fromSetId = this.PrefixedKey("testmovefromset");
            var toSetId = this.PrefixedKey("testmovetoset");
            const string moveMember = "four";
            var fromSetIdMembers = new List<string> { "one", "two", "three", "four" };
            var toSetIdMembers = new List<string> { "five", "six", "seven" };

            fromSetIdMembers.ForEach(x => this.Redis.AddItemToSet(fromSetId, x));
            toSetIdMembers.ForEach(x => this.Redis.AddItemToSet(toSetId, x));

            this.Redis.MoveBetweenSets(fromSetId, toSetId, moveMember);

            fromSetIdMembers.Remove(moveMember);
            toSetIdMembers.Add(moveMember);

            var readFromSetId = this.Redis.GetAllItemsFromSet(fromSetId);
            var readToSetId = this.Redis.GetAllItemsFromSet(toSetId);

            Assert.That(readFromSetId, Is.EquivalentTo(fromSetIdMembers));
            Assert.That(readToSetId, Is.EquivalentTo(toSetIdMembers));
        }

        [Test]
        public void Can_PopFromSet() {
            this._storeMembers.ForEach(x => this.Redis.AddItemToSet(this.SetId, x));

            var member = this.Redis.PopItemFromSet(this.SetId);

            Assert.That(this._storeMembers.Contains(member), Is.True);
        }

        [Test]
        public void Can_Remove_value_from_ICollection_Set() {
            var list = this.Redis.Sets[this.SetId];
            this._storeMembers.ForEach(list.Add);

            this._storeMembers.Remove("two");
            list.Remove("two");

            var members = list.ToList();

            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_RemoveFromSet() {
            const string removeMember = "two";

            this._storeMembers.ForEach(x => this.Redis.AddItemToSet(this.SetId, x));

            this.Redis.RemoveItemFromSet(this.SetId, removeMember);

            this._storeMembers.Remove(removeMember);

            var members = this.Redis.GetAllItemsFromSet(this.SetId);
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Can_Store_DiffBetweenSets() {
            var set1Name = this.PrefixedKey("testdiffset1");
            var set2Name = this.PrefixedKey("testdiffset2");
            var set3Name = this.PrefixedKey("testdiffset3");
            var storeSetName = this.PrefixedKey("testdiffsetstore");
            var set1Members = new List<string> { "one", "two", "three", "four", "five" };
            var set2Members = new List<string> { "four", "five", "six", "seven" };
            var set3Members = new List<string> { "one", "five", "seven", "eleven" };

            set1Members.ForEach(x => this.Redis.AddItemToSet(set1Name, x));
            set2Members.ForEach(x => this.Redis.AddItemToSet(set2Name, x));
            set3Members.ForEach(x => this.Redis.AddItemToSet(set3Name, x));

            this.Redis.StoreDifferencesFromSet(storeSetName, set1Name, set2Name, set3Name);

            var diffMembers = this.Redis.GetAllItemsFromSet(storeSetName);

            Assert.That(diffMembers,
                Is.EquivalentTo(
                    new List<string> { "two", "three" }));
        }

        [Test]
        public void Can_Store_IntersectBetweenSets() {
            var set1Name = this.PrefixedKey("testintersectset1");
            var set2Name = this.PrefixedKey("testintersectset2");
            var storeSetName = this.PrefixedKey("testintersectsetstore");
            var set1Members = new List<string> { "one", "two", "three", "four", "five" };
            var set2Members = new List<string> { "four", "five", "six", "seven" };

            set1Members.ForEach(x => this.Redis.AddItemToSet(set1Name, x));
            set2Members.ForEach(x => this.Redis.AddItemToSet(set2Name, x));

            this.Redis.StoreIntersectFromSets(storeSetName, set1Name, set2Name);

            var intersectingMembers = this.Redis.GetAllItemsFromSet(storeSetName);

            Assert.That(intersectingMembers, Is.EquivalentTo(new List<string> { "four", "five" }));
        }

        [Test]
        public void Can_Store_UnionBetweenSets() {
            var set1Name = this.PrefixedKey("testunionset1");
            var set2Name = this.PrefixedKey("testunionset2");
            var storeSetName = this.PrefixedKey("testunionsetstore");
            var set1Members = new List<string> { "one", "two", "three", "four", "five" };
            var set2Members = new List<string> { "four", "five", "six", "seven" };

            set1Members.ForEach(x => this.Redis.AddItemToSet(set1Name, x));
            set2Members.ForEach(x => this.Redis.AddItemToSet(set2Name, x));

            this.Redis.StoreUnionFromSets(storeSetName, set1Name, set2Name);

            var unionMembers = this.Redis.GetAllItemsFromSet(storeSetName);

            Assert.That(unionMembers,
                Is.EquivalentTo(
                    new List<string> { "one", "two", "three", "four", "five", "six", "seven" }));
        }

        [Test]
        public void Can_Test_Contains_in_ICollection_Set() {
            var list = this.Redis.Sets[this.SetId];
            this._storeMembers.ForEach(list.Add);

            Assert.That(list.Contains("two"), Is.True);
            Assert.That(list.Contains("five"), Is.False);
        }

        [Test]
        public void Can_UnionBetweenSets() {
            var set1Name = this.PrefixedKey("testunionset1");
            var set2Name = this.PrefixedKey("testunionset2");
            var set1Members = new List<string> { "one", "two", "three", "four", "five" };
            var set2Members = new List<string> { "four", "five", "six", "seven" };

            set1Members.ForEach(x => this.Redis.AddItemToSet(set1Name, x));
            set2Members.ForEach(x => this.Redis.AddItemToSet(set2Name, x));

            var unionMembers = this.Redis.GetUnionFromSets(set1Name, set2Name);

            Assert.That(unionMembers,
                Is.EquivalentTo(
                    new List<string> { "one", "two", "three", "four", "five", "six", "seven" }));
        }

        [Test]
        public void Does_SetContainsValue() {
            const string existingMember = "two";
            const string nonExistingMember = "five";

            this._storeMembers.ForEach(x => this.Redis.AddItemToSet(this.SetId, x));

            Assert.That(this.Redis.SetContainsItem(this.SetId, existingMember), Is.True);
            Assert.That(this.Redis.SetContainsItem(this.SetId, nonExistingMember), Is.False);
        }

    }

}
