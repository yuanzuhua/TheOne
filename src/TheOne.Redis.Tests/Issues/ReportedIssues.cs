using System.Collections.Generic;
using NUnit.Framework;
using TheOne.Redis.Pipeline;

namespace TheOne.Redis.Tests.Issues {

    [TestFixture]
    internal sealed class ReportedIssues : RedisClientTestsBase {

        private readonly List<string> _storeMembers = new List<string> { "one", "two", "three", "four" };

        [Test]
        public void Add_range_to_set_fails_if_first_command() {
            this.Redis.AddRangeToSet("testset", this._storeMembers);

            HashSet<string> members = this.Redis.GetAllItemsFromSet("testset");
            Assert.That(members, Is.EquivalentTo(this._storeMembers));
        }

        [Test]
        public void Success_callback_fails_for_pipeline_using_GetItemScoreInSortedSet() {
            double score = 0;
            this.Redis.AddItemToSortedSet("testzset", "value", 1);

            using (IRedisPipeline pipeline = this.Redis.CreatePipeline()) {
                pipeline.QueueCommand(u => u.GetItemScoreInSortedSet("testzset", "value"),
                    x => {
                        // score should be assigned to 1 here
                        score = x;
                    });

                pipeline.Flush();
            }

            Assert.That(score, Is.EqualTo(1));
        }

        [Test]
        public void Transaction_fails_if_first_command() {
            using (IRedisTransaction trans = this.Redis.CreateTransaction()) {
                trans.QueueCommand(r => r.IncrementValue("A"));

                trans.Commit();
            }

            Assert.That(this.Redis.GetValue("A"), Is.EqualTo("1"));
        }

    }

}
