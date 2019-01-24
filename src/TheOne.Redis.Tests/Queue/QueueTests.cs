using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using TheOne.Redis.Queue;

namespace TheOne.Redis.Tests.Queue {

    [TestFixture]
    internal sealed class QueueTests : RedisTestBase {

        private const int _numMessages = 6;
        private readonly IList<string> _messages0 = new List<string>();
        private readonly IList<string> _messages1 = new List<string>();
        private readonly string[] _patients = { "patient0", "patient1" };

        [SetUp]
        public void SetUp() {
            for (var i = 0; i < _numMessages; ++i) {
                this._messages0.Add(string.Format("{0}_message{1}", this._patients[0], i));
                this._messages1.Add(string.Format("{0}_message{1}", this._patients[1], i));
            }
        }

        [Test]
        public void TestChronologicalWorkQueue() {
            using (var queue =
                new RedisChronologicalWorkQueue<string>(10, 10, Config.LocalhostWithPassword, Config.LocalhostPort)) {
                const int numMessages = 6;
                var messages = new List<string>();
                var patients = new List<string>();

                for (var i = 0; i < numMessages; ++i) {
                    patients.Add(string.Format("patient{0}", i));
                    messages.Add(string.Format("{0}_message{1}", patients[i], i));
                    queue.Enqueue(patients[i], messages[i], i);
                }

                // dequeue half of the messages
                var batch = queue.Dequeue(0, numMessages, numMessages / 2);
                // check that half of patient[0] messages are returned
                for (var i = 0; i < numMessages / 2; ++i) {
                    Assert.AreEqual(batch[i].Value, messages[i]);
                }

                // dequeue the rest of the messages
                batch = queue.Dequeue(0, numMessages, 2 * numMessages);
                // check that batch size is respected
                Assert.AreEqual(batch.Count, numMessages / 2);
                for (var i = 0; i < numMessages / 2; ++i) {
                    Assert.AreEqual(batch[i].Value, messages[i + numMessages / 2]);
                }

                // check that there are no more messages in the queue
                batch = queue.Dequeue(0, numMessages, numMessages);
                Assert.AreEqual(batch.Count, 0);
            }
        }

        [Test]
        public void TestSequentialWorkQueue() {
            using (var queue =
                new RedisSequentialWorkQueue<string>(10, 10, Config.LocalhostWithPassword, Config.LocalhostPort, 1)) {
                for (var i = 0; i < _numMessages; ++i) {
                    queue.Enqueue(this._patients[0], this._messages0[i]);
                    queue.Enqueue(this._patients[1], this._messages1[i]);
                }

                queue.PrepareNextWorkItem();
                var batch = queue.Dequeue(_numMessages / 2);
                // check that half of patient[0] messages are returned
                for (var i = 0; i < _numMessages / 2; ++i) {
                    Assert.AreEqual(batch.DequeueItems[i], this._messages0[i]);
                }

                Assert.AreEqual(_numMessages / 2, batch.DequeueItems.Count);
                Thread.Sleep(5000);
                Assert.IsTrue(queue.HarvestZombies());
                for (var i = 0; i < batch.DequeueItems.Count; ++i) {
                    batch.DoneProcessedWorkItem();
                }

                // check that all patient[1] messages are returned
                queue.PrepareNextWorkItem();
                batch = queue.Dequeue(2 * _numMessages);
                // check that batch size is respected
                Assert.AreEqual(batch.DequeueItems.Count, _numMessages);
                for (var i = 0; i < _numMessages; ++i) {
                    Assert.AreEqual(batch.DequeueItems[i], this._messages1[i]);
                    batch.DoneProcessedWorkItem();
                }

                // check that there are numMessages/2 messages in the queue
                queue.PrepareNextWorkItem();
                batch = queue.Dequeue(_numMessages);
                Assert.AreEqual(batch.DequeueItems.Count, _numMessages / 2);

                // test pop and unlock
                batch.DoneProcessedWorkItem();
                var remaining = batch.DequeueItems.Count - 1;
                batch.PopAndUnlock();

                // process remaining items
                Assert.IsTrue(queue.PrepareNextWorkItem());
                batch = queue.Dequeue(remaining);
                Assert.AreEqual(batch.DequeueItems.Count, remaining);
                for (var i = 0; i < batch.DequeueItems.Count; ++i) {
                    batch.DoneProcessedWorkItem();
                }

                Assert.IsFalse(queue.PrepareNextWorkItem());
                batch = queue.Dequeue(remaining);
                Assert.AreEqual(batch.DequeueItems.Count, 0);
            }
        }

        [Test]
        public void TestSequentialWorkQueueUpdate() {
            using (var queue =
                new RedisSequentialWorkQueue<string>(10, 10, Config.LocalhostWithPassword, Config.LocalhostPort, 1)) {
                for (var i = 0; i < _numMessages; ++i) {
                    queue.Enqueue(this._patients[0], this._messages0[i]);
                    queue.Enqueue(this._patients[1], this._messages1[i]);
                }

                for (var i = 0; i < _numMessages / 2; ++i) {
                    queue.Update(this._patients[0], i, this._messages0[i] + "UPDATE");
                }

                queue.PrepareNextWorkItem();
                var batch = queue.Dequeue(_numMessages / 2);
                // check that half of patient[0] messages are returned
                for (var i = 0; i < _numMessages / 2; ++i) {
                    Assert.AreEqual(batch.DequeueItems[i], this._messages0[i] + "UPDATE");
                }
            }
        }

        [Test]
        public void TestSimpleWorkQueue() {
            using (var queue = new RedisSimpleWorkQueue<string>(10, 10, Config.LocalhostWithPassword, Config.LocalhostPort)) {
                var numMessages = 6;
                var messages = new string[numMessages];
                for (var i = 0; i < numMessages; ++i) {
                    messages[i] = string.Format("message#{0}", i);
                    queue.Enqueue(messages[i]);
                }

                var batch = queue.Dequeue(numMessages * 2);
                // test that batch size is respected
                Assert.AreEqual(batch.Count, numMessages);

                // test that messages are returned, in correct order
                for (var i = 0; i < numMessages; ++i) {
                    Assert.AreEqual(messages[i], batch[i]);
                }

                // test that messages were removed from queue
                batch = queue.Dequeue(numMessages * 2);
                Assert.AreEqual(batch.Count, 0);
            }
        }

    }

}
