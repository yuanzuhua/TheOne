using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Tests.Extensions;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisTypedClientAppTests : RedisClientTestsBase {

        #region Models

        public class Answer {

            public long Id { get; set; }
            public long QuestionId { get; set; }
            public string UserId { get; set; }
            public string Content { get; set; }

            public static Answer Create(long id, long questionId) {
                return new Answer {
                    Id = id,
                    QuestionId = questionId,
                    UserId = "User" + id,
                    Content = "Content" + id
                };
            }

            public bool Equals(Answer other) {
                if (ReferenceEquals(null, other)) {
                    return false;
                }

                if (ReferenceEquals(this, other)) {
                    return true;
                }

                return other.Id == this.Id && other.QuestionId == this.QuestionId && Equals(other.UserId, this.UserId) &&
                       Equals(other.Content, this.Content);
            }

            public override bool Equals(object obj) {
                if (ReferenceEquals(null, obj)) {
                    return false;
                }

                if (ReferenceEquals(this, obj)) {
                    return true;
                }

                if (obj.GetType() != typeof(Answer)) {
                    return false;
                }

                return this.Equals((Answer)obj);
            }

            public override int GetHashCode() {
                return this.Id.GetHashCode();
            }

        }

        public class Customer {

            public string Id { get; set; }
            public string Name { get; set; }

        }

        public class CustomerAddress {

            public string Id { get; set; }
            public string Address { get; set; }

        }

        public class Question {

            public long Id { get; set; }
            public string UserId { get; set; }
            public string Title { get; set; }
            public string Content { get; set; }

            public static Question Create(long id) {
                return new Question {
                    Id = id,
                    Content = "Content" + id,
                    Title = "Title" + id,
                    UserId = "User" + id
                };
            }

        }

        #endregion

        private readonly Question _question1 = Question.Create(1);
        private List<Answer> _q1Answers;

        private IRedisTypedClient<Question> _redisQuestions;

        public override void SetUp() {
            base.SetUp();

            this._redisQuestions = this.Redis.As<Question>();
            this._redisQuestions.Db = 10;

            this._q1Answers = new List<Answer> {
                Answer.Create(1, this._question1.Id),
                Answer.Create(2, this._question1.Id),
                Answer.Create(3, this._question1.Id),
                Answer.Create(4, this._question1.Id),
                Answer.Create(5, this._question1.Id)
            };
        }

        [Test]
        public void Can_AddToRecentsList() {
            var redisAnswers = this.Redis.As<Answer>();

            redisAnswers.StoreAll(this._q1Answers);
            this._q1Answers.ForEach(redisAnswers.AddToRecentsList);

            var allAnswers = redisAnswers.GetLatestFromRecentsList(0, int.MaxValue);
            allAnswers.Sort((x, y) => x.Id.CompareTo(y.Id));

            Assert.That(allAnswers.EquivalentTo(this._q1Answers));
        }

        [Test]
        public void Can_DeleteRelatedEntities() {
            this._redisQuestions.Store(this._question1);

            this._redisQuestions.StoreRelatedEntities(this._question1.Id, this._q1Answers);

            this._redisQuestions.DeleteRelatedEntities<Answer>(this._question1.Id);

            var answers = this._redisQuestions.GetRelatedEntities<Answer>(this._question1.Id);

            Assert.That(answers.Count, Is.EqualTo(0));
        }

        [Test]
        public void Can_DeleteRelatedEntity() {
            this._redisQuestions.Store(this._question1);

            this._redisQuestions.StoreRelatedEntities(this._question1.Id, this._q1Answers);

            var answerToDelete = this._q1Answers[3];
            this._redisQuestions.DeleteRelatedEntity<Answer>(this._question1.Id, answerToDelete.Id);

            this._q1Answers.RemoveAll(x => x.Id == answerToDelete.Id);

            var answers = this._redisQuestions.GetRelatedEntities<Answer>(this._question1.Id);

            Assert.That(answers.EquivalentTo(answers));
        }

        [Test]
        public void Can_GetEarliestFromRecentsList() {
            var redisAnswers = this.Redis.As<Answer>();

            redisAnswers.StoreAll(this._q1Answers);
            this._q1Answers.ForEach(redisAnswers.AddToRecentsList);

            var earliest3Answers = redisAnswers.GetEarliestFromRecentsList(0, 3);

            var i = 0;
            var expectedAnswers = new List<Answer> {
                this._q1Answers[i++],
                this._q1Answers[i++],
                this._q1Answers[i]
            };

            Assert.That(expectedAnswers.EquivalentTo(earliest3Answers));
        }

        [Test]
        public void Can_GetLatestFromRecentsList() {
            var redisAnswers = this.Redis.As<Answer>();

            redisAnswers.StoreAll(this._q1Answers);
            this._q1Answers.ForEach(redisAnswers.AddToRecentsList);

            var latest3Answers = redisAnswers.GetLatestFromRecentsList(0, 3);

            var i = this._q1Answers.Count;
            var expectedAnswers = new List<Answer> {
                this._q1Answers[--i],
                this._q1Answers[--i],
                this._q1Answers[--i]
            };

            Assert.That(expectedAnswers.EquivalentTo(latest3Answers));
        }

        [Test]
        public void Can_GetRelatedEntities_When_Empty() {
            this._redisQuestions.Store(this._question1);

            var answers = this._redisQuestions.GetRelatedEntities<Answer>(this._question1.Id);

            Assert.That(answers, Has.Count.EqualTo(0));
        }

        [Test]
        public void Can_StoreRelatedEntities() {
            this._redisQuestions.Store(this._question1);

            this._redisQuestions.StoreRelatedEntities(this._question1.Id, this._q1Answers);

            var actualAnswers = this._redisQuestions.GetRelatedEntities<Answer>(this._question1.Id);
            actualAnswers.Sort((x, y) => x.Id.CompareTo(y.Id));

            Assert.That(actualAnswers.EquivalentTo(this._q1Answers));
        }

        [Test]
        public void Can_StoreRelatedEntities_with_StringId() {
            var redisCustomers = this.Redis.As<Customer>();
            var customer = new Customer { Id = "CUST-01", Name = "Customer" };

            redisCustomers.Store(customer);

            var addresses = new[] {
                new CustomerAddress { Id = "ADDR-01", Address = "1 Home Street" },
                new CustomerAddress { Id = "ADDR-02", Address = "2 Work Road" }
            };

            redisCustomers.StoreRelatedEntities(customer.Id, addresses);

            var actualAddresses = redisCustomers.GetRelatedEntities<CustomerAddress>(customer.Id);

            Assert.That(actualAddresses.Select(x => x.Id).ToArray(),
                Is.EquivalentTo(new[] { "ADDR-01", "ADDR-02" }));
        }

    }

}
