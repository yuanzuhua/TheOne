using System;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.ClientManager;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Examples {

    [TestFixture]
    internal sealed class SimpleUseCase : RedisTestBase {

        #region Models

        public class Todo {

            public long Id { get; set; }
            public string Content { get; set; }
            public int Order { get; set; }
            public bool Done { get; set; }

        }

        #endregion

        [Test]
        public void Can_Add_Update_and_Delete_Todo_item() {
            using (var redisManager = new PooledRedisClientManager(Config.MasterHost)) {
                using (IRedisClient redis = redisManager.GetClient()) {
                    IRedisTypedClient<Todo> redisTodos = redis.As<Todo>();
                    var todo = new Todo {
                        Id = redisTodos.GetNextSequence(),
                        Content = "Learn Redis",
                        Order = 1
                    };

                    redisTodos.Store(todo);

                    Todo savedTodo = redisTodos.GetById(todo.Id);
                    savedTodo.Done = true;
                    redisTodos.Store(savedTodo);

                    Console.WriteLine("Updated Todo:");
                    Console.WriteLine(redisTodos.GetAll().ToList().ToJson());

                    redisTodos.DeleteById(savedTodo.Id);

                    Console.WriteLine("No more Todos:");
                    Console.WriteLine(redisTodos.GetAll().ToList().ToJson());
                }
            }
        }

    }

}
