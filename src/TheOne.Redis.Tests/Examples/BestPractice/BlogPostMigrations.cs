using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Tests.Examples.BestPractice.New;

namespace TheOne.Redis.Tests.Examples.BestPractice {

    namespace New {

        internal class BlogPost {

            public BlogPost() {
                this.Labels = new List<string>();
                this.Tags = new HashSet<string>();
                this.Comments = new List<Dictionary<string, string>>();
            }

            // Changed int types to both a long and a double type
            public long Id { get; set; }
            public double BlogId { get; set; }

            // Added new field
            public BlogPostType PostType { get; set; }

            public string Title { get; set; }
            public string Content { get; set; }

            // Renamed from 'Categories' to 'Labels'
            public List<string> Labels { get; set; }

            // Changed from List to a HashSet
            public HashSet<string> Tags { get; set; }

            // Changed from List of strongly-typed 'BlogPostComment' to loosely-typed string map
            public List<Dictionary<string, string>> Comments { get; set; }

            // Added pointless calculated field
            public int? NoOfComments { get; set; }

        }

        internal enum BlogPostType {

            None,
            Article,
            Summary

        }

    }

    [TestFixture]
    internal sealed class BlogPostMigrations {

        private readonly RedisClient _redisClient = new RedisClient(Config.MasterHost);

        [SetUp]
        public void OnBeforeEachTest() {
            this._redisClient.FlushAll();
        }

        [Test]
        public void Automatically_migrate_to_new_Schema() {
            var repository = new BlogRepository(this._redisClient);

            // Populate the datastore with the old schema from the 'BlogPostBestPractice'
            BlogPostBestPractice.InsertTestData(repository);

            // Create a typed-client based on the new schema
            IRedisTypedClient<New.BlogPost> redisBlogPosts = this._redisClient.As<New.BlogPost>();
            // Automatically retrieve blog posts
            IList<New.BlogPost> allBlogPosts = redisBlogPosts.GetAll();

            // Print out the data in the list of 'New.BlogPost' populated from old 'BlogPost' type
            // Note: renamed fields are lost 
            Console.WriteLine(allBlogPosts.ToJson());
        }

        [Test]
        public void Manually_migrate_to_new_Schema_using_a_custom_tranlation() {
            var repository = new BlogRepository(this._redisClient);

            // Populate the datastore with the old schema from the 'BlogPostBestPractice'
            BlogPostBestPractice.InsertTestData(repository);

            // Create a typed-client based on the new schema
            IRedisTypedClient<BlogPost> redisBlogPosts = this._redisClient.As<BlogPost>();
            IRedisTypedClient<New.BlogPost> redisNewBlogPosts = this._redisClient.As<New.BlogPost>();
            // Automatically retrieve blog posts
            IList<BlogPost> oldBlogPosts = redisBlogPosts.GetAll();

            // Write a custom translation layer to migrate to the new schema
            List<New.BlogPost> migratedBlogPosts = oldBlogPosts.Select(old => new New.BlogPost {
                Id = old.Id,
                BlogId = old.BlogId,
                Title = old.Title,
                Content = old.Content,
                Labels = old.Categories,         // populate with data from renamed field
                PostType = BlogPostType.Article, // select non-default enum value
                Tags = new HashSet<string>(old.Tags),
                Comments = old.Comments.ConvertAll(x =>
                    new Dictionary<string, string> {
                        { "Content", x.Content },
                        { "CreatedDate", x.CreatedDate.ToString() }
                    }),
                NoOfComments = old.Comments.Count // populate using logic from old data
            }).ToList();

            // Persist the new migrated blogposts 
            redisNewBlogPosts.StoreAll(migratedBlogPosts);

            // Read out the newly stored blogposts
            IList<New.BlogPost> refreshedNewBlogPosts = redisNewBlogPosts.GetAll();
            // Note: data renamed fields are successfully migrated to the new schema
            Console.WriteLine(refreshedNewBlogPosts.ToJson());
        }

    }

}
