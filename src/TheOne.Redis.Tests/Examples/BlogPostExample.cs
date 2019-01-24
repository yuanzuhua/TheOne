using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Examples {

    [TestFixture]
    internal sealed class BlogPostExample {

        #region Models

        public class Blog {

            public Blog() {
                this.Tags = new List<string>();
                this.BlogPostIds = new List<long>();
            }

            public long Id { get; set; }
            public long UserId { get; set; }
            public string UserName { get; set; }
            public List<string> Tags { get; set; }
            public List<long> BlogPostIds { get; set; }

        }

        public class BlogPost {

            public BlogPost() {
                this.Categories = new List<string>();
                this.Tags = new List<string>();
                this.Comments = new List<BlogPostComment>();
            }

            public long Id { get; set; }
            public long BlogId { get; set; }
            public string Title { get; set; }
            public string Content { get; set; }
            public List<string> Categories { get; set; }
            public List<string> Tags { get; set; }
            public List<BlogPostComment> Comments { get; set; }

        }

        public class BlogPostComment {

            public string Content { get; set; }
            public DateTime CreatedDate { get; set; }

        }

        /// <summary>
        ///     A complete, self-contained example showing how to create a basic blog application using Redis.
        /// </summary>
        public class User {

            public User() {
                this.BlogIds = new List<long>();
            }

            public long Id { get; set; }
            public string Name { get; set; }
            public List<long> BlogIds { get; set; }

        }

        #endregion

        private readonly RedisClient _redis = new RedisClient(Config.MasterHost);

        public void InsertTestData() {
            var redisUsers = this._redis.As<User>();
            var redisBlogs = this._redis.As<Blog>();
            var redisBlogPosts = this._redis.As<BlogPost>();

            var ayende = new User { Id = redisUsers.GetNextSequence(), Name = "Oren Eini" };
            var mythz = new User { Id = redisUsers.GetNextSequence(), Name = "Demis Bellot" };

            var ayendeBlog = new Blog {
                Id = redisBlogs.GetNextSequence(),
                UserId = ayende.Id,
                UserName = ayende.Name,
                Tags = new List<string> { "Architecture", ".NET", "Databases" }
            };

            var mythzBlog = new Blog {
                Id = redisBlogs.GetNextSequence(),
                UserId = mythz.Id,
                UserName = mythz.Name,
                Tags = new List<string> { "Architecture", ".NET", "Databases" }
            };

            var blogPosts = new List<BlogPost> {
                new BlogPost {
                    Id = redisBlogPosts.GetNextSequence(),
                    BlogId = ayendeBlog.Id,
                    Title = "RavenDB",
                    Categories = new List<string> { "NoSQL", "DocumentDB" },
                    Tags = new List<string> { "Raven", "NoSQL", "JSON", ".NET" },
                    Comments = new List<BlogPostComment> {
                        new BlogPostComment { Content = "First Comment!", CreatedDate = DateTime.UtcNow },
                        new BlogPostComment { Content = "Second Comment!", CreatedDate = DateTime.UtcNow }
                    }
                },
                new BlogPost {
                    Id = redisBlogPosts.GetNextSequence(),
                    BlogId = mythzBlog.Id,
                    Title = "Redis",
                    Categories = new List<string> { "NoSQL", "Cache" },
                    Tags = new List<string> { "Redis", "NoSQL", "Scalability", "Performance" },
                    Comments = new List<BlogPostComment> {
                        new BlogPostComment { Content = "First Comment!", CreatedDate = DateTime.UtcNow }
                    }
                },
                new BlogPost {
                    Id = redisBlogPosts.GetNextSequence(),
                    BlogId = ayendeBlog.Id,
                    Title = "Cassandra",
                    Categories = new List<string> { "NoSQL", "Cluster" },
                    Tags = new List<string> { "Cassandra", "NoSQL", "Scalability", "Hashing" },
                    Comments = new List<BlogPostComment> {
                        new BlogPostComment { Content = "First Comment!", CreatedDate = DateTime.UtcNow }
                    }
                },
                new BlogPost {
                    Id = redisBlogPosts.GetNextSequence(),
                    BlogId = mythzBlog.Id,
                    Title = "Couch Db",
                    Categories = new List<string> { "NoSQL", "DocumentDB" },
                    Tags = new List<string> { "CouchDb", "NoSQL", "JSON" },
                    Comments = new List<BlogPostComment> {
                        new BlogPostComment { Content = "First Comment!", CreatedDate = DateTime.UtcNow }
                    }
                }
            };

            ayende.BlogIds.Add(ayendeBlog.Id);
            ayendeBlog.BlogPostIds.AddRange(blogPosts.Where(x => x.BlogId == ayendeBlog.Id).Select(x => x.Id));

            mythz.BlogIds.Add(mythzBlog.Id);
            mythzBlog.BlogPostIds.AddRange(blogPosts.Where(x => x.BlogId == mythzBlog.Id).Select(x => x.Id));

            redisUsers.Store(ayende);
            redisUsers.Store(mythz);
            redisBlogs.StoreAll(new[] { ayendeBlog, mythzBlog });
            redisBlogPosts.StoreAll(blogPosts);
        }

        [SetUp]
        public void OnBeforeEachTest() {
            this._redis.FlushAll();
            this.InsertTestData();
        }

        [Test]
        public void Add_comment_to_existing_post() {
            var postId = 1;
            var redisBlogPosts = this._redis.As<BlogPost>();
            var blogPost = redisBlogPosts.GetById(postId.ToString());
            blogPost.Comments.Add(
                new BlogPostComment { Content = "Third Post!", CreatedDate = DateTime.UtcNow });
            redisBlogPosts.Store(blogPost);

            var refreshBlogPost = redisBlogPosts.GetById(postId.ToString());
            Console.WriteLine(refreshBlogPost.ToJson());
            /* Output:
            {
                Id: 1,
                BlogId: 1,
                Title: RavenDB,
                Categories: 
                [
                    NoSQL,
                    DocumentDB
                ],
                Tags: 
                [
                    Raven,
                    NoSQL,
                    JSON,
                    .NET
                ],
                Comments: 
                [
                    {
                        Content: First Comment!,
                        CreatedDate: 2010-04-20T21:32:39.9688707Z
                    },
                    {
                        Content: Second Comment!,
                        CreatedDate: 2010-04-20T21:32:39.9688707Z
                    },
                    {
                        Content: Third Post!,
                        CreatedDate: 2010-04-20T21:32:40.2688879Z
                    }
                ]
            }
            */
        }

        [Test]
        public void Show_a_list_of_blogs() {
            var redisBlogs = this._redis.As<Blog>();
            var blogs = redisBlogs.GetAll();
            Console.WriteLine(blogs.ToJson());
        }

        [Test]
        public void Show_a_list_of_recent_posts_and_comments() {
            // Get strongly-typed clients
            var redisBlogPosts = this._redis.As<BlogPost>();
            var redisComments = this._redis.As<BlogPostComment>();
            {
                // To keep this example let's pretend this is a new list of blog posts
                var newIncomingBlogPosts = redisBlogPosts.GetAll();

                // Let's get back an IList<BlogPost> wrapper around a Redis server-side List.
                var recentPosts = redisBlogPosts.Lists["urn:BlogPost:RecentPosts"];
                var recentComments = redisComments.Lists["urn:BlogPostComment:RecentComments"];

                foreach (var newBlogPost in newIncomingBlogPosts) {
                    // Prepend the new blog posts to the start of the 'RecentPosts' list
                    recentPosts.Prepend(newBlogPost);

                    // Prepend all the new blog post comments to the start of the 'RecentComments' list
                    newBlogPost.Comments.ForEach(recentComments.Prepend);
                }

                // Make this a Rolling list by only keep the latest 3 posts and comments
                recentPosts.Trim(0, 2);
                recentComments.Trim(0, 2);

                // Print out the last 3 posts:
                Console.WriteLine(recentPosts.GetAll().ToJson());
                Console.WriteLine(recentComments.GetAll().ToJson());
            }
        }

        [Test]
        public void Show_a_TagCloud() {
            // Get strongly-typed clients
            var redisBlogPosts = this._redis.As<BlogPost>();
            var newIncomingBlogPosts = redisBlogPosts.GetAll();

            foreach (var newBlogPost in newIncomingBlogPosts) {
                // For every tag in each new blog post, increment the number of times each Tag has occurred 
                newBlogPost.Tags.ForEach(x => this._redis.IncrementItemInSortedSet("urn:TagCloud", x, 1));
            }

            // Show top 5 most popular tags with their scores
            var tagCloud = this._redis.GetRangeWithScoresFromSortedSetDesc("urn:TagCloud", 0, 4);
            Console.WriteLine(tagCloud.ToJson());
        }

        [Test]
        public void Show_all_Categories() {
            var redisBlogPosts = this._redis.As<BlogPost>();
            var blogPosts = redisBlogPosts.GetAll();

            foreach (var blogPost in blogPosts) {
                blogPost.Categories.ForEach(x => this._redis.AddItemToSet("urn:Categories", x));
            }

            var uniqueCategories = this._redis.GetAllItemsFromSet("urn:Categories");
            Console.WriteLine(uniqueCategories.ToJson());
        }

        [Test]
        public void Show_all_Posts_for_the_DocumentDB_Category() {
            var redisBlogPosts = this._redis.As<BlogPost>();
            var newIncomingBlogPosts = redisBlogPosts.GetAll();

            foreach (var newBlogPost in newIncomingBlogPosts) {
                // For each post add it's Id into each of it's 'Category > Posts' index
                newBlogPost.Categories.ForEach(x => this._redis.AddItemToSet("urn:Category:" + x, newBlogPost.Id.ToString()));
            }

            // Retrieve all the post ids for the category you want to view
            var documentDbPostIds = this._redis.GetAllItemsFromSet("urn:Category:DocumentDB");

            // Make a batch call to retrieve all the posts containing the matching ids 
            // (i.e. the DocumentDB Category posts)
            var documentDbPosts = redisBlogPosts.GetByIds(documentDbPostIds);

            Console.WriteLine(documentDbPosts.ToJson());
        }

        [Test]
        public void Show_post_and_all_comments() {
            // There is nothing special required here as since comments are Key Value Objects 
            // they are stored and retrieved with the post
            var postId = 1;
            var redisBlogPosts = this._redis.As<BlogPost>();
            var selectedBlogPost = redisBlogPosts.GetById(postId.ToString());

            Console.WriteLine(selectedBlogPost.ToJson());
        }

        [Test]
        public void Store_and_retrieve_some_blogs() {
            // Retrieve strongly-typed Redis clients that let's you natively persist POCO's
            var redisUsers = this._redis.As<User>();
            var redisBlogs = this._redis.As<Blog>();
            // Create the user, getting a unique User Id from the User sequence.
            var mythz = new User { Id = redisUsers.GetNextSequence(), Name = "Demis Bellot" };

            // create some blogs using unique Ids from the Blog sequence. Also adding references
            var mythzBlogs = new List<Blog> {
                new Blog {
                    Id = redisBlogs.GetNextSequence(),
                    UserId = mythz.Id,
                    UserName = mythz.Name,
                    Tags = new List<string> { "Architecture", ".NET", "Redis" }
                },
                new Blog {
                    Id = redisBlogs.GetNextSequence(),
                    UserId = mythz.Id,
                    UserName = mythz.Name,
                    Tags = new List<string> { "Music", "Twitter", "Life" }
                }
            };
            // Add the blog references
            mythzBlogs.ForEach(x => mythz.BlogIds.Add(x.Id));

            // Store the user and their blogs
            redisUsers.Store(mythz);
            redisBlogs.StoreAll(mythzBlogs);

            // retrieve all blogs
            var blogs = redisBlogs.GetAll();

            Console.WriteLine(blogs.ToJson());
        }

        [Test]
        public void Store_and_retrieve_users() {
            var redisUsers = this._redis.As<User>();
            redisUsers.Store(new User { Id = redisUsers.GetNextSequence(), Name = "ayende" });
            redisUsers.Store(new User { Id = redisUsers.GetNextSequence(), Name = "mythz" });

            var allUsers = redisUsers.GetAll();
            Console.WriteLine(allUsers.ToJson());
        }

    }

}
