using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Examples.BestPractice {

    [TestFixture]
    internal sealed class BlogPostBestPractice {

        private readonly RedisClient _redisClient = new RedisClient(Config.MasterHost);
        private IBlogRepository _repository;

        public static void InsertTestData(IBlogRepository repository) {
            var ayende = new User { Name = "ayende" };
            var mythz = new User { Name = "mythz" };

            repository.StoreUsers(ayende, mythz);

            Blog ayendeBlog = ayende.CreateNewBlog(new Blog { Tags = { "Architecture", ".NET", "Databases" } });

            Blog mythzBlog = mythz.CreateNewBlog(new Blog { Tags = { "Architecture", ".NET", "Databases" } });

            ayendeBlog.StoreNewBlogPosts(new BlogPost {
                    Title = "RavenDB",
                    Categories = new List<string> { "NoSQL", "DocumentDB" },
                    Tags = new List<string> { "Raven", "NoSQL", "JSON", ".NET" },
                    Comments = new List<BlogPostComment> {
                        new BlogPostComment { Content = "First Comment!", CreatedDate = DateTime.UtcNow },
                        new BlogPostComment { Content = "Second Comment!", CreatedDate = DateTime.UtcNow }
                    }
                },
                new BlogPost {
                    BlogId = ayendeBlog.Id,
                    Title = "Cassandra",
                    Categories = new List<string> { "NoSQL", "Cluster" },
                    Tags = new List<string> { "Cassandra", "NoSQL", "Scalability", "Hashing" },
                    Comments = new List<BlogPostComment> {
                        new BlogPostComment { Content = "First Comment!", CreatedDate = DateTime.UtcNow }
                    }
                });

            mythzBlog.StoreNewBlogPosts(
                new BlogPost {
                    Title = "Redis",
                    Categories = new List<string> { "NoSQL", "Cache" },
                    Tags = new List<string> { "Redis", "NoSQL", "Scalability", "Performance" },
                    Comments = new List<BlogPostComment> {
                        new BlogPostComment { Content = "First Comment!", CreatedDate = DateTime.UtcNow }
                    }
                },
                new BlogPost {
                    Title = "Couch Db",
                    Categories = new List<string> { "NoSQL", "DocumentDB" },
                    Tags = new List<string> { "CouchDb", "NoSQL", "JSON" },
                    Comments = new List<BlogPostComment> {
                        new BlogPostComment { Content = "First Comment!", CreatedDate = DateTime.UtcNow }
                    }
                });
        }

        [SetUp]
        public void OnBeforeEachTest() {
            this._redisClient.FlushAll();
            this._repository = new BlogRepository(this._redisClient);

            InsertTestData(this._repository);
        }

        [Test]
        public void Add_comment_to_existing_post() {
            var postId = 1;
            BlogPost blogPost = this._repository.GetBlogPost(postId);

            blogPost.Comments.Add(
                new BlogPostComment { Content = "Third Comment!", CreatedDate = DateTime.UtcNow });

            this._repository.StoreBlogPost(blogPost);

            BlogPost refreshBlogPost = this._repository.GetBlogPost(postId);
            Console.WriteLine(refreshBlogPost.ToJson());
        }

        [Test]
        public void Show_a_list_of_blogs() {
            List<Blog> blogs = this._repository.GetAllBlogs();
            Console.WriteLine(blogs.ToJson());
        }

        [Test]
        public void Show_a_list_of_recent_posts_and_comments() {
            // Recent posts are already maintained in the repository
            List<BlogPost> recentPosts = this._repository.GetRecentBlogPosts();
            List<BlogPostComment> recentComments = this._repository.GetRecentBlogPostComments();

            Console.WriteLine("Recent Posts:\n" + recentPosts.ToJson());
            Console.WriteLine("Recent Comments:\n" + recentComments.ToJson());
        }

        [Test]
        public void Show_a_TagCloud() {
            // Tags are maintained in the repository
            IDictionary<string, double> tagCloud = this._repository.GetTopTags(5);
            Console.WriteLine(tagCloud.ToJson());
        }

        [Test]
        public void Show_all_Categories() {
            // Categories are maintained in the repository
            HashSet<string> allCategories = this._repository.GetAllCategories();
            Console.WriteLine(allCategories.ToJson());
        }

        [Test]
        public void Show_all_Posts_for_a_Category() {
            List<BlogPost> documentDbPosts = this._repository.GetBlogPostsByCategory("DocumentDB");
            Console.WriteLine(documentDbPosts.ToJson());
        }

        [Test]
        public void Show_post_and_all_comments() {
            var postId = 1;
            BlogPost blogPost = this._repository.GetBlogPost(postId);
            Console.WriteLine(blogPost.ToJson());
        }

        [Test]
        public void View_test_data() {
            User mythz = this._repository.GetAllUsers().First(x => x.Name == "mythz");
            IEnumerable<long> mythzBlogPostIds = mythz.GetBlogs().SelectMany(x => x.BlogPostIds);
            List<BlogPost> mythzBlogPosts = this._repository.GetBlogPosts(mythzBlogPostIds);

            Console.WriteLine(mythzBlogPosts.ToJson());
        }

    }

}
