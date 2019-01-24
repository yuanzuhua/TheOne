using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Examples.BestPractice {

    #region Blog Models	

    internal class User : IHasBlogRepository {

        public long Id { get; set; }
        public string Name { get; set; }
        public List<long> BlogIds { get; set; } = new List<long>();
        public IBlogRepository Repository { private get; set; }

        public List<Blog> GetBlogs() {
            return this.Repository.GetBlogs(this.BlogIds);
        }

        public Blog CreateNewBlog(Blog blog) {
            this.Repository.StoreBlogs(this, blog);

            return blog;
        }

    }

    internal class Blog : IHasBlogRepository {

        public long Id { get; set; }
        public long UserId { get; set; }
        public string UserName { get; set; }
        public List<string> Tags { get; set; } = new List<string>();
        public List<long> BlogPostIds { get; set; } = new List<long>();
        public IBlogRepository Repository { private get; set; }

        public List<BlogPost> GetBlogPosts() {
            return this.Repository.GetBlogPosts(this.BlogPostIds);
        }

        public void StoreNewBlogPosts(params BlogPost[] blogPosts) {
            this.Repository.StoreNewBlogPosts(this, blogPosts);
        }

    }

    internal class BlogPost {

        public long Id { get; set; }
        public long BlogId { get; set; }
        public string Title { get; set; }
        public string Content { get; set; }
        public List<string> Categories { get; set; } = new List<string>();
        public List<string> Tags { get; set; } = new List<string>();
        public List<BlogPostComment> Comments { get; set; } = new List<BlogPostComment>();

    }

    internal class BlogPostComment {

        public string Content { get; set; }
        public DateTime CreatedDate { get; set; }

    }

    #endregion

    #region Blog Repository

    internal interface IHasBlogRepository {

        IBlogRepository Repository { set; }

    }

    internal class BlogRepository : IBlogRepository {

        private const string _categoryTypeName = "Category";
        private const string _tagCloudKey = "urn:TagCloud";
        private const string _allCategoriesKey = "urn:Categories";
        private const string _recentBlogPostsKey = "urn:BlogPosts:RecentPosts";
        private const string _recentBlogPostCommentsKey = "urn:BlogPostComment:RecentComments";

        private readonly IRedisClient _redis;

        public BlogRepository(IRedisClient client) {
            this._redis = client;
        }

        public void StoreUsers(params User[] users) {
            var redisUsers = this._redis.As<User>();
            this.Inject(users);
            foreach (var value in users.Where(x => x.Id == default(int))) {
                value.Id = redisUsers.GetNextSequence();
            }

            redisUsers.StoreAll(users);
        }

        public List<User> GetAllUsers() {
            var redisUsers = this._redis.As<User>();
            return this.Inject(redisUsers.GetAll());
        }

        public void StoreBlogs(User user, params Blog[] blogs) {
            var redisBlogs = this._redis.As<Blog>();
            foreach (var blog in blogs) {
                blog.Id = blog.Id != default(int) ? blog.Id : redisBlogs.GetNextSequence();
                blog.UserId = user.Id;
                blog.UserName = user.Name;

                if (!user.BlogIds.Contains(blog.Id)) {
                    user.BlogIds.Add(blog.Id);
                }
            }

            using (var trans = this._redis.CreateTransaction()) {
                trans.QueueCommand(x => x.Store(user));
                trans.QueueCommand(x => x.StoreAll(blogs));

                trans.Commit();
            }

            this.Inject(blogs);
        }

        public List<Blog> GetBlogs(IEnumerable<long> blogIds) {
            var redisBlogs = this._redis.As<Blog>();
            return this.Inject(
                redisBlogs.GetByIds(blogIds.Select(x => x.ToString())));
        }

        public List<Blog> GetAllBlogs() {
            var redisBlogs = this._redis.As<Blog>();
            return this.Inject(redisBlogs.GetAll());
        }

        public List<BlogPost> GetBlogPosts(IEnumerable<long> blogPostIds) {
            var redisBlogPosts = this._redis.As<BlogPost>();
            return redisBlogPosts.GetByIds(blogPostIds.Select(x => x.ToString())).ToList();
        }

        public void StoreNewBlogPosts(Blog blog, params BlogPost[] blogPosts) {
            var redisBlogPosts = this._redis.As<BlogPost>();
            var redisComments = this._redis.As<BlogPostComment>();

            // Get wrapper around a strongly-typed Redis server-side List
            var recentPosts = redisBlogPosts.Lists[_recentBlogPostsKey];
            var recentComments = redisComments.Lists[_recentBlogPostCommentsKey];

            foreach (var blogPost in blogPosts) {
                blogPost.Id = blogPost.Id != default(int) ? blogPost.Id : redisBlogPosts.GetNextSequence();
                blogPost.BlogId = blog.Id;
                if (!blog.BlogPostIds.Contains(blogPost.Id)) {
                    blog.BlogPostIds.Add(blogPost.Id);
                }

                // List of Recent Posts and comments
                recentPosts.Prepend(blogPost);
                blogPost.Comments.ForEach(recentComments.Prepend);

                // Tag Cloud
                blogPost.Tags.ForEach(x => this._redis.IncrementItemInSortedSet(_tagCloudKey, x, 1));

                // List of all post categories
                blogPost.Categories.ForEach(x => this._redis.AddItemToSet(_allCategoriesKey, x));

                // Map of Categories to BlogPost Ids
                blogPost.Categories.ForEach(x => this._redis.AddItemToSet(UrnId.Create(_categoryTypeName, x), blogPost.Id.ToString()));
            }

            // Rolling list of recent items, only keep the last 5
            recentPosts.Trim(0, 4);
            recentComments.Trim(0, 4);

            using (var trans = this._redis.CreateTransaction()) {
                trans.QueueCommand(x => x.Store(blog));
                trans.QueueCommand(x => x.StoreAll(blogPosts));

                trans.Commit();
            }
        }

        public List<BlogPost> GetRecentBlogPosts() {
            var redisBlogPosts = this._redis.As<BlogPost>();
            return redisBlogPosts.Lists[_recentBlogPostsKey].GetAll();
        }

        public List<BlogPostComment> GetRecentBlogPostComments() {
            var redisComments = this._redis.As<BlogPostComment>();
            return redisComments.Lists[_recentBlogPostCommentsKey].GetAll();
        }

        public IDictionary<string, double> GetTopTags(int take) {
            return this._redis.GetRangeWithScoresFromSortedSetDesc(_tagCloudKey, 0, take - 1);
        }

        public HashSet<string> GetAllCategories() {
            return this._redis.GetAllItemsFromSet(_allCategoriesKey);
        }

        public void StoreBlogPost(BlogPost blogPost) {
            this._redis.Store(blogPost);
        }

        public BlogPost GetBlogPost(int postId) {
            return this._redis.GetById<BlogPost>(postId);
        }

        public List<BlogPost> GetBlogPostsByCategory(string categoryName) {
            var categoryUrn = UrnId.Create(_categoryTypeName, categoryName);
            var documentDbPostIds = this._redis.GetAllItemsFromSet(categoryUrn);

            return this._redis.GetByIds<BlogPost>(documentDbPostIds.ToArray()).ToList();
        }

        public List<T> Inject<T>(IEnumerable<T> entities)
            where T : IHasBlogRepository {
            var entitiesList = entities.ToList();
            entitiesList.ForEach(x => x.Repository = this);
            return entitiesList;
        }

    }

    internal interface IBlogRepository {

        void StoreUsers(params User[] users);
        List<User> GetAllUsers();

        void StoreBlogs(User user, params Blog[] users);
        List<Blog> GetBlogs(IEnumerable<long> blogIds);
        List<Blog> GetAllBlogs();

        List<BlogPost> GetBlogPosts(IEnumerable<long> blogPostIds);
        void StoreNewBlogPosts(Blog blog, params BlogPost[] blogPosts);

        List<BlogPost> GetRecentBlogPosts();
        List<BlogPostComment> GetRecentBlogPostComments();
        IDictionary<string, double> GetTopTags(int take);
        HashSet<string> GetAllCategories();

        void StoreBlogPost(BlogPost blogPost);
        BlogPost GetBlogPost(int postId);
        List<BlogPost> GetBlogPostsByCategory(string categoryName);

    }

    #endregion

}
