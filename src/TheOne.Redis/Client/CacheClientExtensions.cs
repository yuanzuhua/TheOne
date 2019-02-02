using System;
using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public static class CacheClientExtensions {

        /// <summary>
        ///     Removes items from cache that have keys matching the specified wildcard pattern
        /// </summary>
        /// <param name="cacheClient" >Cache client</param>
        /// <param name="pattern" >The wildcard, where "*" means any sequence of characters and "?" means any single character.</param>
        public static void RemoveByPattern(this ICacheClient cacheClient, string pattern) {
            var canRemoveByPattern = cacheClient as IRemoveByPattern;
            if (canRemoveByPattern == null) {
                throw new NotImplementedException(
                    "IRemoveByPattern is not implemented on: " + cacheClient.GetType().FullName);
            }

            canRemoveByPattern.RemoveByPattern(pattern);
        }

        /// <summary>
        ///     Removes items from the cache based on the specified regular expression pattern
        /// </summary>
        /// <param name="cacheClient" >Cache client</param>
        /// <param name="regex" >Regular expression pattern to search cache keys</param>
        public static void RemoveByRegex(this ICacheClient cacheClient, string regex) {
            var canRemoveByPattern = cacheClient as IRemoveByPattern;
            if (canRemoveByPattern == null) {
                throw new NotImplementedException("IRemoveByPattern is not implemented by: " + cacheClient.GetType().FullName);
            }

            canRemoveByPattern.RemoveByRegex(regex);
        }

        public static IEnumerable<string> GetKeysByPattern(this ICacheClient cache, string pattern) {
            var extendedCache = cache as ICacheClientExtended;
            if (extendedCache == null) {
                throw new NotImplementedException("ICacheClientExtended is not implemented by: " + cache.GetType().FullName);
            }

            return extendedCache.GetKeysByPattern(pattern);
        }

        public static IEnumerable<string> GetAllKeys(this ICacheClient cache) {
            return cache.GetKeysByPattern("*");
        }

        public static IEnumerable<string> GetKeysStartingWith(this ICacheClient cache, string prefix) {
            return cache.GetKeysByPattern(prefix + "*");
        }

        public static T GetOrCreate<T>(this ICacheClient cache,
            string key, Func<T> createFn) {
            var value = cache.Get<T>(key);
            if (Equals(value, default(T))) {
                value = createFn();
                cache.Set(key, value);
            }

            return value;
        }

        public static T GetOrCreate<T>(this ICacheClient cache,
            string key, TimeSpan expiresIn, Func<T> createFn) {
            var value = cache.Get<T>(key);
            if (Equals(value, default(T))) {
                value = createFn();
                cache.Set(key, value, expiresIn);
            }

            return value;
        }

        public static TimeSpan? GetTimeToLive(this ICacheClient cache, string key) {
            var extendedCache = cache as ICacheClientExtended;
            if (extendedCache == null) {
                throw new InvalidOperationException("GetTimeToLive is not implemented by: " + cache.GetType().FullName);
            }

            return extendedCache.GetTimeToLive(key);
        }

    }

}
