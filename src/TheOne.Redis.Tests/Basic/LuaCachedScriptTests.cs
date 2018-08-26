using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;
using TheOne.Redis.Pipeline;

namespace TheOne.Redis.Tests.Basic {

    [TestFixture]
    internal sealed class LuaCachedScriptTests : RedisTestBase {

        #region Models

        public class SearchResult {

            public string Id { get; set; }
            public string Type { get; set; }
            public long Ttl { get; set; }
            public long Size { get; set; }

        }

        #endregion

        private const string _luaScript = @"
local limit = tonumber(ARGV[2])
local pattern = ARGV[1]
local cursor = 0
local len = 0
local results = {}

repeat
    local r = redis.call('scan', cursor, 'MATCH', pattern, 'COUNT', limit)
    cursor = tonumber(r[1])
    for k,v in ipairs(r[2]) do
        table.insert(results, v)
        len = len + 1
        if len == limit then break end
    end
until cursor == 0 or len == limit

return results
";

        private const string _keyAttributesScript = @"
local limit = tonumber(ARGV[2])
local pattern = ARGV[1]
local cursor = 0
local len = 0
local keys = {}

repeat
    local r = redis.call('scan', cursor, 'MATCH', pattern, 'COUNT', limit)
    cursor = tonumber(r[1])
    for k,v in ipairs(r[2]) do
        table.insert(keys, v)
        len = len + 1
        if len == limit then break end
    end
until cursor == 0 or len == limit

local keyAttrs = {}
for i,key in ipairs(keys) do
    local type = redis.call('type', key)['ok']
    local pttl = redis.call('pttl', key)
    local size = 0
    if type == 'string' then
        size = redis.call('strlen', key)
    elseif type == 'list' then
        size = redis.call('llen', key)
    elseif type == 'set' then
        size = redis.call('scard', key)
    elseif type == 'zset' then
        size = redis.call('zcard', key)
    elseif type == 'hash' then
        size = redis.call('hlen', key)
    end

    local attrs = {['id'] = key, ['type'] = type, ['ttl'] = pttl, ['size'] = size}

    table.insert(keyAttrs, attrs)
end

return cjson.encode(keyAttrs)";

        private static void AddTestKeys(RedisClient redis, int count) {
            for (var i = 0; i < count; i++) {
                redis.SetValue("key:" + i, "value:" + i);
            }
        }

        [Test]
        public void Can_call_Cached_Lua() {
            using (var redis = new RedisClient(Config.MasterHost)) {
                AddTestKeys(redis, 20);

                RedisText r = redis.ExecCachedLua(_luaScript, sha1 => redis.ExecLuaSha(sha1, "key:*", "10"));
                Assert.That(r.Children.Count, Is.EqualTo(10));

                r = redis.ExecCachedLua(_luaScript, sha1 => redis.ExecLuaSha(sha1, "key:*", "10"));
                Assert.That(r.Children.Count, Is.EqualTo(10));
            }
        }

        [Test]
        public void Can_call_Cached_Lua_even_after_script_is_flushed() {
            using (var redis = new RedisClient(Config.MasterHost)) {
                RedisText r = redis.ExecCachedLua(_luaScript, sha1 => redis.ExecLuaSha(sha1, "key:*", "10"));
                Assert.That(r.Children.Count, Is.EqualTo(10));

                redis.ScriptFlush();

                r = redis.ExecCachedLua(_luaScript, sha1 => redis.ExecLuaSha(sha1, "key:*", "10"));
                Assert.That(r.Children.Count, Is.EqualTo(10));
            }
        }

        [Test]
        public void Can_call_repeated_scans_in_LUA() {
            using (var redis = new RedisClient(Config.MasterHost)) {
                AddTestKeys(redis, 20);

                RedisText r = redis.ExecLua(_luaScript, "key:*", "10");
                Assert.That(r.Children.Count, Is.EqualTo(10));

                r = redis.ExecLua(_luaScript, "key:*", "40");
                Assert.That(r.Children.Count, Is.EqualTo(20));
            }
        }

        [Test]
        public void Can_call_script_with_complex_response() {
            using (var redis = new RedisClient(Config.MasterHost)) {
                var r = redis.ExecCachedLua(_keyAttributesScript, sha1 => redis.ExecLuaShaAsString(sha1, "key:*", "10"));

                Console.WriteLine(r);

                var results = r.FromJson<List<SearchResult>>();

                Assert.That(results.Count, Is.EqualTo(10));

                SearchResult result = results[0];
                Assert.That(result.Id.StartsWith("key:"));
                Assert.That(result.Type, Is.EqualTo("string"));
                Assert.That(result.Size, Is.GreaterThan("value:".Length));
                Assert.That(result.Ttl, Is.EqualTo(-1));
            }
        }

        [Test]
        public void Can_merge_multiple_SearchResults() {
            var redis = new RedisClient(Config.MasterHost);
            var limit = 10;
            var query = "key:*";

            List<string> keys = redis.ScanAllKeys(query, limit).Take(limit).ToList();

            var keyTypes = new Dictionary<string, string>();
            var keyTtls = new Dictionary<string, long>();
            var keySizes = new Dictionary<string, long>();

            if (keys.Count > 0) {
                using (IRedisPipeline pipeline = redis.CreatePipeline()) {
                    foreach (var value in keys) {
                        pipeline.QueueCommand(r => r.Type(value), x => keyTypes[value] = x);
                    }

                    foreach (var value1 in keys) {
                        pipeline.QueueCommand(r => ((RedisNativeClient)r).PTtl(value1), x => keyTtls[value1] = x);
                    }

                    pipeline.Flush();
                }

                using (IRedisPipeline pipeline = redis.CreatePipeline()) {
                    foreach (KeyValuePair<string, string> entry in keyTypes) {
                        var key = entry.Key;
                        switch (entry.Value) {
                            case "string":
                                pipeline.QueueCommand(r => r.GetStringCount(key), x => keySizes[key] = x);
                                break;
                            case "list":
                                pipeline.QueueCommand(r => r.GetListCount(key), x => keySizes[key] = x);
                                break;
                            case "set":
                                pipeline.QueueCommand(r => r.GetSetCount(key), x => keySizes[key] = x);
                                break;
                            case "zset":
                                pipeline.QueueCommand(r => r.GetSortedSetCount(key), x => keySizes[key] = x);
                                break;
                            case "hash":
                                pipeline.QueueCommand(r => r.GetHashCount(key), x => keySizes[key] = x);
                                break;
                        }
                    }

                    pipeline.Flush();
                }
            }

            List<SearchResult> results = keys.Select(x => new SearchResult {
                Id = x,
                Type = keyTypes.ContainsKey(x) ? keyTypes[x] : default,
                Ttl = keyTtls.ContainsKey(x) ? keyTtls[x] : default,
                Size = keySizes.ContainsKey(x) ? keySizes[x] : default
            }).ToList();

            Assert.That(results.Count, Is.EqualTo(limit));

            SearchResult result = results[0];
            Assert.That(result.Id.StartsWith("key:"));
            Assert.That(result.Type, Is.EqualTo("string"));
            Assert.That(result.Size, Is.GreaterThan("value:".Length));
            Assert.That(result.Ttl, Is.EqualTo(-1));

            redis.Dispose();
        }

    }

}
