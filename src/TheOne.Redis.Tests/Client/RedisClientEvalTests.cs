using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using TheOne.Redis.Client;
using TheOne.Redis.Common;

namespace TheOne.Redis.Tests.Client {

    [TestFixture]
    internal sealed class RedisClientEvalTests : RedisClientTestsBase {

        [Test]
        public void Can_create_ZPop_with_lua() {
            var luaBody = @"
                local val = redis.call('zrange', KEYS[1], 0, ARGV[1]-1)
                if val then redis.call('zremrangebyrank', KEYS[1], 0, ARGV[1]-1) end
                return val";

            var i = 0;
            List<string> alphabet = Enumerable.Range(0, 26).Select(c => ((char)('A' + c)).ToString()).ToList();
            alphabet.ForEach(x => this.Redis.AddItemToSortedSet("zalphabet", x, i++));

            List<string> letters = this.Redis.ExecLuaAsList(luaBody, new[] { "zalphabet" }, new[] { "3" });

            Console.WriteLine(letters.ToJson());
            Assert.That(letters, Is.EquivalentTo(new[] { "A", "B", "C" }));
        }

        [Test]
        public void Can_create_ZRevPop_with_lua() {
            var luaBody = @"
                local val = redis.call('zrange', KEYS[1], -ARGV[1], -1)
                if val then redis.call('zremrangebyrank', KEYS[1], -ARGV[1], -1) end
                return val";

            var i = 0;
            List<string> alphabet = Enumerable.Range(0, 26).Select(c => ((char)('A' + c)).ToString()).ToList();
            alphabet.ForEach(x => this.Redis.AddItemToSortedSet("zalphabet", x, i++));

            List<string> letters = this.Redis.ExecLuaAsList(luaBody, new[] { "zalphabet" }, new[] { "3" });

            Console.WriteLine(letters.ToJson());
            Assert.That(letters, Is.EquivalentTo(new[] { "X", "Y", "Z" }));
        }

        [Test]
        public void Can_detect_which_scripts_exist() {
            var sha1 = this.Redis.LoadLuaScript("return 'script1'");
            var sha2 = this.Redis.CalculateSha1("return 'script2'");
            var sha3 = this.Redis.LoadLuaScript("return 'script3'");

            Assert.That(this.Redis.HasLuaScript(sha1));

            Dictionary<string, bool> existsMap = this.Redis.WhichLuaScriptsExists(sha1, sha2, sha3);
            Assert.That(existsMap[sha1]);
            Assert.That(!existsMap[sha2]);
            Assert.That(existsMap[sha3]);
        }

        [Test]
        public void Can_Eval_HelloWorld_string() {
            var strVal = this.Redis.ExecLuaAsString(@"return 'Hello, ' .. ARGV[1] .. '!'", "Redis Lua");
            Assert.That(strVal, Is.EqualTo("Hello, Redis Lua!"));
        }

        [Test]
        public void Can_Eval_int() {
            var intVal = this.Redis.ExecLuaAsInt("return 3141591");
            Assert.That(intVal, Is.EqualTo(3141591));
        }

        [Test]
        public void Can_Eval_int_with_args() {
            var intVal = this.Redis.ExecLuaAsInt("return 3141591", "20", "30", "40");
            Assert.That(intVal, Is.EqualTo(3141591));
        }

        [Test]
        public void Can_Eval_int_with_keys_and_args() {
            var intVal = this.Redis.ExecLuaAsInt("return KEYS[1] + ARGV[1]", new[] { "20" }, new[] { "30", "40" });
            Assert.That(intVal, Is.EqualTo(50));
        }

        [Test]
        public void Can_Eval_int2() {
            var intVal = this.Redis.ExecLuaAsInt("return ARGV[1] + ARGV[2]", "10", "20");
            Assert.That(intVal, Is.EqualTo(30));
        }

        [Test]
        public void Can_Eval_multidata_with_args() {
            List<string> strVals = this.Redis.ExecLuaAsList(@"return {ARGV[1],ARGV[2],ARGV[3]}", "at", "dot", "com");
            Assert.That(strVals, Is.EquivalentTo(new List<string> { "at", "dot", "com" }));
        }

        [Test]
        public void Can_Eval_multidata_with_keys_and_args() {
            List<string> strVals = this.Redis.ExecLuaAsList(@"return {KEYS[1],ARGV[1],ARGV[2]}", new[] { "at" }, new[] { "dot", "com" });
            Assert.That(strVals, Is.EquivalentTo(new List<string> { "at", "dot", "com" }));
        }

        [Test]
        public void Can_Eval_string() {
            var strVal = this.Redis.ExecLuaAsString(@"return 'abc'");
            Assert.That(strVal, Is.EqualTo("abc"));
        }

        [Test]
        public void Can_Eval_string_with_args() {
            var strVal = this.Redis.ExecLuaAsString(@"return 'abc'", "at", "dot", "com");
            Assert.That(strVal, Is.EqualTo("abc"));
        }

        [Test]
        public void Can_Eval_string_with_keys_an_args() {
            var strVal = this.Redis.ExecLuaAsString(@"return KEYS[1] .. ARGV[1]", new[] { "at" }, new[] { "dot", "com" });
            Assert.That(strVal, Is.EqualTo("atdot"));
        }

        [Test]
        public void Can_EvalSha_int() {
            var luaBody = "return 3141591";
            this.Redis.ExecLuaAsInt(luaBody);
            var sha1 = this.Redis.CalculateSha1(luaBody);
            var intVal = this.Redis.ExecLuaShaAsInt(sha1);
            Assert.That(intVal, Is.EqualTo(3141591));
        }

        [Test]
        public void Can_Load_and_Exec_script() {
            var luaBody = "return 'load script and exec'";
            var sha1 = this.Redis.LoadLuaScript(luaBody);
            var result = this.Redis.ExecLuaShaAsString(sha1);
            Assert.That(result, Is.EqualTo("load script and exec"));
        }

        [Test]
        public void Can_return_DaysOfWeek_as_list() {
            Enum.GetNames(typeof(DayOfWeek)).ToList()
                .ForEach(x => this.Redis.AddItemToList("DaysOfWeek", x));
            Console.WriteLine(this.Redis.ExecLuaAsList("return redis.call('LRANGE', 'DaysOfWeek', 0, -1)").ToJson());
        }

        [Test]
        public void Does_flush_all_scripts() {
            var luaBody = "return 'load script and exec'";
            var sha1 = this.Redis.LoadLuaScript(luaBody);
            var result = this.Redis.ExecLuaShaAsString(sha1);
            Assert.That(result, Is.EqualTo("load script and exec"));

            this.Redis.RemoveAllLuaScripts();

            try {
                result = this.Redis.ExecLuaShaAsString(sha1);
                Console.WriteLine(result);
                Assert.Fail("script shouldn't exist");
            } catch (RedisResponseException ex) {
                Assert.That(ex.Message, Does.Contain("NOSCRIPT"));
            }
        }

    }

}
