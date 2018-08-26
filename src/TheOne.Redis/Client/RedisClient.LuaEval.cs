using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using TheOne.Redis.Client.Internal;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client {

    public partial class RedisClient {

        private static readonly ConcurrentDictionary<string, string> _cachedLuaSha1Map =
            new ConcurrentDictionary<string, string>();

        public T ExecCachedLua<T>(string scriptBody, Func<string, T> scriptSha1) {
            if (!_cachedLuaSha1Map.TryGetValue(scriptBody, out var sha1)) {
                _cachedLuaSha1Map[scriptBody] = sha1 = this.LoadLuaScript(scriptBody);
            }

            try {
                return scriptSha1(sha1);
            } catch (RedisResponseException ex) {
                if (!ex.Message.StartsWith("NOSCRIPT")) {
                    throw;
                }

                _cachedLuaSha1Map[scriptBody] = sha1 = this.LoadLuaScript(scriptBody);
                return scriptSha1(sha1);
            }
        }

        public RedisText ExecLua(string body, params string[] args) {
            RedisData data = this.EvalCommand(body, 0, args.ToMultiByteArray());
            return data.ToRedisText();
        }

        public RedisText ExecLua(string luaBody, string[] keys, string[] args) {
            RedisData data = this.EvalCommand(luaBody, keys.Length, this.MergeAndConvertToBytes(keys, args));
            return data.ToRedisText();
        }

        public RedisText ExecLuaSha(string sha1, params string[] args) {
            RedisData data = this.EvalShaCommand(sha1, 0, args.ToMultiByteArray());
            return data.ToRedisText();
        }

        public RedisText ExecLuaSha(string sha1, string[] keys, string[] args) {
            RedisData data = this.EvalShaCommand(sha1, keys.Length, this.MergeAndConvertToBytes(keys, args));
            return data.ToRedisText();
        }

        public long ExecLuaAsInt(string body, params string[] args) {
            return this.EvalInt(body, 0, args.ToMultiByteArray());
        }

        public long ExecLuaAsInt(string luaBody, string[] keys, string[] args) {
            return this.EvalInt(luaBody, keys.Length, this.MergeAndConvertToBytes(keys, args));
        }

        public long ExecLuaShaAsInt(string sha1, params string[] args) {
            return this.EvalShaInt(sha1, 0, args.ToMultiByteArray());
        }

        public long ExecLuaShaAsInt(string sha1, string[] keys, string[] args) {
            return this.EvalShaInt(sha1, keys.Length, this.MergeAndConvertToBytes(keys, args));
        }

        public string ExecLuaAsString(string body, params string[] args) {
            return this.EvalStr(body, 0, args.ToMultiByteArray());
        }

        public string ExecLuaAsString(string sha1, string[] keys, string[] args) {
            return this.EvalStr(sha1, keys.Length, this.MergeAndConvertToBytes(keys, args));
        }

        public string ExecLuaShaAsString(string sha1, params string[] args) {
            return this.EvalShaStr(sha1, 0, args.ToMultiByteArray());
        }

        public string ExecLuaShaAsString(string sha1, string[] keys, string[] args) {
            return this.EvalShaStr(sha1, keys.Length, this.MergeAndConvertToBytes(keys, args));
        }

        public List<string> ExecLuaAsList(string body, params string[] args) {
            return this.Eval(body, 0, args.ToMultiByteArray()).ToStringList();
        }

        public List<string> ExecLuaAsList(string luaBody, string[] keys, string[] args) {
            return this.Eval(luaBody, keys.Length, this.MergeAndConvertToBytes(keys, args)).ToStringList();
        }

        public List<string> ExecLuaShaAsList(string sha1, params string[] args) {
            return this.EvalSha(sha1, 0, args.ToMultiByteArray()).ToStringList();
        }

        public List<string> ExecLuaShaAsList(string sha1, string[] keys, string[] args) {
            return this.EvalSha(sha1, keys.Length, this.MergeAndConvertToBytes(keys, args)).ToStringList();
        }


        public bool HasLuaScript(string sha1Ref) {
            return this.WhichLuaScriptsExists(sha1Ref)[sha1Ref];
        }

        public Dictionary<string, bool> WhichLuaScriptsExists(params string[] sha1Refs) {
            byte[][] intFlags = this.ScriptExists(sha1Refs.ToMultiByteArray());
            var map = new Dictionary<string, bool>();
            for (var i = 0; i < sha1Refs.Length; i++) {
                var sha1Ref = sha1Refs[i];
                map[sha1Ref] = intFlags[i].FromUtf8Bytes() == "1";
            }

            return map;
        }

        public void RemoveAllLuaScripts() {
            this.ScriptFlush();
        }

        public void KillRunningLuaScript() {
            this.ScriptKill();
        }

        public string LoadLuaScript(string body) {
            return this.ScriptLoad(body).FromUtf8Bytes();
        }

    }

}
