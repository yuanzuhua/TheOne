using System;
using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public partial interface IRedisClient {

        T ExecCachedLua<T>(string scriptBody, Func<string, T> scriptSha1);

        RedisText ExecLua(string body, params string[] args);

        RedisText ExecLua(string luaBody, string[] keys, string[] args);

        RedisText ExecLuaSha(string sha1, params string[] args);

        RedisText ExecLuaSha(string sha1, string[] keys, string[] args);

        string ExecLuaAsString(string luaBody, params string[] args);

        string ExecLuaAsString(string luaBody, string[] keys, string[] args);

        string ExecLuaShaAsString(string sha1, params string[] args);

        string ExecLuaShaAsString(string sha1, string[] keys, string[] args);

        long ExecLuaAsInt(string luaBody, params string[] args);

        long ExecLuaAsInt(string luaBody, string[] keys, string[] args);

        long ExecLuaShaAsInt(string sha1, params string[] args);

        long ExecLuaShaAsInt(string sha1, string[] keys, string[] args);

        List<string> ExecLuaAsList(string luaBody, params string[] args);

        List<string> ExecLuaAsList(string luaBody, string[] keys, string[] args);

        List<string> ExecLuaShaAsList(string sha1, params string[] args);

        List<string> ExecLuaShaAsList(string sha1, string[] keys, string[] args);

        string CalculateSha1(string luaBody);

        bool HasLuaScript(string sha1Ref);

        Dictionary<string, bool> WhichLuaScriptsExists(params string[] sha1Refs);

        void RemoveAllLuaScripts();

        void KillRunningLuaScript();

        string LoadLuaScript(string body);

    }

}
