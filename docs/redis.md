# TheOne.Redis - C#/.NET Client for Redis

- [TheOne.Redis - C#/.NET Client for Redis](#theoneredis-cnet-client-for-redis)
    - [Getting Started](#getting-started)
    - [Redis Connection Strings](#redis-connection-strings)
    - [Redis Client Managers](#redis-client-managers)
        - [RedisManagerPool](#redismanagerpool)
        - [PooledRedisClientManager](#pooledredisclientmanager)
        - [BasicRedisClientManager](#basicredisclientmanager)
    - [Redis Sentinel](#redis-sentinel)
        - [Custom Redis Connection String](#custom-redis-connection-string)
        - [Change to use RedisManagerPool](#change-to-use-redismanagerpool)
        - [Start monitoring Sentinels](#start-monitoring-sentinels)
    - [Managed Pub/Sub Server](#managed-pubsub-server)
    - [Redis Stats](#redis-stats)
    - [Automatic Retries](#automatic-retries)
    - [Redis GEO](#redis-geo)
    - [Custom Redis Commands](#custom-redis-commands)
    - [Lex Operations](#lex-operations)
    - [HyperLog](#hyperlog)
    - [Exec Cached Lua Scripts](#exec-cached-lua-scripts)
    - [Scan](#scan)
        - [Efficient SCAN in LUA](#efficient-scan-in-lua)
            - [Alternative Complex Response](#alternative-complex-response)

* * *

**TheOne.Redis** is a simple, high-performance and feature-rich C# Client for Redis with native support and high-level abstractions for serializing POCOs and Complex Types.

There are a number of different apis available with the `RedisClient` implementing the following interfaces:

 * `ICacheClient` - using Redis solely as a cache.
 * `IRedisNativeClient` - low-level raw byte access where you can control your own serialization/deserialization that map 1:1 with Redis operations of the same name.
 * `IRedisClient` - access to Redis specific functionality, provides a friendlier, more descriptive api that lets you store values as strings (UTF8 encoding).
 * `IRedisTypedClient` - created with `IRedisClient.As<T>()`, returns a *strongly-typed client* that provides a typed-interface for all redis value operations that works against any C#/.NET POCO type.

An overview of class hierarchy for the C# Redis clients looks like:

    RedisTypedClient (POCO) > RedisClient (string) > RedisNativeClient (raw byte[])

With each client providing different layers of abstraction:

  * The `RedisNativeClient` exposes raw `byte[]` apis and does no marshalling and passes all values directly to redis.
  * The `RedisClient` assumes `string` values and simply converts strings to UTF8 bytes before sending to Redis.
  * The `RedisTypedClient` provides a generic interface allowing you to add POCO values. The POCO types are serialized using [Newtonsoft.Json](https://www.nuget.org/packages/Newtonsoft.Json/) which is then converted to UTF8 bytes and sent to Redis.

## Getting Started

install via NuGet:

    PM> Install-Package TheOne.Redis

See unit test for examples:

* [Todo App](../src/TheOne.Redis.Tests/Examples/TodoApp.cs)
* [Simple Use Case](../src/TheOne.Redis.Tests/Examples/SimpleUseCase.cs)
* [and more...](../src/TheOne.Redis.Tests/Examples)

Change connection strings in [Config.cs](../src/TheOne.Redis.Tests/Config.cs) as as necessary,
or use [scripts](../redis/start-cmd.txt) to setup redis.

## Redis Connection Strings

Redis Connection Strings supports multiple URI-like formats,
from a simple **hostname** or **IP Address and port** pair to a fully-qualified **URI** with multiple options specified on the QueryString.

Some examples of supported formats:

    localhost
    127.0.0.1:6379
    redis://localhost:6379
    password@localhost:6379
    clientid:password@localhost:6379
    redis://clientid:password@localhost:6380?ssl=true&db=1

See [ConnectionStringTests.cs](../src/TheOne.Redis.Tests/Basic/ConnectionStringTests.cs) for more examples.

Any additional configuration can be specified as QueryString parameters.
The full list of options that can be specified can be found in [RedisEndpoint.cs](../src/TheOne.Redis/RedisEndpoint.cs).

## Redis Client Managers

The recommended way to access `RedisClient` instances is to use one of the available Thread-Safe Client Managers below.
Client Managers are connection factories which is ideally registered as a Singleton either in your IOC or static classes.

### RedisManagerPool

```csharp
container.Register<IRedisClientManager>(c => new RedisManagerPool(redisConnectionString));
```

Any connections required after the maximum Pool size has been reached will be created and disposed outside of the Pool.
By not being restricted to a maximum pool size,
the pooling behavior in `RedisManagerPool` can maintain a smaller connection pool size
at the cost of potentially having a higher opened/closed connection count.

### PooledRedisClientManager

If you prefer to define options on the Client Manager itself,
or you want to provide separate Read/Write and ReadOnly (i.e. Master and Slave) redis-servers,
use the `PooledRedisClientManager` instead:

```csharp
container.Register<IRedisClientManager>(c =>
    new PooledRedisClientManager(redisReadWriteHosts, redisReadOnlyHosts) {
        ConnectTimeout = 100,
        // ...
    });
```

The `PooledRedisClientManager` imposes a maximum connection limit and
when its maximum pool size has been reached,
will instead block on any new connection requests until the next `RedisClient` is released back into the pool.
If no client became available within `PoolTimeout`, a Pool `TimeoutException` will be thrown.

### BasicRedisClientManager

If don't want to use connection pooling (i.e. your accessing a local redis-server instance),
you can use a basic (non-pooled) Client Manager which creates a new `RedisClient` instance each time:

```csharp
container.Register<IRedisClientManager>(c => new BasicRedisClientManager(redisConnectionString));
```

Once registered, accessing the RedisClient is the same in all Client Managers, e.g:

```csharp
var clientsManager = container.Resolve<IRedisClientManager>();
using (IRedisClient redis = clientsManager.GetClient()) {
    redis.IncrementValue("counter");
    List<string> days = redis.GetAllItemsFromList("days");

    // Access Typed api
    var redisTodos = redis.As<Todo>();

    redisTodos.Store(new Todo {
        Id = redisTodos.GetNextSequence(),
        Content = "Learn Redis",
    });

    var todo = redisTodos.GetById(1);

    // Access Native Client
    var redisNative = (IRedisNativeClient)redis;

    redisNative.Incr("counter");
    List<string> days = redisNative.LRange("days", 0, -1);
}
```

A more detailed list of the available RedisClient apis used in the example can be found in following interfaces:

 - [IRedisClient](../src/TheOne.Redis/Client/Interfaces/IRedisClient.cs)
 - [IRedisTypedClient<T>](../src/TheOne.Redis/Client/Interfaces/IRedisTypedClient.cs)
 - [IRedisNativeClient](../src/TheOne.Redis/Client/Interfaces/IRedisNativeClient.cs)

## Redis Sentinel

To use the Sentinel support, instead of populating the Redis Client Managers
with the connection string of the master and slave instances,
you would create a single `RedisSentinel` instance configured with the connection string of the running Redis Sentinels:

```csharp
var sentinelHosts = new[]{ "sentinel1", "sentinel2:6390", "sentinel3" };
var sentinel = new RedisSentinel(sentinelHosts, masterName: "mymaster");
```

This configues a `RedisSentinel` with 3 sentinel hosts looking at **mymaster** group.
As the default port for sentinels when unspecified is **26379** and how RedisSentinel is able to auto-discover other sentinels,
the minimum configuration required is with a single Sentinel host:

```csharp
var sentinel = new RedisSentinel("sentinel1");
```

### Custom Redis Connection String

The host the RedisSentinel is configured with only applies to that Sentinel Host,
to use the flexibility of [Redis Connection Strings](#redis-connection-strings) to apply configuration on individual Redis Clients
you need to register a custom `HostFilter`:

```csharp
sentinel.HostFilter = host => $"{host}?db=1&RetryTimeout=5000";
```

An alternative to using connection strings for configuring clients is to modify
[default configuration on RedisConfig](../src/TheOne.Redis/RedisConfig.cs).

### Change to use RedisManagerPool

By default RedisSentinel uses a `PooledRedisClientManager`, this can be changed to use the `RedisManagerPool` with:

```csharp
sentinel.RedisManagerFactory = (master,slaves) => new RedisManagerPool(master);
```

### Start monitoring Sentinels

Once configured, you can start monitoring the Redis Sentinel servers and access the pre-configured client manager with:

```csharp
IRedisClientManager redisManager = sentinel.Start();
```

Which as before, can be registered in your preferred IOC as a **singleton** instance:

```csharp
container.Register<IRedisClientManager>(c => sentinel.Start());
```

## Managed Pub/Sub Server

`RedisPubSubServer` processes messages in a managed background thread that automatically reconnects
when the redis-server connection fails and works like an independent background Service that can be
stopped and started on command.

See public api in the [IRedisPubSubServer](../src/TheOne.Redis/PubSub/IRedisPubSubServer.cs) interface.

To use `RedisPubSubServer`, initialize it with the channels you want to subscribe to and assign handlers
for each of the events you want to handle. At a minimum you'll want to handle `OnMessage`:

```csharp
var clientsManager = new PooledRedisClientManager();
var redisPubSub = new RedisPubSubServer(clientsManager, "channel-1", "channel-2") {
        OnMessage = (channel, msg) => Console.WriteLine($"Received '{msg}' from '{channel}'")
    }.Start();
```

Calling `Start()` after it's initialized will get it to start listening
and processing any messages published to the subscribed channels.

## Redis Stats

Use the [Redis Stats](../src/TheOne.Redis/Misc/RedisStats.cs) class for visibility and introspection into your running instances.

## Automatic Retries

To improve the resilience of client connections, `RedisClient` will transparently retry failed
Redis operations due to Socket and I/O Exceptions in an exponential backoff starting from
**10ms** up until the `RetryTimeout` of **10000ms**. These defaults can be tweaked with:

```csharp
RedisConfig.DefaultRetryTimeout = 10000;
RedisConfig.BackOffMultiplier = 10;
```

## Redis GEO

See [RedisGeoTests](../src/TheOne.Redis.Tests/Basic/RedisGeoTests.cs) and [RedisGeoNativeClientTests](../src/TheOne.Redis.Tests/Basic/RedisGeoNativeClientTests.cs).

## Custom Redis Commands

With the `Custom` and `RawCommand` apis on `IRedisClient` and `IRedisNativeClient`
you can use the RedisClient to send your own custom commands that can call adhoc Redis commands.

These Custom apis take a flexible `object[]` arguments which accepts any serializable value e.g.
`byte[]`, `string`, `int` as well as any user-defined Complex Types
which are transparently serialized as JSON and send across the wire as UTF-8 bytes.

There are also convenient extension methods
on [RedisData](../src/TheOne.Redis/Client/Models/RedisData.cs) and [RedisText](../src/TheOne.Redis/Client/Models/RedisText.cs) that make it easy to access structured data.

See [CustomCommandTests.cs](../src/TheOne.Redis.Tests/Basic/CustomCommandTests.cs) for examples.

## Lex Operations

The [ZRANGEBYLEX](http://redis.io/commands/zrangebylex) sorted set operations allowing you to query a sorted set lexically have been added.
A good showcase for this is available on [autocomplete.redis.io](http://autocomplete.redis.io/).

Just like NuGet version matchers, Redis uses `[` char to express inclusiveness and `(` char for exclusiveness.
Since the `IRedisClient` apis defaults to inclusive searches, these two apis are the same:

```csharp
Redis.SearchSortedSetCount("zset", "a", "c")
Redis.SearchSortedSetCount("zset", "[a", "[c")
```

Alternatively you can specify one or both bounds to be exclusive by using the `(` prefix, e.g:

```csharp
Redis.SearchSortedSetCount("zset", "a", "(c")
Redis.SearchSortedSetCount("zset", "(a", "(c")
```

See [LexTests.cs](../src/TheOne.Redis.Tests/Basic/LexTests.cs) for examples.

## HyperLog

Maintain an efficient way to count and merge unique elements in a set without having to store its elements.

See [RedisHyperLogTests.cs](../src/TheOne.Redis.Tests/Basic/RedisHyperLogTests.cs) for examples.

## Exec Cached Lua Scripts

`ExecCachedLua` is a convenient high-level api that eliminates the bookkeeping
required for executing high-performance server LUA Scripts
which suffers from many of the problems that RDBMS stored procedures have which depends on pre-existing state in the RDBMS
that needs to be updated with the latest version of the Stored Procedure.

With Redis LUA you either have the option to send, parse, load then execute the entire LUA script each time it's called or
alternatively you could pre-load the LUA Script into Redis once on StartUp and then execute it using the Script's SHA1 hash.
The issue with this is that if the Redis server is accidentally flushed you're left with a broken application relying on a
pre-existing script that's no longer there.

The `ExecCachedLua` api provides the best of both worlds where it will always execute the compiled SHA1 script, saving bandwidth and CPU but will also re-create the LUA Script if it no longer exists.

You can instead execute the compiled LUA script above by its SHA1 identifier, which continues to work regardless if it never existed or was removed at runtime, e.g:

```csharp
// #1: Loads LUA script and caches SHA1 hash in Redis Client
r = redis.ExecCachedLua(FastScanScript, sha1 =>
    redis.ExecLuaSha(sha1, "key:*", "10"));

// #2: Executes using cached SHA1 hash
r = redis.ExecCachedLua(FastScanScript, sha1 =>
    redis.ExecLuaSha(sha1, "key:*", "10"));

// Deletes all existing compiled LUA scripts
redis.ScriptFlush();

// #3: Executes using cached SHA1 hash, gets NOSCRIPT Error,
//     re-creates then re-executes the LUA script using its SHA1 hash
r = redis.ExecCachedLua(FastScanScript, sha1 =>
    redis.ExecLuaSha(sha1, "key:*", "10"));
```

See [IRedisClient.Lua.cs](../src/TheOne.Redis/Client/Interfaces/IRedisClient.Lua.cs) for `IRedisClient` public apis.

See [LuaCachedScriptTests](../src/TheOne.Redis.Tests/Basic/LuaCachedScriptTests.cs) for usage examples.

## Scan

[SCAN](http://redis.io/commands/scan) operation provides an optimal strategy for traversing a redis instance entire keyset in managable-size chunks utilizing only a client-side cursor and without introducing any server state.

It's a higher performance alternative and should be used instead of [KEYS](http://redis.io/commands/keys) in application code.

The `IRedisClient` provides a higher-level api that abstracts away the client cursor to expose a lazy Enumerable sequence to provide an optimal way to stream scanned results that integrates nicely with LINQ, e.g:

```csharp
var scanUsers = Redis.ScanAllKeys("urn:User:*");
var sampleUsers = scanUsers.Take(10000).ToList(); // Stop after retrieving 10000 user keys
```

See [RedisScanTests.cs](../src/TheOne.Redis.Tests/Basic/RedisScanTests.cs) for examples.

### Efficient SCAN in LUA

The C# api below returns the first 10 results matching the `key:*` pattern:

```csharp
var keys = Redis.ScanAllKeys(pattern: "key:*", pageSize: 10).Take(10).ToList();
```

However the C# Streaming api above requires an unknown number of Redis Operations (bounded to the number of keys in Redis) to complete the request.
The number of SCAN calls can be reduced by choosing a higher `pageSize` to tell Redis to scan more keys each time the SCAN operation is called.

As the number of api calls has the potential to result in a large number of Redis Operations,
it can end up yielding an unacceptable delay due to the latency of multiple dependent remote network calls.
An easy solution is to instead have the multiple SCAN calls performed in-process on the Redis Server, eliminating the network latency of multiple SCAN calls, e.g:

```csharp
const string FastScanScript = @"
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
return results";

RedisText r = redis.ExecLua(FastScanScript, "key:*", "10");
Console.WriteLine(r.Children.Count) // = 10
```

The `ExecLua` api returns this complex LUA table response in the `Children` collection of the `RedisText` Response.

#### Alternative Complex Response

Another way to return complex data structures in a LUA operation is to serialize the result as JSON:

    return cjson.encode(results)

Which you can access as raw JSON by parsing the response as a String with:

```csharp
string json = redis.ExecLuaAsString(FastScanScript, "key:*", "10");
```
