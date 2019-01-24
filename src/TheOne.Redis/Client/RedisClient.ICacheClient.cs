using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client {

    public partial class RedisClient {

        // Looking up Dictionary<Type,bool> for type is faster than HashSet<Type>.
        private static readonly Dictionary<Type, bool> _numericTypes = new Dictionary<Type, bool> {
            { typeof(byte), true },
            { typeof(sbyte), true },
            { typeof(short), true },
            { typeof(ushort), true },
            { typeof(int), true },
            { typeof(uint), true },
            { typeof(long), true },
            { typeof(ulong), true },
            { typeof(double), true },
            { typeof(float), true },
            { typeof(decimal), true }
        };

        /// <inheritdoc />
        public void RemoveAll(IEnumerable<string> keys) {
            this.Exec(r => r.RemoveEntry(keys.ToArray()));
        }

        /// <inheritdoc />
        public T Get<T>(string key) {
            return this.Exec(r =>
                typeof(T) == typeof(byte[])
                    ? (T)(object)r.Get(key)
                    : r.GetValue(key).FromJson<T>()
            );
        }

        /// <inheritdoc />
        public long Increment(string key, uint amount) {
            return this.Exec(r => r.IncrementValueBy(key, (int)amount));
        }

        /// <inheritdoc />
        public long Decrement(string key, uint amount) {
            return this.Exec(r => this.DecrementValueBy(key, (int)amount));
        }

        /// <inheritdoc />
        public bool Add<T>(string key, T value) {
            return this.Exec(r => r.Set(key, ToBytes(value), false));
        }

        /// <inheritdoc />
        public bool Set<T>(string key, T value) {
            this.Exec(r => ((RedisNativeClient)r).Set(key, ToBytes(value)));
            return true;
        }

        /// <inheritdoc />
        public bool Replace<T>(string key, T value) {
            return this.Exec(r => r.Set(key, ToBytes(value), true));
        }

        /// <inheritdoc />
        public bool Add<T>(string key, T value, DateTime expiresAt) {
            this.AssertNotInTransaction();

            return this.Exec(r => {
                if (r.Add(key, value)) {
                    r.ExpireEntryAt(key, this.ConvertToServerDate(expiresAt));
                    return true;
                }

                return false;
            });
        }

        /// <inheritdoc />
        public bool Add<T>(string key, T value, TimeSpan expiresIn) {
            return this.Exec(r => r.Set(key, ToBytes(value), false, expiryMs: (long)expiresIn.TotalMilliseconds));
        }

        /// <inheritdoc />
        public bool Set<T>(string key, T value, TimeSpan expiresIn) {
            if (this.AssertServerVersionNumber() >= 2600) {
                this.Exec(r => r.Set(key, ToBytes(value), 0, (long)expiresIn.TotalMilliseconds));
            } else {
                this.Exec(r => r.SetEx(key, (int)expiresIn.TotalSeconds, ToBytes(value)));
            }

            return true;
        }

        /// <inheritdoc />
        public bool Set<T>(string key, T value, DateTime expiresAt) {
            this.AssertNotInTransaction();

            this.Exec(r => {
                this.Set(key, value);
                this.ExpireEntryAt(key, this.ConvertToServerDate(expiresAt));
            });
            return true;
        }

        /// <inheritdoc />
        public bool Replace<T>(string key, T value, DateTime expiresAt) {
            this.AssertNotInTransaction();

            return this.Exec(r => {
                if (r.Replace(key, value)) {
                    r.ExpireEntryAt(key, this.ConvertToServerDate(expiresAt));
                    return true;
                }

                return false;
            });
        }

        /// <inheritdoc />
        public bool Replace<T>(string key, T value, TimeSpan expiresIn) {
            return this.Exec(r => r.Set(key, ToBytes(value), true, expiryMs: (long)expiresIn.TotalMilliseconds));
        }

        /// <inheritdoc />
        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys) {
            return this.Exec(r => {
                var keysArray = keys.ToArray();
                var keyValues = r.MGet(keysArray);
                var results = new Dictionary<string, T>();
                var isBytes = typeof(T) == typeof(byte[]);

                var i = 0;
                foreach (var keyValue in keyValues) {
                    var key = keysArray[i++];

                    if (keyValue == null) {
                        results[key] = default;
                        continue;
                    }

                    if (isBytes) {
                        results[key] = (T)(object)keyValue;
                    } else {
                        var keyValueString = keyValue.FromUtf8Bytes();
                        results[key] = keyValueString.FromJson<T>();
                    }
                }

                return results;
            });
        }

        /// <inheritdoc />
        public void SetAll<T>(IDictionary<string, T> values) {
            this.Exec(r => {
                var keys = values.Keys.ToArray();
                var valBytes = new byte[values.Count][];
                var isBytes = typeof(T) == typeof(byte[]);

                var i = 0;
                foreach (var value in values.Values) {
                    if (!isBytes) {
                        var t = value.ToJson();
                        if (t != null) {
                            valBytes[i] = t.ToUtf8Bytes();
                        } else {
                            valBytes[i] = Array.Empty<byte>();
                        }
                    } else {
                        valBytes[i] = (byte[])(object)value ?? Array.Empty<byte>();
                    }

                    i++;
                }

                r.MSet(keys, valBytes);
            });
        }

        public T Exec<T>(Func<RedisClient, T> action) {
            return action(this);
        }

        public void Exec(Action<RedisClient> action) {
            action(this);
        }

        private static byte[] ToBytes<T>(T value) {
            var bytesValue = value as byte[];
            if (bytesValue == null && (_numericTypes.ContainsKey(typeof(T)) || !Equals(value, default(T)))) {
                bytesValue = value.ToJson().ToUtf8Bytes();
            }

            return bytesValue;
        }

    }

}
