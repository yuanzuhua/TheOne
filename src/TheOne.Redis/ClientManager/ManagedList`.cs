using System.Collections;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client;

namespace TheOne.Redis.ClientManager {

    public class ManagedList<T> : IList<T> {

        private readonly string _key;
        private readonly IRedisClientManager _manager;

        /// <inheritdoc />
        private ManagedList() { }

        /// <inheritdoc />
        public ManagedList(IRedisClientManager manager, string key) {
            this._key = key;
            this._manager = manager;
        }

        /// <inheritdoc />
        public int IndexOf(T item) {
            return this.GetRedisList().IndexOf(item);
        }

        /// <inheritdoc />
        public void Insert(int index, T item) {
            using (var redis = this.GetClient()) {
                redis.As<T>().Lists[this._key].Insert(index, item);
            }
        }

        /// <inheritdoc />
        public void RemoveAt(int index) {
            using (var redis = this.GetClient()) {
                redis.As<T>().Lists[this._key].RemoveAt(index);
            }
        }

        /// <inheritdoc />
        public T this[int index] {
            get => this.GetRedisList()[index];
            set {
                using (var redis = this.GetClient()) {
                    redis.As<T>().Lists[this._key][index] = value;
                }
            }
        }

        /// <inheritdoc />
        public void Add(T item) {
            using (var redis = this.GetClient()) {
                redis.As<T>().Lists[this._key].Add(item);
            }
        }

        /// <inheritdoc />
        public void Clear() {
            using (var redis = this.GetClient()) {
                redis.As<T>().Lists[this._key].Clear();
            }
        }

        /// <inheritdoc />
        public bool Contains(T item) {
            return this.GetRedisList().Contains(item);
        }

        /// <inheritdoc />
        public void CopyTo(T[] array, int arrayIndex) {
            this.GetRedisList().CopyTo(array, arrayIndex);
        }

        /// <inheritdoc />
        public int Count => this.GetRedisList().Count();

        /// <inheritdoc />
        public bool IsReadOnly => false;

        /// <inheritdoc />
        public bool Remove(T item) {
            var index = this.IndexOf(item);
            if (index != -1) {
                this.RemoveAt(index);
                return true;
            }

            return false;
        }

        /// <inheritdoc />
        public IEnumerator<T> GetEnumerator() {
            return this.GetRedisList().GetEnumerator();
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator() {
            return ((IEnumerable)this.GetRedisList()).GetEnumerator();
        }

        private IRedisClient GetClient() {
            return this._manager.GetClient();
        }

        private List<T> GetRedisList() {
            using (var redis = this.GetClient()) {
                var client = redis.As<T>();
                return client.Lists[this._key].ToList();
            }
        }

    }

}
