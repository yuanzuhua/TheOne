using System.Collections;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client;

namespace TheOne.Redis.ClientManager {

    public class ManagedList<T> : IList<T> {

        private readonly string _key;
        private readonly IRedisClientManager _manager;
        private ManagedList() { }

        public ManagedList(IRedisClientManager manager, string key) {
            this._key = key;
            this._manager = manager;
        }

        public int IndexOf(T item) {
            return this.GetRedisList().IndexOf(item);
        }

        public void Insert(int index, T item) {
            using (IRedisClient redis = this.GetClient()) {
                redis.As<T>().Lists[this._key].Insert(index, item);
            }
        }

        public void RemoveAt(int index) {
            using (IRedisClient redis = this.GetClient()) {
                redis.As<T>().Lists[this._key].RemoveAt(index);
            }
        }

        public T this[int index] {
            get => this.GetRedisList()[index];
            set {
                using (IRedisClient redis = this.GetClient()) {
                    redis.As<T>().Lists[this._key][index] = value;
                }
            }
        }


        public void Add(T item) {
            using (IRedisClient redis = this.GetClient()) {
                redis.As<T>().Lists[this._key].Add(item);
            }
        }

        public void Clear() {
            using (IRedisClient redis = this.GetClient()) {
                redis.As<T>().Lists[this._key].Clear();
            }
        }

        public bool Contains(T item) {
            return this.GetRedisList().Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex) {
            this.GetRedisList().CopyTo(array, arrayIndex);
        }

        public int Count => this.GetRedisList().Count();

        public bool IsReadOnly => false;

        public bool Remove(T item) {
            var index = this.IndexOf(item);
            if (index != -1) {
                this.RemoveAt(index);
                return true;
            }

            return false;
        }

        public IEnumerator<T> GetEnumerator() {
            return this.GetRedisList().GetEnumerator();
        }


        IEnumerator IEnumerable.GetEnumerator() {
            return ((IEnumerable)this.GetRedisList()).GetEnumerator();
        }

        private IRedisClient GetClient() {
            return this._manager.GetClient();
        }

        private List<T> GetRedisList() {
            using (IRedisClient redis = this.GetClient()) {
                IRedisTypedClient<T> client = redis.As<T>();
                return client.Lists[this._key].ToList();
            }
        }

    }

}
