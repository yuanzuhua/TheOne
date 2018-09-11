using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client {

    public partial class RedisClient {

        private Dictionary<string, HashSet<string>> _registeredTypeIdsWithinPipelineMap = new Dictionary<string, HashSet<string>>();

        /// <inheritdoc />
        public T GetById<T>(object id) {
            var key = this.UrnKey<T>(id);
            var valueString = this.GetValue(key);
            var value = valueString.FromJson<T>();
            return value;
        }

        /// <inheritdoc />
        public IList<T> GetByIds<T>(ICollection ids) {
            if (ids == null || ids.Count == 0) {
                return new List<T>();
            }

            List<string> urnKeys = ids.Cast<object>().Select(this.UrnKey<T>).ToList();
            return this.GetValues<T>(urnKeys);
        }

        /// <inheritdoc />
        public T Store<T>(T entity) {
            var urnKey = this.UrnKey(entity);
            var valueString = entity.ToJson();

            this.SetValue(urnKey, valueString);
            this.RegisterTypeId(entity);

            return entity;
        }

        /// <inheritdoc />
        public object StoreObject(object entity) {
            if (entity == null) {
                throw new ArgumentNullException(nameof(entity));
            }

            object id = entity.GetObjectId();
            Type entityType = entity.GetType();
            var urnKey = this.UrnKey(entityType, id);
            var valueString = entity.ToJson();

            this.SetValue(urnKey, valueString);

            this.RegisterTypeId(this.GetTypeIdsSetKey(entityType), id.ToString());

            return entity;
        }

        /// <inheritdoc />
        public void StoreAll<TEntity>(IEnumerable<TEntity> entities) {
            this._StoreAll(entities);
        }

        /// <inheritdoc />
        public T GetFromHash<T>(object id) {
            var key = this.UrnKey<T>(id);
            return this.GetAllEntriesFromHash(key).ToJson().FromJson<T>();
        }

        /// <summary>
        ///     Store object fields as a dictionary of values in a Hash value.
        ///     Conversion to Dictionary can be customized with RedisClient.ConvertToHashFn
        /// </summary>
        public void StoreAsHash<T>(T entity) {
            var key = this.UrnKey(entity);
            Dictionary<string, string> hash = ConvertToHashFn(entity);
            this.SetRangeInHash(key, hash);
            this.RegisterTypeId(entity);
        }

        /// <inheritdoc />
        public void WriteAll<TEntity>(IEnumerable<TEntity> entities) {
            if (entities == null) {
                return;
            }

            List<TEntity> entitiesList = entities.ToList();
            var len = entitiesList.Count;

            var keys = new byte[len][];
            var values = new byte[len][];

            for (var i = 0; i < len; i++) {
                keys[i] = this.UrnKey(entitiesList[i]).ToUtf8Bytes();
                values[i] = entitiesList[i].ToJsonUtf8Bytes();
            }

            this.MSet(keys, values);
        }

        /// <inheritdoc />
        public void Delete<T>(T entity) {
            var urnKey = this.UrnKey(entity);
            this.Remove(urnKey);
            this.RemoveTypeIds(entity);
        }

        /// <inheritdoc />
        public void DeleteById<T>(object id) {
            var urnKey = this.UrnKey<T>(id);
            this.Remove(urnKey);
            this.RemoveTypeIds<T>(id.ToString());
        }

        /// <inheritdoc />
        public void DeleteByIds<T>(ICollection ids) {
            if (ids == null || ids.Count == 0) {
                return;
            }

            IEnumerable<object> idsList = ids.Cast<object>().ToList();
            List<string> urnKeys = idsList.Select(this.UrnKey<T>).ToList();
            this.RemoveEntry(urnKeys.ToArray());
            this.RemoveTypeIds<T>(idsList.Select(x => x.ToString()).ToArray());
        }

        /// <inheritdoc />
        public void DeleteAll<T>() {
            var typeIdsSetKey = this.GetTypeIdsSetKey<T>();
            HashSet<string> ids = this.GetAllItemsFromSet(typeIdsSetKey);
            if (ids.Count > 0) {
                List<string> urnKeys = ids.ToList().ConvertAll(this.UrnKey<T>);
                this.RemoveEntry(urnKeys.ToArray());
                this.Remove(typeIdsSetKey);
            }
        }

        /// <summary>
        ///     Returns key with automatic object id detection in provided value with
        ///     <typeparam name="T" >generic type</typeparam>
        ///     .
        /// </summary>
        public string UrnKey<T>(T value) {
            return string.Concat(this.NamespacePrefix, value.CreateUrn());
        }

        /// <summary>
        ///     Returns key with explicit object id.
        /// </summary>
        public string UrnKey<T>(object id) {
            return string.Concat(this.NamespacePrefix, IdUtils.CreateUrn<T>(id));
        }

        /// <summary>
        ///     Returns key with explicit object type and id.
        /// </summary>
        public string UrnKey(Type type, object id) {
            return string.Concat(this.NamespacePrefix, IdUtils.CreateUrn(type, id));
        }

        internal HashSet<string> GetRegisteredTypeIdsWithinPipeline(string typeIdsSet) {
            if (!this._registeredTypeIdsWithinPipelineMap.TryGetValue(typeIdsSet, out HashSet<string> registeredTypeIdsWithinPipeline)) {
                registeredTypeIdsWithinPipeline = new HashSet<string>();
                this._registeredTypeIdsWithinPipelineMap[typeIdsSet] = registeredTypeIdsWithinPipeline;
            }

            return registeredTypeIdsWithinPipeline;
        }

        internal void RegisterTypeId<T>(T value) {
            var typeIdsSetKey = this.GetTypeIdsSetKey<T>();
            var id = value.GetId().ToString();

            this.RegisterTypeId(typeIdsSetKey, id);
        }

        internal void RegisterTypeId(string typeIdsSetKey, string id) {
            if (this.Pipeline != null) {
                HashSet<string> registeredTypeIdsWithinPipeline = this.GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                registeredTypeIdsWithinPipeline.Add(id);
            } else {
                this.AddItemToSet(typeIdsSetKey, id);
            }
        }

        internal void RegisterTypeIds<T>(IEnumerable<T> values) {
            var typeIdsSetKey = this.GetTypeIdsSetKey<T>();
            List<string> ids = values.Select(x => x.GetId().ToString()).ToList();

            if (this.Pipeline != null) {
                HashSet<string> registeredTypeIdsWithinPipeline = this.GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                ids.ForEach(x => registeredTypeIdsWithinPipeline.Add(x));
            } else {
                this.AddRangeToSet(typeIdsSetKey, ids);
            }
        }

        internal void RemoveTypeIds<T>(params string[] ids) {
            var typeIdsSetKey = this.GetTypeIdsSetKey<T>();
            if (this.Pipeline != null) {
                HashSet<string> registeredTypeIdsWithinPipeline = this.GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                foreach (var value in ids) {
                    registeredTypeIdsWithinPipeline.Remove(value);
                }

            } else {
                foreach (var value in ids) {
                    this.RemoveItemFromSet(typeIdsSetKey, value);
                }
            }
        }

        internal void RemoveTypeIds<T>(params T[] values) {
            var typeIdsSetKey = this.GetTypeIdsSetKey<T>();
            if (this.Pipeline != null) {
                HashSet<string> registeredTypeIdsWithinPipeline = this.GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                foreach (T value in values) {
                    registeredTypeIdsWithinPipeline.Remove(value.GetId().ToString());
                }

            } else {
                foreach (T value in values) {
                    this.RemoveItemFromSet(typeIdsSetKey, value.GetId().ToString());
                }

            }
        }

        // Called just after original Pipeline is closed.
        internal void AddTypeIdsRegisteredDuringPipeline() {
            foreach (KeyValuePair<string, HashSet<string>> entry in this._registeredTypeIdsWithinPipelineMap) {
                this.AddRangeToSet(entry.Key, entry.Value.ToList());
            }

            this._registeredTypeIdsWithinPipelineMap = new Dictionary<string, HashSet<string>>();
        }

        internal void ClearTypeIdsRegisteredDuringPipeline() {
            this._registeredTypeIdsWithinPipelineMap = new Dictionary<string, HashSet<string>>();
        }

        public IList<T> GetAll<T>() {
            var typeIdsSetKy = this.GetTypeIdsSetKey<T>();
            HashSet<string> allTypeIds = this.GetAllItemsFromSet(typeIdsSetKy);
            List<string> urnKeys = allTypeIds.Cast<object>().Select(this.UrnKey<T>).ToList();
            return this.GetValues<T>(urnKeys);
        }

        // Without the Generic Constraints
        internal void _StoreAll<TEntity>(IEnumerable<TEntity> entities) {
            if (entities == null) {
                return;
            }

            List<TEntity> entitiesList = entities.ToList();
            var len = entitiesList.Count;
            if (len == 0) {
                return;
            }

            var keys = new byte[len][];
            var values = new byte[len][];

            for (var i = 0; i < len; i++) {
                keys[i] = this.UrnKey(entitiesList[i]).ToUtf8Bytes();
                values[i] = entitiesList[i].ToJsonUtf8Bytes();
            }

            this.MSet(keys, values);
            this.RegisterTypeIds(entitiesList);
        }

        public RedisClient CloneClient() {
            return new RedisClient(this.Host, this.Port, this.Password, this.Db) {
                SendTimeout = this.SendTimeout,
                ReceiveTimeout = this.ReceiveTimeout
            };
        }

    }

}
