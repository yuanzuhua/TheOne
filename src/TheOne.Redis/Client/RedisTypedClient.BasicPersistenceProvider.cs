using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client {

    public partial class RedisTypedClient<T> {

        /// <inheritdoc />
        public T GetById(object id) {
            var key = this._client.UrnKey<T>(id);
            return this.GetValue(key);
        }

        /// <inheritdoc />
        public IList<T> GetByIds(IEnumerable ids) {
            if (ids != null) {
                List<string> urnKeys = ids.Cast<object>().Select(x => this._client.UrnKey<T>(x)).ToList();
                if (urnKeys.Count != 0) {
                    return this.GetValues(urnKeys);
                }
            }

            return new List<T>();
        }

        /// <inheritdoc />
        public IList<T> GetAll() {
            HashSet<string> allKeys = this._client.GetAllItemsFromSet(this.TypeIdsSetKey);
            return this.GetByIds(allKeys.ToArray());
        }

        /// <inheritdoc />
        public T Store(T entity) {
            var urnKey = this._client.UrnKey(entity);
            this.SetValue(urnKey, entity);
            return entity;
        }

        /// <inheritdoc />
        public T Store(T entity, TimeSpan expireIn) {
            var urnKey = this._client.UrnKey(entity);
            this.SetValue(urnKey, entity, expireIn);
            return entity;
        }

        /// <inheritdoc />
        public void StoreAll(IEnumerable<T> entities) {
            if (entities == null) {
                return;
            }

            List<T> entitiesList = entities.ToList();
            var len = entitiesList.Count;

            var keys = new byte[len][];
            var values = new byte[len][];

            for (var i = 0; i < len; i++) {
                keys[i] = this._client.UrnKey(entitiesList[i]).ToUtf8Bytes();
                values[i] = Client.RedisClient.SerializeToUtf8Bytes(entitiesList[i]);
            }

            this._client.MSet(keys, values);
            this._client.RegisterTypeIds(entitiesList);
        }

        /// <inheritdoc />
        public void Delete(T entity) {
            var urnKey = this._client.UrnKey(entity);
            this.RemoveEntry(urnKey);
            this._client.RemoveTypeIds(entity);
        }

        /// <inheritdoc />
        public void DeleteById(object id) {
            var urnKey = this._client.UrnKey<T>(id);

            this.RemoveEntry(urnKey);
            this._client.RemoveTypeIds<T>(id.ToString());
        }

        /// <inheritdoc />
        public void DeleteByIds(IEnumerable ids) {
            List<object> cast = ids?.Cast<object>().ToList() ?? new List<object>();
            List<string> urnKeys = cast.Select(t => this._client.UrnKey<T>(t)).ToList();
            if (urnKeys.Count > 0) {
                this.RemoveEntry(urnKeys.ToArray());
                this._client.RemoveTypeIds<T>(cast.Select(x => x.ToString()).ToArray());
            }
        }

        /// <inheritdoc />
        public void DeleteAll() {
            HashSet<string> ids = this._client.GetAllItemsFromSet(this.TypeIdsSetKey);
            List<string> urnKeys = ids.Select(t => this._client.UrnKey<T>(t)).ToList();
            if (urnKeys.Count > 0) {
                this.RemoveEntry(urnKeys.ToArray());
                this.RemoveEntry(this.TypeIdsSetKey);
            }
        }

    }

}
