using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using TheOne.Redis.Common;

namespace TheOne.Redis.ClientManager.Internal {

    internal class ConsistentHash<T> {

        // multiple replicas of each node improves distribution
        private const int _replicas = 200;
        private readonly SortedDictionary<ulong, T> _circle = new SortedDictionary<ulong, T>();

        // default hash function
        private readonly Func<string, ulong> _hashFunction = Md5Hash;

        public ConsistentHash() { }

        public ConsistentHash(IEnumerable<KeyValuePair<T, int>> nodes, Func<string, ulong> hashFunction = null) {
            if (hashFunction != null) {
                this._hashFunction = hashFunction;
            }

            foreach (KeyValuePair<T, int> node in nodes) {
                this.AddTarget(node.Key, node.Value);
            }
        }

        public T GetTarget(string key) {
            var hash = this._hashFunction(key);
            var firstNode = ModifiedBinarySearch(this._circle.Keys.ToArray(), hash);
            return this._circle[firstNode];
        }

        /// <summary>
        ///     Adds a node and maps points across the circle
        /// </summary>
        /// <param name="node" > node to add </param>
        /// <param name="weight" > An arbitrary number, specifies how often it occurs relative to other targets. </param>
        public void AddTarget(T node, int weight) {
            // increase the replicas of the node by weight
            var repeat = weight > 0 ? weight * _replicas : _replicas;

            for (var i = 0; i < repeat; i++) {
                var identifier = node.GetHashCode() + "-" + i;
                var hashCode = this._hashFunction(identifier);
                this._circle.Add(hashCode, node);
            }
        }

        /// <summary>
        ///     A variation of Binary Search algorithm. Given a number, matches the next highest number from the sorted array.
        ///     If a higher number does not exist, then the first number in the array is returned.
        /// </summary>
        /// <param name="sortedArray" > a sorted array to perform the search </param>
        /// <param name="val" > number to find the next highest number against </param>
        /// <returns> next highest number </returns>
        public static ulong ModifiedBinarySearch(ulong[] sortedArray, ulong val) {
            var min = 0;
            var max = sortedArray.Length - 1;

            if (val < sortedArray[min] || val > sortedArray[max]) {
                return sortedArray[0];
            }

            while (max - min > 1) {
                var mid = (max + min) / 2;
                if (sortedArray[mid] >= val) {
                    max = mid;
                } else {
                    min = mid;
                }
            }

            return sortedArray[max];
        }

        /// <summary>
        ///     Given a key, generates an unsigned 64 bit hash code using MD5
        /// </summary>
        public static ulong Md5Hash(string key) {
            using (MD5 hash = MD5.Create()) {
                byte[] data = hash.ComputeHash(key.ToUtf8Bytes());
                var a = BitConverter.ToUInt64(data, 0);
                var b = BitConverter.ToUInt64(data, 8);
                var hashCode = a ^ b;
                return hashCode;
            }
        }

    }

}
