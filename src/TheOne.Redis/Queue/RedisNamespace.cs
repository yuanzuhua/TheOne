using TheOne.Redis.Queue.Locking;

namespace TheOne.Redis.Queue {

    /// <summary>
    ///     manages a "region" in the redis key space
    ///     namespace can be cleared by incrementing the generation
    /// </summary>
    public class RedisNamespace {

        private const string _uniqueCharacter = "?";

        /// <summary>
        ///     make reserved keys unique by tacking N of these to the beginning of the string
        /// </summary>
        private const string _reservedTag = "@" + _uniqueCharacter + "@";

        /// <summary>
        ///     unique separator between namespace and key
        /// </summary>
        private const string _namespaceKeySeparator = "#" + _uniqueCharacter + "#";

        /// <summary>
        ///     make non-static keys unique by tacking on N of these to the end of the string
        /// </summary>
        public const string KeyTag = "%" + _uniqueCharacter + "%";

        public const string NamespaceTag = "!" + _uniqueCharacter + "!";

        /// <summary>
        ///     remove any odd numbered runs of the UniqueCharacter character
        /// </summary>
        private const string _sanitizer = _uniqueCharacter + _uniqueCharacter;

        /// <summary>
        ///     key for list of keys slated for garbage collection
        /// </summary>
        public const string NamespacesGarbageKey = _reservedTag + "REDIS_NAMESPACES_GARBAGE";

        public const int NumTagsForKey = 0;
        public const int NumTagsForLockKey = 1;

        /// <summary>
        ///     key for set of all global keys in this namespace
        /// </summary>
        private readonly string _globalKeysKey;

        /// <summary>
        ///     key for namespace generation
        /// </summary>
        private readonly string _namespaceGenerationKey;

        /// <summary>
        ///     sanitized name for namespace (includes namespace generation)
        /// </summary>
        private readonly string _namespacePrefix;

        /// <summary>
        ///     reserved, unique name for meta entries for this namespace
        /// </summary>
        // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
        private readonly string _namespaceReservedName;

        /// <summary>
        ///     namespace generation - when generation changes, namespace is slated for garbage collection
        /// </summary>
        private long _namespaceGeneration = -1;

        public RedisNamespace(string name) {
            this._namespacePrefix = Sanitize(name);
            this._namespaceReservedName = NamespaceTag + this._namespacePrefix;
            this._globalKeysKey = this._namespaceReservedName;
            // get generation
            this._namespaceGenerationKey = this._namespaceReservedName + "_" + "generation";
            this.LockingStrategy = new ReaderWriterLockingStrategy();
        }

        /// <summary>
        ///     get locking strategy
        /// </summary>
        public ILockingStrategy LockingStrategy { get; set; }

        /// <summary>
        ///     get current generation
        /// </summary>
        public long GetGeneration() {
            using (this.LockingStrategy.ReadLock()) {
                return this._namespaceGeneration;
            }
        }

        /// <summary>
        ///     set new generation
        /// </summary>
        public void SetGeneration(long generation) {
            if (generation < 0) {
                return;
            }

            using (this.LockingStrategy.WriteLock()) {
                if (this._namespaceGeneration == -1 || generation > this._namespaceGeneration) {
                    this._namespaceGeneration = generation;
                }
            }
        }

        /// <summary>
        ///     redis key for generation
        /// </summary>
        public string GetGenerationKey() {
            return this._namespaceGenerationKey;
        }

        /// <summary>
        ///     get redis key that holds all namespace keys
        /// </summary>
        public string GetGlobalKeysKey() {
            return this._globalKeysKey;
        }

        /// <summary>
        ///     get global cache key
        /// </summary>
        public string GlobalCacheKey(object key) {
            return this.GlobalKey(key, NumTagsForKey);
        }

        public string GlobalLockKey(object key) {
            return this.GlobalKey(key, NumTagsForLockKey) + "LOCK";
        }

        /// <summary>
        ///     get global key inside of this namespace
        /// </summary>
        /// <param name="key" >key</param>
        /// <param name="numUniquePrefixes" >prefixes can be added for name deconfliction</param>
        public string GlobalKey(object key, int numUniquePrefixes) {
            var rc = Sanitize(key);
            if (this._namespacePrefix != null && !this._namespacePrefix.Equals("")) {
                rc = this._namespacePrefix + "_" + this.GetGeneration() + _namespaceKeySeparator + rc;
            }

            for (var i = 0; i < numUniquePrefixes; ++i) {
                rc += KeyTag;
            }

            return rc;
        }

        /// <summary>
        ///     replace UniqueCharacter with its double, to avoid name clash
        /// </summary>
        private static string Sanitize(string dirtyString) {
            return dirtyString?.Replace(_uniqueCharacter, _sanitizer);
        }

        private static string Sanitize(object dirtyString) {
            return Sanitize(dirtyString.ToString());
        }

    }

}
