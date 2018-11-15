using System;

namespace TheOne.Redis.Queue {

    /// <summary>
    ///     wraps a serialized representation of an object
    /// </summary>
    [Serializable]
    internal struct SerializedObjectWrapper {

        /// <summary>
        ///     Initializes a new instance of <see cref="SerializedObjectWrapper" />.
        /// </summary>
        /// <param name="flags" >Custom item data.</param>
        /// <param name="data" >The serialized item.</param>
        public SerializedObjectWrapper(ushort flags, ArraySegment<byte> data) {
            this.Data = data;
            this.Flags = flags;
        }

        /// <summary>
        ///     The data representing the item being stored/retrieved.
        /// </summary>
        public ArraySegment<byte> Data { get; set; }

        /// <summary>
        ///     Flags set for this instance.
        /// </summary>
        public ushort Flags { get; set; }

    }

}
