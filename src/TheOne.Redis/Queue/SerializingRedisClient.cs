using System.Collections;
using System.Collections.Generic;
using TheOne.Redis.Client;

namespace TheOne.Redis.Queue {

    /// <inheritdoc />
    public class SerializingRedisClient : RedisClient {

        private ISerializer _serializer = new ObjectSerializer();

        /// <inheritdoc />
        public SerializingRedisClient(string host) : base(host) { }

        /// <inheritdoc />
        public SerializingRedisClient(RedisEndpoint config) : base(config) { }

        /// <inheritdoc />
        public SerializingRedisClient(string host, int port) : base(host, port) { }

        /// <summary>
        ///     customize the client serializer
        /// </summary>
        public ISerializer Serializer {
            set => this._serializer = value;
        }

        /// <summary>
        ///     Serialize object to buffer
        /// </summary>
        /// <param name="value" >serializable object</param>
        public byte[] Serialize(object value) {
            return this._serializer.Serialize(value);
        }

        /// <param name="values" >array of serializable objects</param>
        public List<byte[]> Serialize(object[] values) {
            var rc = new List<byte[]>();
            foreach (var value in values) {
                var bytes = this.Serialize(value);
                if (bytes != null) {
                    rc.Add(bytes);
                }
            }

            return rc;
        }

        /// <summary>
        ///     Deserialize buffer to object
        /// </summary>
        /// <param name="someBytes" >byte array to deserialize</param>
        public object Deserialize(byte[] someBytes) {
            return this._serializer.Deserialize(someBytes);
        }

        /// <summary>
        ///     deserialize an array of byte arrays
        /// </summary>
        public IList Deserialize(byte[][] byteArray) {
            IList rc = new ArrayList();
            foreach (var someBytes in byteArray) {
                var obj = this.Deserialize(someBytes);
                if (obj != null) {
                    rc.Add(obj);
                }
            }

            return rc;
        }

    }

}
