using System.Collections;
using System.Collections.Generic;
using TheOne.Redis.Client;

namespace TheOne.Redis.Queue {

    public class SerializingRedisClient : RedisClient {

        private ISerializer _serializer = new ObjectSerializer();

        public SerializingRedisClient(string host)
            : base(host) { }

        public SerializingRedisClient(RedisEndpoint config)
            : base(config) { }

        public SerializingRedisClient(string host, int port)
            : base(host, port) { }

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
            foreach (object value in values) {
                byte[] bytes = this.Serialize(value);
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
            foreach (byte[] someBytes in byteArray) {
                object obj = this.Deserialize(someBytes);
                if (obj != null) {
                    rc.Add(obj);
                }
            }

            return rc;
        }

    }

}
