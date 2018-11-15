using System;
using System.IO;
using System.Text;

namespace TheOne.Redis.Queue {

    /// <summary>
    ///     Optimized  <see cref="ISerializer" /> implementation. Primitive types are manually serialized, the rest are serialized using binary
    ///     serializer />.
    /// </summary>
    public class OptimizedObjectSerializer : ObjectSerializer {

        internal const ushort RawDataFlag = 0xfa52;

        /// <inheritdoc />
        public override byte[] Serialize(object value) {
            SerializedObjectWrapper temp = this.SerializeToWrapper(value);
            return base.Serialize(temp);
        }

        /// <inheritdoc />
        public override object Deserialize(byte[] someBytes) {
            var temp = (SerializedObjectWrapper)base.Deserialize(someBytes);
            return this.Unwrap(temp);
        }

        /// <summary>
        ///     serialize value and wrap with <see cref="SerializedObjectWrapper" />
        /// </summary>
        private SerializedObjectWrapper SerializeToWrapper(object value) {
            // raw data is a special case when some1 passes in a buffer (byte[] or ArraySegment<byte>)
            if (value is ArraySegment<byte> bytes) {
                // ArraySegment<byte> is only passed in when a part of buffer is being 
                // serialized, usually from a MemoryStream (To avoid duplicating arrays 
                // the byte[] returned by MemoryStream.GetBuffer is placed into an ArraySegment.)
                // 
                return new SerializedObjectWrapper(RawDataFlag, bytes);
            }

            // - or we just received a byte[]. No further processing is needed.
            if (value is byte[] tmpByteArray) {
                return new SerializedObjectWrapper(RawDataFlag, new ArraySegment<byte>(tmpByteArray));
            }

            byte[] data;
            var length = -1;
            TypeCode code = value == null ? TypeCode.DBNull : Type.GetTypeCode(value.GetType());
            switch (code) {
                case TypeCode.DBNull:
                    data = Array.Empty<byte>();
                    length = 0;
                    break;
                case TypeCode.String:
                    // ReSharper disable once AssignNullToNotNullAttribute
                    data = Encoding.UTF8.GetBytes((string)value);
                    break;
                case TypeCode.Boolean:
                    // ReSharper disable once PossibleNullReferenceException
                    data = BitConverter.GetBytes((bool)value);
                    break;
                case TypeCode.Int16:
                    // ReSharper disable once PossibleNullReferenceException
                    data = BitConverter.GetBytes((short)value);
                    break;
                case TypeCode.Int32:
                    // ReSharper disable once PossibleNullReferenceException
                    data = BitConverter.GetBytes((int)value);
                    break;
                case TypeCode.Int64:
                    // ReSharper disable once PossibleNullReferenceException
                    data = BitConverter.GetBytes((long)value);
                    break;
                case TypeCode.UInt16:
                    // ReSharper disable once PossibleNullReferenceException
                    data = BitConverter.GetBytes((ushort)value);
                    break;
                case TypeCode.UInt32:
                    // ReSharper disable once PossibleNullReferenceException
                    data = BitConverter.GetBytes((uint)value);
                    break;
                case TypeCode.UInt64:
                    // ReSharper disable once PossibleNullReferenceException
                    data = BitConverter.GetBytes((ulong)value);
                    break;
                case TypeCode.Char:
                    // ReSharper disable once PossibleNullReferenceException
                    data = BitConverter.GetBytes((char)value);
                    break;
                case TypeCode.DateTime:
                    // ReSharper disable once PossibleNullReferenceException
                    data = BitConverter.GetBytes(((DateTime)value).ToBinary());
                    break;
                case TypeCode.Double:
                    // ReSharper disable once PossibleNullReferenceException
                    data = BitConverter.GetBytes((double)value);
                    break;
                case TypeCode.Single:
                    // ReSharper disable once PossibleNullReferenceException
                    data = BitConverter.GetBytes((float)value);
                    break;
                default:
                    using (var ms = new MemoryStream()) {
                        // ReSharper disable once AssignNullToNotNullAttribute
                        this.Formatter.Serialize(ms, value);
                        code = TypeCode.Object;
                        data = ms.GetBuffer();
                        length = (int)ms.Length;
                    }

                    break;
            }

            if (length < 0) {
                length = data.Length;
            }

            return new SerializedObjectWrapper((ushort)((ushort)code | 0x0100), new ArraySegment<byte>(data, 0, length));
        }

        /// <summary>
        ///     Unwrap object wrapped in <see cref="SerializedObjectWrapper" />
        /// </summary>
        private object Unwrap(SerializedObjectWrapper item) {
            if (item.Data.Array == null) {
                return null;
            }

            if (item.Flags == RawDataFlag) {
                ArraySegment<byte> tmp = item.Data;

                if (tmp.Array == null) {
                    return null;
                }

                if (tmp.Count == tmp.Array.Length) {
                    return tmp.Array;
                }

                // we should never arrive here, but it's better to be safe than sorry
                var retval = new byte[tmp.Count];

                Array.Copy(tmp.Array, tmp.Offset, retval, 0, tmp.Count);

                return retval;
            }

            var code = (TypeCode)(item.Flags & 0x00ff);

            byte[] data = item.Data.Array;
            var offset = item.Data.Offset;
            var count = item.Data.Count;

            switch (code) {
                // incrementing a non-existing key then getting it
                // returns as a string, but the flag will be 0
                // so treat all 0 flagged items as string
                // this may help inter-client data management as well
                //
                // however we store 'null' as Empty + an empty array, 
                // so this must special-cased for compatibility with 
                // earlier versions. we introduced DBNull as null marker in emc2.6
                case TypeCode.Empty:
                    return data == null || count == 0
                        ? null
                        : Encoding.UTF8.GetString(data, offset, count);

                case TypeCode.DBNull:
                    return null;

                case TypeCode.String:
                    return Encoding.UTF8.GetString(data, offset, count);

                case TypeCode.Boolean:
                    return BitConverter.ToBoolean(data, offset);

                case TypeCode.Int16:
                    return BitConverter.ToInt16(data, offset);

                case TypeCode.Int32:
                    return BitConverter.ToInt32(data, offset);

                case TypeCode.Int64:
                    return BitConverter.ToInt64(data, offset);

                case TypeCode.UInt16:
                    return BitConverter.ToUInt16(data, offset);

                case TypeCode.UInt32:
                    return BitConverter.ToUInt32(data, offset);

                case TypeCode.UInt64:
                    return BitConverter.ToUInt64(data, offset);

                case TypeCode.Char:
                    return BitConverter.ToChar(data, offset);

                case TypeCode.DateTime:
                    return DateTime.FromBinary(BitConverter.ToInt64(data, offset));

                case TypeCode.Double:
                    return BitConverter.ToDouble(data, offset);

                case TypeCode.Single:
                    return BitConverter.ToSingle(data, offset);

                case TypeCode.Object:
                    using (var ms = new MemoryStream(data, offset, count)) {
                        return this.Formatter.Deserialize(ms);
                    }

                default: throw new InvalidOperationException("Unknown TypeCode was returned: " + code);
            }
        }

    }

}
