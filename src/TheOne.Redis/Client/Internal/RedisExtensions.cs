using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Sockets;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client.Internal {

    internal static class RedisExtensionsInternal {

        private static readonly NumberFormatInfo _doubleFormatProvider = new NumberFormatInfo {
            PositiveInfinitySymbol = "+inf",
            NegativeInfinitySymbol = "-inf"
        };

        public static bool IsConnected(this Socket socket) {
            try {
                return !(socket.Poll(1, SelectMode.SelectRead) && socket.Available == 0);
            } catch (SocketException) {
                return false;
            }
        }

        public static string[] GetIds(this IHasRedisStringId[] itemsWithId) {
            var ids = new string[itemsWithId.Length];
            for (var i = 0; i < itemsWithId.Length; i++) {
                ids[i] = itemsWithId[i].Id;
            }

            return ids;
        }

        public static List<string> ToStringList(this byte[][] multiDataList) {
            if (multiDataList == null) {
                return new List<string>();
            }

            var results = new List<string>();
            foreach (var multiData in multiDataList) {
                results.Add(multiData.FromUtf8Bytes());
            }

            return results;
        }

        public static string[] ToStringArray(this byte[][] multiDataList) {
            if (multiDataList == null) {
                return Array.Empty<string>();
            }

            var to = new string[multiDataList.Length];
            for (var i = 0; i < multiDataList.Length; i++) {
                to[i] = multiDataList[i].FromUtf8Bytes();
            }

            return to;
        }

        public static Dictionary<string, string> ToStringDictionary(this byte[][] multiDataList) {
            var map = new Dictionary<string, string>();

            if (multiDataList == null) {
                return map;
            }

            for (var i = 0; i < multiDataList.Length; i += 2) {
                var key = multiDataList[i].FromUtf8Bytes();
                map[key] = multiDataList[i + 1].FromUtf8Bytes();
            }

            return map;
        }

        public static byte[] ToFastUtf8Bytes(this double value) {
            return FastToUtf8Bytes(value.ToString("R", _doubleFormatProvider));
        }

        private static byte[] FastToUtf8Bytes(string strVal) {
            var bytes = new byte[strVal.Length];
            for (var i = 0; i < strVal.Length; i++) {
                bytes[i] = (byte)strVal[i];
            }

            return bytes;
        }

        public static byte[][] ToMultiByteArray(this string[] args) {
            var byteArgs = new byte[args.Length][];
            for (var i = 0; i < args.Length; ++i) {
                byteArgs[i] = args[i].ToUtf8Bytes();
            }

            return byteArgs;
        }

        public static byte[][] PrependByteArray(this byte[][] args, byte[] valueToPrepend) {
            var newArgs = new byte[args.Length + 1][];
            newArgs[0] = valueToPrepend;
            var i = 1;
            foreach (var arg in args) {
                newArgs[i++] = arg;
            }

            return newArgs;
        }

        public static byte[][] PrependInt(this byte[][] args, int valueToPrepend) {
            return args.PrependByteArray(valueToPrepend.ToUtf8Bytes());
        }

    }

}
