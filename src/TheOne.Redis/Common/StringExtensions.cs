using System;
using System.Globalization;
using System.Text;
using Newtonsoft.Json;
using TheOne.Redis.External;

namespace TheOne.Redis.Common {

    internal static class StringExtensions {

        private static readonly char[] _systemTypeChars = { '<', '>', '+' };

        /// <summary>
        ///     Encoding.UTF8.GetString
        /// </summary>
        public static string FromUtf8Bytes(this byte[] bytes) {
            return bytes == null
                ? null
                : Encoding.UTF8.GetString(bytes, 0, bytes.Length);
        }

        /// <summary>
        ///     Encoding.UTF8.GetBytes
        /// </summary>
        public static byte[] ToUtf8Bytes(this string value) {
            return Encoding.UTF8.GetBytes(value);
        }

        /// <summary>
        ///     Encoding.UTF8.GetBytes(value.ToJson());
        /// </summary>
        public static byte[] ToJsonUtf8Bytes<T>(this T value) {
            return Encoding.UTF8.GetBytes(value.ToJson());
        }

        public static T FromJsonUtf8Bytes<T>(this byte[] value) {
            return value.FromUtf8Bytes().FromJson<T>();
        }

        /// <summary>
        ///     GetBytes
        /// </summary>
        public static byte[] ToUtf8Bytes(this int intVal) {
            return FastToUtf8Bytes(intVal.ToString());
        }

        /// <summary>
        ///     GetBytes
        /// </summary>
        public static byte[] ToUtf8Bytes(this long longVal) {
            return FastToUtf8Bytes(longVal.ToString());
        }

        /// <summary>
        ///     GetBytes
        /// </summary>
        public static byte[] ToUtf8Bytes(this ulong ulongVal) {
            return FastToUtf8Bytes(ulongVal.ToString());
        }

        /// <summary>
        ///     GetBytes
        /// </summary>
        public static byte[] ToUtf8Bytes(this double doubleVal) {
            var doubleStr = doubleVal.ToString(CultureInfo.InvariantCulture.NumberFormat);

            if (doubleStr.IndexOf('E') != -1 || doubleStr.IndexOf('e') != -1) {
                doubleStr = DoubleConverter.ToExactString(doubleVal);
            }

            return FastToUtf8Bytes(doubleStr);
        }

        /// <summary>
        ///     Skip the encoding process for 'safe strings'
        /// </summary>
        private static byte[] FastToUtf8Bytes(string strVal) {
            var bytes = new byte[strVal.Length];
            for (var i = 0; i < strVal.Length; i++) {
                bytes[i] = (byte)strVal[i];
            }

            return bytes;
        }

        public static string[] SplitOnFirst(this string strVal, char needle) {
            if (strVal == null) {
                return Array.Empty<string>();
            }

            var pos = strVal.IndexOf(needle);
            return pos == -1
                ? new[] { strVal }
                : new[] { strVal.Substring(0, pos), strVal.Substring(pos + 1) };
        }

        public static string[] SplitOnLast(this string strVal, char needle) {
            if (strVal == null) {
                return Array.Empty<string>();
            }

            var pos = strVal.LastIndexOf(needle);
            return pos == -1
                ? new[] { strVal }
                : new[] { strVal.Substring(0, pos), strVal.Substring(pos + 1) };
        }

        public static string ToJson<T>(this T obj) {
            if (obj == null) {
                return default;
            }

            if (obj is string abc) {
                return abc;
            }

            return JsonConvert.SerializeObject(obj);
        }

        public static T FromJson<T>(this string json) {
            if (string.IsNullOrEmpty(json)) {
                return default;
            }

            if (typeof(T) == typeof(string)) {
                return (T)(object)json;
            }

            return JsonConvert.DeserializeObject<T>(json);
        }

        public static int IndexOfAny(this string text, params string[] needles) {
            return IndexOfAny(text, 0, needles);
        }

        public static int IndexOfAny(this string text, int startIndex, params string[] needles) {
            var firstPos = -1;
            if (text != null) {
                foreach (var needle in needles) {
                    var pos = text.IndexOf(needle, startIndex, StringComparison.Ordinal);
                    if (pos >= 0 && (firstPos == -1 || pos < firstPos)) {
                        firstPos = pos;
                    }
                }
            }

            return firstPos;
        }

        public static bool IsUserType(this Type type) {
            return type.IsClass
                   && !type.IsSystemType();
        }

        public static bool IsSystemType(this Type type) {
            return type.Namespace == null
                   || type.Namespace.StartsWith("System")
                   || type.Name.IndexOfAny(_systemTypeChars) >= 0;
        }

    }

}
