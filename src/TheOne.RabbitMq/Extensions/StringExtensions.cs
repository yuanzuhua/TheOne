using System.Text;
using Newtonsoft.Json;

namespace TheOne.RabbitMq.Extensions {

    internal static class StringExtensions {

        public static string FromUtf8Bytes(this byte[] bytes) {
            return bytes == null ? null : Encoding.UTF8.GetString(bytes);
        }

        public static byte[] ToUtf8Bytes(this string value) {
            return string.IsNullOrEmpty(value) ? null : Encoding.UTF8.GetBytes(value);
        }

        public static string ToJson<T>(this T obj) {
            return JsonConvert.SerializeObject(obj);
        }

        public static T FromJson<T>(this string json) {
            return JsonConvert.DeserializeObject<T>(json);
        }
    }
}
