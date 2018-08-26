using System.Collections.Generic;
using System.Globalization;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client {

    public class RedisData {

        public byte[] Data { get; set; }

        public List<RedisData> Children { get; set; }

    }

    public static class RedisDataExtensions {

        public static RedisText ToRedisText(this RedisData data) {
            if (data == null) {
                return null; // In Transaction
            }

            var to = new RedisText();

            if (data.Data != null) {
                to.Text = data.Data.FromUtf8Bytes();
            }

            if (data.Children != null) {
                to.Children = data.Children.ConvertAll(x => x.ToRedisText());
            }

            return to;
        }

        public static double ToDouble(this RedisData data) {
            return double.Parse(data.Data.FromUtf8Bytes(),
                NumberStyles.Float,
                CultureInfo.InvariantCulture);
        }

        public static long ToInt64(this RedisData data) {
            return long.Parse(data.Data.FromUtf8Bytes(),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture);
        }

    }

}
