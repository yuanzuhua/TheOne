using System;
using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client {

    public class RedisText {

        public string Text { get; set; }

        public List<RedisText> Children { get; set; }

    }

    public static class RedisTextExtensions {

        public static string GetResult(this RedisText from) {
            return from.Text;
        }

        public static string ToJsonInfo(this RedisText redisText) {
            var source = redisText.GetResult();
            return Parse(source);
        }

        public static T GetResult<T>(this RedisText from) {
            return from.Text.FromJson<T>();
        }

        public static List<string> GetResults(this RedisText from) {
            return from.Children == null
                ? new List<string>()
                : from.Children.ConvertAll(x => x.Text);
        }

        public static List<T> GetResults<T>(this RedisText from) {
            return from.Children == null
                ? new List<T>()
                : from.Children.ConvertAll(x => x.Text.FromJson<T>());
        }

        #region Private

        private static string Parse(string source) {
            var result = new Dictionary<string, Dictionary<string, string>>();
            var section = new Dictionary<string, string>();

            IEnumerable<string> rows = SplitRows(source);

            foreach (var row in rows) {
                if (row.IndexOf("#", StringComparison.Ordinal) == 0) {
                    var name = ParseSection(row);
                    section = new Dictionary<string, string>();
                    result.Add(name, section);
                } else {
                    KeyValuePair<string, string>? pair = ParseKeyValue(row);
                    if (pair.HasValue) {
                        section.Add(pair.Value.Key, pair.Value.Value);
                    }
                }
            }

            return result.ToJson();
        }

        private static IEnumerable<string> SplitRows(string source) {
            return source.Split(new[] { "\r\n" }, StringSplitOptions.None).Where(n => !string.IsNullOrWhiteSpace(n));
        }

        private static string ParseSection(string source) {
            return source.IndexOf("#", StringComparison.Ordinal) == 0
                ? source.Trim('#').Trim()
                : string.Empty;
        }

        private static KeyValuePair<string, string>? ParseKeyValue(string source) {
            KeyValuePair<string, string>? result = null;

            var devider = source.IndexOf(":", StringComparison.Ordinal);
            if (devider > 0) {
                var name = source.Substring(0, devider);
                var value = source.Substring(devider + 1);
                result = new KeyValuePair<string, string>(name.Trim(), value.Trim());
            }

            return result;
        }

        #endregion Private

    }

}
