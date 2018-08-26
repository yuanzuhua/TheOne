using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using TheOne.Redis.Common;

namespace TheOne.Redis.Client {

    public partial class RedisClient {

        public IEnumerable<SlowlogItem> GetSlowlog(int? numberOfRecords = null) {
            object[] data = this.Slowlog(numberOfRecords);
            var list = new SlowlogItem[data.Length];
            for (var i = 0; i < data.Length; i++) {
                var log = (object[])data[i];

                string[] arguments = ((object[])log[3]).OfType<byte[]>()
                                                       .Select(t => t.FromUtf8Bytes())
                                                       .ToArray();


                list[i] = new SlowlogItem(
                    int.Parse((string)log[0], CultureInfo.InvariantCulture),
                    int.Parse((string)log[1], CultureInfo.InvariantCulture).FromUnixTime(),
                    int.Parse((string)log[2], CultureInfo.InvariantCulture),
                    arguments
                );
            }

            return list;
        }

    }

    public class SlowlogItem {

        public SlowlogItem(int id, DateTime timeStamp, int duration, string[] arguments) {
            this.Id = id;
            this.Timestamp = timeStamp;
            this.Duration = duration;
            this.Arguments = arguments;
        }

        public int Id { get; }
        public int Duration { get; }
        public DateTime Timestamp { get; }
        public string[] Arguments { get; }

    }

}
