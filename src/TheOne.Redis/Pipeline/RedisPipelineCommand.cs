using System.Collections.Generic;
using System.Linq;
using TheOne.Redis.Client;

namespace TheOne.Redis.Pipeline {

    public class RedisPipelineCommand {

        private readonly RedisNativeClient _client;
        private int _cmdCount;

        public RedisPipelineCommand(RedisNativeClient client) {
            this._client = client;
        }

        public void WriteCommand(params byte[][] cmdWithBinaryArgs) {
            this._client.WriteAllToSendBuffer(cmdWithBinaryArgs);
            this._cmdCount++;
        }

        public List<long> ReadAllAsInts() {
            var results = new List<long>();
            while (this._cmdCount-- > 0) {
                results.Add(this._client.ReadLong());
            }

            return results;
        }

        public bool ReadAllAsIntsHaveSuccess() {
            List<long> allResults = this.ReadAllAsInts();
            return allResults.All(x => x == RedisNativeClient.Success);
        }

        public void Flush() {
            this._client.FlushAndResetSendBuffer();
        }

    }

}
