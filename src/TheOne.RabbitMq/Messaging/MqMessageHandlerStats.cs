using System;
using System.Text;
using TheOne.RabbitMq.Interfaces;

namespace TheOne.RabbitMq.Messaging {

    /// <inheritdoc />
    public class MqMessageHandlerStats : IMqMessageHandlerStats {

        public MqMessageHandlerStats(string name) {
            this.Name = name;
        }

        public MqMessageHandlerStats(string name, int totalMessagesProcessed, int totalMessagesFailed, int totalRetries,
            int totalNormalMessagesReceived, DateTime? lastMessageProcessed) {
            this.Name = name;
            this.TotalMessagesProcessed = totalMessagesProcessed;
            this.TotalMessagesFailed = totalMessagesFailed;
            this.TotalRetries = totalRetries;
            this.TotalNormalMessagesReceived = totalNormalMessagesReceived;
            this.LastMessageProcessed = lastMessageProcessed;
        }

        /// <inheritdoc />
        public string Name { get; }

        /// <inheritdoc />
        public DateTime? LastMessageProcessed { get; private set; }

        /// <inheritdoc />
        public int TotalMessagesProcessed { get; private set; }

        /// <inheritdoc />
        public int TotalMessagesFailed { get; private set; }

        /// <inheritdoc />
        public int TotalRetries { get; private set; }

        /// <inheritdoc />
        public int TotalNormalMessagesReceived { get; private set; }

        /// <inheritdoc />
        public virtual void Add(IMqMessageHandlerStats stats) {
            this.TotalMessagesProcessed += stats.TotalMessagesProcessed;
            this.TotalMessagesFailed += stats.TotalMessagesFailed;
            this.TotalRetries += stats.TotalRetries;
            this.TotalNormalMessagesReceived += stats.TotalNormalMessagesReceived;
            if (this.LastMessageProcessed == null || stats.LastMessageProcessed > this.LastMessageProcessed) {
                this.LastMessageProcessed = stats.LastMessageProcessed;
            }
        }

        /// <inheritdoc />
        public override string ToString() {
            var sb = new StringBuilder();
            sb.AppendLine($"STATS for {this.Name}:").AppendLine();
            sb.AppendLine($"  TotalNormalMessagesReceived:    {this.TotalNormalMessagesReceived}");
            sb.AppendLine($"  TotalProcessed:                 {this.TotalMessagesProcessed}");
            sb.AppendLine($"  TotalRetries:                   {this.TotalRetries}");
            sb.AppendLine($"  TotalFailed:                    {this.TotalMessagesFailed}");
            sb.AppendLine($"  LastMessageProcessed:           {this.LastMessageProcessed?.ToString() ?? ""}");
            return sb.ToString();
        }

    }

}
