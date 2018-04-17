using System;
using System.Collections.Generic;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq.Interfaces {

    /// <summary>
    ///     Mq message
    /// </summary>
    public interface IMqMessage {

        Guid Id { get; }
        DateTime CreatedDate { get; }

        long Priority { get; set; }

        int RetryAttempts { get; set; }

        Guid? ReplyId { get; set; }

        string ReplyTo { get; set; }

        MqErrorStatus Error { get; set; }

        /// <summary>
        ///     DeliveryTag
        /// </summary>
        string Tag { get; set; }

        object Body { get; set; }

        Dictionary<string, string> Meta { get; set; }
    }

    public interface IMqMessage<out T> : IMqMessage {
        T GetBody();
    }
}
