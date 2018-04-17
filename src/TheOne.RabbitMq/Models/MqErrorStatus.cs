using System;
using System.Collections.Generic;

namespace TheOne.RabbitMq.Models {

    /// <summary>
    /// </summary>
    public class MqErrorStatus {
        /// <summary>
        ///     A status without an errorcode == success
        /// </summary>
        public MqErrorStatus() { }

        /// <summary>
        ///     A status with an errorcode == failure
        /// </summary>
        public MqErrorStatus(string errorCode) {
            this.ErrorCode = errorCode;
        }

        /// <summary>
        ///     A status with an errorcode == failure
        /// </summary>
        public MqErrorStatus(string errorCode, string message) : this(errorCode) {
            this.Message = message;
        }

        public MqErrorStatus(Exception exception) {
            this.ErrorCode = exception.GetType().Name;
            this.Message = exception.Message;
            this.StackTrace = exception.ToString();

            if (exception.Data != null) {
                foreach (KeyValuePair<string, object> o in exception.Data) {
                    this.Meta[o.Key] = o.Value?.ToString();
                }
            }
        }

        /// <summary>
        ///     the name of the Exception type, e.g. typeof(Exception).Name
        /// </summary>
        public string ErrorCode { get; set; }

        /// <summary>
        ///     A human friendly error message
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// </summary>
        public string StackTrace { get; set; }

        /// <summary>
        ///     For additional custom metadata about the error
        /// </summary>
        public Dictionary<string, string> Meta { get; set; } = new Dictionary<string, string>();
    }
}
