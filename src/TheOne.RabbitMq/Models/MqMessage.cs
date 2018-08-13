using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using TheOne.RabbitMq.Interfaces;

namespace TheOne.RabbitMq.Models {

    /// <summary>
    ///     Basic implementation of IMqMessage[T]
    /// </summary>
    /// <inheritdoc cref="MqMessage" />
    public class MqMessage<T> : MqMessage, IMqMessage<T> {

        private object _body;

        /// <inheritdoc />
        public MqMessage() {
            this.Id = Guid.NewGuid();
            this.CreatedDate = DateTime.Now;
        }

        /// <inheritdoc />
        public MqMessage(T body) : this() {
            this.Body = body;
        }

        /// <inheritdoc cref="MqMessage.Body" />
        public sealed override object Body {
            get {
                if (this._body != null) {
                    if (this._body.GetType() != typeof(T)) {
                        this._body = JToken.FromObject(this._body).ToObject<T>();
                    }
                }

                return this._body;
            }
            set => this._body = value;
        }

        /// <inheritdoc />
        public T GetBody() {
            return (T)this.Body;
        }

        public static IMqMessage Create(object oBody) {
            return new MqMessage<T>((T)oBody);
        }

        /// <inheritdoc />
        public override string ToString() {
            return $"CreatedDate={this.CreatedDate}, Id={this.Id:N}, Type={typeof(T).Name}, Retry={this.RetryAttempts}";
        }

    }

    /// <inheritdoc />
    public class MqMessage : IMqMessage {

        /// <inheritdoc />
        public Guid Id { get; set; }

        /// <inheritdoc />
        public DateTime CreatedDate { get; set; }

        /// <inheritdoc />
        public long Priority { get; set; }

        /// <inheritdoc />
        public int RetryAttempts { get; set; }

        /// <inheritdoc />
        public Guid? ReplyId { get; set; }

        /// <inheritdoc />
        public string ReplyTo { get; set; }

        /// <inheritdoc />
        public MqErrorStatus Error { get; set; }

        /// <inheritdoc />
        public string Tag { get; set; }

        /// <inheritdoc />
        public Dictionary<string, string> Meta { get; set; }

        /// <inheritdoc />
        public virtual object Body { get; set; }

    }

}
