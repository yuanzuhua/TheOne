using RabbitMQ.Client;

namespace TheOne.RabbitMq {

    /// <summary>
    ///     <see cref="ConnectionFactory" />
    /// </summary>
    public class RabbitMqConnectionModel {

        /// <summary>
        ///     <see cref="ConnectionFactory.HostName" />
        /// </summary>
        public string HostName { get; set; } = "localhost";

        /// <summary>
        ///     <see cref="ConnectionFactory.Password" />
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        ///     <see cref="ConnectionFactory.Port" />
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        ///     <see cref="ConnectionFactory.UserName" />
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        ///     <see cref="ConnectionFactory.VirtualHost" />
        /// </summary>
        public string VirtualHost { get; set; }

        public static ConnectionFactory CreateConnectionFactory(RabbitMqConnectionModel model) {
            var factory = new ConnectionFactory();
            if (!string.IsNullOrEmpty(model.VirtualHost)) {
                factory.VirtualHost = model.VirtualHost;
            }

            if (model.UserName != null) {
                factory.UserName = model.UserName;
            }

            if (model.Password != null) {
                factory.Password = model.Password;
            }

            if (model.Port > 0) {
                factory.Port = model.Port;
            }

            if (model.VirtualHost != null) {
                factory.VirtualHost = model.VirtualHost;
            }

            if (model.HostName != null) {
                factory.HostName = model.HostName;
            }

            return factory;
        }

    }

}
