using System;

namespace TheOne.RabbitMq.Extensions {

    internal static class ExceptionExtensions {

        public static Exception UnwrapIfSingleException(this Exception ex) {
            var aex = ex as AggregateException;
            if (aex == null) {
                return ex;
            }

            if (aex.InnerExceptions != null
                && aex.InnerExceptions.Count == 1) {
                return aex.InnerExceptions[0].UnwrapIfSingleException();
            }

            return aex;
        }

    }

}
