using System;
using TheOne.RabbitMq.Models;

namespace TheOne.RabbitMq.Tests.Messaging.Models {

    internal class UnRetryableFail {

        public string Name { get; set; }

    }

    internal class UnRetryableFailResponse {

        public string Result { get; set; }

    }

    internal class UnRetryableFailService {

        public int TimesCalled { get; set; }
        public string Result { get; set; }

        public UnRetryableFailResponse Any(UnRetryableFail request) {
            this.TimesCalled++;

            throw new UnRetryableMqMessagingException(
                "This request should not get retried",
                new NotSupportedException("This service always fails"));
        }

    }

}
