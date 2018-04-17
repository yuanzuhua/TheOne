using System;

namespace TheOne.RabbitMq.Tests.Messaging.Models {

    internal class AlwaysFail {
        public string Name { get; set; }
    }

    internal class AlwaysFailResponse {
        public string Result { get; set; }
    }

    internal class AlwaysFailService {
        public int TimesCalled { get; set; }
        public string Result { get; set; }

        public AlwaysFailResponse Any(AlwaysFail request) {
            this.TimesCalled++;

            throw new NotSupportedException("This service always fails");
        }
    }
}
