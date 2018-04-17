namespace TheOne.RabbitMq.Tests.Messaging.Models {

    internal class Greet {
        public string Name { get; set; }
    }

    internal class GreetResponse {
        public string Result { get; set; }
    }

    internal class GreetService {
        public int TimesCalled { get; set; }
        public string Result { get; set; }

        public GreetResponse Any(Greet request) {
            this.TimesCalled++;

            this.Result = "Hello, " + request.Name;
            return new GreetResponse { Result = this.Result };
        }
    }
}
