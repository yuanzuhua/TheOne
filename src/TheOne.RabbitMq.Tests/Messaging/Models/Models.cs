namespace TheOne.RabbitMq.Tests.Messaging.Models {

    internal class AlwaysThrows {

        public string Value { get; set; }

    }

    internal class Hello {

        public string Name { get; set; }

    }

    internal class HelloResponse {

        public string Result { get; set; }

    }

    internal class HelloRabbit {

        public string Name { get; set; }

    }

    internal class HelloNull {

        public string Name { get; set; }

    }

    internal class HelloIntro {

        public string Name { get; set; }

    }

    internal class HelloIntroResponse {

        public string Result { get; set; }

    }

    internal class Incr {

        public int Value { get; set; }

    }

    internal class IncrResponse {

        public int Result { get; set; }

    }

    internal class Reverse {

        public string Value { get; set; }

    }

    internal class ReverseResponse {

        public string Value { get; set; }

    }

    internal class NothingHere {

        public string Value { get; set; }

    }

    internal class NothingHereResponse {

        public string Value { get; set; }

    }

    internal class Wait {

        public int ForMs { get; set; }

    }

}
