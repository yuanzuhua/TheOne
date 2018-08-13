namespace TheOne.RabbitMq.Models {

    public static class MqWorkerOperation {

        public const string ControlCommand = "CTRL";

        public const int NoOp = 0;
        public const int Stop = 1;
        public const int Reset = 2;
        public const int Restart = 3;

    }

}
