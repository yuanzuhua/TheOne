namespace TheOne.Redis.PubSub {

    public partial class RedisPubSubServer {

        private static class Operation {

            public const int NoOp = 0;
            public const int Stop = 1;
            public const int Reset = 2;
            public const int Restart = 3;

            public static string GetName(int op) {
                switch (op) {
                    case NoOp:
                        return "NoOp";
                    case Stop:
                        return "Stop";
                    case Reset:
                        return "Reset";
                    case Restart:
                        return "Restart";
                    default:
                        return null;
                }
            }

        }

        private static class ControlCommand {

            public const string Control = "CTRL";
            public const string Pulse = "PULSE";

        }

        private class Status {

            public const int Disposed = -1;
            public const int Stopped = 0;
            public const int Stopping = 1;
            public const int Starting = 2;
            public const int Started = 3;

        }

    }

}
