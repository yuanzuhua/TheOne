using System;

namespace TheOne.RabbitMq.Extensions {

    /// <summary>
    /// </summary>
    internal static class DateTimeExtensions {

        public const long UnixEpoch = 621355968000000000L;
        private static readonly DateTime _unixEpochDateTimeUtc = new DateTime(UnixEpoch, DateTimeKind.Utc);

        public static DateTime FromUnixTime(this int unixTime) {
            return _unixEpochDateTimeUtc + TimeSpan.FromSeconds(unixTime);
        }

        public static DateTime FromUnixTime(this double unixTime) {
            return _unixEpochDateTimeUtc + TimeSpan.FromSeconds(unixTime);
        }

        public static DateTime FromUnixTime(this long unixTime) {
            return _unixEpochDateTimeUtc + TimeSpan.FromSeconds(unixTime);
        }

        public static long ToUnixTime(this DateTime dateTime) {
            return dateTime.ToDateTimeSinceUnixEpoch().Ticks / TimeSpan.TicksPerSecond;
        }

        private static TimeSpan ToDateTimeSinceUnixEpoch(this DateTime dateTime) {
            DateTime dtUtc = dateTime.ToUniversalTime();
            TimeSpan universal = dtUtc.Subtract(_unixEpochDateTimeUtc);
            return universal;
        }
    }
}
