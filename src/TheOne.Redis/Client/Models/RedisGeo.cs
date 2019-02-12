namespace TheOne.Redis.Client {

    /// <summary>
    ///     Store Lat/Long coordinates in Redis and query locations within a specified radius
    /// </summary>
    public class RedisGeo {

        public RedisGeo() { }

        public RedisGeo(double longitude, double latitude, string member) {
            this.Longitude = longitude;
            this.Latitude = latitude;
            this.Member = member;
        }

        public double Longitude { get; set; }
        public double Latitude { get; set; }
        public string Member { get; set; }

    }

    public class RedisGeoResult {

        public string Member { get; set; }
        public double Longitude { get; set; }
        public double Latitude { get; set; }
        public long Hash { get; set; }
        public string Unit { get; set; }
        public double Distance { get; set; }

    }

    public static class RedisGeoUnit {

        public const string Meters = "m";
        public const string Kilometers = "km";
        public const string Miles = "mi";
        public const string Feet = "ft";

    }

}
