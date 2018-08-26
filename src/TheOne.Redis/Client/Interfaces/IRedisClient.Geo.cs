using System.Collections.Generic;

namespace TheOne.Redis.Client {

    public partial interface IRedisClient {

        long AddGeoMember(string key, double longitude, double latitude, string member);

        long AddGeoMembers(string key, params RedisGeo[] geoPoints);

        double CalculateDistanceBetweenGeoMembers(string key, string fromMember, string toMember, string unit = null);

        string[] GetGeohashes(string key, params string[] members);

        List<RedisGeo> GetGeoCoordinates(string key, params string[] members);

        string[] FindGeoMembersInRadius(string key, double longitude, double latitude, double radius, string unit);

        List<RedisGeoResult> FindGeoResultsInRadius(string key, double longitude, double latitude, double radius, string unit,
            int? count = null, bool? sortByNearest = null);

        string[] FindGeoMembersInRadius(string key, string member, double radius, string unit);

        List<RedisGeoResult> FindGeoResultsInRadius(string key, string member, double radius, string unit, int? count = null,
            bool? sortByNearest = null);

    }

}
