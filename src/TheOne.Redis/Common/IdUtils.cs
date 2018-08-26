using System;
using System.Linq.Expressions;
using System.Reflection;

namespace TheOne.Redis.Common {

    internal static class IdUtils<T> {

        internal static GetMemberDelegate<T> CanGetId;

        static IdUtils() {

            Type memberInfo = typeof(T);
            Type[] hasIdInterfaces =
                memberInfo.FindInterfaces((t, critera) => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IHasRedisId<>), null);

            if (hasIdInterfaces.Length > 0) {
                CanGetId = HasId<T>.GetId;
                return;
            }

            if (memberInfo.IsClass || memberInfo.IsInterface) {
                PropertyInfo piId = memberInfo.GetIdProperty();
                if (piId?.GetGetMethod(true) != null) {
                    CanGetId = HasPropertyId<T>.GetId;
                    return;
                }
            }

            if (memberInfo == typeof(object)) {
                CanGetId = x => {
                    PropertyInfo piId = x.GetType().GetIdProperty();
                    if (piId?.GetGetMethod(true) != null) {
                        return x.GetObjectId();
                    }

                    return x.GetHashCode();
                };
                return;
            }

            CanGetId = x => x.GetHashCode();
        }

        public static object GetId(T entity) {
            return CanGetId(entity);
        }

    }

    internal static class HasPropertyId<TEntity> {

        private static readonly GetMemberDelegate<TEntity> _getIdFn;

        static HasPropertyId() {
            PropertyInfo pi = typeof(TEntity).GetIdProperty();
            _getIdFn = CreateGetter<TEntity>(pi);
        }

        public static GetMemberDelegate<T> CreateGetter<T>(PropertyInfo propertyInfo) {
            MethodInfo getMethodInfo = propertyInfo.GetGetMethod(true);
            if (getMethodInfo == null) {
                return null;
            }

            return o => propertyInfo.GetGetMethod(true).Invoke(o, Array.Empty<object>());
        }

        public static object GetId(TEntity entity) {
            return _getIdFn(entity);
        }

    }

    internal static class HasId<TEntity> {

        private static readonly Func<TEntity, object> _getIdFn;

        static HasId() {
            Type type = typeof(TEntity);
            Type[] hasIdInterfaces =
                type.FindInterfaces((t, critera) => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IHasRedisId<>), null);
            Type genericArg = hasIdInterfaces[0].GetGenericArguments()[0];
            Type genericType = typeof(HasIdGetter<,>).MakeGenericType(type, genericArg);

            ParameterExpression oInstanceParam = Expression.Parameter(type, "oInstanceParam");
            MethodCallExpression exprCallStaticMethod = Expression.Call(
                genericType,
                "GetId",
                Array.Empty<Type>(),
                oInstanceParam
            );
            _getIdFn = Expression.Lambda<Func<TEntity, object>>(
                exprCallStaticMethod,
                oInstanceParam
            ).Compile();
        }

        public static object GetId(TEntity entity) {
            return _getIdFn(entity);
        }

    }

    internal class HasIdGetter<TEntity, TId> where TEntity : IHasRedisId<TId> {

        public static object GetId(TEntity entity) {
            return entity.Id;
        }

    }

    internal static class IdUtils {

        public const string IdField = "Id";

        public static object GetObjectId(this object entity) {
            return entity.GetType().GetIdProperty().GetGetMethod(true).Invoke(entity, Array.Empty<object>());
        }

        public static object ToId<T>(this T entity) {
            return entity.GetId();
        }

        public static string ToUrn<T>(this T entity) {
            return entity.CreateUrn();
        }

        public static string ToSafePathCacheKey<T>(this string idValue) {
            return CreateCacheKeyPath<T>(idValue);
        }

        public static string ToUrn<T>(this object id) {
            return CreateUrn<T>(id);
        }

        public static object GetId<T>(this T entity) {
            return IdUtils<T>.GetId(entity);
        }

        public static string CreateUrn<T>(object id) {
            return $"urn:{typeof(T).Name.ToLowerInvariant()}:{id}";
        }

        public static string CreateUrn(Type type, object id) {
            return $"urn:{type.Name.ToLowerInvariant()}:{id}";
        }

        public static string CreateUrn<T>(this T entity) {
            object id = GetId(entity);
            return $"urn:{typeof(T).Name.ToLowerInvariant()}:{id}";
        }

        public static string CreateCacheKeyPath<T>(string idValue) {
            if (idValue.Length < 4) {
                idValue = idValue.PadLeft(4, '0');
            }

            idValue = idValue.Replace(" ", "-");

            var rootDir = typeof(T).Name;
            var dir1 = idValue.Substring(0, 2);
            var dir2 = idValue.Substring(2, 2);

            var path = $"{rootDir}/{dir1}/{dir2}/{idValue}";

            return path;
        }

        public static PropertyInfo GetIdProperty(this Type type) {
            foreach (PropertyInfo pi in type.GetProperties()) {
                if (string.Equals(IdField, pi.Name, StringComparison.OrdinalIgnoreCase)) {
                    return pi;
                }
            }

            return null;
        }

    }

}
