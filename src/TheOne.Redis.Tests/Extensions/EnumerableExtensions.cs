using System;
using System.Collections.Generic;

namespace TheOne.Redis.Tests.Extensions {

    internal static class EnumerableExtensions {

        public static bool EquivalentTo<T>(this IEnumerable<T> thisList, IEnumerable<T> otherList, Func<T, T, bool> comparer = null) {
            if (comparer == null) {
                comparer = (v1, v2) => v1.Equals(v2);
            }

            if (thisList == null || otherList == null) {
                return thisList == otherList;
            }

            var otherEnum = otherList.GetEnumerator();
            foreach (var item in thisList) {
                if (!otherEnum.MoveNext()) {
                    return false;
                }

                var thisIsDefault = Equals(item, default(T));
                var otherIsDefault = Equals(otherEnum.Current, default(T));
                if (thisIsDefault || otherIsDefault) {
                    return thisIsDefault && otherIsDefault;
                }

                if (!comparer(item, otherEnum.Current)) {
                    return false;
                }
            }

            var hasNoMoreLeftAsWell = !otherEnum.MoveNext();
            return hasNoMoreLeftAsWell;
        }

    }

}
