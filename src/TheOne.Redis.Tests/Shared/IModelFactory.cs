using System.Collections.Generic;

namespace TheOne.Redis.Tests.Shared {

    internal interface IModelFactory<T> {

        T ExistingValue { get; }
        T NonExistingValue { get; }
        void AssertListsAreEqual(List<T> actualList, IList<T> expectedList);
        void AssertIsEqual(T actual, T expected);
        List<T> CreateList();
        List<T> CreateList2();
        T CreateInstance(int i);

    }

}
