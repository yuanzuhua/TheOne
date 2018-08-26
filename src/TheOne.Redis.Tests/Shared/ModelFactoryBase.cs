using System.Collections.Generic;
using NUnit.Framework;

namespace TheOne.Redis.Tests.Shared {

    internal abstract class ModelFactoryBase<T> : IModelFactory<T> {

        public void AssertListsAreEqual(List<T> actualList, IList<T> expectedList) {
            Assert.That(actualList, Has.Count.EqualTo(expectedList.Count));
            var i = 0;

            actualList.ForEach(x => this.AssertIsEqual(x, expectedList[i++]));
        }

        public abstract T CreateInstance(int i);

        public abstract void AssertIsEqual(T actual, T expected);

        public T ExistingValue => this.CreateInstance(4);

        public T NonExistingValue => this.CreateInstance(5);

        public List<T> CreateList() {
            return new List<T> {
                this.CreateInstance(1),
                this.CreateInstance(2),
                this.CreateInstance(3),
                this.CreateInstance(4)
            };
        }

        public List<T> CreateList2() {
            return new List<T> {
                this.CreateInstance(5),
                this.CreateInstance(6),
                this.CreateInstance(7)
            };
        }

    }

}
