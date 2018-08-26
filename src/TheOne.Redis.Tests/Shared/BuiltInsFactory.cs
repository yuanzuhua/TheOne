using System;
using NUnit.Framework;

namespace TheOne.Redis.Tests.Shared {

    internal sealed class BuiltInsFactory : ModelFactoryBase<string> {

        private readonly string[] _stringValues = {
            "one",
            "two",
            "three",
            "four",
            "five",
            "six",
            "seven"
        };

        public override void AssertIsEqual(string actual, string expected) {
            Assert.That(actual, Is.EqualTo(expected));
        }

        public override string CreateInstance(int i) {
            return i < this._stringValues.Length
                ? this._stringValues[i]
                : i.ToString();
        }

    }

    internal sealed class IntFactory : ModelFactoryBase<int> {

        public override void AssertIsEqual(int actual, int expected) {
            Assert.That(actual, Is.EqualTo(expected));
        }

        public override int CreateInstance(int i) {
            return i;
        }

    }

    internal sealed class DateTimeFactory : ModelFactoryBase<DateTime> {

        public override void AssertIsEqual(DateTime actual, DateTime expected) {
            Assert.That(actual, Is.EqualTo(expected));
        }

        public override DateTime CreateInstance(int i) {
            return new DateTime(i, DateTimeKind.Utc);
        }

    }

}
