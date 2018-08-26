using NUnit.Framework;

namespace TheOne.Redis.Tests.Shared {

    internal sealed class ModelWithIdAndName {

        public ModelWithIdAndName() { }

        public ModelWithIdAndName(int id) {
            this.Id = id;
            this.Name = "Name" + id;
        }

        public int Id { get; set; }

        public string Name { get; set; }

        public static ModelWithIdAndName Create(int id) {
            return new ModelWithIdAndName(id);
        }

        public static void AssertIsEqual(ModelWithIdAndName actual, ModelWithIdAndName expected) {
            if (actual == null || expected == null) {
                Assert.That(Equals(actual, expected), Is.True);
                return;
            }

            Assert.That(actual.Id, Is.EqualTo(expected.Id));
            Assert.That(actual.Name, Is.EqualTo(expected.Name));
        }

        public bool Equals(ModelWithIdAndName other) {
            if (ReferenceEquals(null, other)) {
                return false;
            }

            if (ReferenceEquals(this, other)) {
                return true;
            }

            return other.Id == this.Id && Equals(other.Name, this.Name);
        }

        public override bool Equals(object obj) {
            if (ReferenceEquals(null, obj)) {
                return false;
            }

            if (ReferenceEquals(this, obj)) {
                return true;
            }

            if (obj.GetType() != typeof(ModelWithIdAndName)) {
                return false;
            }

            return this.Equals((ModelWithIdAndName)obj);
        }

        public override int GetHashCode() {
            unchecked {
                return (this.Id * 397) ^ (this.Name != null ? this.Name.GetHashCode() : 0);
            }
        }

    }

}
