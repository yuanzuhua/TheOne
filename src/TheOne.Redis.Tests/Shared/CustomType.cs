namespace TheOne.Redis.Tests.Shared {

    internal sealed class CustomType {

        public long CustomId { get; set; }
        public string CustomName { get; set; }

        public bool Equals(CustomType other) {
            if (ReferenceEquals(null, other)) {
                return false;
            }

            if (ReferenceEquals(this, other)) {
                return true;
            }

            return other.CustomId == this.CustomId;
        }

        public override bool Equals(object obj) {
            if (ReferenceEquals(null, obj)) {
                return false;
            }

            if (ReferenceEquals(this, obj)) {
                return true;
            }

            if (obj.GetType() != typeof(CustomType)) {
                return false;
            }

            return this.Equals((CustomType)obj);
        }

        public override int GetHashCode() {
            return this.CustomId.GetHashCode();
        }

    }

}
