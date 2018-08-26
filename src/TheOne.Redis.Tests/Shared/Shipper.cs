using System;

namespace TheOne.Redis.Tests.Shared {

    internal sealed class Shipper : IHasRedisIntId {

        public string CompanyName { get; set; }
        public ShipperType ShipperType { get; set; }
        public DateTime DateCreated { get; set; }
        public Guid UniqueRef { get; set; }
        public int Id { get; set; }

        public override bool Equals(object obj) {
            var other = obj as Shipper;
            if (other == null) {
                return false;
            }

            return this.Id == other.Id && this.UniqueRef == other.UniqueRef;
        }

        public override int GetHashCode() {
            return string.Concat(this.Id, this.UniqueRef).GetHashCode();
        }

    }

    public enum ShipperType {

        All = Planes | Trains | Automobiles,
        Unknown = 0,
        Planes = 1,
        Trains = 2,
        Automobiles = 4

    }

}
