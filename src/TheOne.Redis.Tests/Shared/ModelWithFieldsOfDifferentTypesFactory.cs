namespace TheOne.Redis.Tests.Shared {

    internal sealed class ModelWithFieldsOfDifferentTypesFactory : ModelFactoryBase<ModelWithFieldsOfDifferentTypes> {

        public override void AssertIsEqual(
            ModelWithFieldsOfDifferentTypes actual, ModelWithFieldsOfDifferentTypes expected) {
            ModelWithFieldsOfDifferentTypes.AssertIsEqual(actual, expected);
        }

        public override ModelWithFieldsOfDifferentTypes CreateInstance(int i) {
            return ModelWithFieldsOfDifferentTypes.CreateConstant(i);
        }

    }

}
