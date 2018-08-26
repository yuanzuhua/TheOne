namespace TheOne.Redis.Client {

    public interface IHasNamed<T> {

        T this[string listId] { get; set; }

    }

}
