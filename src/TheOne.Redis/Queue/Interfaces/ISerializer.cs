namespace TheOne.Redis.Queue {

    public interface ISerializer {

        byte[] Serialize(object value);
        object Deserialize(byte[] someBytes);

    }

}
