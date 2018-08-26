# TheOne.RabbitMq

## Getting Started

install via NuGet:

    PM> Install-Package TheOne.RabbitMq

### RabbitMqServer

RabbitMqServer is basically a high-level POCO-based Mq server.

By default, RabbitMqServer looks for a Rabbit Mq Server instance on **localhost** at Rabbit Mq's default port **5672**:

```csharp
var mqServer = new RabbitMqServer();
```
or
```csharp
var mqServer = new RabbitMqServer("localhost");
```

See [RabbitMqServerIntroTests](../src/TheOne.RabbitMq.Tests/Messaging/RabbitMqServerTests.cs) for examples.

#### Message Filters

There are optional `PublishMessageFilter` and `GetMessageFilter` callbacks which can be used to intercept outgoing and incoming messages.
The Type name of the message body that was published is available in `IBasicProperties.Type`, e.g:

```csharp
var mqServer = new RabbitMqServer("localhost") {
    PublishMessageFilter = (queueName, properties, msg) => {
        properties.AppId = $"app:{queueName}";
    },
    GetMessageFilter = (queueName, basicMsg) => {
        var props = basicMsg.BasicProperties;
        receivedMsgType = props.Type;
        receivedMsgApp = props.AppId;
    }
};

using (var mqClient = mqServer.CreateMessageQueueClient()) {
    mqClient.Publish(new Hello { Name = "Bugs Bunny" });
}

Assert.That(receivedMsgApp, Is.EqualTo(string.Format("app:{0}", MqQueueNames<Hello>.Direct)));
Assert.That(receivedMsgType, Is.EqualTo(typeof(Hello).Name));
```

#### CreateQueueFilter

WARNING: if a queue already exists, you cannot change it, this action shoud be deterministic.

```csharp
RabbitMqExtensions.CreateQueueFilter = (s, args) => {
    if (s == MqQueueNames<Hello>.Direct) {
        args.Remove(RabbitMqExtensions.XMaxPriority);
    }
};
```

### POCO Messages

You can use any POCO for your messages (aka Request DTOs) which are serialized with JSON and embedded as the body payload, a simple example is just:

```csharp
public class Hello {
    public string Name { get; set; }
}
```

### Registering Message Handlers

Now that we have a message we can use, we can start listening to any of these messages by registering handlers for it.
Here's how to register a simple handler that just prints out each message it receieves:

```csharp
mqServer.RegisterHandler<Hello>(m => {
    Hello request = m.GetBody();
    Console.WriteLine("Hello, {0}!", request.Name);
    return null;
});
```

Each handler receives an [IMqMessage<T>](../src/TheOne.RabbitMq/Interfaces/IMqMessage.cs)
which is just the body of the message that was sent (i.e. `T`) wrapped inside an `IMqMessage` container containing the metadata of the received message.
Inside your handler you can use `IMqMessage.GetBody()` to extract the typed body, and in this case there is no response and simply returning `null`.

### Starting the Rabbit MQ Server

Once all your handlers are registered you can start listening to messages by starting the Mq Server:

```csharp
mqServer.Start();
```

Starting the MQ Server spawns 1 threads for each handler.

### Allocating multiple threads for specific operations

By default only 1 thread is allocated to handle each message type, but this is easily configurable at registration.
E.g. you can spawn 4 threads to handle a CPU-intensive operation with:

```csharp
mqServer.RegisterHandler<Hello>(m => { .. }, noOfThreads:4);
```

### Publishing messages

With the mqServer started, you're now ready to start publishing messages,
you can do with a message queue client that you can get from a new **RabbitMqMessageFactory** or the mqServer directly, e.g:

```csharp
using (var mqClient = mqServer.CreateMessageQueueClient()) {
    mqClient.Publish(new Hello { Name = "World" });
}
```

The above shows the most common usage where you can publish POCO's directly,
behind the scenes this gets serialized as JSON and embedded as the payload of a new persistent message
that's sent using a **routing key** of the same name as the destination queue which by convention is mapped 1:1 to a queue of the same name,
i.e: **theone:mq.Hello.direct**. In effect,
publishing messages are sent to a distinct direct queue that's reserved for each message type,
essentially behaving as a work queue.

## Message Workflow

### Priority queue

By default, `.direct` queues has `x-max-priority` header with value `10` set. you can use `IMqMessage.Priority` set the message priority.

### Notification messages

`MqQueueNames<...>.Topic` is non-durable topic, designed to be transient,
and only used for notification purposes,
it's not meant to be relied on as a durable queue for persisting all Request DTO's processed.

```csharp
using (IMqMessageQueueClient mqClient = mqServer.CreateMessageQueueClient()) {
    var name = new HelloNull { Name = "Into the Void" };
    mqClient.Notify(MqQueueNames<HelloNull>.Topic, new MqMessage<HelloNull> { Body = name });
    IMqMessage<HelloNull> msg = mqClient.Get<HelloNull>(MqQueueNames<HelloNull>.Topic, TimeSpan.FromSeconds(5));
    Assert.AreEqual(name.Name, msg.GetBody().Name);
}
```

### Handler returns null Messages are ignored

When a handler returns a `null` response, that response is simply ignored.

### Handler with Responses are published to the Response .direct

Often message handlers will just return a POCO response after it processes a message, e.g:

```csharp
mqServer.RegisterHandler<Hello>(m =>
    new HelloResponse { Result = string.Format("Hello, {0}!", m.GetBody().Name) };
```

Whenever there's a response, the response message is sent to the **.direct** of the response message type, which for a `HelloResponse` type is just **theone:mq.HelloResponse.direct**, e.g:

```csharp
mqClient.Publish(new Hello { Name = "World" });

var responseMsg = mqClient.Get<HelloResponse>(MqQueueNames<HelloResponse>.Direct);
mqClient.Ack(responseMsg);
responseMsg.GetBody().Result // = Hello, World!
```

### Responses from Handlers with ReplyTo are published to that address

Whilst for the most part you'll only need to publish POCO messages,
you can also alter the default behavior by providing a customized `IMqMessage<T>` wrapper which `RabbitMqServer` will send instead,
e.g. you can specify your own **ReplyTo** address to change the queue where the response gets published, e.g:

```csharp
const string replyToMq = mqClient.GetTempQueueName();
mqClient.Publish(new Message<Hello>(new Hello { Name = "World" }) {
    ReplyTo = replyToMq
});

IMqMessage<HelloResponse> responseMsg = mqClient.Get<HelloResponse>(replyToMq);
mqClient.Ack(responseMsg);
responseMsg.GetBody().Result // = Hello, World!
```

### Messages that generate exceptions can be retried, then published to the dead-letter-queue (.dlq)

By default Rabbit Mq Server lets you specify whether or not you want messages that cause an exception to be retried by specifying a RetryCount of 1 (default),
or if you don't want any messages retried, specify a value of 0, e.g:

```csharp
var mqServer = new RabbitMqServer { RetryCount = 1 };
```

To illustrate how this works we'll keep a counter of how many times a message handler is invoked, then throw an exception to force an error condition, e.g:

```csharp
var called = 0;
mqServer.RegisterHandler<Hello>(m => {
    called++;
    throw new ArgumentException("Name");
});
```

Now when we publish a message the response instead gets published to the messages **.dlq**, after it's first transparently retried.
We can verify this behavior by checking `called=2`:

```csharp
mqClient.Publish(new Hello { Name = "World" });

IMqMessage<Hello> dlqMsg = mqClient.Get<Hello>(MqQueueNames<Hello>.Dlq);
mqClient.Ack(dlqMsg);

Assert.That(called, Is.EqualTo(2));
```

`.dlq` Messages retains the original message in their body as well as the last exception serialized in the `IMqMessage.Error` MqErrorStatus metadata property, e.g:

```csharp
dlqMsg.GetBody().Name   // = World
dlqMsg.Error.ErrorCode  // = typeof(ArgumentException).Name
dlqMsg.Error.Message    // = Name
```

Since the body of the original message is left in-tact, you're able to retry failed messages by removing them from the dead-letter-queue then re-publishing the original message, e.g:

```csharp
IMqMessage<Hello> dlqMsg = mqClient.Get<Hello>(MqQueueNames<Hello>.Dlq);
mqClient.Publish(dlqMsg.GetBody());
mqClient.Ack(dlqMsg);
```

This is useful for recovering failed messages after identifying and fixing bugs that were previously causing exceptions,
where you can replay and re-process `.dlq` messages and continue processing them as normal.

## Mq Queue Name convention

Queue name is simply the POCO's name combine with customizable prefix and postfix.

see [MqQueueNames.cs](../src/TheOne.RabbitMq/Models/MqQueueNames.cs) and [MqQueueNamesTests](../src/TheOne.RabbitMq.Tests/Messaging/MqQueueNamesTests.cs) for example.
