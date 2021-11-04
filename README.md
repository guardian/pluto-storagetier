# pluto-storagetier

## What is it?

This is a monorepo, which contains a number of subprojects.  They are all pure-backend Scala projects, intended
to run against JVM 1.8.

Taken together, the are the components which allow pluto to move media from one storage tier to another.

Each specific transfer has its own component, i.e. online -> archive is one component, online -> nearline, etc.

## How do the components communicate?
The components communicate with each other, themselves and the rest of the Pluto system via the RabbitMQ
message bus.

For a good grounding in RabbitMQ terminology, have a look here: https://www.rabbitmq.com/getstarted.html.

The protocol used throughout the wider Pluto system is that when a component _does_ something that another
component may be interested in, it pushes a JSON message to its own _Exchange_.  Another component can then
receive this notification onto its own _Queue_ by _subscribing_ to the producer's _Exchange_.

In other words, a _producer_ owns the Exchange and a _consumer_ owns the queue.  

To take a concrete example, consider the fact that many different apps may want to be notified about
events from Vidispine e.g. the Deliverables component, different storage-tier components, etc.

Vidispine does not directly interface to RabbitMQ but sends HTTP messages to given endpoints, so we use
a tool called "pluto-vsrelay" (see https://gitlab.com/codmill/customer-projects/guardian/pluto-vs-relay) which
receives these HTTP notifications and pushes them to an Exchange.

The key point is that it does not need to know or care about what other processes may want to consume
these messages.  Furthermore, it is responsible for "declaring" (in RabbitMQ parlance) the exchange i.e.
ensuring that it is created and configured properly.

Say that we now have a new app that needs to know about some kind of specific event from Vidispine, e.g. an
item metadata change.

Our app "declares" its own _queue_ in RabbitMQ and asks the broker (RabbitMQ server) to _subscribe_ this queue onto the Exchange
that pluto-vsrelay created.  In this way events that are pushed to the Exchange will be copied to the queue.
Any number of queues can be subscribed to an exchange, and they will _all_ receive the same messages from the exchange.

As the name suggests, a _queue_ will _hold_ a message until it is consumed.  An exchange, on the other hand, will
pass a message on to subscribers and then forget about it.

![Generic rabbitMQ exchange/queue usage](doc/rmq-generic.png)

You can imagine, though, that in this example there are a lot of other events coming from Vidispine that
our app is _not_ interested in (we only want item metadata updates).  It would be nice if we could only
receive onto our queue these specific events rather than everything.

This is where the concept of a "routing key" comes in (see https://www.rabbitmq.com/tutorials/tutorial-four-python.html).
All the exchanges used in Pluto are "topic" exchanges, meaning that they _require_  a routing key to be present
on a message when it is sent.

A routing key is a set of strings separated by the period `.` character - e.g. `vidispine.item.metadata.modify`.
RabbitMQ itself does not care about the specific content or meaning of the routing key, but we stick to
a least specific -> most specific logic (in this case, literally `modify` the `metadata` of an `item` 
in `vidispine`).

When you make a subscription to a Topic exchange, you need to pass in a specification for the routing key(s) that
you want to receive.  The wildcard characters `*` and `#` are useful here - `*` means "match anything in this part of
the routing key" and `#` means "match anything from here on in". 
For example: 
 - `vidispine.item.#` would match `vidispine.item.metadata.modify` and `vidispine.item.shape.create` etc.
 - `vidispine.item.*.create` would match `vidispine.item.shape.create` but not `vidispine.item.metadata.modify`.

You can see these subscriptions in action in the respective Main classes of the components:
```scala
      ProcessorConfiguration(
        "assetsweeper",
        "assetsweeper.asset_folder_importer.file.#",
        "storagetier.onlinearchive.newfile",
        new AssetSweeperMessageProcessor(plutoConfig)
      )
```
Is the code that sets up an instance of the AssetSweeperMessageProcessor class to receive all `file` messages
from `asset_folder_importer` via the `assetsweeper` exchange and sends success messages with a routing key of
`storagetier.onlinearchive.newfile.success` to the component's designated output exchange.

`ProcessorConfiguration` is defined in [ProcessorConfiguration.scala](common/src/scala/com/gu/multimedia/storagetier/framework/ProcessorConfiguration.scala)

## How do the 'components' work?

Each component is based around the same fundamental logic, which is encapsulated in the `com.gu.multimedia.storagetier.framework` module.
They are designed to respond to messages that occur elsewhere in the system which are notified via the message queue,
and then ensure that a copy of the given media is present in the required storage tier.  Once this has been done, 
another message is output indicating that the operation took place.

Failures are split into two kinds; "retryable" and "permanent" failures.  
- If a permanent failure (i.e. one for which there is no point retrying) occurs during the processing of a message, 
then the original message is sent to a dead-letter queue via a dead-letter exchange, with a number of fields set to 
indicate what went wrong.  
- If a retryable failure occurs, then the original message is sent to a "retry" queue via a 
retry exchange.  The "retry" queue is not directly subscribed, but all messages have a TTL (time-to-live) value set on 
them.  Once this TTL expires they are re-routed back to the "retry-input" exchange, which is picked up by an app instance 
and replayed. In this way, retries are kept outside the scope of any running instance so it is safe for instances to 
crash or be restarted at any point.

Schematically, the logic looks like this:

![RabbitMQ subscription](doc/rmq-subscription.png)

