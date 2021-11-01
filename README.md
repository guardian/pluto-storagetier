# pluto-storagetier

## What is it?

This is a monorepo, which contains a number of subprojects.  They are all pure-backend Scala projects, intended
to run against JVM 1.8.

Taken together, the are the components which allow pluto to move media from one storage tier to another.

Each specific transfer has its own component, i.e. online -> archive is one component, online -> nearline, etc.

## How do the 'components' work?

Each component is based around the same fundamental logic, which is encapsulated in the `common` module.
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
