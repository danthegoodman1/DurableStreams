# DurableStreams

## Difference from Workers PubSub and Workers Queues

It's a funadamentally different model, that same reason you'd use Kafka over RabbitMQ or Redis list: Streams are immutable, ordered, and consumers can pull them when ever.

PubSub doesn't hold an infinite history, and queues don't let consumers operate in full isolation (nor have infinite history).

You need streams if you want a log that can persist for long durations, and handle starting consuming from 3 months ago.

## Differences from Kafka-like systems

### Publish is not socketed

This makes it easier to quickly publish messages and go away. You don't need to set up and manage a connection.

You can publish multiple records in one request, and those are guaranteed to be in order.

### Consumers track their own offsets/no consumer groups

Tracking offsets is only really needed if you are managing consumer groups, and individual consumers can come and go on behalf of the group.

Because there are no groups, we don't need to track offsets for consumers.

The decision to not support groups is in 2:

1. That's a lot more complex (would mean a lot more code before releasing)
2. A single DO probably won't have the bandwidth such that multiple groups are even needed

If you do need to fan-out (e.g. heavy GPU workload), you can have a consumer that manages fanning them out. Or do something like simple like each consumer only actually processes `Murmur3(offset) % N`.

## Other notes

### Isn't this just effectively a batching NDJSON merge engine, with a monotonic hybrid clock?

Yes. That's effectively what streams are. Sometimes they have extra features like managed consumer groups too :P

### But wait then isn't this effectively [IceDB](https://github.com/danthegoodman1/icedb/), which is a parquet merge engine in S3 but NDJSON, if you're having consumers track their own offsets?

... next question
