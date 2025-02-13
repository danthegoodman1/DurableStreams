# DurableStreams

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
