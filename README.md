# DurableStreams

Durable bottomless event streaming with Cloudflare Durable Objects and R2.

## Usage

The name of the stream is determined by the path at which you publish and consume from. A consumer is permitted to begin consuming (e.g. long-poll for new records) before publishing begins.

Also check the tests, they're digestible and cover lots of features.

### Publishing

Publishing records is as simple as making a POST request with a JSON body:

```
curl -X POST "https://your-worker.example.com/your-stream-name" \
  -H "Content-Type: application/json" \
  -H "auth: YOUR_AUTH_HEADER" \
  -d '{
    "records": [
      {"key": "value1"},
      {"key": "value2"}
    ]
  }'
```

#### Publish version (fencing token)

You can optionally include a version parameter when publishing to implement fencing tokens or leader election:

```
curl -X POST "https://your-worker.example.com/your-stream-name?version=1" \
  -H "Content-Type: application/json" \
  -H "auth: YOUR_AUTH_HEADER" \
  -d '{
    "records": []
  }'
```

The version acts as a fencing token:

- Must be a number
- Only allows writes if version >= current version
- Updates stored version when a higher version is provided
- Returns 409 if version < current version
- Optional - if not provided, writes are always allowed

This is useful for:

- Preventing stale/zombie producers from writing
- Handling changes in higher-level partition rebalancing (prevent producers from writing to the wrong partition during inconsistency window of producer and partition count)

You can choose to omit records when publishing to purely increment the producer version, which can be used between creating the new partition (and making it available for discovery), and before pushing updates down the publishers, to consistently handle rebalancing. That will return the following JSON response:

```
{ version: this.metadata.producer_version }
```

### Consuming

To consume messages, perform a GET request to the stream:

```
curl "https://your-worker.example.com/your-stream-name?offset=-&limit=5&timeout_sec=10" \
  -H "auth: YOUR_AUTH_HEADER"
```

Records will come back with:

```ts
export interface GetMessagesResponse {
	records: Record[]
}

export interface Record {
	offset: string
	data: any
}
```

If you make a subsequent request to consume from the offset

#### Parameters

- `offset`: The record offset to start from. Use `-` to request messages from the beginning, and leave blank to long-poll for the next message batch produced.
- `limit`: The maximum number of records to return, default `10`.
- `timeout_sec`: Duration the request will long-poll before returning if no records are found, default `0` (return immediately).

### Auth header

You can set the `AUTH_HEADER` env var. If set, requests will be checked for a matching value in the `auth` header.

### Flush interval

By default, published records are stacked in memory and flushed on an interval. This helps increase throughput and reduce the number of segments created at the expense of memory usage.

Depending on how often you write, and how large your records are, you may need to adjust this, or even go as far as make stream shards (stream-1, stream-2, etc.)

### Segment sizes

You want to think of a segment size in terms of a row group for parquet: Every time you look up a batch of records, or a single record, it's going to pull at least one segment, so it should be relatively fast to get that even if it's the last segment.

### Reading from a point in time

Stream offsets are 32 bytes, where the first 16 bytes are the zero-padded epoch interval when the event was
flushed to storage, and the second 16 bytes being a 128-bit incrementing counter (it's probably impossible that this ever exceeds tens of thousands unless you have a massive epoch interval).

Therefore if you want to read from a specific point in time, like now - 30 days, you could join a zero-padded now-30d unix milliseconds with 16 `0`'s to generate a timestamp like `00017399959663730000000000000000`. That will represent all events _flushed_ after that time, so you may want to additionally subtract your flush interval (or a few) to be safe.

### Compaction settings

Because Durable Objects are limited to 128MB of memory, we have to be mindful of memory. The largest use of memory (beyond pending writes) will be the segment metadata index. As a result, when your stream grows (really large) in size, you'll have to start increasing the compaction threshold to reduce the number of total segments, thus reducing memory usage. It's safe to adjust compaction settings on the fly by redeploying, but at the moment there's no way to change it for a single stream (see GH issue).

## Difference from Workers PubSub and Workers Queues

It's a funadamentally different model, that same reason you'd use Kafka over RabbitMQ or Redis list: Streams are immutable, ordered, and consumers can pull them when ever.

PubSub doesn't hold an infinite history, and queues don't let consumers operate in full isolation (nor have infinite history).

You need streams if you want a event that can persist for long durations, and handle starting consuming from 3 months ago.

## Differences from Kafka-like systems

It's more like Redis streams, without the consumer group.

You can build Kafka-like semantics on top as needed.

The TLDR is you can think of a Durable Stream as a single Kafka partition with its own timestamp oracle. If you want offset persistence, consumer groups, etc. you can build that as a layer on top (possible also with Durable Objects).

The really awesome thing about Durable Streams is exactly that isolation: if you need more partitions, just make more! You're only going to be scale-bound by the system that talks to the Durable Streams, not each stream itself, because of that horizontal scalability.

### Based on requests, not persistent connections

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

### Limitations

The major limitation is Durable Objects are limited to 128MB of memory.

As a result, many possible performance optimizations have been dropped in favor of reducing memory overhead (e.g. purging oprhans doing per-object double KV lookups).

You also have to keep in mind the 50GB total storage limit for KV for Durable Objects, which can be raised by contacting Cloudflare, but it's unknown at this time whether you'd get a notification, or just hit some hard wall.

There's obviously a per-stream performance limit, however as mentioned in other sections, you can just horizontally scale as needed (assuming your workload can handle ordering at the partition level).

### Isn't this just effectively a batching NDJSON merge engine, with a monotonic hybrid clock?

Yes. That's effectively what streams are. Sometimes they have extra features like managed consumer groups too :P

### Why not Postgres with BIGSERIAL/SEQUENCE?

Because that's not:

1. Horizontally scalable (at least not nearly as easily)
2. Requires Postgres
3. Doesn't allow you to start by time (see [reading from a point in time](#reading-from-a-point-in-time))
4. Not bottomless
5. Manual setup of every unique stream
6. Less convenient than HTTP requests

### But wait then isn't this effectively [IceDB](https://github.com/danthegoodman1/icedb/), which is a parquet merge engine in S3 but NDJSON, if you're having consumers track their own offsets, and you added a clock for ordering?

Kinda, that's why I was able to make it in <1000 loc and <10hrs of dev work

### Isn't this stuck on Cloudflare now though?

Yes, but you can see how it's pretty easy to transplant this class to a generic HTTP framework, and swap out the KV and R2 specific bits for something like FDB and S3.

In fact it's so simple, I bet o3-mini could port it to another language.
