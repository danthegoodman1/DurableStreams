# DurableStreams

Durable bottomless event streaming with Cloudflare Durable Objects and R2.

## Usage

The URL path names the stream: `POST /orders` publishes to the `orders` stream and `GET /orders` reads from it. Paths can contain slashes (`tenant-1/orders`), and names are limited to 256 characters. A consumer can start consuming (e.g. long-poll for new records) before anything is published.

The tests in `test/` are digestible and cover every feature.

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

Records can be any JSON values. The response lists each record's offset once the records are durable in R2:

```json
{ "offsets": ["00017909450393370000000000000000", "00017909450393370000000000000001"] }
```

Records in one request are stored contiguously and in order.

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

- Must be a non-negative integer
- Only allows writes if version >= current version
- Updates the stored version when a higher version is provided
- Returns 409 if version < current version, with `current_version` and `provided_version` in the body
- Optional - if not provided, writes are always allowed

This is useful for:

- Preventing stale/zombie producers from writing
- Handling changes in higher-level partition rebalancing (prevent producers from writing to the wrong partition during inconsistency window of producer and partition count)

You can omit records to only raise the producer version, for example between creating a new partition (and making it available for discovery) and pushing updates down to the publishers, to consistently handle rebalancing. That returns the current version:

```json
{ "version": 1 }
```

### Consuming

To consume records, make a GET request to the stream:

```
curl "https://your-worker.example.com/your-stream-name?offset=-&limit=5&timeout_sec=10" \
  -H "auth: YOUR_AUTH_HEADER"
```

Records come back in offset order:

```ts
interface GetMessagesResponse {
	records: Record[]
}

interface Record {
	offset: string
	data: any
}
```

To continue, request again with the last record's offset. A response may hold fewer than `limit` records even when more exist, so keep paging until a request returns no records.

#### Parameters

- `offset`: Return records after this offset. Use `-` to read from the beginning. Omit it to long-poll for the next records published.
- `limit`: The maximum number of records to return, default `10`, max `1000`.
- `timeout_sec`: How long to wait for new records when none are available, default `0` (return immediately), max `300`. Fractions are allowed.

### Deleting a stream

`DELETE /your-stream-name` deletes every record and resets the producer version. The stream can be published to again afterwards.

### Auth header

Set the `AUTH_HEADER` secret (`wrangler secret put AUTH_HEADER`) to require every request to send a matching `auth` header. Unauthorized requests are rejected before they reach a stream.

### Configuration

These optional variables tune every stream; set them under `vars` in `wrangler.jsonc`. Changing them and redeploying is safe.

| Variable                  | Default    | Effect                                                                  |
| ------------------------- | ---------- | ----------------------------------------------------------------------- |
| `FLUSH_INTERVAL_MS`       | `200`      | How long published records wait so concurrent publishes share a segment |
| `COMPACTION_MAX_SEGMENTS` | `10`       | How many segments compaction merges at once                             |
| `COMPACTION_MAX_RECORDS`  | `5000`     | Segments with this many records are full and never compacted again      |
| `COMPACTION_MAX_BYTES`    | `10000000` | Segments with this many bytes are full and never compacted again        |

A longer flush interval raises throughput and creates fewer segments at the cost of publish latency. Depending on how often you write and how large your records are, you may need to adjust it, or even go as far as sharding a stream (stream-1, stream-2, etc.).

Think of a full segment as a parquet row group: reading any record pulls at least the segment that holds it, so it should be quick to fetch even when it's the last segment you need.

### Reading from a point in time

Offsets are 32 digits: the first 16 are the zero-padded epoch (Unix milliseconds) when the record was flushed to storage, and the last 16 are a counter within that flush.

Therefore if you want to read from a specific point in time, like now - 30 days, pass the zero-padded Unix milliseconds of that time as the offset, e.g. `offset=0001739995966373`. That returns all records _flushed_ at or after that time, so you may want to additionally subtract your flush interval (or a few) to be safe.

## How it works

The Worker authenticates and parses each request, then calls the stream's Durable Object over RPC. Each stream is one SQLite-backed Durable Object, addressed by name.

- **Publishing**: the object buffers publishes for the flush interval, then writes them to R2 as one segment: an immutable object holding a newline-delimited run of records, each prefixed by its offset. It then commits the segment to its SQLite index and responds. Flushes run one at a time so segments commit in offset order.
- **Reading**: the object finds the first segment holding records after the requested offset and streams segments from R2 until it has `limit` records. Long-polling readers receive each committed batch directly from memory.
- **Compaction**: an alarm merges small adjacent segments in tiers, like an LSM tree. Flushes write level-0 segments; merging `COMPACTION_MAX_SEGMENTS` segments of one level produces one segment a level up, until segments are full. Each record is rewritten only a few times, and reads touch few segments.
- **Cleanup**: compaction keeps replaced segments for a day so in-flight reads finish, then deletes them. Every upload is recorded before it starts, so an upload that never commits (e.g. after a crash) is deleted too.

R2 keys are `<URL-encoded stream name>/<first offset>-<last offset>.seg`, so a stream's objects never share a prefix with another stream's.

## Development

```
npm install
npm test           # vitest, running inside the Workers runtime
npm run typecheck
npm run dev        # local server with local R2 and Durable Objects
```

To deploy, create the bucket once with `npx wrangler r2 bucket create durable-streams`, then run `npm run deploy`. Run `npm run cf-typegen` after changing `wrangler.jsonc` to regenerate `worker-configuration.d.ts`.

## Difference from Workers PubSub and Workers Queues

It's a fundamentally different model, the same reason you'd use Kafka over RabbitMQ or Redis list: Streams are immutable, ordered, and consumers can pull them whenever.

PubSub doesn't hold an infinite history, and queues don't let consumers operate in full isolation (nor have infinite history).

You need streams if you want an event that can persist for long durations, and handle starting consuming from 3 months ago.

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

If you do need to fan-out (e.g. heavy GPU workload), you can have a consumer that manages fanning them out. Or do something simple like each consumer only actually processes `Murmur3(offset) % N`.

## Other notes

### Limitations

Each stream is a single Durable Object, so per-stream throughput is bounded by one object: every publish and read passes through it. As mentioned in other sections, you can horizontally scale with more streams (assuming your workload can handle ordering at the partition level).

Records are stored in R2, so streams are bottomless. The index holds one small row per segment in the object's SQLite database, so it stays far below the 10 GB per-object limit.

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

Yes, but you can see how it's pretty easy to transplant this class to a generic HTTP framework, and swap out the SQLite and R2 specific bits for something like FDB and S3.

In fact it's so simple, I bet o3-mini could port it to another language.
