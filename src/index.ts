import { EventEmitter } from "node:events"
import { DurableObject } from "cloudflare:workers"
import {
	calculateCompactWindow,
	generateLogSegmentName,
	generateLogSegmentPath,
	parseLogSegmentName,
	readLines,
	SegmentMetadata,
} from "./segment"
import { RBTree } from "bintrees"
import { Mutex } from "async-mutex"

const FlushIntervalMs = 200
const hour = 1000 * 60 * 60
const day = hour * 24
const MaxTombstoneAgeMs = day * 1
const CompactLogSegmentsChance = 1 // temp for testing
const CleanTombstonesChance = 0.01
const OrphanPurgingChance = 0.0001

const activeLogSegmentKey = "active_log_segment::" // what logs segments are actually active, used for compaction, tombstone cleaning, and queries
const tombstoneKey = "tombstone::" // what logs segments are actually active, used for compaction, tombstone cleaning, and queries

function buildLogSegmentIndexKey(segmentName: string): string {
	return `${activeLogSegmentKey}${segmentName}`
}

function buildTombstoneKey(segmentName: string): string {
	return `${tombstoneKey}${segmentName}`
}

export interface PendingMessage {
	emitter: EventEmitter<{ resolve: [string[]]; error: [Error] }>
	/**
	 * Pre-serialized records so we can pre-calculate the length of write streams
	 */
	records: string[]
}

export interface GetMessagesRequest {
	consumerID: string

	// "" (or the same offset as the last message) means we long poll until a new message comes in, "-" means we start from the first offset
	offset: string
	limit: number
	// only used for long polling
	timeout_sec: number
}

export interface GetMessagesResponse {
	records: Record[]
}

export interface Record {
	offset: string
	data: any
}

export interface ProduceBody {
	records: any[]
}

export interface ProduceResponse {
	offsets: string[][]
}

function parseOffset(offset: string): { epoch: number; counter: number } {
	const epoch = offset.slice(0, 16)
	const counter = offset.slice(16)
	return { epoch: Number(epoch), counter: Number(counter) }
}

function serializeOffset(epoch: number, counter: number | string): string {
	// 16 digits is max safe integer for JS
	return `${epoch.toString().padStart(16, "0")}${counter.toString().padStart(16, "0")}`
}

export class StreamCoordinator extends DurableObject<Env> {
	consumers: Map<string, { emitter: EventEmitter<{ records: [Record[]] }>; limit: number }> = new Map()

	lastOffset: string = ""
	streamName: string = ""
	epoch: number = Date.now()
	counter: number = 0

	tree: RBTree<SegmentMetadata>
	treeMutex = new Mutex()

	// Messages that are pending persistence in the flush interval
	pendingMessages: PendingMessage[] = []

	setup_listener?: EventEmitter<{ finish: [] }>
	setup = false

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env)

		// We know 2 things to be true about SegmentMetadata:
		// 1. firstOffset is always less than or equal to lastOffset
		// 2. No instances SegmentMetadata can have intersecting offset ranges
		// Therefore we can make decisions entirely off of the first offset
		this.tree = new RBTree<SegmentMetadata>((a, b) => {
			// Return 0 if a == b
			// > 0 if a>b
			// < 0 if a<b
			if (a.firstOffset < b.firstOffset) {
				// a is before b
				return -1
			}
			if (a.firstOffset > b.firstOffset) {
				// a is after b
				return 1
			}

			// They are the same segment (search key)
			return 0
		})
	}

	finishSetup() {
		this.setup = true
		this.setup_listener!.emit("finish")
	}

	/**
	 * Ensures that we load the latest state from storage before we being processing requests
	 */
	async ensureSetup() {
		const release = await this.treeMutex.acquire()
		try {
			if (this.setup_listener) {
				console.log("Waiting for setup to finish")
				// We are not the first instance to start up, so wait for the setup to finish
				await new Promise<void>((resolve) => this.setup_listener!.once("finish", resolve))
				return
			}

			console.log("Doing setup")

			// We are the first instance to start up, so we need to do the setup
			this.setup_listener = new EventEmitter<{ finish: [] }>()

			console.log("Building index from storage")
			await this.buildIndexFromStorage()

			if (this.tree.size === 0) {
				console.debug("No segments found, skipping setup")
				return this.finishSetup()
			}

			// Load the previous epoch if we have it. When we go to write
			// we'll increment this and handle clock drift
			const maxRecord = this.tree.max()!
			this.epoch = parseOffset(maxRecord.lastOffset).epoch
			const { stream } = parseLogSegmentName(maxRecord.name)
			this.streamName = stream
			console.debug(`Setup complete, epoch: ${this.epoch}, streamName: ${this.streamName}`)

			this.finishSetup()
		} finally {
			release()
		}
	}

	async fetch(request: Request): Promise<Response> {
		if (this.env.AUTH_HEADER && request.headers.get("auth") !== this.env.AUTH_HEADER) {
			return new Response("Unauthorized", { status: 401 })
		}

		console.log("fetch", request.url, request.method)
		if (!this.streamName) {
			// Set it if we don't have it yet
			this.streamName = new URL(request.url).pathname
		}

		if (!this.setup) {
			await this.ensureSetup()
		}

		if (request.method === "PUT") {
			return this.handleMetaRequest(request)
		}

		if (request.method === "POST") {
			return this.handleProduce(request)
		}

		const url = new URL(request.url)
		const consumerID = url.searchParams.get("consumer_id")
		if (!consumerID) {
			return new Response("Missing consumer_id query parameter", { status: 400 })
		}

		const offset = url.searchParams.get("offset")
		const limit = url.searchParams.get("limit")
		const timeout_sec = url.searchParams.get("timeout_sec")

		const payload: GetMessagesRequest = {
			consumerID,
			offset: offset ?? "",
			limit: Number(limit) ?? 10, // low default avoid OOM
			timeout_sec: Number(timeout_sec) ?? 10,
		}

		return this.handleGetMessagesRequest(payload)
	}

	async handleMetaRequest(request: Request): Promise<Response> {
		// TODO: add method to force compaction for testing
		// TODO: add method to force tombstone cleanup for testing
		// TODO: add method to set compaction settings
		return new Response("NOT IMPLEMENTED", { status: 405 })
	}

	async handleProduce(request: Request): Promise<Response> {
		const body: ProduceBody = await request.json()

		// Submit for persistence and wait
		const emitter = new EventEmitter<{ resolve: [string[]]; error: [Error] }>()
		this.pendingMessages.push({ emitter, records: body.records.map((r) => JSON.stringify(r)) })
		if (this.pendingMessages.length === 1) {
			// Set the alarm to flush the pending messages
			await this.ctx.storage.setAlarm(Date.now() + FlushIntervalMs)
		}
		console.debug("waiting for flush")
		const offsetOrError = await Promise.any([
			new Promise<string[]>((resolve) => emitter.once("resolve", resolve)),
			new Promise<Error>((resolve) => emitter.once("error", resolve)),
		])

		if (offsetOrError instanceof Error) {
			return new Response(JSON.stringify({ error: offsetOrError.message }), {
				status: 500,
			})
		}

		// Return the persistence result
		return new Response(JSON.stringify({ offsets: offsetOrError }), {
			status: 200,
		})
	}

	async handleGetMessagesRequest(payload: GetMessagesRequest): Promise<Response> {
		let records: Record[] = []
		if (payload.offset) {
			records = await this.getMessagesFromOffset(payload.offset, payload.limit)
		}

		if ((payload.offset && payload.offset !== "-") || records.length > 0) {
			return new Response(JSON.stringify({ records } as GetMessagesResponse), {
				status: 200,
			})
		}

		// If we have a "-" offset and no records, we need to long poll

		// For long polling, capture the current lastOffset as the starting point.
		const emitter = new EventEmitter<{ records: [Record[]] }>()
		this.consumers.set(payload.consumerID, { emitter, limit: payload.limit })
		const res = await Promise.race([
			new Promise<Record[]>((resolve) => emitter.once("records", resolve)),
			new Promise<Error>((resolve) => setTimeout(() => resolve(new Error("timeout")), payload.timeout_sec * 1000)), // timeout after timeout_sec seconds
		])

		if (res instanceof Error) {
			return new Response(JSON.stringify({ records: [] } as GetMessagesResponse), {
				status: 200,
			})
		}

		return new Response(JSON.stringify({ records: res } as GetMessagesResponse), {
			status: 200,
		})
	}

	async getMessagesFromOffset(offset: string, limit: number): Promise<Record[]> {
		// get the item from the tree that's below the offset
		const segment = await this.getSegmentAfterOffset(offset)
		if (!segment) {
			// We didn't find a segment that contains the offset, so we return an empty array
			console.debug(`no messages segment found for offset ${offset}`)
			return []
		}

		// verify the offset is in the range
		if (offset !== "-" && segment.lastOffset < offset) {
			// The offset is outside the range, this is a bug
			console.error("Offset is outside the range of the segment", offset, segment)
			return []
		}

		// Load the segment from R2
		const segmentData = await this.env.StreamData.get(generateLogSegmentPath(this.streamName, segment.name))
		if (!segmentData) {
			// The segment doesn't exist, this is a bug
			console.error("Segment does not exist", segment)
			return []
		}

		// Stream the records from the segment up to limit
		const records: Record[] = []
		for await (const line of readLines(segmentData.body)) {
			console.debug(`reading line ${line}`)
			const recordOffset = line.slice(0, 32)
			const data = line.slice(32)
			if (recordOffset > offset) {
				let jsonData: any
				try {
					console.debug(`parsing record ${data}`)
					jsonData = JSON.parse(data) // get 32:-1 to remove the offset and newline
				} catch (e) {
					// This is a bug, the segment is corrupt
					throw Error(`Error parsing record (is the segment corrupt?): ${e}`)
				}

				records.push({ offset: recordOffset, data: jsonData })
				if (records.length >= limit) {
					break
				}
			}
		}

		// if we didn't hit the limit, repeat from the new offset (last record we streamed)
		if (records.length < limit) {
			console.debug(`getting more messages from offset ${records[records.length - 1].offset}`)
			records.push(...(await this.getMessagesFromOffset(records[records.length - 1].offset, limit - records.length)))
		}

		return records
	}

	async alarm(alarmInfo?: AlarmInvocationInfo) {
		console.debug("alarm waking up")
		// Do these sequentially so they're not racing for locks
		await this.flushPendingMessages()
		await this.compactLogSegments()
		await this.cleanTombstones()
		await this.purgeOrphans()
	}

	calculateTotalLength(pendingMessages: PendingMessage[]) {
		// Calculate the total overhead: 33 bytes per record (32 bytes for the offset name + 1 byte for the newline)
		const totalOverhead = pendingMessages.reduce((acc, m) => acc + m.records.length, 0) * 33
		// Sum of lengths of all the JSON record strings
		const totalRecordsLength = pendingMessages.reduce((acc, m) => acc + m.records.reduce((acc, r) => acc + r.length, 0), 0)
		return totalOverhead + totalRecordsLength
	}

	async flushPendingMessages() {
		console.debug("flushing pending messages")
		// Increment the epoch and reset the counter
		const oldEpoch = this.epoch
		this.epoch = Date.now()
		this.counter = 0
		if (this.epoch <= oldEpoch) {
			// What the heck, we went back in time? Clocks man... Let's just jump forward by 1
			console.warn("Clock went back in time, incrementing epoch")
			this.epoch = oldEpoch + 1
		}

		const segmentName = generateLogSegmentName(this.epoch)

		const offsets: string[][] = []
		for (const message of this.pendingMessages) {
			// For each of the writes, we need to generate an offset for each record
			const messageOffsets = []
			for (const _ of message.records) {
				// For each record, we need to generate an offset
				messageOffsets.push(serializeOffset(this.epoch, this.counter))
				this.counter++
			}
			offsets.push(messageOffsets)
		}

		this.lastOffset = offsets[offsets.length - 1][offsets[offsets.length - 1].length - 1]

		// Write the pending messages to the log segment
		console.debug("writing pending messages to log segment")
		await this.writePendingMessagesToLogSegment(this.streamName, segmentName, offsets, this.pendingMessages)

		// Write the log segment metadata so the segment is persisted
		console.debug("writing log segment metadata")
		await this.treeMutex.runExclusive(async () => {
			await this.writeLogSegmentMetadata({
				name: segmentName,
				firstOffset: offsets[0][0],
				lastOffset: offsets[offsets.length - 1][offsets[offsets.length - 1].length - 1],
				createdMS: Date.now(),
				records: this.counter, // the counter is the number of records written
				bytes: this.calculateTotalLength(this.pendingMessages),
			})
		})

		// Notify producer emitters to return
		for (let i = 0; i < this.pendingMessages.length; i++) {
			this.pendingMessages[i].emitter.emit("resolve", offsets[i])
		}

		// Clear the pending messages
		this.pendingMessages = []

		// Send records to waiting consumers
		console.debug("poking consumers to get new messages")
		for (const [consumerID, consumer] of this.consumers.entries()) {
			// We need to use the offset just below this epoch, so we don't miss any records so decrement epoch and use max counter
			const pokeOffset = serializeOffset(this.epoch - 1, "9".repeat(16))
			console.debug(`getting messages from offset ${offsets[0][0]} using poke offset ${pokeOffset} for consumer ${consumerID}`)
			const records = await this.getMessagesFromOffset(pokeOffset, consumer.limit)
			console.debug(`got ${records.length} records for consumer ${consumerID}`)
			if (records.length > 0) {
				console.debug(`emitting ${records.length} records to consumer ${consumerID}`)
				consumer.emitter.emit("records", records)
				this.consumers.delete(consumerID)
			}
		}
	}

	async writePendingMessagesToLogSegment(streamName: string, segmentName: string, offsets: string[][], pendingMessages: PendingMessage[]) {
		const totalLength = this.calculateTotalLength(pendingMessages)

		const segmentPath = generateLogSegmentPath(streamName, segmentName)

		const { readable, writable } = new FixedLengthStream(totalLength)
		const writer = writable.getWriter()

		// Start streaming the records to the file
		const writePromise = this.env.StreamData.put(segmentPath, readable)

		// Write the records to the file
		let records = 0
		let actualLength = 0
		for (let i = 0; i < pendingMessages.length; i++) {
			for (let j = 0; j < pendingMessages[i].records.length; j++) {
				const name = offsets[i][j]
				const nameBuffer = new TextEncoder().encode(name)
				const jsonBuffer = new TextEncoder().encode(pendingMessages[i].records[j])
				writer.write(nameBuffer)
				writer.write(jsonBuffer)
				writer.write(new TextEncoder().encode("\n"))
				actualLength += nameBuffer.length + jsonBuffer.length + 1
				records++
			}
		}

		console.debug(`Writing ${records} records to ${segmentPath} with actual length ${actualLength} and expected length ${totalLength}`)
		await Promise.all([writer.close(), writePromise])
		console.log(`Wrote ${records} records to ${segmentPath}`)
	}

	// Tree must be locked before calling this
	async buildIndexFromStorage() {
		const segments = await this.ctx.storage.list<SegmentMetadata>({
			prefix: activeLogSegmentKey,
		})

		for (const [_, segment] of segments) {
			this.tree.insert(segment)
		}
	}

	// Tree must be locked before calling this
	async writeLogSegmentMetadata(metadata: SegmentMetadata) {
		// First we need to durably store it
		await this.ctx.storage.put(buildLogSegmentIndexKey(metadata.name), metadata)
		// Then we can add it to the memory index
		this.tree.insert(metadata)
	}

	async compactLogSegments() {
		if (Math.random() > CompactLogSegmentsChance) {
			console.debug("NOT compacting log segments")
			return
		}

		console.debug("compacting log segments")
		const segmentWindow = await this.treeMutex.runExclusive(async () => calculateCompactWindow(this.tree))
		if (segmentWindow.length < 2) {
			// We don't have enough segments to compact, so we can't compact
			console.debug("not enough segments to compact after iteration, exiting")
			return
		}
		console.debug(`compacting ${segmentWindow.length} segments: ${segmentWindow.map((s) => JSON.stringify(s, null, 2)).join(", ")}`)

		// k-way merge the segments with line readers to a new segment file
		const totalLength = segmentWindow.reduce((acc, s) => acc + s.bytes, 0)
		const newSegmentName = generateLogSegmentName(this.epoch, ".compacted.seg")

		const { readable, writable } = new FixedLengthStream(totalLength)
		const writer = writable.getWriter()

		// Start streaming the records to the file
		const writePromise = this.env.StreamData.put(generateLogSegmentPath(this.streamName, newSegmentName), readable)

		// Create a reader for each of the segments
		const readers = await Promise.all(
			segmentWindow.map(async (s) => this.env.StreamData.get(generateLogSegmentPath(this.streamName, s.name)))
		)
		// Verify all the readers are valid
		for (let i = 0; i < segmentWindow.length; i++) {
			if (!readers[i]) {
				console.error(`Segment ${segmentWindow[i].name} does not exist, data is corrupted`)
				return
			}
		}

		// Begin the k-way merge: this will write records from all segments in order.
		const mergePromise = kWayMerge(
			readers.map((r) => readLines(r!.body)),
			writer
		)

		// Wait for the merge to finish (which in turn closes the writer)
		await mergePromise

		// Finally, wait for the record to be persisted to R2.
		await writePromise

		const newSegment: SegmentMetadata = {
			name: newSegmentName,
			firstOffset: segmentWindow[0].firstOffset,
			lastOffset: segmentWindow[segmentWindow.length - 1].lastOffset,
			createdMS: Date.now(),
			records: segmentWindow.reduce((acc, s) => acc + s.records, 0),
			bytes: totalLength,
		}

		// transaction to update log segments and store tombstones
		// if we crash here it's ok since we recover the tree
		await this.ctx.storage.transaction(async (tx) => {
			for (const segment of segmentWindow) {
				await tx.delete(buildLogSegmentIndexKey(segment.name))
				await tx.put(buildTombstoneKey(segment.name), segment)
			}
			await tx.put(buildLogSegmentIndexKey(newSegment.name), newSegment)
		})

		// grab tree lock and update the tree
		await this.treeMutex.runExclusive(async () => {
			for (const segment of segmentWindow) {
				this.tree.remove(segment)
			}
			this.tree.insert(newSegment)
		})

		console.debug("compacted log segments into", newSegment)
	}

	async cleanTombstones() {
		if (Math.random() > CleanTombstonesChance) {
			return
		}

		console.debug("cleaning tombstones")

		const items = await this.ctx.storage.list<SegmentMetadata>({
			prefix: tombstoneKey,
		})
		const now = Date.now()
		for (const [_, item] of items) {
			if (item.createdMS < now - MaxTombstoneAgeMs) {
				console.debug(`Deleting tombstone ${item.name}`)
				// Delete it from R2 first to make sure we do it
				try {
					await this.env.StreamData.delete(generateLogSegmentPath(this.streamName, item.name))
				} catch (error) {
					console.error(`Error deleting tombstone ${item.name} from R2, did it already get deleted?`, error)
				}

				// Delete it from the tombstone index
				await this.ctx.storage.delete(buildTombstoneKey(item.name))
			}
		}
	}

	async purgeOrphans() {
		if (Math.random() > OrphanPurgingChance) {
			return
		}

		// TODO: list through all files in R2
		// TODO: check if the files exist in the KV or the tombstones
		// TODO: if none, log warning and delete the file from R2

		console.debug("purging orphans")
	}

	// This helper function returns the SegmentMetadata that will contain the first offset AFTER the given offset
	async getSegmentAfterOffset(offset: string): Promise<SegmentMetadata | null> {
		// Create a dummy search key with the provided offset.
		const searchKey = { firstOffset: offset } as SegmentMetadata

		const release = await this.treeMutex.acquire()
		try {
			if (offset === "-") {
				// If the offset is "-", we want the first segment
				console.debug("getting first segment from '-' offset")
				return this.tree.min()
			}

			// Use the tree's lowerBound to find the first element whose firstOffset is >= offset.
			const it = this.tree.lowerBound(searchKey)
			let candidate: SegmentMetadata | null = null

			// If lowerBound doesn't return an element, the tree may be empty or the offset is greater than all segments.
			const segment = it.data()
			console.debug("segment", segment)
			if (segment === null) {
				console.debug(`no lower bound segment found for offset ${offset}`)
				candidate = this.tree.max()
			} else if (segment.lastOffset > offset) {
				// Otherwise the segment is the candidate
				candidate = segment
			}

			// Verify the candidate's last offset is greater than the offset (single record segment)
			if (candidate && candidate.lastOffset > offset) {
				console.debug(`found segment ${candidate.name} covering offset ${offset}`)
				return candidate
			}

			console.debug(`no segment covering offset ${offset} was found`)
			return null
		} finally {
			release()
		}
	}
}

export default {
	async fetch(request, env, ctx): Promise<Response> {
		console.log("fetch", request.url)
		let id = env.StreamCoordinator.idFromName(new URL(request.url).pathname)
		let stub = env.StreamCoordinator.get(id)
		return stub.fetch(request)
	},
} satisfies ExportedHandler<Env>

/**
 * Merges multiple sorted line readers into a single writer.
 * Each line is assumed to have a record ID in its first 32 characters.
 * The function "peeks" one line from each reader and writes out the smallest ID line,
 * advancing that reader only when its line has been written.
 */
async function kWayMerge(lineReaders: AsyncIterable<string>[], writer: WritableStreamDefaultWriter<Uint8Array>) {
	interface BufferEntry {
		line: string
		iterator: AsyncIterator<string>
	}

	// Initialize each reader by fetching its first line
	const buffers: BufferEntry[] = []
	for (const reader of lineReaders) {
		const iterator = reader[Symbol.asyncIterator]()
		const result = await iterator.next()
		if (!result.done) {
			buffers.push({ line: result.value, iterator })
		}
	}

	// Utility function to extract record ID from a line.
	// In our case the record ID is the first 32 characters.
	const getRecordID = (line: string): string => line.slice(0, 32)

	// Continue until every reader has been exhausted
	while (buffers.length > 0) {
		// Find the buffered line with the smallest record ID.
		let minIndex = 0
		let minRecordID = getRecordID(buffers[0].line)
		for (let i = 1; i < buffers.length; i++) {
			const currID = getRecordID(buffers[i].line)
			if (currID < minRecordID) {
				minRecordID = currID
				minIndex = i
			}
		}
		const smallestEntry = buffers[minIndex]

		// Write the chosen line (append a newline if it wasn't included)
		const buffer = new TextEncoder().encode(smallestEntry.line + "\n")
		await writer.write(buffer)

		// Advance the iterator for the reader we just used.
		const nextResult = await smallestEntry.iterator.next()
		if (nextResult.done) {
			// Remove this reader from our buffers if there are no more lines.
			buffers.splice(minIndex, 1)
		} else {
			buffers[minIndex].line = nextResult.value
		}
	}
	await writer.close()
}
