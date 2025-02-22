import { EventEmitter } from "node:events"
import { DurableObject } from "cloudflare:workers"
import { generateLogSegmentName, parseLogSegmentName, readLines, SegmentMetadata } from "./segment"
import { RBTree } from "bintrees"
import { Mutex } from "async-mutex"

const FlushIntervalMs = 200
const hour = 1000 * 60 * 60
const day = hour * 24
const MaxStaleSegmentMs = day * 1
const CompactLogSegmentsChance = 0.05
const CleanTombstonesChance = 0.01

const activeLogSegmentKey = "active_log_segment::" // what logs segments are actually active, used for compaction, tombstone cleaning, and queries

function buildLogSegmentIndexKey(segmentName: string): string {
	return `${activeLogSegmentKey}${segmentName}`
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

function serializeOffset(epoch: number, counter: number): string {
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
		console.log("fetch", request.url, request.method)
		if (!this.streamName) {
			// Set it if we don't have it yet
			this.streamName = new URL(request.url).pathname
		}

		if (!this.setup) {
			await this.ensureSetup()
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
		if (payload.offset) {
			const records = await this.getMessagesFromOffset(payload.offset, payload.limit)
			return new Response(JSON.stringify({ records } as GetMessagesResponse), {
				status: 200,
			})
		}

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
		if (segment.firstOffset > offset || segment.lastOffset < offset) {
			// The offset is outside the range, this is a bug
			console.error("Offset is outside the range of the segment", offset, segment)
			return []
		}

		// Load the segment from R2
		const segmentData = await this.env.StreamData.get(segment.name)
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

		const segmentName = generateLogSegmentName(this.streamName, this.epoch)

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
		await this.writePendingMessagesToLogSegment(segmentName, offsets, this.pendingMessages)

		// Write the log segment metadata so the segment is persisted
		console.debug("writing log segment metadata")
		await this.treeMutex.runExclusive(async () => {
			await this.writeLogSegmentMetadata({
				name: segmentName,
				firstOffset: offsets[0][0],
				lastOffset: offsets[offsets.length - 1][offsets[offsets.length - 1].length - 1],
				createdMS: Date.now(),
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
			console.debug(`getting messages from offset ${offsets[0][0]} for consumer ${consumerID}`)
			const records = await this.getMessagesFromOffset(offsets[0][0], consumer.limit)
			console.debug(`got ${records.length} records for consumer ${consumerID}`)
			if (records.length > 0) {
				console.debug(`emitting ${records.length} records to consumer ${consumerID}`)
				consumer.emitter.emit("records", records)
				this.consumers.delete(consumerID)
			}
		}
	}

	async writePendingMessagesToLogSegment(segmentName: string, offsets: string[][], pendingMessages: PendingMessage[]) {
		// Calculate the total overhead: 33 bytes per record (32 bytes for the offset name + 1 byte for the newline)
		const totalOverhead = pendingMessages.reduce((acc, m) => acc + m.records.length, 0) * 33
		// Sum of lengths of all the JSON record strings
		const totalRecordsLength = pendingMessages.reduce((acc, m) => acc + m.records.reduce((acc, r) => acc + r.length, 0), 0)
		const totalLength = totalOverhead + totalRecordsLength

		const { readable, writable } = new FixedLengthStream(totalLength)
		const writer = writable.getWriter()

		// Start streaming the records to the file
		const writePromise = this.env.StreamData.put(segmentName, readable)

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

		console.debug(`Writing ${records} records to ${segmentName} with actual length ${actualLength} and expected length ${totalLength}`)
		await Promise.all([writer.close(), writePromise])
		console.log(`Wrote ${records} records to ${segmentName}`)
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
		await this.ctx.storage.put(buildLogSegmentIndexKey(metadata.name), JSON.stringify(metadata))
		// Then we can add it to the memory index
		this.tree.insert(metadata)
	}

	async compactLogSegments() {
		if (Math.random() < CompactLogSegmentsChance) {
			return
		}

		console.debug("compacting log segments")
		// TODO: check metadata to see if we need to compact log segments
		// TODO: k-way merge the segments with line readers
		// TODO: transaction to update log segments
	}

	async cleanTombstones() {
		if (Math.random() < CleanTombstonesChance) {
			return
		}

		console.debug("cleaning tombstones")

		// TODO: get snapshot of what segments are active
		// TODO: list R2 to find non-active segments that are older than the retention policy
		// TODO: delete the segments from R2
	}

	// This helper function returns the SegmentMetadata that will contain the first offset AFTER the given offset
	async getSegmentAfterOffset(offset: string): Promise<SegmentMetadata | null> {
		// Create a dummy search key with the provided offset.
		const searchKey = { firstOffset: offset } as SegmentMetadata

		const release = await this.treeMutex.acquire()
		try {
			// Use the tree's lowerBound to find the first element whose firstOffset is >= offset.
			const it = this.tree.lowerBound(searchKey)
			let candidate: SegmentMetadata | null = null

			// If lowerBound doesn't return an element, the tree may be empty or the offset is greater than all segments.
			if (it.data() === null) {
				console.debug(`no lower bound segment found for offset ${offset}`)
				candidate = this.tree.max()
			} else {
				// If the found element exactly matches the offset, then that's our candidate.
				if (it.data()!.firstOffset === offset) {
					console.debug(`found exact segment ${it.data()!.name} for offset ${offset}`)
					candidate = it.data()
				} else {
					// Otherwise, lowerBound returned the smallest element with firstOffset > offset.
					// The candidate is the predecessor (largest segment with firstOffset less than offset).
					const pred = it.prev()
					if (pred === null) {
						console.debug(`no predecessor found for offset ${offset}`)
						candidate = null
					} else {
						console.debug(`found predecessor ${pred.name} for offset ${offset}`)
						candidate = pred
					}
				}
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
