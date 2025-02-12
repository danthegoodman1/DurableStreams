import { EventEmitter } from "node:events"
import { DurableObject } from "cloudflare:workers"
import { generateLogSegmentName, parseLogSegmentName, SegmentMetadata } from "./segment"
import { RBTree } from "bintrees"

const FlushIntervalMs = 200
const hour = 1000 * 60 * 60
const day = hour * 24
const MaxStaleSegmentMs = day * 1

const consumerOffsetKeyPrefix = "consumer_offset::"
const activeLogSegmentKey = "active_log_segment::" // what logs segments are actually active, used for compaction, tombstone cleaning, and queries

function buildLogSegmentIndexKey(segmentName: string): string {
	return `${activeLogSegmentKey}${segmentName}`
}

interface PendingMessage {
	emitter: EventEmitter<{ resolve: [string[]]; error: [Error] }>
	/**
	 * Pre-serialized records so we can pre-calculate the length of write streams
	 */
	records: string[]
}

interface AckRPC {
	offset: string
}

interface ProduceBody {
	records: any[]
}

function parseOffset(offset: string): { epoch: number; counter: number } {
	const [epoch, counter] = offset.split(":")
	return { epoch: Number(epoch), counter: Number(counter) }
}

function serializeOffset(epoch: number, counter: number): string {
	// 16 digits is max safe integer for JS
	return `${epoch.toString().padStart(16, "0")}${counter.toString().padStart(16, "0")}`
}

export class StreamCoordinator extends DurableObject<Env> {
	connectedWebsockets: number = 0
	consumers: Map<WebSocket, string> = new Map()
	consumerOffsets: Map<string, string> = new Map()

	lastOffset: string = ""
	streamName: string = ""
	epoch: number = Date.now()
	counter: number = 0

	tree: RBTree<SegmentMetadata>

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

			// should we crash here?
			console.error("2 segments with intersecting offset ranges - this is a consistency bug", a, b)
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
	}

	async fetch(request: Request): Promise<Response> {
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
		const fromOffset = url.searchParams.get("from_offset")
		this.consumerOffsets.set(consumerID, fromOffset || "")

		const webSocketPair = new WebSocketPair()
		const [client, server] = Object.values(webSocketPair)

		this.ctx.acceptWebSocket(server)
		this.connectedWebsockets++
		this.consumers.set(server, consumerID)

		return new Response(null, {
			status: 101,
			webSocket: client,
		})
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

	async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
		let params: AckRPC
		try {
			params = JSON.parse(message.toString())
		} catch (error) {
			console.error(error)
			return
		}

		this.handleAck(ws, params)
	}

	async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
		this.connectedWebsockets--
		this.consumers.delete(ws)
	}

	async webSocketError(ws: WebSocket, error: unknown) {}

	async handleAck(ws: WebSocket, params: AckRPC) {
		// TODO persist the ack if it's forward of where it currently is (if exists)
	}

	async onConsumerConnect(ws: WebSocket, consumerID: string) {
		// TODO: check get their current offset from storage to check if they are overwriting
		// TODO: if the offset is "-" then we should set them to the latest offset
		// TODO: otherwise we need to send messages to them from the latest offset until the current offset
	}

	async alarm(alarmInfo?: AlarmInvocationInfo) {
		console.debug("alarm waking up")
		await this.flushPendingMessages()
		// TODO: Check if we need to compact log segments (random chance, increasing with segment index size)
		// TODO: Check if we need to tombstone (random chance)
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

				// TODO We also need to write it to the log segment
			}
			offsets.push(messageOffsets)
		}

		// Write the pending messages to the log segment
		console.debug("writing pending messages to log segment")
		await this.writePendingMessagesToLogSegment(segmentName, offsets, this.pendingMessages)

		// Write the log segment index so we actually persist the segment
		console.debug("writing log segment metadata")
		await this.writeLogSegmentMetadata({
			name: segmentName,
			firstOffset: serializeOffset(this.epoch, this.counter),
			lastOffset: serializeOffset(this.epoch, this.counter),
			createdMS: Date.now(),
		})

		// Notify producer emitters to return
		for (let i = 0; i < this.pendingMessages.length; i++) {
			this.pendingMessages[i].emitter.emit("resolve", offsets[i])
		}

		// Clear the pending messages
		this.pendingMessages = []

		// TODO: push logs to consumers
		console.debug("pushing logs to consumers")
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

	async buildIndexFromStorage() {
		const segments = await this.ctx.storage.list<SegmentMetadata>({
			prefix: activeLogSegmentKey,
		})

		for (const [_, segment] of segments) {
			this.tree.insert(segment)
		}
	}

	async writeLogSegmentMetadata(metadata: SegmentMetadata) {
		// First we need to durably store it
		await this.ctx.storage.put(buildLogSegmentIndexKey(metadata.name), JSON.stringify(metadata))
		// Then we can add it to the memory index
		this.tree.insert(metadata)
	}

	async compactLogSegments(segments: SegmentMetadata[]) {
		// TODO: check metadata to see if we need to compact log segments
		// TODO: k-way merge the segments with line readers
		// TODO: transaction to update log segments
	}

	async cleanTombstones() {
		// TODO: get snapshot of what segments are active
		// TODO: list R2 to find non-active segments that are older than the retention policy
		// TODO: delete the segments from R2
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
