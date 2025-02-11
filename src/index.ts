import { EventEmitter } from "node:events"
import { generateLogSegmentName, parseLogSegmentName, SegmentMetadata } from "./segment"
import { SegmentIndex } from "./segment_index"

const FlushIntervalMs = 200

const consumerOffsetKeyPrefix = "consumer_offset::"

interface LatestOffset {
	/**
	 * The latest offset that has been staged for persistence
	 */
	staged: string
	/**
	 * The latest offset that has been comitted to R2
	 */
	comitted: string
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
	return `${epoch.toString().padStart(16, "0")}:${counter.toString().padStart(16, "0")}`
}

export class StreamCoordinator extends SegmentIndex<Env> {
	connectedWebsockets: number = 0
	consumers: Map<WebSocket, string> = new Map()
	consumerOffsets: Map<string, string> = new Map()

	lastOffset: string = ""
	streamName: string = ""
	epoch: number = Date.now()
	counter: number = 0

	// Messages that are pending persistence in the flush interval
	pendingMessages: Set<{
		emitter: EventEmitter<{ resolve: [string[]]; error: [Error] }>
		records: any[]
	}> = new Set()

	setup_listener?: EventEmitter<{ finish: [] }>
	setup = false

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env)
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

		if (this.tree.size === 0) return this.finishSetup()

		// Load the previous epoch if we have it. When we go to write
		// we'll increment this and handle clock drift
		const maxRecord = this.tree.max()!
		this.epoch = parseOffset(maxRecord.lastOffset).epoch
		const { stream } = parseLogSegmentName(maxRecord.name)
		this.streamName = stream

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
		this.pendingMessages.add({ emitter, records: body.records })
		if (this.pendingMessages.size === 1) {
			// Set the alarm to flush the pending messages
			await this.ctx.storage.setAlarm(Date.now() + FlushIntervalMs)
		}
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
		await this.flushPendingMessages()
		// TODO: Check if we need to compact log segments (random chance)
		// TODO: Check if we need to tombstone (random chance)
	}

	async flushPendingMessages() {
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

		// TODO persist logs
		// TODO: - this is a whole ordeal with keeping track of what is merged and what's not via Kv storage?

		// Write the log segment index
		await this.ctx.storage.transaction(async (tx) => {
			await this.writeLogSegmentIndex(tx, {
				name: segmentName,
				firstOffset: serializeOffset(this.epoch, this.counter),
				lastOffset: serializeOffset(this.epoch, this.counter),
				createdMS: Date.now(),
			})
		})
	}

	async writeLogSegment(segmentName: string, records: any[]) {
		// TODO: write the segment to R2, named after the first record in the segment
		// TODO: each record is a new line, 33 bytes for the name, then the JSON record
		const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>()
		const writer = writable.getWriter()

		// start streaming the records to the file
		// TODO: with the first segment
		// TODO: With the correct key that's to be stored in the index
		const writePromise = this.env.StreamData.put(segmentName, readable)

		// TODO: write the records to the stream
		for (const record of records) {
			const name = record.name // TODO: fix this
			const json = JSON.stringify(record)
			const nameBuffer = new TextEncoder().encode(name)
			const jsonBuffer = new TextEncoder().encode(json)
			writer.write(nameBuffer)
			writer.write(jsonBuffer)
		}

		await Promise.all([writer.close(), writePromise])
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
		let id = env.StreamCoordinator.idFromName(new URL(request.url).pathname)
		let stub = env.StreamCoordinator.get(id)
		return stub.fetch(request)
	},
} satisfies ExportedHandler<Env>
