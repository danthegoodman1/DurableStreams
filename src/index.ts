import { DurableObject } from "cloudflare:workers"
import { EventEmitter } from "node:events"

const FlushIntervalMs = 100

const latestOffsetKey = "_latest_offset"
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

export class StreamCoordinator extends DurableObject<Env> {
	connectedWebsockets: number = 0
	consumers: Map<WebSocket, string> = new Map()
	consumerOffsets: Map<string, string> = new Map()

	lastOffset: string = ""
	streamName: string = ""

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

	buildR2Key(offset: string) {
		return `${this.streamName}/${offset}`
	}

	async storeLatestOffset(latest: string, comitted: string) {
		await this.ctx.storage.put(latestOffsetKey, JSON.stringify({ latest, comitted }))
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

		// load the latest offset from storage
		const latestOffset = await this.ctx.storage.get<LatestOffset>(latestOffsetKey)
		if (!latestOffset) {
			// This is a fresh instance
			console.log("No offset found, is this a fresh instance?")
			this.finishSetup()
			return
		}
		if (latestOffset.staged === latestOffset.comitted) {
			// The offsets match, so we can finish setup
			console.log("Offsets match, finishing setup")
			this.finishSetup()
			return
		}

		// If the offsets don't match, check R2
		console.warn("Offsets don't match, checking R2 to see if we committed")
		const segmentFile = await this.env.StreamData.get(this.buildR2Key(latestOffset.staged))

		if (segmentFile) {
			// We have a segment file, so we can implicitly commit the offset
			console.log(`Segment file found, implicitly committing offset to staged offset: ${latestOffset.staged}`)
			await this.storeLatestOffset(latestOffset.staged, latestOffset.staged)
			this.finishSetup()
			return
		}

		// Otherwise we died during the 2PC, so we need to truncate
		console.warn(`No segment file found, truncating to last committed offset: ${latestOffset.comitted}`)
		await this.storeLatestOffset(latestOffset.comitted, latestOffset.comitted)

		this.finishSetup()
	}

	async fetch(request: Request): Promise<Response> {
		this.streamName = new URL(request.url).pathname
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
	}

	async flushPendingMessages() {
		const offsets: string[][] = []
		// TODO persist logs
		// TODO persist latest offset with 2PC (with intended R2 segment write)
		// TODO respond to the sockets with their new offsets
	}
}

export default {
	async fetch(request, env, ctx): Promise<Response> {
		let id = env.StreamCoordinator.idFromName(new URL(request.url).pathname)
		let stub = env.StreamCoordinator.get(id)
		return stub.fetch(request)
	},
} satisfies ExportedHandler<Env>
