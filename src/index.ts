import { DurableObject } from "cloudflare:workers"
import { EventEmitter } from "node:events"

const FlushIntervalMs = 100

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

	// Messages that are pending persistence in the flush interval
	pendingMessages: Set<{
		emitter: EventEmitter<{ resolve: [string[]]; error: [Error] }>
		records: any[]
	}> = new Set() // TODO: signal to the request handlers that they can respond

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env)
	}

	async fetch(request: Request): Promise<Response> {
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
		// TODO persist the ack if it's forward oh where it currently is (if exists)
		// TODO
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
		const pendingResponses = new Map<WebSocket, string>()
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
