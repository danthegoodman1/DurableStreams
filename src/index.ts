import { DurableObject } from "cloudflare:workers"

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

	lastOffset: string = ""

	// Messages that are pending persistence in the flush interval
	pendingMessages: Set<any> = new Set() // TODO: signal to the request handlers that they can respond

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
		const body = await request.json()
		const messageID = body.id
		const payload = body.payload

		// TODO: persist the message
		return new Response(JSON.stringify({ type: "produce", id: messageID }))
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
