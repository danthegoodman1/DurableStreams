import { DurableObject } from "cloudflare:workers"

const FlushIntervalMs = 100

interface RPCMessage {
	method: "produce" | "consume" | "ack"
	payload: ProducePayload | ConsumePayload | AckPayload
	id: string
}

type ProducePayload = any[]

interface ConsumePayload {
	consumerID: string

	/**
	 * Optional offset to start consuming from, overwrites the current consumer
	 */
	offset?: string

	/**
	 * Whether to persist the offset for the consumer
	 */
	persistOffset?: boolean
}

interface AckPayload {
	messageOffset: string
}

interface PendingMessage {
	content: any[]
	websocket: WebSocket
}

export class MyDurableObject extends DurableObject<Env> {
	connectedWebsockets: number = 0
	producers: Map<WebSocket, string> = new Map()
	consumers: Map<WebSocket, string> = new Map()

	lastOffset: string = ""

	// Messages that are pending persistence in the flush interval
	pendingMessages: Set<PendingMessage> = new Set()

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env)
	}

	async fetch(request: Request): Promise<Response> {
		const webSocketPair = new WebSocketPair()
		const [client, server] = Object.values(webSocketPair)

		this.ctx.acceptWebSocket(server)

		const connectionId = crypto.randomUUID()
		this.connectedWebsockets++
		server.send(JSON.stringify({ type: "connectionId", connectionId }))

		return new Response(null, {
			status: 101,
			webSocket: client,
		})
	}

	async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
		let messageJson: RPCMessage
		try {
			messageJson = JSON.parse(message.toString())
		} catch (error) {
			console.error(error)
			return
		}

		switch (messageJson.method) {
			case "produce":
				this.handleProduce(ws, messageJson.id, messageJson.payload as ProducePayload)
				break
			case "consume":
				this.handleConsume(ws, messageJson.id, messageJson.payload as ConsumePayload)
				break
			case "ack":
				this.handleAck(ws, messageJson.id, messageJson.payload as AckPayload)
				break
		}
	}

	async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
		this.connectedWebsockets--
		this.producers.delete(ws)
		this.consumers.delete(ws)
	}

	async webSocketError(ws: WebSocket, error: unknown) {}

	async handleProduce(ws: WebSocket, messageID: string, payload: ProducePayload) {
		let producerID: string
		if (!this.producers.has(ws)) {
			// Register the producer
			producerID = crypto.randomUUID()
			this.producers.set(ws, producerID)
		} else {
			producerID = this.producers.get(ws)!
		}

		if (this.pendingMessages.size === 0) {
			// Set the alarm in the future
			console.log(`Setting alarm for now + ${FlushIntervalMs}ms`)
			this.ctx.storage.setAlarm(Date.now() + FlushIntervalMs)
		}

		// Stage the message for persistence
		this.pendingMessages.add({
			content: payload,
			websocket: ws,
		})
	}

	async handleConsume(ws: WebSocket, messageID: string, payload: ConsumePayload) {
		// Register consumer
		if (this.consumers.has(ws)) {
			// TODO: send a rejection message
		} else {
			this.consumers.set(ws, messageID)
		}
	}

	async handleAck(ws: WebSocket, messageID: string, payload: AckPayload) {
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
		let id = env.MyDO.idFromName(new URL(request.url).pathname)
		let stub = env.MyDO.get(id)
		return stub.fetch(request)
	},
} satisfies ExportedHandler<Env>
