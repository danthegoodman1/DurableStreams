import type { StoredRecord } from "./segment"
import { StreamManager } from "./stream_manager"

export { StreamManager }

const DEFAULT_LIMIT = 10
const MAX_LIMIT = 1000
const MAX_TIMEOUT_SEC = 300
const MAX_STREAM_NAME_LENGTH = 256

type Stream = DurableObjectStub<StreamManager>

/**
 * HTTP API. The URL path names the stream: POST publishes, GET reads, DELETE deletes the stream.
 * Parsing and serialization happen here so the stream's Durable Object only orders and stores records.
 */
export default {
	async fetch(request, env): Promise<Response> {
		if (!isAuthorized(request, env)) {
			return errorResponse(401, "Unauthorized")
		}

		const url = new URL(request.url)
		const name = url.pathname.slice(1)
		if (name === "") {
			return errorResponse(400, "The URL path must name a stream")
		}
		if (name.length > MAX_STREAM_NAME_LENGTH) {
			return errorResponse(414, `Stream names are limited to ${MAX_STREAM_NAME_LENGTH} characters`)
		}
		const stream = env.STREAMS.getByName(name)

		try {
			switch (request.method) {
				case "GET":
					return await read(stream, url.searchParams)
				case "POST":
					return await publish(stream, request, url.searchParams)
				case "DELETE":
					await stream.destroy()
					return Response.json({ success: true })
				default:
					return errorResponse(405, "Method not allowed", { Allow: "GET, POST, DELETE" })
			}
		} catch (error) {
			console.error(`${request.method} ${name} failed`, error)
			return errorResponse(500, error instanceof Error ? error.message : String(error))
		}
	},
} satisfies ExportedHandler<Env>

const encoder = new TextEncoder()

function isAuthorized(request: Request, env: Env): boolean {
	if (!env.AUTH_HEADER) {
		return true
	}
	const provided = encoder.encode(request.headers.get("auth") ?? "")
	const expected = encoder.encode(env.AUTH_HEADER)
	return provided.byteLength === expected.byteLength && crypto.subtle.timingSafeEqual(provided, expected)
}

async function publish(stream: Stream, request: Request, params: URLSearchParams): Promise<Response> {
	let body: unknown
	try {
		body = await request.json()
	} catch {
		return errorResponse(400, "Invalid JSON body")
	}
	const records = typeof body === "object" && body !== null ? (body as { records?: unknown }).records : undefined
	if (!Array.isArray(records)) {
		return errorResponse(400, 'Body must be a JSON object with a "records" array')
	}

	const versionParam = params.get("version")
	const version = versionParam === null ? undefined : parseNonNegativeInteger(versionParam)
	if (Number.isNaN(version)) {
		return errorResponse(400, "Invalid version parameter")
	}

	const result = await stream.publish(
		records.map((record) => JSON.stringify(record)),
		version,
	)
	switch (result.type) {
		case "published":
			return Response.json({ offsets: result.offsets })
		case "version":
			return Response.json({ version: result.version })
		case "fenced":
			return Response.json(
				{ error: "Producer version too old", current_version: result.currentVersion, provided_version: version },
				{ status: 409 },
			)
	}
}

async function read(stream: Stream, params: URLSearchParams): Promise<Response> {
	const limitParam = params.get("limit")
	const limit = limitParam === null ? DEFAULT_LIMIT : parseNonNegativeInteger(limitParam)
	if (!(limit >= 1)) {
		return errorResponse(400, "limit must be a positive integer")
	}
	const timeoutParam = params.get("timeout_sec")
	const timeoutSec = timeoutParam === null || timeoutParam === "" ? 0 : Number(timeoutParam)
	if (!(timeoutSec >= 0)) {
		return errorResponse(400, "timeout_sec must be a non-negative number")
	}

	// "-" reads from the beginning; no offset waits for records published after the request.
	const offset = params.get("offset")
	const records = await stream.read({
		after: offset === "-" ? "" : offset || undefined,
		limit: Math.min(limit, MAX_LIMIT),
		timeoutMs: Math.min(timeoutSec, MAX_TIMEOUT_SEC) * 1000,
	})
	return new Response(recordsBody(records), { headers: { "Content-Type": "application/json" } })
}

/** Builds the response from each record's stored JSON text, skipping a parse and re-serialize. */
function recordsBody(records: StoredRecord[]): string {
	return `{"records":[${records.map((record) => `{"offset":"${record.offset}","data":${record.data}}`).join(",")}]}`
}

/** Returns NaN unless `value` is a base-10 integer in [0, 2^53). */
function parseNonNegativeInteger(value: string): number {
	return /^\d+$/.test(value) && Number.isSafeInteger(Number(value)) ? Number(value) : NaN
}

function errorResponse(status: number, error: string, headers?: HeadersInit): Response {
	return Response.json({ error }, { status, headers })
}
