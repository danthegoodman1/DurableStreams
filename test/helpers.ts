import { env as cloudflareEnv, exports } from "cloudflare:workers"
import { expect } from "vitest"
import type { StreamManager } from "../src/stream_manager"

export const env = cloudflareEnv as Env

export type ReadRecord = { offset: string; data: unknown }

export function uniqueStream(): string {
	return `test-${crypto.randomUUID()}`
}

export function api(path: string, init: RequestInit = {}): Promise<Response> {
	const headers = new Headers(init.headers)
	headers.set("auth", env.AUTH_HEADER!)
	return exports.default.fetch(new Request(`https://streams.test/${path}`, { ...init, headers }))
}

export async function publish(stream: string, records: unknown[], query = ""): Promise<string[]> {
	const response = await api(`${stream}${query}`, { method: "POST", body: JSON.stringify({ records }) })
	expect(response.status, await response.clone().text()).toBe(200)
	return ((await response.json()) as { offsets: string[] }).offsets
}

export async function read(stream: string, query: Record<string, string | number> = {}): Promise<ReadRecord[]> {
	const params = new URLSearchParams(Object.entries(query).map(([key, value]) => [key, String(value)]))
	const response = await api(`${stream}?${params}`)
	expect(response.status, await response.clone().text()).toBe(200)
	return ((await response.json()) as { records: ReadRecord[] }).records
}

/** Pages through the whole stream `limit` records at a time. */
export async function readAll(stream: string, limit = 1000): Promise<ReadRecord[]> {
	const all: ReadRecord[] = []
	let offset = "-"
	for (;;) {
		const page = await read(stream, { offset, limit })
		if (page.length === 0) {
			return all
		}
		all.push(...page)
		offset = page[page.length - 1].offset
	}
}

export function stub(stream: string): DurableObjectStub<StreamManager> {
	return env.STREAMS.getByName(stream)
}

export type SegmentRow = { first_offset: string; last_offset: string; records: number; bytes: number; level: number }

export function segmentRows(state: DurableObjectState): SegmentRow[] {
	return state.storage.sql.exec<SegmentRow>("SELECT * FROM segments ORDER BY last_offset").toArray()
}

export function garbageRows(state: DurableObjectState): { key: string; delete_at: number }[] {
	return state.storage.sql.exec<{ key: string; delete_at: number }>("SELECT * FROM garbage ORDER BY key").toArray()
}

export async function r2Keys(prefix: string): Promise<string[]> {
	const keys: string[] = []
	let cursor: string | undefined
	do {
		const page = await env.SEGMENTS.list({ prefix, cursor })
		keys.push(...page.objects.map((object) => object.key))
		cursor = page.truncated ? page.cursor : undefined
	} while (cursor !== undefined)
	return keys
}

/** Replaces methods of the instance's R2 bucket binding, for fault injection. */
export function patchBucket(instance: StreamManager, overrides: Partial<R2Bucket>): void {
	const internals = instance as unknown as { env: Env }
	const bucket = internals.env.SEGMENTS
	internals.env = {
		...internals.env,
		SEGMENTS: new Proxy(bucket, {
			get(target, property) {
				if (property in overrides) {
					return overrides[property as keyof R2Bucket]
				}
				const value = Reflect.get(target, property)
				return typeof value === "function" ? value.bind(target) : value
			},
		}),
	}
}

export function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms))
}
