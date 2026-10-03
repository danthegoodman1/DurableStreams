import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test"
import { describe, expect, it, vi } from "vitest"
import { streamPrefix } from "../src/segment"
import type { StreamManager } from "../src/stream_manager"
import {
	api,
	env,
	garbageRows,
	hang,
	patchBucket,
	publish,
	r2Keys,
	readAll,
	segmentRows,
	shortenTimeouts,
	stub,
	uniqueStream,
} from "./helpers"

async function expectError(response: Response, message: string): Promise<void> {
	expect(response.status).toBe(500)
	expect(((await response.json()) as { error: string }).error).toContain(message)
}

describe("R2 timeouts", () => {
	it("fails publishes whose upload hangs, keeps flushing, and deletes the upload if it lands late", async () => {
		const stream = uniqueStream()
		const releaseHungUpload = Promise.withResolvers<void>()
		await runInDurableObject(stub(stream), (instance: StreamManager) => {
			shortenTimeouts(instance, 100)
			const bucket = env.SEGMENTS
			let uploads = 0
			patchBucket(instance, {
				put: async (...args: Parameters<R2Bucket["put"]>) => {
					if (uploads++ === 0) {
						await releaseHungUpload.promise
					}
					return bucket.put(...args)
				},
			})
		})

		await expectError(await api(stream, { method: "POST", body: JSON.stringify({ records: ["lost"] }) }), "timed out")
		await publish(stream, ["kept"])

		releaseHungUpload.resolve()
		await vi.waitFor(async () => expect(await r2Keys(streamPrefix(stream))).toHaveLength(2))
		expect((await readAll(stream)).map((r) => r.data)).toEqual(["kept"])

		await runInDurableObject(stub(stream), (_, state) => {
			state.storage.sql.exec("UPDATE garbage SET delete_at = 0")
		})
		await runDurableObjectAlarm(stub(stream))
		expect(await r2Keys(streamPrefix(stream))).toHaveLength(1)
		expect((await readAll(stream)).map((r) => r.data)).toEqual(["kept"])
	})

	it("fails reads whose R2 requests hang", async () => {
		const stream = uniqueStream()
		await publish(stream, ["a"])
		await runInDurableObject(stub(stream), (instance: StreamManager) => {
			shortenTimeouts(instance, 100)
			patchBucket(instance, { get: hang })
		})
		await expectError(await api(`${stream}?offset=-`), "read timed out")

		await runInDurableObject(stub(stream), (instance: StreamManager) => {
			patchBucket(instance, { get: (key: string) => env.SEGMENTS.get(key) })
		})
		expect((await readAll(stream)).map((r) => r.data)).toEqual(["a"])
	})

	it("abandons a hung compaction and retries it later", async () => {
		const stream = uniqueStream()
		for (let i = 0; i < 10; i++) {
			await publish(stream, [i])
		}
		await runInDurableObject(stub(stream), async (instance: StreamManager, state) => {
			shortenTimeouts(instance, 100)
			patchBucket(instance, { get: hang })
			await state.storage.deleteAlarm()
			await instance.alarm()
			expect(segmentRows(state)).toHaveLength(10)
			expect(await state.storage.getAlarm()).toBeGreaterThan(Date.now() + 30_000)
			patchBucket(instance, { get: (key: string) => env.SEGMENTS.get(key) })
		})

		expect(await runDurableObjectAlarm(stub(stream))).toBe(true)
		expect(await runInDurableObject(stub(stream), (_, state) => segmentRows(state))).toHaveLength(1)
		expect((await readAll(stream)).map((r) => r.data)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
	})

	it("retries garbage deletion that hangs", async () => {
		const stream = uniqueStream()
		await publish(stream, ["a"])
		await runInDurableObject(stub(stream), async (instance: StreamManager, state) => {
			shortenTimeouts(instance, 100)
			patchBucket(instance, { delete: hang })
			await instance.destroy()
			await state.storage.deleteAlarm()
			await instance.alarm()
			expect(garbageRows(state)).toHaveLength(1)
			expect(await state.storage.getAlarm()).toBeGreaterThan(Date.now() + 30_000)
			patchBucket(instance, { delete: (keys: string | string[]) => env.SEGMENTS.delete(keys) })
		})

		expect(await runDurableObjectAlarm(stub(stream))).toBe(true)
		expect(await r2Keys(streamPrefix(stream))).toEqual([])
	})
})
