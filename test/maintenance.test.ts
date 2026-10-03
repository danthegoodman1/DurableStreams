import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { streamPrefix } from "../src/segment"
import type { StreamManager } from "../src/stream_manager"
import {
	api,
	env,
	garbageRows,
	patchBucket,
	publish,
	r2Keys,
	readAll,
	segmentRows,
	settleMaintenance,
	sleep,
	stub,
	uniqueStream,
} from "./helpers"

const DAY_MS = 24 * 60 * 60 * 1000

/** Publishes each batch separately so each becomes its own segment. */
async function publishSegments(stream: string, count: number, size = 3): Promise<unknown[]> {
	const published: unknown[] = []
	for (let segment = 0; segment < count; segment++) {
		const records = Array.from({ length: size }, (_, i) => ({ segment, i, text: "ünïcødé 🌊" }))
		await publish(stream, records)
		published.push(...records)
	}
	return published
}

describe("compaction", () => {
	it("merges ten level-0 segments into one level-1 segment", async () => {
		const stream = uniqueStream()
		const published = await publishSegments(stream, 10)
		const before = await runInDurableObject(stub(stream), (_, state) => segmentRows(state))
		expect(before).toHaveLength(10)

		expect(await runDurableObjectAlarm(stub(stream))).toBe(true)

		const after = await runInDurableObject(stub(stream), (_, state) => segmentRows(state))
		expect(after).toEqual([
			{
				first_offset: before[0].first_offset,
				last_offset: before[9].last_offset,
				records: 30,
				bytes: before.reduce((sum, row) => sum + row.bytes, 0),
				level: 1,
			},
		])
		expect((await readAll(stream)).map((r) => r.data)).toEqual(published)
		for (const limit of [1, 4]) {
			expect((await readAll(stream, limit)).map((r) => r.data)).toEqual(published)
		}
	})

	it("leaves runs shorter than maxSegments alone", async () => {
		const stream = uniqueStream()
		await publishSegments(stream, 9)
		await runDurableObjectAlarm(stub(stream))
		expect(await runInDurableObject(stub(stream), (_, state) => segmentRows(state).length)).toBe(9)
	})

	it("reads across compacted and newer segments", async () => {
		const stream = uniqueStream()
		const published = await publishSegments(stream, 10)
		await runDurableObjectAlarm(stub(stream))
		published.push(...(await publishSegments(stream, 3)))
		expect(await runInDurableObject(stub(stream), (_, state) => segmentRows(state).map((row) => row.level))).toEqual([1, 0, 0, 0])
		for (const limit of [1, 2, 5, 100]) {
			expect((await readAll(stream, limit)).map((r) => r.data)).toEqual(published)
		}
	})

	it("keeps replaced segments for a grace period, then deletes them", async () => {
		const stream = uniqueStream()
		const prefix = streamPrefix(stream)
		await publishSegments(stream, 10)
		const inputs = await r2Keys(prefix)
		await runDurableObjectAlarm(stub(stream))

		expect(await r2Keys(prefix)).toHaveLength(11)
		const { garbage, alarm } = await runInDurableObject(stub(stream), async (_, state) => ({
			garbage: garbageRows(state),
			alarm: await state.storage.getAlarm(),
		}))
		expect(garbage.map((row) => row.key)).toEqual(inputs)
		for (const row of garbage) {
			expect(row.delete_at).toBeGreaterThan(Date.now() + DAY_MS - 60_000)
		}
		expect(alarm).toBe(garbage[0].delete_at)

		await runInDurableObject(stub(stream), (_, state) => {
			state.storage.sql.exec("UPDATE garbage SET delete_at = 0")
		})
		await runDurableObjectAlarm(stub(stream))
		expect(await r2Keys(prefix)).toHaveLength(1)
		expect(await runInDurableObject(stub(stream), (_, state) => garbageRows(state))).toEqual([])
		expect(await runInDurableObject(stub(stream), (_, state) => state.storage.getAlarm())).toBeNull()
		expect(await readAll(stream)).toHaveLength(30)
	})

	it("compacts repeatedly in one alarm, producing higher levels", async () => {
		const stream = uniqueStream()
		const published = await publishSegments(stream, 100, 1)
		await runDurableObjectAlarm(stub(stream))
		expect(await runInDurableObject(stub(stream), (_, state) => segmentRows(state).map((row) => row.level))).toEqual([2])
		expect((await readAll(stream, 7)).map((r) => r.data)).toEqual(published)
	})

	it("keeps the sooner alarm when a flush lands during maintenance", async () => {
		const stream = uniqueStream()
		await publishSegments(stream, 10)

		const uploading = Promise.withResolvers<void>()
		const releaseUpload = Promise.withResolvers<void>()
		await runInDurableObject(stub(stream), (instance: StreamManager) => {
			const bucket = env.SEGMENTS
			patchBucket(instance, {
				put: async (...args: Parameters<R2Bucket["put"]>) => {
					uploading.resolve()
					await releaseUpload.promise
					return bucket.put(...args)
				},
			})
		})

		const maintenance = runInDurableObject(stub(stream), (instance: StreamManager) => instance.alarm())
		await uploading.promise
		const publishing = publish(stream, ["during maintenance"])
		await sleep(100)
		releaseUpload.resolve()
		await Promise.all([maintenance, publishing])

		const alarm = await runInDurableObject(stub(stream), (_, state) => state.storage.getAlarm())
		expect(alarm).not.toBeNull()
		expect(alarm!).toBeLessThanOrEqual(Date.now() + 10_000)
	})

	it("discards the merged segment when the stream is deleted mid-compaction", async () => {
		const stream = uniqueStream()
		const prefix = streamPrefix(stream)
		await publishSegments(stream, 10)

		const uploaded = Promise.withResolvers<void>()
		const releaseUpload = Promise.withResolvers<void>()
		await runInDurableObject(stub(stream), (instance: StreamManager) => {
			const bucket = env.SEGMENTS
			patchBucket(instance, {
				put: async (...args: Parameters<R2Bucket["put"]>) => {
					const object = await bucket.put(...args)
					uploaded.resolve()
					await releaseUpload.promise
					return object
				},
			})
		})

		const compaction = runInDurableObject(stub(stream), (instance: StreamManager) => instance.alarm())
		await uploaded.promise
		const deleted = api(stream, { method: "DELETE" })
		await sleep(100)
		releaseUpload.resolve()
		await Promise.all([compaction, deleted])
		await settleMaintenance(stream)

		expect(await r2Keys(prefix)).toEqual([])
		expect(await runInDurableObject(stub(stream), (_, state) => segmentRows(state))).toEqual([])
		expect(await readAll(stream)).toEqual([])
	})

	it("compacts small segments behind full ones, before and after deleting the stream", async () => {
		const stream = uniqueStream()
		await publish(
			stream,
			Array.from({ length: 5_000 }, (_, i) => i),
		)
		await publishSegments(stream, 10, 1)
		await runDurableObjectAlarm(stub(stream))
		expect(await runInDurableObject(stub(stream), (_, state) => segmentRows(state).map((row) => [row.records, row.level]))).toEqual([
			[5_000, 0],
			[10, 1],
		])

		await api(stream, { method: "DELETE" })
		await settleMaintenance(stream)
		await publishSegments(stream, 10, 1)
		await runDurableObjectAlarm(stub(stream))
		expect(await runInDurableObject(stub(stream), (_, state) => segmentRows(state).map((row) => [row.records, row.level]))).toEqual([
			[10, 1],
		])
	})

	it("deletes more garbage than one R2 call accepts", async () => {
		const stream = uniqueStream()
		const prefix = streamPrefix(stream)
		await publish(stream, ["keep"])
		const keys = Array.from({ length: 1_100 }, (_, i) => `${prefix}garbage-${i}`)
		for (let i = 0; i < keys.length; i += 100) {
			await Promise.all(keys.slice(i, i + 100).map((key) => env.SEGMENTS.put(key, "x")))
		}
		await runInDurableObject(stub(stream), (_, state) => {
			for (const key of keys) {
				state.storage.sql.exec("INSERT INTO garbage (key, delete_at) VALUES (?, 0)", key)
			}
		})
		await runDurableObjectAlarm(stub(stream))
		expect(await r2Keys(prefix)).toHaveLength(1)
		expect((await readAll(stream)).map((r) => r.data)).toEqual(["keep"])
	})

	it("retries failed maintenance later", async () => {
		const stream = uniqueStream()
		await publishSegments(stream, 10)
		await runInDurableObject(stub(stream), async (instance: StreamManager, state) => {
			patchBucket(instance, {
				get: async () => {
					throw new Error("R2 unavailable")
				},
			})
			await state.storage.deleteAlarm()
			await instance.alarm()
			expect(segmentRows(state)).toHaveLength(10)
			expect(await state.storage.getAlarm()).toBeGreaterThan(Date.now() + 30_000)
			patchBucket(instance, { get: (key: string) => env.SEGMENTS.get(key) })
		})
		await runDurableObjectAlarm(stub(stream))
		expect(await runInDurableObject(stub(stream), (_, state) => segmentRows(state))).toHaveLength(1)
	})
})
