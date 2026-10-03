import { evictDurableObject, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test"
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
	read,
	readAll,
	segmentRows,
	settleMaintenance,
	sleep,
	stub,
	uniqueStream,
} from "./helpers"

describe("paging", () => {
	it("pages through records spread across segments with any limit", async () => {
		const stream = uniqueStream()
		const expected: unknown[] = []
		for (let batch = 0; batch < 3; batch++) {
			const records = Array.from({ length: 5 }, (_, i) => ({ batch, i }))
			await publish(stream, records)
			expected.push(...records)
		}
		expect(await runInDurableObject(stub(stream), (_, state) => segmentRows(state).length)).toBe(3)

		for (const limit of [1, 2, 3, 4, 7, 15, 100]) {
			expect((await readAll(stream, limit)).map((r) => r.data)).toEqual(expected)
		}
	})

	it("continues from an offset in the middle of an earlier segment", async () => {
		const stream = uniqueStream()
		const first = await publish(stream, ["a0", "a1", "a2", "a3"])
		await publish(stream, ["b0", "b1"])
		expect((await read(stream, { offset: first[1], limit: 3 })).map((r) => r.data)).toEqual(["a2", "a3", "b0"])
	})

	it("continues past single-record segments", async () => {
		const stream = uniqueStream()
		for (let i = 0; i < 5; i++) {
			await publish(stream, [i])
		}
		expect((await readAll(stream, 1)).map((r) => r.data)).toEqual([0, 1, 2, 3, 4])
	})

	it("pages through more segments than one read visits", async () => {
		const stream = uniqueStream()
		for (let i = 0; i < 70; i++) {
			await publish(stream, [i])
		}
		const first = await read(stream, { offset: "-", limit: 1000 })
		expect(first.length).toBeGreaterThan(0)
		expect(first.length).toBeLessThan(70)
		expect((await readAll(stream)).map((r) => r.data)).toEqual(Array.from({ length: 70 }, (_, i) => i))
	})
})

describe("publishing", () => {
	it("batches concurrent publishes into one segment, preserving each request's order", async () => {
		const stream = uniqueStream()
		const requests = Array.from({ length: 20 }, (_, request) => Array.from({ length: 5 }, (_, i) => ({ request, i })))
		const results = await Promise.all(requests.map((records) => publish(stream, records)))

		const all = await readAll(stream)
		expect(all).toHaveLength(100)
		const byOffset = new Map(all.map((record) => [record.offset, record.data]))
		results.forEach((offsets, request) => {
			expect(offsets.map((offset) => byOffset.get(offset))).toEqual(requests[request])
		})
		expect(await runInDurableObject(stub(stream), (_, state) => segmentRows(state).length)).toBeLessThan(20)
	})

	it("accepts publishes that arrive while a flush is writing to R2", async () => {
		const stream = uniqueStream()
		const putStarted = Promise.withResolvers<void>()
		const releasePut = Promise.withResolvers<void>()
		await runInDurableObject(stub(stream), (instance: StreamManager) => {
			const bucket = env.SEGMENTS
			patchBucket(instance, {
				put: async (...args: Parameters<R2Bucket["put"]>) => {
					putStarted.resolve()
					await releasePut.promise
					return bucket.put(...args)
				},
			})
		})

		const first = publish(stream, ["first"])
		await putStarted.promise
		expect(await runInDurableObject(stub(stream), (_, state) => state.storage.getAlarm())).not.toBeNull()
		const second = publish(stream, ["second"])
		await sleep(100)
		releasePut.resolve()

		const [[firstOffset], [secondOffset]] = await Promise.all([first, second])
		expect(secondOffset > firstOffset).toBe(true)
		expect((await readAll(stream)).map((r) => r.data)).toEqual(["first", "second"])
	})

	it("fails publishes whose segment upload fails, then recovers", async () => {
		const stream = uniqueStream()
		await runInDurableObject(stub(stream), (instance: StreamManager) => {
			const bucket = env.SEGMENTS
			let failures = 1
			patchBucket(instance, {
				put: async (...args: Parameters<R2Bucket["put"]>) => {
					if (failures-- > 0) {
						throw new Error("R2 unavailable")
					}
					return bucket.put(...args)
				},
			})
		})

		const failed = await api(stream, { method: "POST", body: JSON.stringify({ records: ["lost"] }) })
		expect(failed.status).toBe(500)
		expect(((await failed.json()) as { error: string }).error).toContain("R2 unavailable")

		await publish(stream, ["kept"])
		expect((await readAll(stream)).map((r) => r.data)).toEqual(["kept"])
	})

	it("garbage-collects an upload whose commit failed", async () => {
		const stream = uniqueStream()
		await runInDurableObject(stub(stream), (instance: StreamManager) => {
			const index = (instance as unknown as { index: { commit: (...args: unknown[]) => void } }).index
			index.commit = () => {
				throw new Error("commit failed")
			}
		})
		const failed = await api(stream, { method: "POST", body: JSON.stringify({ records: ["orphan"] }) })
		expect(failed.status).toBe(500)

		const [orphan] = await r2Keys(streamPrefix(stream))
		expect(orphan).toBeDefined()
		await runInDurableObject(stub(stream), async (_, state) => {
			const garbage = garbageRows(state)
			expect(garbage.map((row) => row.key)).toEqual([orphan])
			expect(garbage[0].delete_at).toBeGreaterThan(Date.now() + 30 * 60 * 1000)
			expect(await state.storage.getAlarm()).not.toBeNull()
			state.storage.sql.exec("UPDATE garbage SET delete_at = 0")
		})

		expect(await runDurableObjectAlarm(stub(stream))).toBe(true)
		expect(await r2Keys(streamPrefix(stream))).toEqual([])
		expect(await runInDurableObject(stub(stream), (_, state) => garbageRows(state))).toEqual([])
	})

	it("limits the size of a read, returning at least one record", async () => {
		const stream = uniqueStream()
		const big = "x".repeat(1024 * 1024)
		for (let i = 0; i < 3; i++) {
			await publish(stream, [big, big, big, big])
		}
		const first = await read(stream, { offset: "-", limit: 1000 })
		expect(first.length).toBeGreaterThan(0)
		expect(first.length).toBeLessThan(12)
		expect(first.reduce((sum, record) => sum + (record.data as string).length, 0)).toBeLessThanOrEqual(8 * 1024 * 1024)
		expect(await readAll(stream)).toHaveLength(12)

		const huge = "y".repeat(7 * 1024 * 1024)
		await publish(stream, [huge])
		const [last] = (await readAll(stream)).slice(-1)
		expect(last.data).toBe(huge)
	})

	it("limits the size of a batch delivered to a long-polling reader", async () => {
		const stream = uniqueStream()
		const reading = read(stream, { limit: 1000, timeout_sec: 5 })
		await sleep(50)
		const big = "x".repeat(1024 * 1024)
		await Promise.all([publish(stream, [big, big, big, big, big, big]), publish(stream, [big, big, big, big, big, big])])
		const delivered = await reading
		expect(delivered.length).toBeGreaterThan(0)
		expect(delivered.reduce((sum, record) => sum + (record.data as string).length, 0)).toBeLessThanOrEqual(8 * 1024 * 1024)
	})

	it("rejects publish bodies over 8 MiB", async () => {
		const body = JSON.stringify({ records: ["x".repeat(8 * 1024 * 1024)] })
		const response = await api(uniqueStream(), { method: "POST", body })
		expect(response.status).toBe(413)
	})

	it("flushes as soon as pending records pass the size threshold", async () => {
		const stream = uniqueStream()
		await runInDurableObject(stub(stream), (instance: StreamManager) => {
			const internals = instance as unknown as { config: { flushIntervalMs: number } }
			internals.config = { ...internals.config, flushIntervalMs: 60_000 }
		})
		const started = Date.now()
		const big = "x".repeat(1024 * 1024)
		await Promise.all([publish(stream, [big, big, big, big, big]), publish(stream, [big, big, big, big, big])])
		expect(Date.now() - started).toBeLessThan(30_000)
	})

	it("schedules maintenance after a flush", async () => {
		const stream = uniqueStream()
		await publish(stream, [1])
		const alarm = await runInDurableObject(stub(stream), (_, state) => state.storage.getAlarm())
		expect(alarm).not.toBeNull()
		expect(alarm!).toBeLessThanOrEqual(Date.now() + 10_000)
	})
})

describe("producer versions", () => {
	it("fences publishes with an older version", async () => {
		const stream = uniqueStream()
		await publish(stream, ["v1"], "?version=1")
		await publish(stream, ["v2"], "?version=2")
		await publish(stream, ["v2 again"], "?version=2")

		const stale = await api(`${stream}?version=1`, { method: "POST", body: JSON.stringify({ records: ["stale"] }) })
		expect(stale.status).toBe(409)
		expect(await stale.json()).toEqual({ error: "Producer version too old", current_version: 2, provided_version: 1 })

		await publish(stream, ["unversioned"])
		expect((await readAll(stream)).map((r) => r.data)).toEqual(["v1", "v2", "v2 again", "unversioned"])
	})

	it("bumps the version without records", async () => {
		const stream = uniqueStream()
		const response = await api(`${stream}?version=7`, { method: "POST", body: JSON.stringify({ records: [] }) })
		expect(await response.json()).toEqual({ version: 7 })
		const unversioned = await api(stream, { method: "POST", body: JSON.stringify({ records: [] }) })
		expect(await unversioned.json()).toEqual({ version: 7 })
		expect((await api(`${stream}?version=6`, { method: "POST", body: JSON.stringify({ records: [] }) })).status).toBe(409)
		expect(await readAll(stream)).toEqual([])
	})

	it("fences publishes still waiting to flush when a newer version arrives", async () => {
		const stream = uniqueStream()
		const [stale, bump] = await runInDurableObject(stub(stream), (instance: StreamManager) =>
			Promise.all([instance.publish(['"stale"'], 1), instance.publish([], 2)]),
		)
		expect(bump).toEqual({ type: "version", version: 2 })
		expect(stale).toEqual({ type: "fenced", currentVersion: 2 })
		expect(await readAll(stream)).toEqual([])
	})

	it("acknowledges a new version only after the flush in progress commits", async () => {
		const stream = uniqueStream()
		const putStarted = Promise.withResolvers<void>()
		const releasePut = Promise.withResolvers<void>()
		await runInDurableObject(stub(stream), (instance: StreamManager) => {
			const bucket = env.SEGMENTS
			patchBucket(instance, {
				put: async (...args: Parameters<R2Bucket["put"]>) => {
					putStarted.resolve()
					await releasePut.promise
					return bucket.put(...args)
				},
			})
		})

		const stale = publish(stream, ["in flight"], "?version=1")
		await putStarted.promise
		let bumped = false
		const bump = api(`${stream}?version=2`, { method: "POST", body: JSON.stringify({ records: [] }) }).then((response) => {
			bumped = true
			return response
		})
		await sleep(100)
		expect(bumped).toBe(false)

		releasePut.resolve()
		await bump
		expect((await readAll(stream)).map((r) => r.data)).toEqual(["in flight"])
		await stale
	})
})

describe("restarts", () => {
	it("serves committed records and continues offsets after eviction", async () => {
		const stream = uniqueStream()
		const before = await publish(stream, ["a", "b"], "?version=3")
		await evictDurableObject(stub(stream))

		expect((await readAll(stream)).map((r) => r.data)).toEqual(["a", "b"])
		const [after] = await publish(stream, ["c"])
		expect(after > before[1]).toBe(true)
		expect((await api(`${stream}?version=2`, { method: "POST", body: JSON.stringify({ records: ["stale"] }) })).status).toBe(409)
	})
})

describe("long polling", () => {
	it("waits for the next batch when no offset is given", async () => {
		const stream = uniqueStream()
		await publish(stream, ["old"])
		const reading = read(stream, { timeout_sec: 5 })
		await sleep(50)
		await publish(stream, ["new 1", "new 2"])
		expect((await reading).map((r) => r.data)).toEqual(["new 1", "new 2"])
	})

	it("waits from an offset at the end of the stream", async () => {
		const stream = uniqueStream()
		const [last] = await publish(stream, ["old"])
		const reading = read(stream, { offset: last, timeout_sec: 5 })
		await sleep(50)
		await publish(stream, ["new"])
		expect((await reading).map((r) => r.data)).toEqual(["new"])
	})

	it("waits for the first records of an empty stream", async () => {
		const stream = uniqueStream()
		const reading = read(stream, { offset: "-", timeout_sec: 5 })
		await sleep(50)
		await publish(stream, ["first"])
		expect((await reading).map((r) => r.data)).toEqual(["first"])
	})

	it("returns available records without waiting", async () => {
		const stream = uniqueStream()
		await publish(stream, ["ready"])
		const started = Date.now()
		expect((await read(stream, { offset: "-", timeout_sec: 10 })).map((r) => r.data)).toEqual(["ready"])
		expect(Date.now() - started).toBeLessThan(1_000)
	})

	it("applies the limit to the delivered batch", async () => {
		const stream = uniqueStream()
		const reading = read(stream, { limit: 2, timeout_sec: 5 })
		await sleep(50)
		await publish(stream, [1, 2, 3, 4, 5])
		expect((await reading).map((r) => r.data)).toEqual([1, 2])
	})

	it("delivers a batch to every waiting reader", async () => {
		const stream = uniqueStream()
		const readers = Array.from({ length: 5 }, () => read(stream, { timeout_sec: 5 }))
		await sleep(50)
		await publish(stream, ["shared"])
		for (const reader of readers) {
			expect((await reader).map((r) => r.data)).toEqual(["shared"])
		}
	})

	it("returns no records after the timeout", async () => {
		const stream = uniqueStream()
		const started = Date.now()
		expect(await read(stream, { timeout_sec: 0.2 })).toEqual([])
		expect(Date.now() - started).toBeGreaterThanOrEqual(200)
	})
})

describe("deleting a stream", () => {
	it("removes records, segments and the producer version", async () => {
		const stream = uniqueStream()
		await publish(stream, ["a", "b"], "?version=5")
		await publish(stream, ["c"])
		expect(await r2Keys(streamPrefix(stream))).toHaveLength(2)

		const response = await api(stream, { method: "DELETE" })
		expect(await response.json()).toEqual({ success: true })
		expect(await readAll(stream)).toEqual([])
		await settleMaintenance(stream)
		expect(await r2Keys(streamPrefix(stream))).toEqual([])

		await publish(stream, ["fresh"], "?version=1")
		expect((await readAll(stream)).map((r) => r.data)).toEqual(["fresh"])
	})

	it("leaves streams with overlapping names untouched", async () => {
		const stream = uniqueStream()
		const neighbors = [`${stream}/child`, `${stream}-sibling`, `${stream}%2Fencoded`]
		await publish(stream, ["doomed"])
		for (const neighbor of neighbors) {
			await publish(neighbor, [neighbor])
		}

		await api(stream, { method: "DELETE" })
		await settleMaintenance(stream)
		for (const neighbor of neighbors) {
			expect((await readAll(neighbor)).map((r) => r.data)).toEqual([neighbor])
			expect(await r2Keys(streamPrefix(neighbor))).toHaveLength(1)
		}
	})

	it("stays deleted when removing objects from R2 fails, and retries later", async () => {
		const stream = uniqueStream()
		await publish(stream, ["a"])
		await publish(stream, ["b"])
		await runInDurableObject(stub(stream), (instance: StreamManager) => {
			patchBucket(instance, {
				delete: async () => {
					throw new Error("R2 unavailable")
				},
			})
			return instance.destroy()
		})
		expect(await readAll(stream)).toEqual([])
		expect(await r2Keys(streamPrefix(stream))).toHaveLength(2)

		await runInDurableObject(stub(stream), async (instance: StreamManager, state) => {
			await state.storage.deleteAlarm()
			await instance.alarm()
			expect(garbageRows(state)).toHaveLength(2)
			expect(await state.storage.getAlarm()).toBeGreaterThan(Date.now() + 30_000)
			patchBucket(instance, { delete: (keys: string | string[]) => env.SEGMENTS.delete(keys) })
		})
		expect(await runDurableObjectAlarm(stub(stream))).toBe(true)
		expect(await r2Keys(streamPrefix(stream))).toEqual([])
		expect(await readAll(stream)).toEqual([])
	})

	it("releases waiting readers", async () => {
		const stream = uniqueStream()
		const reading = read(stream, { timeout_sec: 10 })
		await sleep(50)
		const started = Date.now()
		await api(stream, { method: "DELETE" })
		expect(await reading).toEqual([])
		expect(Date.now() - started).toBeLessThan(5_000)
	})
})
