import { runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { formatOffset, type SegmentMetadata } from "../src/segment"
import { SegmentIndex } from "../src/segment_index"
import { garbageRows, segmentRows, stub, uniqueStream } from "./helpers"

function segment(first: number, last: number, level = 0): SegmentMetadata {
	return { firstOffset: formatOffset(1, first), lastOffset: formatOffset(1, last), records: last - first + 1, bytes: 10, level }
}

/** Runs `test` against a fresh index in its own Durable Object's storage. */
function withIndex(test: (index: SegmentIndex, state: DurableObjectState) => void): Promise<void> {
	return runInDurableObject(stub(uniqueStream()), (_, state) => test(new SegmentIndex(state.storage), state))
}

describe("SegmentIndex", () => {
	it("finds the segments holding records after an offset", () =>
		withIndex((index) => {
			const [a, b, c] = [segment(0, 4), segment(5, 9), segment(10, 10)]
			for (const s of [a, b, c]) {
				index.trackUpload(s.lastOffset, 0)
				index.commit(s, s.lastOffset)
			}
			expect(index.lastOffset()).toBe(c.lastOffset)
			expect(index.segmentsAfter("", 10)).toEqual([a, b, c])
			expect(index.segmentsAfter(formatOffset(1, 2), 10)).toEqual([a, b, c])
			expect(index.segmentsAfter(a.lastOffset, 10)).toEqual([b, c])
			expect(index.segmentsAfter(formatOffset(1, 7), 1)).toEqual([b])
			expect(index.segmentsAfter(c.lastOffset, 10)).toEqual([])
			expect([...index.segments(a.lastOffset)]).toEqual([b, c])
			expect(index.dueGarbage(Number.MAX_SAFE_INTEGER, 10)).toEqual([])
		}))

	it("replaces inputs atomically, or not at all when one is gone", () =>
		withIndex((index, state) => {
			const [a, b] = [segment(0, 4), segment(5, 9)]
			index.commit(a, "a")
			index.commit(b, "b")
			const merged = segment(0, 9, 1)

			expect(index.replace([a, segment(5, 8)], ["a", "x"], merged, "m", 100)).toBe(false)
			expect(segmentRows(state)).toHaveLength(2)
			expect(garbageRows(state)).toEqual([])

			index.trackUpload("m", 50)
			expect(index.replace([a, b], ["a", "b"], merged, "m", 100)).toBe(true)
			expect(index.segmentsAfter("", 10)).toEqual([merged])
			expect(garbageRows(state)).toEqual([
				{ key: "a", delete_at: 100 },
				{ key: "b", delete_at: 100 },
			])
			expect(index.nextGarbageAt()).toBe(100)
		}))

	it("discards an upload only when no live segment owns it", () =>
		withIndex((index, state) => {
			const live = segment(0, 4)
			index.commit(live, "live")
			index.discardUpload(live, "live", 0)
			expect(garbageRows(state)).toEqual([])
			index.discardUpload(segment(0, 9, 1), "orphan", 0)
			expect(garbageRows(state)).toEqual([{ key: "orphan", delete_at: 0 }])
		}))

	it("clears the stream into garbage that is due immediately", () =>
		withIndex((index, state) => {
			index.commit(segment(0, 4), "a")
			index.trackUpload("tombstone", 1_000)
			index.setProducerVersion(3)
			index.clear(["a"], 10)
			expect(index.lastOffset()).toBe("")
			expect(index.producerVersion()).toBe(0)
			expect(garbageRows(state)).toEqual([
				{ key: "a", delete_at: 10 },
				{ key: "tombstone", delete_at: 10 },
			])
			expect(index.dueGarbage(10, 1)).toHaveLength(1)
			index.removeGarbage(["a", "tombstone"])
			expect(index.nextGarbageAt()).toBeNull()
		}))
})
