import { describe, expect, it } from "vitest"
import { isFull, pickCompactionWindow, type CompactionLimits } from "../src/compaction"
import { formatOffset, type SegmentMetadata } from "../src/segment"

const limits: CompactionLimits = { maxSegments: 4, maxRecords: 100, maxBytes: 1_000 }

/** Builds adjacent segments from [records, bytes, level] triples. */
function segments(...specs: [records: number, bytes: number, level?: number][]): SegmentMetadata[] {
	let next = 0
	return specs.map(([records, bytes, level = 0]) => {
		const segment = { firstOffset: formatOffset(1, next), lastOffset: formatOffset(1, next + records - 1), records, bytes, level }
		next += records
		return segment
	})
}

const names = (window: SegmentMetadata[], all: SegmentMetadata[]) => window.map((segment) => all.indexOf(segment))

describe("pickCompactionWindow", () => {
	it("waits until a run reaches maxSegments", () => {
		expect(pickCompactionWindow(segments([1, 10], [1, 10], [1, 10]), limits)).toEqual([])
	})

	it("returns the oldest maxSegments same-level segments", () => {
		const all = segments([1, 10], [1, 10], [1, 10], [1, 10], [1, 10], [1, 10])
		expect(names(pickCompactionWindow(all, limits), all)).toEqual([0, 1, 2, 3])
	})

	it("returns a shorter run once its records reach the limit", () => {
		const all = segments([60, 10], [50, 10], [1, 10])
		expect(names(pickCompactionWindow(all, limits), all)).toEqual([0, 1])
	})

	it("returns a shorter run once its bytes reach the limit", () => {
		const all = segments([1, 600], [1, 500], [1, 10])
		expect(names(pickCompactionWindow(all, limits), all)).toEqual([0, 1])
	})

	it("only merges segments of the same level", () => {
		const all = segments([4, 40, 1], [4, 40, 1], [1, 10], [1, 10], [1, 10], [1, 10])
		expect(names(pickCompactionWindow(all, limits), all)).toEqual([2, 3, 4, 5])
	})

	it("skips full segments, which also break runs", () => {
		const all = segments([1, 10], [1, 10], [1, 10], [100, 10], [1, 10], [1, 10], [1, 10], [1, 10])
		expect(names(pickCompactionWindow(all, limits), all)).toEqual([4, 5, 6, 7])
	})

	it("treats a segment exactly at a limit as full instead of stalling behind it", () => {
		for (const exact of [
			[100, 10],
			[1, 1_000],
		] as [number, number][]) {
			const all = segments(exact, [1, 10], [1, 10], [1, 10], [1, 10])
			expect(isFull(all[0], limits)).toBe(true)
			expect(names(pickCompactionWindow(all, limits), all)).toEqual([1, 2, 3, 4])
		}
	})

	it("reads lazily and stops at the first ready window", () => {
		let visited = 0
		function* lazy() {
			for (const segment of segments(...Array.from({ length: 100 }, (): [number, number] => [1, 10]))) {
				visited++
				yield segment
			}
		}
		expect(pickCompactionWindow(lazy(), limits)).toHaveLength(limits.maxSegments)
		expect(visited).toBe(limits.maxSegments)
	})
})

describe("tiered compaction policy", () => {
	/** Flushes `flushes` segments of `recordsPerFlush`, compacting fully after each, and tracks every write. */
	function simulate(flushes: number, recordsPerFlush: number, simLimits: CompactionLimits) {
		let all: SegmentMetadata[] = []
		let next = 0
		let recordsWritten = 0
		for (let flush = 0; flush < flushes; flush++) {
			all.push({
				firstOffset: formatOffset(1, next),
				lastOffset: formatOffset(1, next + recordsPerFlush - 1),
				records: recordsPerFlush,
				bytes: recordsPerFlush * 10,
				level: 0,
			})
			next += recordsPerFlush
			recordsWritten += recordsPerFlush
			for (let window = pickCompactionWindow(all, simLimits); window.length > 0; window = pickCompactionWindow(all, simLimits)) {
				const merged: SegmentMetadata = {
					firstOffset: window[0].firstOffset,
					lastOffset: window[window.length - 1].lastOffset,
					records: window.reduce((sum, s) => sum + s.records, 0),
					bytes: window.reduce((sum, s) => sum + s.bytes, 0),
					level: window[0].level + 1,
				}
				all.splice(all.indexOf(window[0]), window.length, merged)
				recordsWritten += merged.records
			}
		}
		return { all, total: next, recordsWritten }
	}

	it("keeps segments contiguous and bounds rewrites and segment count", () => {
		const simLimits = { maxSegments: 10, maxRecords: 5_000, maxBytes: 10_000_000 }
		const { all, total, recordsWritten } = simulate(12_345, 1, simLimits)

		for (let i = 1; i < all.length; i++) {
			expect(all[i].firstOffset).toBe(formatOffset(1, Number(all[i - 1].lastOffset.slice(16)) + 1))
		}
		expect(all.reduce((sum, s) => sum + s.records, 0)).toBe(total)

		// Levels 0..3 hold 1, 10, 100 and 1,000 records; the next merge fills a segment. That is at most
		// five writes per record, where merging every new segment into the newest would be ~2,500.
		expect(recordsWritten / total).toBeLessThanOrEqual(5)
		const open = all.filter((segment) => !isFull(segment, simLimits))
		expect(open.length).toBeLessThanOrEqual(4 * (simLimits.maxSegments - 1))
		expect(all.filter((segment) => isFull(segment, simLimits)).every((segment) => segment.records < 2 * simLimits.maxRecords)).toBe(true)
	})

	it("fills segments directly when flushes are large", () => {
		const simLimits = { maxSegments: 10, maxRecords: 5_000, maxBytes: 10_000_000 }
		const { all, total, recordsWritten } = simulate(50, 2_000, simLimits)
		// Three flushes fill a segment, so each record is written at most twice.
		expect(recordsWritten / total).toBeLessThanOrEqual(2)
		expect(all.filter((segment) => !isFull(segment, simLimits)).length).toBeLessThan(3)
	})
})
