import { unstable_dev } from "wrangler"
import type { Unstable_DevWorker } from "wrangler"
import { describe, expect, it, beforeAll, afterAll, beforeEach } from "vitest"
import type { ProduceResponse, GetMessagesResponse } from "../src/index"
import { RBTree } from "bintrees"
import { SegmentMetadata, calculateCompactWindow } from "../src/segment"

interface VersionErrorResponse {
	error: string
	current_version?: number
	provided_version?: number
}

describe("Worker", () => {
	let worker: Unstable_DevWorker

	beforeAll(async () => {
		worker = await unstable_dev("src/index.ts", {
			experimental: { disableExperimentalWarning: true },
		})
	})

	afterAll(async () => {
		await worker.stop()
	})

	describe("durable object", () => {
		it("should create a new stream and produce messages", async () => {
			const streamName = crypto.randomUUID()
			// Create the stream with a post request
			const resp = await worker.fetch(`http://example.com/${streamName}`, {
				method: "POST",
				body: JSON.stringify({
					records: [
						{ value: "hello" },
						{ value: "world" },
						{ value: "world" },
						{ value: "world" },
						{ value: "world" },
						{ value: "world" },
						{ value: "world" },
						{ value: "world" },
					],
				}),
			})
			console.log(await resp.text())
			expect(resp.status).toBe(200)
		})
	})

	// This test suite validates that consumers can:
	// 1. Get messages from the oldest offset (using "-" as the offset)
	// 2. Get messages starting from a provided offset
	// 3. Long-poll for new messages and receive them immediately when produced
	describe("StreamCoordinator - Consumer", () => {
		it("should allow a consumer to get messages from the oldest offset with a limit", async () => {
			// Create a unique stream name
			const streamName = crypto.randomUUID()

			// Produce some messages via a POST request
			const produceResponse = await worker.fetch(`http://example.com/${streamName}`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "msg-1" }, { value: "msg-2" }, { value: "msg-3" }],
				}),
			})
			expect(produceResponse.status).toBe(200)
			const produceData = (await produceResponse.json()) as ProduceResponse
			expect(produceData.offsets).toBeDefined()

			// Ask the consumer to get messages starting from the oldest offset using "-"
			const consumerResponse = await worker.fetch(
				`http://example.com/${streamName}?consumer_id=consumerOld&offset=-&limit=2&timeout_sec=10`
			)
			expect(consumerResponse.status).toBe(200)
			const consumerData = (await consumerResponse.json()) as GetMessagesResponse
			expect(Array.isArray(consumerData.records)).toBe(true)
			// Since the limit is 2, we expect at most 2 records
			expect(consumerData.records.length).toBe(2)

			// Optionally verify each record contains an offset and some data
			if (consumerData.records.length > 0) {
				expect(consumerData.records[0]).toHaveProperty("offset")
				expect(consumerData.records[0]).toHaveProperty("data")
			}

			// Verify it's the records we produced
			expect(consumerData.records[0].data.value).toBe("msg-1")
			expect(consumerData.records[1].data.value).toBe("msg-2")
		})

		it("should allow a consumer to get messages from a given offset with a limit", async () => {
			const streamName = crypto.randomUUID()

			// Produce a set of messages
			const produceResponse = await worker.fetch(`http://example.com/${streamName}`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "first" }, { value: "second" }, { value: "third" }, { value: "fourth" }],
				}),
			})
			expect(produceResponse.status).toBe(200)
			const produceData = (await produceResponse.json()) as ProduceResponse
			expect(produceData.offsets).toBeDefined()

			// Extract the offset of the second record in the batch. (produceData.offsets is an array of arrays.)
			const firstOffset = produceData.offsets[0]

			console.log(`getting from offset: ${firstOffset}`)

			// Get messages starting from the extracted offset, limiting to 2 messages.
			const consumerResponse = await worker.fetch(
				`http://example.com/${streamName}?consumer_id=consumerOffset&offset=${firstOffset}&limit=1&timeout_sec=10`
			)
			expect(consumerResponse.status).toBe(200)
			const consumerData = (await consumerResponse.json()) as GetMessagesResponse
			expect(Array.isArray(consumerData.records)).toBe(true)
			expect(consumerData.records.length).toBeLessThanOrEqual(1)
			console.log(`consumerData: ${JSON.stringify(consumerData)}`)
			if (consumerData.records.length > 0) {
				expect(consumerData.records[0]).toHaveProperty("offset")
				expect(consumerData.records[0]).toHaveProperty("data")
				expect(consumerData.records[0].data.value).toBe("second")
			}

			// Verify the data is in the correct segment
			expect(consumerData.records[0].data.value).toBe("second")
		})

		it("should allow a consumer to long-poll and receive new messages immediately", async () => {
			const streamName = crypto.randomUUID()

			// Initiate a consumer request without an offset (an empty offset triggers long-poll mode)
			const consumerPromise = worker.fetch(`http://example.com/${streamName}?consumer_id=consumerLong&limit=5&timeout_sec=5`)
			// Give the consumer a moment to be registered and wait for new records.
			await new Promise((resolve) => setTimeout(resolve, 500))

			// Now produce new messages that the long-polling consumer should receive.
			const produceResponse = await worker.fetch(`http://example.com/${streamName}`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "live-1" }, { value: "live-2" }],
				}),
			})
			expect(produceResponse.status).toBe(200)
			const produceData = (await produceResponse.json()) as ProduceResponse
			expect(produceData.offsets).toBeDefined()

			// Await the consumer's long-poll response.
			const consumerResponse = await consumerPromise
			expect(consumerResponse.status).toBe(200)
			const consumerData = (await consumerResponse.json()) as GetMessagesResponse
			expect(Array.isArray(consumerData.records)).toBe(true)
			// We expect the long-polling consumer to return at least one new record.
			expect(consumerData.records.length).toBeGreaterThan(0)
			if (consumerData.records.length > 0) {
				expect(consumerData.records[0]).toHaveProperty("offset")
				expect(consumerData.records[0]).toHaveProperty("data")
			}

			expect(consumerData.records[0].data.value).toBe("live-1")
			expect(consumerData.records[1].data.value).toBe("live-2")
		})

		it("should time out waiting for messages on an unwritten stream", async () => {
			// Create a unique stream name that is never written to
			const streamName = crypto.randomUUID()

			// Initiate a consumer request in long polling mode (no offset provided)
			const consumerResponse = await worker.fetch(`http://example.com/${streamName}?consumer_id=consumerTimeout&limit=5&timeout_sec=2`)
			expect(consumerResponse.status).toBe(200)

			const consumerData = (await consumerResponse.json()) as GetMessagesResponse
			// Since this stream has never been written to, after timeout we expect empty records
			expect(Array.isArray(consumerData.records)).toBe(true)
			expect(consumerData.records.length).toBe(0)
		})
	})

	describe("StreamCoordinator - Merge", () => {
		it("should merge segments and return merged records", async () => {
			const streamName = crypto.randomUUID()

			// Produce the first batch of messages.
			const produceResp1 = await worker.fetch(`http://example.com/${streamName}`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "a" }, { value: "b" }, { value: "c" }],
				}),
			})
			expect(produceResp1.status).toBe(200)
			const produceData1 = (await produceResp1.json()) as ProduceResponse
			expect(produceData1.offsets).toBeDefined()

			// Produce the second batch of messages.
			const produceResp2 = await worker.fetch(`http://example.com/${streamName}`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "d" }, { value: "e" }],
				}),
			})
			expect(produceResp2.status).toBe(200)
			const produceData2 = (await produceResp2.json()) as ProduceResponse
			expect(produceData2.offsets).toBeDefined()

			// Increase the wait time to ensure the merge (compaction) completes.
			await new Promise((resolve) => setTimeout(resolve, 500))

			// Consume messages from the beginning of the stream.
			const consumerResp = await worker.fetch(`http://example.com/${streamName}?consumer_id=mergeConsumer&offset=-&limit=10&timeout_sec=5`)
			expect(consumerResp.status).toBe(200)
			const consumerData = (await consumerResp.json()) as GetMessagesResponse
			expect(Array.isArray(consumerData.records)).toBe(true)
			// We expect all 5 messages (3 from the first, 2 from the second) to be merged.
			expect(consumerData.records.length).toBe(5)
			expect(consumerData.records[0].data.value).toBe("a")
			expect(consumerData.records[1].data.value).toBe("b")
			expect(consumerData.records[2].data.value).toBe("c")
			expect(consumerData.records[3].data.value).toBe("d")
			expect(consumerData.records[4].data.value).toBe("e")
		})
	})

	describe("StreamCoordinator - Producer Versioning", () => {
		it("should handle producer versioning correctly", async () => {
			const streamName = crypto.randomUUID()

			// First produce with version 1
			const produceResp1 = await worker.fetch(`http://example.com/${streamName}?version=1`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "msg1" }],
				}),
			})
			expect(produceResp1.status).toBe(200)

			// Produce with version 2 should succeed
			const produceResp2 = await worker.fetch(`http://example.com/${streamName}?version=2`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "msg2" }],
				}),
			})
			expect(produceResp2.status).toBe(200)

			// Produce with version 2 again should succeed
			const produceResp2_2 = await worker.fetch(`http://example.com/${streamName}?version=2`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "msg2_2" }],
				}),
			})
			expect(produceResp2_2.status).toBe(200)

			// Produce with version 1 should fail (older version)
			const produceResp3 = await worker.fetch(`http://example.com/${streamName}?version=1`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "msg3" }],
				}),
			})
			expect(produceResp3.status).toBe(409)
			const errorData = (await produceResp3.json()) as VersionErrorResponse
			expect(errorData.error).toBe("Producer version too old")
			expect(errorData.current_version).toBe(2)
			expect(errorData.provided_version).toBe(1)

			// Produce without version should succeed (opt-in)
			const produceResp4 = await worker.fetch(`http://example.com/${streamName}`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "msg4" }],
				}),
			})
			expect(produceResp4.status).toBe(200)

			// Invalid version should fail
			const produceResp5 = await worker.fetch(`http://example.com/${streamName}?version=invalid`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "msg5" }],
				}),
			})
			expect(produceResp5.status).toBe(400)
			const invalidData = (await produceResp5.json()) as VersionErrorResponse
			expect(invalidData.error).toBe("Invalid version parameter")

			// Verify we can read all successfully produced messages
			const consumerResp = await worker.fetch(`http://example.com/${streamName}?consumer_id=versionTest&offset=-&limit=10`)
			expect(consumerResp.status).toBe(200)
			const consumerData = (await consumerResp.json()) as GetMessagesResponse
			expect(consumerData.records).toHaveLength(4)
			expect(consumerData.records[0].data.value).toBe("msg1")
			expect(consumerData.records[1].data.value).toBe("msg2")
			expect(consumerData.records[2].data.value).toBe("msg2_2")
			expect(consumerData.records[3].data.value).toBe("msg4")
		})
	})
})

describe("calculateCompactWindow", () => {
	let tree: RBTree<SegmentMetadata>

	beforeEach(() => {
		// Create a dummy coordinator. (We pass empty objects; our tests only use calculateCompactWindow.)
		tree = new RBTree<SegmentMetadata>((a, b) => {
			if (a.firstOffset < b.firstOffset) return -1
			if (a.firstOffset > b.firstOffset) return 1
			return 0
		})
	})

	it("should return the full tree when all segments are within bounds", () => {
		// Insert three valid segments (all values are within limits).
		const seg1: SegmentMetadata = {
			name: "seg1",
			firstOffset: "0000000000000001",
			lastOffset: "0000000000000001",
			createdMS: 100,
			records: 1,
			bytes: 1,
		}
		const seg2: SegmentMetadata = {
			name: "seg2",
			firstOffset: "0000000000000002",
			lastOffset: "0000000000000002",
			createdMS: 200,
			records: 1,
			bytes: 1,
		}
		const seg3: SegmentMetadata = {
			name: "seg3",
			firstOffset: "0000000000000003",
			lastOffset: "0000000000000003",
			createdMS: 300,
			records: 1,
			bytes: 1,
		}

		tree.insert(seg1)
		tree.insert(seg2)
		tree.insert(seg3)

		// Since all segments are valid and we have more than one, the window should include all.
		const window = calculateCompactWindow(tree)
		expect(window).toHaveLength(3)
		expect(window[0].name).toBe("seg1")
		expect(window[1].name).toBe("seg2")
		expect(window[2].name).toBe("seg3")
	})

	it("should return a window early when an item exceeding the bytes limit is encountered", () => {
		// Create two normal segments then one that exceeds MaxBytes.
		const normal1: SegmentMetadata = {
			name: "normal1",
			firstOffset: "0000000000000001",
			lastOffset: "0000000000000001",
			createdMS: 100,
			records: 1,
			bytes: 1,
		}
		const normal2: SegmentMetadata = {
			name: "normal2",
			firstOffset: "0000000000000002",
			lastOffset: "0000000000000002",
			createdMS: 200,
			records: 1,
			bytes: 1,
		}
		// Set this segment so that its bytes exceed the limit (MaxBytes is 10MB).
		const exceedingBytes: SegmentMetadata = {
			name: "exceeding",
			firstOffset: "0000000000000003",
			lastOffset: "0000000000000003",
			createdMS: 300,
			records: 1,
			bytes: 10_000_001,
		}
		// Even if we add another normal segment after this one, the window should stop before it.
		const normal3: SegmentMetadata = {
			name: "normal3",
			firstOffset: "0000000000000004",
			lastOffset: "0000000000000004",
			createdMS: 400,
			records: 1,
			bytes: 1,
		}

		tree.insert(normal1)
		tree.insert(normal2)
		tree.insert(exceedingBytes)
		tree.insert(normal3)

		const window = calculateCompactWindow(tree)
		// The algorithm adds normal1 and normal2 then sees the exceeding segment.
		// Since at that point there are at least 2 segments, it breaks.
		expect(window).toHaveLength(2)
		expect(window[0].name).toBe("normal1")
		expect(window[1].name).toBe("normal2")
	})

	it("should return a window early when an item exceeding the records limit is encountered", () => {
		// Create two normal segments then one that exceeds MaxRecords.
		const normal1: SegmentMetadata = {
			name: "normal1",
			firstOffset: "0000000000000001",
			lastOffset: "0000000000000001",
			createdMS: 100,
			records: 1,
			bytes: 1,
		}
		const normal2: SegmentMetadata = {
			name: "normal2",
			firstOffset: "0000000000000002",
			lastOffset: "0000000000000002",
			createdMS: 200,
			records: 1,
			bytes: 1,
		}
		// Set this segment so that its records exceed the limit (MaxRecords is 5000).
		const exceedingRecords: SegmentMetadata = {
			name: "exceeding",
			firstOffset: "0000000000000003",
			lastOffset: "0000000000000003",
			createdMS: 300,
			records: 5001,
			bytes: 1,
		}
		// Another normal segment that follows.
		const normal3: SegmentMetadata = {
			name: "normal3",
			firstOffset: "0000000000000004",
			lastOffset: "0000000000000004",
			createdMS: 400,
			records: 1,
			bytes: 1,
		}

		tree.insert(normal1)
		tree.insert(normal2)
		tree.insert(exceedingRecords)
		tree.insert(normal3)

		const window = calculateCompactWindow(tree)
		// The window should include the two normal segments only.
		expect(window).toHaveLength(2)
		expect(window[0].name).toBe("normal1")
		expect(window[1].name).toBe("normal2")
	})

	it("should return a window early when an item the trips the bytes limit is encountered", () => {
		// Create two normal segments then one that exceeds MaxBytes.
		const normal1: SegmentMetadata = {
			name: "normal1",
			firstOffset: "0000000000000001",
			lastOffset: "0000000000000001",
			createdMS: 100,
			records: 1,
			bytes: 1,
		}
		const normal2: SegmentMetadata = {
			name: "normal2",
			firstOffset: "0000000000000002",
			lastOffset: "0000000000000002",
			createdMS: 200,
			records: 1,
			bytes: 1,
		}
		// Set this segment so that its bytes exceed the limit (MaxBytes is 10MB).
		const exceedingBytes: SegmentMetadata = {
			name: "exceeding",
			firstOffset: "0000000000000003",
			lastOffset: "0000000000000003",
			createdMS: 300,
			records: 1,
			bytes: 10_000_000,
		}
		// Even if we add another normal segment after this one, the window should stop before it.
		const normal3: SegmentMetadata = {
			name: "normal3",
			firstOffset: "0000000000000004",
			lastOffset: "0000000000000004",
			createdMS: 400,
			records: 1,
			bytes: 1,
		}

		tree.insert(normal1)
		tree.insert(normal2)
		tree.insert(exceedingBytes)
		tree.insert(normal3)

		const window = calculateCompactWindow(tree)
		// The algorithm adds normal1 and normal2 then sees the exceeding segment.
		// Since at that point there are at least 2 segments, it breaks.
		expect(window).toHaveLength(3)
		expect(window[0].name).toBe("normal1")
		expect(window[1].name).toBe("normal2")
		expect(window[2].name).toBe("exceeding")
	})

	it("should return a window early when an item the trips the records limit is encountered", () => {
		// Create two normal segments then one that exceeds MaxRecords.
		const normal1: SegmentMetadata = {
			name: "normal1",
			firstOffset: "0000000000000001",
			lastOffset: "0000000000000001",
			createdMS: 100,
			records: 1,
			bytes: 1,
		}
		const normal2: SegmentMetadata = {
			name: "normal2",
			firstOffset: "0000000000000002",
			lastOffset: "0000000000000002",
			createdMS: 200,
			records: 1,
			bytes: 1,
		}
		// Set this segment so that its records exceed the limit (MaxRecords is 5000).
		const exceedingRecords: SegmentMetadata = {
			name: "exceeding",
			firstOffset: "0000000000000003",
			lastOffset: "0000000000000003",
			createdMS: 300,
			records: 5000,
			bytes: 1,
		}
		// Another normal segment that follows.
		const normal3: SegmentMetadata = {
			name: "normal3",
			firstOffset: "0000000000000004",
			lastOffset: "0000000000000004",
			createdMS: 400,
			records: 1,
			bytes: 1,
		}

		tree.insert(normal1)
		tree.insert(normal2)
		tree.insert(exceedingRecords)
		tree.insert(normal3)

		const window = calculateCompactWindow(tree)
		// The window should include the two normal segments only.
		expect(window).toHaveLength(3)
		expect(window[0].name).toBe("normal1")
		expect(window[1].name).toBe("normal2")
		expect(window[2].name).toBe("exceeding")
	})

	it("should skip initial segments that exceed limits and return a window from later valid segments", () => {
		// In this scenario, the very first segment exceeds a limit so it is skipped.
		const exceeding: SegmentMetadata = {
			name: "exceeding",
			firstOffset: "0000000000000001",
			lastOffset: "0000000000000001",
			createdMS: 100,
			records: 1,
			bytes: 10_000_001, // exceeds MaxBytes
		}
		const normal1: SegmentMetadata = {
			name: "normal1",
			firstOffset: "0000000000000002",
			lastOffset: "0000000000000002",
			createdMS: 200,
			records: 1,
			bytes: 1,
		}
		const normal2: SegmentMetadata = {
			name: "normal2",
			firstOffset: "0000000000000003",
			lastOffset: "0000000000000003",
			createdMS: 300,
			records: 1,
			bytes: 1,
		}

		tree.insert(exceeding)
		tree.insert(normal1)
		tree.insert(normal2)

		const window = calculateCompactWindow(tree)
		// The skipped initial item should not be in the window.
		expect(window).toHaveLength(2)
		expect(window[0].name).toBe("normal1")
		expect(window[1].name).toBe("normal2")
	})

	it("should return an empty array if there are fewer than two valid segments", () => {
		// When only one valid segment exists, compaction should not occur.
		const normal1: SegmentMetadata = {
			name: "normal1",
			firstOffset: "0000000000000001",
			lastOffset: "0000000000000001",
			createdMS: 100,
			records: 1,
			bytes: 1,
		}
		tree.insert(normal1)

		const window = calculateCompactWindow(tree)
		expect(window).toHaveLength(0)
	})
})
