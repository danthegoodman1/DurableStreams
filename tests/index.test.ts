import { unstable_dev } from "wrangler"
import type { Unstable_DevWorker } from "wrangler"
import { describe, expect, it, beforeAll, afterAll } from "vitest"
import { WebSocket } from "ws"
import type { ProduceResponse, GetMessagesResponse } from "../src/index"

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
			expect(consumerData.records.length).toBeLessThanOrEqual(2)

			// Optionally verify each record contains an offset and some data
			if (consumerData.records.length > 0) {
				expect(consumerData.records[0]).toHaveProperty("offset")
				expect(consumerData.records[0]).toHaveProperty("data")
			}
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
})
