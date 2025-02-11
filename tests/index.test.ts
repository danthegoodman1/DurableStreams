import { unstable_dev } from "wrangler"
import type { Unstable_DevWorker } from "wrangler"
import { describe, expect, it, beforeAll, afterAll } from "vitest"
import { WebSocket } from "ws"

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
			const streamName = "test"
			// Create the stream with a post request
			const resp = await worker.fetch(`http://example.com/${streamName}`, {
				method: "POST",
				body: JSON.stringify({
					records: [{ value: "hello" }],
				}),
			})
			expect(resp.status).toBe(200)
		})
	})
})
