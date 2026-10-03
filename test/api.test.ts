import { exports } from "cloudflare:workers"
import { describe, expect, it } from "vitest"
import worker from "../src/index"
import { api, env, publish, read, uniqueStream } from "./helpers"

describe("auth", () => {
	it("rejects requests without the auth header before reaching a stream", async () => {
		const response = await exports.default.fetch(`https://streams.test/${uniqueStream()}?offset=-`)
		expect(response.status).toBe(401)
		expect(await response.json()).toEqual({ error: "Unauthorized" })
	})

	it("rejects a wrong auth header, including one that shares a prefix", async () => {
		for (const value of ["wrong", `${env.AUTH_HEADER}x`, env.AUTH_HEADER!.slice(0, -1)]) {
			const response = await exports.default.fetch(`https://streams.test/${uniqueStream()}`, { headers: { auth: value } })
			expect(response.status).toBe(401)
		}
	})

	it("allows every request when AUTH_HEADER is unset", async () => {
		const response = await worker.fetch(new Request(`https://streams.test/${uniqueStream()}?offset=-`), { ...env, AUTH_HEADER: undefined })
		expect(response.status).toBe(200)
	})
})

describe("routing", () => {
	it("requires a stream name in the path", async () => {
		const response = await api("")
		expect(response.status).toBe(400)
	})

	it("limits stream name length", async () => {
		expect((await api("x".repeat(256))).status).toBe(200)
		expect((await api("x".repeat(257))).status).toBe(414)
	})

	it("rejects unsupported methods", async () => {
		const response = await api(uniqueStream(), { method: "PUT" })
		expect(response.status).toBe(405)
		expect(response.headers.get("Allow")).toBe("GET, POST, DELETE")
	})

	it("treats each path as its own stream", async () => {
		const parent = uniqueStream()
		await publish(parent, ["parent"])
		await publish(`${parent}/child`, ["child"])
		expect((await read(parent, { offset: "-" })).map((r) => r.data)).toEqual(["parent"])
		expect((await read(`${parent}/child`, { offset: "-" })).map((r) => r.data)).toEqual(["child"])
	})
})

describe("publish", () => {
	it("returns one offset per record, increasing within and across requests", async () => {
		const stream = uniqueStream()
		const first = await publish(stream, [1, 2, 3])
		const second = await publish(stream, [4])
		const all = [...first, ...second]
		expect(all).toHaveLength(4)
		for (const offset of all) {
			expect(offset).toMatch(/^\d{32}$/)
		}
		expect([...all].sort()).toEqual(all)
		expect(new Set(all).size).toBe(4)
	})

	it("responds with JSON", async () => {
		const response = await api(uniqueStream(), { method: "POST", body: JSON.stringify({ records: [1] }) })
		expect(response.headers.get("Content-Type")).toBe("application/json")
	})

	it.each([
		["invalid JSON", "{", "Invalid JSON body"],
		["a missing records field", "{}", 'Body must be a JSON object with a "records" array'],
		["a non-array records field", '{"records": {"a": 1}}', 'Body must be a JSON object with a "records" array'],
		["a JSON array body", "[1, 2]", 'Body must be a JSON object with a "records" array'],
		["a null body", "null", 'Body must be a JSON object with a "records" array'],
	])("rejects %s", async (_case, body, error) => {
		const response = await api(uniqueStream(), { method: "POST", body })
		expect(response.status).toBe(400)
		expect(await response.json()).toEqual({ error })
	})

	it.each(["abc", "1abc", "-1", "1.5", ""])("rejects version %j", async (version) => {
		const response = await api(`${uniqueStream()}?version=${version}`, { method: "POST", body: '{"records": [1]}' })
		expect(response.status).toBe(400)
		expect(await response.json()).toEqual({ error: "Invalid version parameter" })
	})
})

describe("read", () => {
	it("round-trips every kind of JSON value exactly", async () => {
		const stream = uniqueStream()
		const records = [
			{ nested: { array: [1, "two", null, true], empty: {} } },
			"multi\nline\r\ntext with   separators",
			"emoji 🌊 accents é ü CJK 漢字",
			42,
			-0.5,
			null,
			false,
			[],
			"",
		]
		await publish(stream, records)
		expect((await read(stream, { offset: "-", limit: 100 })).map((r) => r.data)).toEqual(records)
	})

	it("returns records with their offsets", async () => {
		const stream = uniqueStream()
		const offsets = await publish(stream, ["a", "b"])
		expect(await read(stream, { offset: "-" })).toEqual([
			{ offset: offsets[0], data: "a" },
			{ offset: offsets[1], data: "b" },
		])
	})

	it("responds with JSON", async () => {
		const response = await api(`${uniqueStream()}?offset=-`)
		expect(response.headers.get("Content-Type")).toBe("application/json")
		expect(await response.json()).toEqual({ records: [] })
	})

	it("returns 10 records by default", async () => {
		const stream = uniqueStream()
		await publish(
			stream,
			Array.from({ length: 15 }, (_, i) => i),
		)
		expect(await read(stream, { offset: "-" })).toHaveLength(10)
	})

	it("caps limit at 1000", async () => {
		const stream = uniqueStream()
		await publish(
			stream,
			Array.from({ length: 1001 }, (_, i) => i),
		)
		expect(await read(stream, { offset: "-", limit: 5000 })).toHaveLength(1000)
	})

	it.each(["0", "-1", "abc", "1.5", ""])("rejects limit %j", async (limit) => {
		const response = await api(`${uniqueStream()}?offset=-&limit=${limit}`)
		expect(response.status).toBe(400)
	})

	it.each(["-1", "abc"])("rejects timeout_sec %j", async (timeout) => {
		const response = await api(`${uniqueStream()}?offset=-&timeout_sec=${timeout}`)
		expect(response.status).toBe(400)
	})

	it("reads records after an offset", async () => {
		const stream = uniqueStream()
		const offsets = await publish(stream, ["a", "b", "c", "d"])
		expect((await read(stream, { offset: offsets[1] })).map((r) => r.data)).toEqual(["c", "d"])
		expect(await read(stream, { offset: offsets[3] })).toEqual([])
	})

	it("reads from a point in time using a 16-digit epoch prefix", async () => {
		const stream = uniqueStream()
		await publish(stream, ["before"])
		const [after] = await publish(stream, ["after"])
		expect((await read(stream, { offset: after.slice(0, 16) })).map((r) => r.data)).toEqual(["after"])
	})

	it("returns immediately without an offset or timeout", async () => {
		const stream = uniqueStream()
		await publish(stream, ["a"])
		expect(await read(stream)).toEqual([])
	})
})
